"""
File Chunker — JSON (JSONL) & CSV
===================================
Splits large JSONL or CSV files into smaller chunks based on a target file size.
Chunks are always split on logical boundaries (per JSON object / per CSV row)
so no record is ever malformed or truncated.

Supports:
  - .ftm.json / .jsonl  — one JSON object per line (JSONL format)
  - .json               — JSON array (splits per array element)
  - .csv                — splits per row, header is repeated in every chunk

Usage:
    python chunk_files.py --input entities.ftm.json --size 100MB
    python chunk_files.py --input targets.simple.csv --size 50MB
    python chunk_files.py --input entities.ftm.json --size 100MB --output chunks/ --compress
    python chunk_files.py --input data.json --size 200MB --format json-array
"""

import argparse
import csv
import gzip
import io
import json
import os
import sys
from pathlib import Path

# ── Size parser ────────────────────────────────────────────────────────────────

def parse_size(size_str: str) -> int:
    """Convert human-readable size like '100MB', '1.5GB', '500KB' to bytes."""
    size_str = size_str.strip().upper().replace(" ", "")
    units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    for suffix, multiplier in sorted(units.items(), key=lambda x: -len(x[0])):
        if size_str.endswith(suffix):
            number = float(size_str[: -len(suffix)])
            return int(number * multiplier)
    # Plain number = bytes
    return int(size_str)


def fmt_size(n_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n_bytes < 1024:
            return f"{n_bytes:.1f} {unit}"
        n_bytes /= 1024
    return f"{n_bytes:.1f} TB"


# ── Output file helpers ────────────────────────────────────────────────────────

def open_output(path: Path, compress: bool):
    """Open an output file, optionally gzip-compressed."""
    if compress:
        path = path.with_suffix(path.suffix + ".gz")
        return path, gzip.open(path, "wt", encoding="utf-8")
    return path, path.open("w", encoding="utf-8")


# ── JSONL chunker ──────────────────────────────────────────────────────────────

def chunk_jsonl(input_path: Path, out_dir: Path, target_bytes: int,
                compress: bool, validate: bool) -> list[Path]:
    """
    Split a JSONL file (one JSON object per line) into chunks.
    Each chunk is a valid JSONL file.
    """
    stem      = input_path.stem.replace(".ftm", "")   # strip double extension
    suffix    = input_path.suffix
    chunks    = []
    chunk_idx = 0
    cur_bytes = 0
    cur_fh    = None
    cur_path  = None
    total_records = 0
    chunk_records = 0

    def open_next():
        nonlocal chunk_idx, cur_fh, cur_path, cur_bytes, chunk_records
        if cur_fh:
            cur_fh.close()
            print(f"  Chunk {chunk_idx:04d}: {cur_path.name}  "
                  f"({fmt_size(cur_bytes)}, {chunk_records:,} records)")
        chunk_idx += 1
        chunk_records = 0
        cur_bytes = 0
        fname     = out_dir / f"{stem}_chunk_{chunk_idx:04d}{suffix}"
        cur_path, cur_fh = open_output(fname, compress)
        chunks.append(cur_path)

    open_next()

    with input_path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.rstrip("\n")
            if not line:
                continue

            if validate:
                try:
                    json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"  WARNING: Skipping malformed JSON on line {lineno}: {e}")
                    continue

            encoded = (line + "\n").encode("utf-8")
            line_size = len(encoded)

            # Roll to next chunk if this line would exceed the target
            if cur_bytes > 0 and cur_bytes + line_size > target_bytes:
                open_next()

            cur_fh.write(line + "\n")
            cur_bytes     += line_size
            total_records += 1
            chunk_records += 1

    # Close final chunk
    if cur_fh:
        cur_fh.close()
        print(f"  Chunk {chunk_idx:04d}: {cur_path.name}  "
              f"({fmt_size(cur_bytes)}, {chunk_records:,} records)")

    return chunks, total_records


# ── JSON Array chunker ─────────────────────────────────────────────────────────

def chunk_json_array(input_path: Path, out_dir: Path, target_bytes: int,
                     compress: bool) -> tuple[list[Path], int]:
    """
    Split a JSON file containing a top-level array into chunks.
    Each chunk is a valid JSON array.
    """
    print("  Loading JSON array into memory …")
    with input_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    if not isinstance(data, list):
        raise ValueError("JSON file does not contain a top-level array.")

    stem      = input_path.stem
    suffix    = input_path.suffix
    chunks    = []
    chunk_idx = 0
    cur_items = []
    cur_bytes = 0
    total_records = len(data)

    def flush_chunk():
        nonlocal chunk_idx, cur_items, cur_bytes
        chunk_idx += 1
        fname = out_dir / f"{stem}_chunk_{chunk_idx:04d}{suffix}"
        fpath, fh = open_output(fname, compress)
        json.dump(cur_items, fh, ensure_ascii=False, indent=None)
        fh.close()
        print(f"  Chunk {chunk_idx:04d}: {fpath.name}  "
              f"({fmt_size(cur_bytes)}, {len(cur_items):,} records)")
        chunks.append(fpath)
        cur_items = []
        cur_bytes = 0

    for item in data:
        serialized = json.dumps(item, ensure_ascii=False)
        item_size  = len(serialized.encode("utf-8")) + 2  # +2 for comma/newline overhead

        if cur_bytes > 0 and cur_bytes + item_size > target_bytes:
            flush_chunk()

        cur_items.append(item)
        cur_bytes += item_size

    if cur_items:
        flush_chunk()

    return chunks, total_records


# ── CSV chunker ────────────────────────────────────────────────────────────────

def chunk_csv(input_path: Path, out_dir: Path, target_bytes: int,
              compress: bool) -> tuple[list[Path], int]:
    """
    Split a CSV file into chunks.
    The header row is written to every chunk so each chunk is a standalone valid CSV.
    """
    stem   = input_path.stem
    suffix = input_path.suffix

    chunks        = []
    chunk_idx     = 0
    cur_bytes     = 0
    cur_fh_raw    = None
    cur_writer    = None
    cur_path      = None
    total_records = 0
    chunk_records = 0
    header        = None

    def open_next(hdr):
        nonlocal chunk_idx, cur_fh_raw, cur_writer, cur_path, cur_bytes, chunk_records
        if cur_fh_raw:
            cur_fh_raw.close()
            print(f"  Chunk {chunk_idx:04d}: {cur_path.name}  "
                  f"({fmt_size(cur_bytes)}, {chunk_records:,} rows)")
        chunk_idx    += 1
        chunk_records = 0
        cur_bytes     = 0
        fname         = out_dir / f"{stem}_chunk_{chunk_idx:04d}{suffix}"
        if compress:
            cur_path   = fname.with_suffix(suffix + ".gz")
            cur_fh_raw = gzip.open(cur_path, "wt", encoding="utf-8", newline="")
        else:
            cur_path   = fname
            cur_fh_raw = cur_path.open("w", encoding="utf-8", newline="")
        cur_writer = csv.DictWriter(cur_fh_raw, fieldnames=hdr)
        cur_writer.writeheader()
        # Count header size
        hdr_line  = ",".join(hdr) + "\n"
        cur_bytes = len(hdr_line.encode("utf-8"))
        chunks.append(cur_path)

    with input_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames
        if not header:
            raise ValueError("CSV file has no header row.")

        open_next(header)

        for row in reader:
            # Estimate row size
            row_str  = ",".join(f'"{v}"' if "," in str(v) else str(v)
                                for v in row.values()) + "\n"
            row_size = len(row_str.encode("utf-8"))

            if cur_bytes > 0 and cur_bytes + row_size > target_bytes:
                open_next(header)

            cur_writer.writerow(row)
            cur_bytes     += row_size
            total_records += 1
            chunk_records += 1

    if cur_fh_raw:
        cur_fh_raw.close()
        print(f"  Chunk {chunk_idx:04d}: {cur_path.name}  "
              f"({fmt_size(cur_bytes)}, {chunk_records:,} rows)")

    return chunks, total_records


# ── Auto-detect format ─────────────────────────────────────────────────────────

def detect_format(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".csv"):
        return "csv"
    if name.endswith(".json"):
        # Peek at first non-whitespace char to distinguish array vs JSONL
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                stripped = line.strip()
                if stripped:
                    return "json-array" if stripped.startswith("[") else "jsonl"
    if name.endswith((".jsonl", ".ftm.json", ".ndjson")):
        return "jsonl"
    return "jsonl"   # safe default


# ── Write manifest ─────────────────────────────────────────────────────────────

def write_manifest(out_dir: Path, input_path: Path, chunks: list[Path],
                   total_records: int, target_bytes: int, fmt: str):
    manifest = {
        "source":        str(input_path),
        "format":        fmt,
        "target_size":   fmt_size(target_bytes),
        "total_records": total_records,
        "total_chunks":  len(chunks),
        "chunks": [
            {
                "index": i + 1,
                "file":  c.name,
                "size":  fmt_size(c.stat().st_size),
                "bytes": c.stat().st_size,
            }
            for i, c in enumerate(chunks)
        ],
    }
    mpath = out_dir / f"{input_path.stem}_manifest.json"
    with mpath.open("w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\n  Manifest : {mpath}")
    return mpath


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Split JSONL or CSV files into logical chunks by size."
    )
    parser.add_argument("--input",    required=True, help="Input file path")
    parser.add_argument("--size",     default="100MB",
                        help="Target chunk size e.g. 100MB, 1GB, 500KB (default: 100MB)")
    parser.add_argument("--output",   default=None,
                        help="Output directory (default: <input_stem>_chunks/)")
    parser.add_argument("--format",   default="auto",
                        choices=["auto", "jsonl", "json-array", "csv"],
                        help="File format override (default: auto-detect)")
    parser.add_argument("--compress", action="store_true",
                        help="Gzip-compress each chunk (.gz)")
    parser.add_argument("--validate", action="store_true",
                        help="Validate each JSON line when chunking JSONL (slower)")
    parser.add_argument("--no-manifest", action="store_true",
                        help="Skip writing the manifest JSON file")
    return parser.parse_args()


def main():
    args      = parse_args()
    inp       = Path(args.input)
    if not inp.exists():
        print(f"ERROR: File not found: {inp}")
        sys.exit(1)

    target    = parse_size(args.size)
    out_dir   = Path(args.output) if args.output else inp.parent / f"{inp.stem}_chunks"
    out_dir.mkdir(parents=True, exist_ok=True)

    fmt = args.format if args.format != "auto" else detect_format(inp)

    file_size = inp.stat().st_size
    print(f"\nFile Chunker")
    print(f"{'─'*55}")
    print(f"  Input      : {inp}  ({fmt_size(file_size)})")
    print(f"  Format     : {fmt}")
    print(f"  Chunk size : {fmt_size(target)}")
    print(f"  Output dir : {out_dir}")
    print(f"  Compress   : {args.compress}")
    print(f"{'─'*55}\n")

    expected_chunks = max(1, file_size // target)
    print(f"  Estimated ~{expected_chunks} chunk(s)\n")

    # ── Dispatch ──
    if fmt == "jsonl":
        chunks, total = chunk_jsonl(inp, out_dir, target, args.compress, args.validate)
    elif fmt == "json-array":
        chunks, total = chunk_json_array(inp, out_dir, target, args.compress)
    elif fmt == "csv":
        chunks, total = chunk_csv(inp, out_dir, target, args.compress)
    else:
        print(f"ERROR: Unknown format '{fmt}'")
        sys.exit(1)

    # ── Summary ──
    total_output = sum(c.stat().st_size for c in chunks)
    print(f"\n{'─'*55}")
    print(f"  Done!")
    print(f"  Total records : {total:,}")
    print(f"  Chunks created: {len(chunks)}")
    print(f"  Output size   : {fmt_size(total_output)}")

    if not args.no_manifest:
        write_manifest(out_dir, inp, chunks, total, target, fmt)

    print(f"{'─'*55}\n")


if __name__ == "__main__":
    main()