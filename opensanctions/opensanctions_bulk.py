"""
OpenSanctions Bulk Data Downloader & Parser
============================================
Downloads and processes bulk data from OpenSanctions (https://www.opensanctions.org/)
which uses the FollowTheMoney (FtM) data model.

Free for non-commercial use. Commercial use requires a data license.
Docs: https://www.opensanctions.org/docs/bulk/

Usage:
    pip install requests tqdm
    python opensanctions_bulk.py

    # Or with options:
    python opensanctions_bulk.py --dataset default --format ftm --output data/ --filter-topic sanction
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("ERROR: 'requests' not installed. Run: pip install requests")
    sys.exit(1)

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("TIP: Install tqdm for progress bars: pip install tqdm")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_DATA_URL = "https://data.opensanctions.org/datasets/latest/{dataset}/{filename}"
METADATA_URL  = "https://data.opensanctions.org/datasets/latest/{dataset}/index.json"
DATASETS_URL  = "https://www.opensanctions.org/datasets/?format=json"

# Common datasets / collections
POPULAR_DATASETS = {
    "default":         "Recommended: full screening dataset (PEPs + sanctions + watchlists)",
    "sanctions":       "Global consolidated sanctions list only",
    "peps":            "Politically exposed persons (PEPs) only",
    "debarment":       "World Bank / MDB debarment lists",
    "crime":           "Criminal watchlists",
    "us_ofac_sdn":     "US OFAC Specially Designated Nationals",
    "eu_fsf":          "EU Financial Sanctions File",
    "un_sc_sanctions": "UN Security Council sanctions",
}

# Available bulk export filenames per dataset
FORMAT_FILES = {
    "ftm":        "entities.ftm.json",   # FollowTheMoney JSONL (recommended)
    "csv":        "targets.simple.csv",  # Simplified CSV of sanctioned targets
    "nested":     "entities.nested.json",# Nested JSON (larger, easier for ad-hoc use)
    "delta":      "delta.json",          # Incremental delta since last run
}

# Risk topic tags (filter helpers)
RISK_TOPICS = [
    "sanction", "sanction.linked", "sanction.debarment",
    "pep", "pep.national", "pep.state", "pep.local", "pep.linked",
    "crime", "crime.fraud", "crime.cyber", "crime.terror",
    "wanted",
]


# ---------------------------------------------------------------------------
# HTTP session with retries
# ---------------------------------------------------------------------------

def build_session(api_key: Optional[str] = None) -> requests.Session:
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1.0,
                  status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    if api_key:
        session.headers["Authorization"] = f"ApiKey {api_key}"
    session.headers["User-Agent"] = "opensanctions-bulk-script/1.0"
    return session


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------

def fetch_dataset_metadata(dataset: str, session: requests.Session) -> dict:
    """Fetch the index.json metadata for a dataset."""
    url = METADATA_URL.format(dataset=dataset)
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def print_dataset_info(meta: dict) -> None:
    print(f"\n{'='*60}")
    print(f"  Dataset  : {meta.get('name', '?')}")
    print(f"  Title    : {meta.get('title', '?')}")
    print(f"  Entities : {meta.get('entity_count', '?'):,}")
    print(f"  Updated  : {meta.get('updated_at', '?')}")
    print(f"  Coverage : {meta.get('coverage', {}).get('start', '?')} → "
          f"{meta.get('coverage', {}).get('end', '?')}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def download_file(url: str, dest: Path, session: requests.Session,
                  chunk_size: int = 65536) -> Path:
    """Stream-download a (potentially large) file with an optional progress bar."""
    print(f"Downloading: {url}")
    resp = session.get(url, stream=True, timeout=60)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))

    if HAS_TQDM:
        bar = tqdm(total=total, unit="B", unit_scale=True,
                   desc=dest.name, ncols=80)

    with dest.open("wb") as fh:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            fh.write(chunk)
            if HAS_TQDM:
                bar.update(len(chunk))

    if HAS_TQDM:
        bar.close()

    size_mb = dest.stat().st_size / 1_048_576
    print(f"Saved {dest} ({size_mb:.1f} MB)")
    return dest


def get_download_url(dataset: str, fmt: str) -> str:
    filename = FORMAT_FILES[fmt]
    return BASE_DATA_URL.format(dataset=dataset, filename=filename)


# ---------------------------------------------------------------------------
# FtM JSONL parser (entities.ftm.json)
# ---------------------------------------------------------------------------

def iter_ftm_entities(path: Path) -> Generator[dict, None, None]:
    """Yield one FtM entity dict per line from a .ftm.json file."""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def entity_has_topic(entity: dict, topic: str) -> bool:
    props = entity.get("properties", {})
    return topic in props.get("topics", [])


def entity_get(entity: dict, prop: str, default="") -> str:
    """Return first value of a property, or default."""
    vals = entity.get("properties", {}).get(prop, [])
    return vals[0] if vals else default


# ---------------------------------------------------------------------------
# Processing / output helpers
# ---------------------------------------------------------------------------

def process_ftm_to_csv(input_path: Path, output_path: Path,
                       filter_topic: Optional[str] = None,
                       schema_filter: Optional[str] = None,
                       limit: Optional[int] = None) -> int:
    """
    Parse a .ftm.json file and write a flat CSV of key fields.
    Returns number of rows written.
    """
    import csv

    FIELDS = [
        "id", "schema", "name", "country", "topics",
        "birthDate", "nationality", "position",
        "registrationNumber", "address",
        "sanctions", "datasets", "sourceUrl",
    ]

    count = 0
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()

        for entity in iter_ftm_entities(input_path):
            if limit and count >= limit:
                break

            schema = entity.get("schema", "")
            if schema_filter and schema != schema_filter:
                continue

            topics = entity.get("properties", {}).get("topics", [])
            if filter_topic and filter_topic not in topics:
                continue

            props = entity.get("properties", {})

            row = {
                "id":                 entity.get("id", ""),
                "schema":             schema,
                "name":               "; ".join(props.get("name", [])),
                "country":            "; ".join(props.get("country", [])),
                "topics":             "; ".join(topics),
                "birthDate":          "; ".join(props.get("birthDate", [])),
                "nationality":        "; ".join(props.get("nationality", [])),
                "position":           "; ".join(props.get("position", [])),
                "registrationNumber": "; ".join(props.get("registrationNumber", [])),
                "address":            "; ".join(props.get("address", [])),
                "sanctions":          "; ".join(props.get("sanctions", [])),
                "datasets":           "; ".join(entity.get("datasets", [])),
                "sourceUrl":          "; ".join(props.get("sourceUrl", [])),
            }
            writer.writerow(row)
            count += 1

    return count


def print_summary_stats(input_path: Path,
                        filter_topic: Optional[str] = None) -> None:
    """Print a quick summary of the downloaded dataset."""
    schema_counts  = defaultdict(int)
    topic_counts   = defaultdict(int)
    country_counts = defaultdict(int)
    total = 0

    print("\nAnalysing dataset …")
    for entity in iter_ftm_entities(input_path):
        topics = entity.get("properties", {}).get("topics", [])
        if filter_topic and filter_topic not in topics:
            continue

        schema_counts[entity.get("schema", "Unknown")] += 1
        for t in topics:
            topic_counts[t] += 1
        for c in entity.get("properties", {}).get("country", []):
            country_counts[c] += 1
        total += 1

    print(f"\n{'─'*50}")
    print(f"  Total entities : {total:,}")
    print(f"\n  Top schemas:")
    for schema, n in sorted(schema_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"    {schema:<30} {n:>8,}")
    print(f"\n  Top topics:")
    for topic, n in sorted(topic_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"    {topic:<30} {n:>8,}")
    print(f"\n  Top countries (by entity count):")
    for country, n in sorted(country_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"    {country:<10} {n:>8,}")
    print(f"{'─'*50}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Download and process OpenSanctions bulk data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dataset", default="default",
        help=f"Dataset name (default: 'default'). Popular: {', '.join(POPULAR_DATASETS)}"
    )
    parser.add_argument(
        "--format", dest="fmt", default="ftm",
        choices=list(FORMAT_FILES.keys()),
        help="Bulk data format to download (default: ftm / JSONL)"
    )
    parser.add_argument(
        "--output", default="opensanctions_data",
        help="Output directory (created if missing, default: opensanctions_data/)"
    )
    parser.add_argument(
        "--filter-topic", metavar="TOPIC",
        help=f"Only include entities with this topic. Options: {', '.join(RISK_TOPICS)}"
    )
    parser.add_argument(
        "--schema", metavar="SCHEMA",
        help="Only include entities with this schema (e.g. Person, Company, Vessel)"
    )
    parser.add_argument(
        "--to-csv", action="store_true",
        help="Convert FtM JSONL output to a flat CSV file"
    )
    parser.add_argument(
        "--stats", action="store_true",
        help="Print summary statistics after download"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Limit number of entities when writing CSV (useful for testing)"
    )
    parser.add_argument(
        "--api-key", default=os.environ.get("OPENSANCTIONS_API_KEY"),
        help="API key for commercial/authenticated access (or set OPENSANCTIONS_API_KEY env var)"
    )
    parser.add_argument(
        "--list-datasets", action="store_true",
        help="Print popular datasets and exit"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.list_datasets:
        print("\nPopular OpenSanctions datasets:\n")
        for name, desc in POPULAR_DATASETS.items():
            print(f"  {name:<25} {desc}")
        print()
        return

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    session = build_session(api_key=args.api_key)

    # --- Fetch metadata ---
    print(f"\nFetching metadata for dataset: '{args.dataset}' …")
    try:
        meta = fetch_dataset_metadata(args.dataset, session)
        print_dataset_info(meta)
    except requests.HTTPError as exc:
        print(f"ERROR: Could not fetch metadata — {exc}")
        print(f"       Check dataset name. Use --list-datasets to see common options.")
        sys.exit(1)

    # --- Download bulk data ---
    url      = get_download_url(args.dataset, args.fmt)
    filename = FORMAT_FILES[args.fmt]
    dest     = out_dir / f"{args.dataset}_{filename}"

    if dest.exists():
        print(f"File already exists: {dest}  (delete it to re-download)")
    else:
        try:
            download_file(url, dest, session)
        except requests.HTTPError as exc:
            print(f"ERROR downloading data: {exc}")
            sys.exit(1)

    # --- Optional stats ---
    if args.stats and args.fmt == "ftm":
        print_summary_stats(dest, filter_topic=args.filter_topic)

    # --- Optional CSV conversion ---
    if args.to_csv and args.fmt == "ftm":
        csv_name = dest.stem + (f"_{args.filter_topic}" if args.filter_topic else "") + ".csv"
        csv_path = out_dir / csv_name
        print(f"\nConverting to CSV → {csv_path}")
        n = process_ftm_to_csv(
            dest, csv_path,
            filter_topic=args.filter_topic,
            schema_filter=args.schema,
            limit=args.limit,
        )
        print(f"Done. {n:,} rows written to {csv_path}")

    print("\nAll done.")


if __name__ == "__main__":
    main()
