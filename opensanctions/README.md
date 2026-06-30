python3 -m venv venv
source venv/bin/activate
pip install requests tqdm
python opensanctions_bulk.py
python chunk_files.py --input opensanctions_data/default_entities.ftm.json --size 25MB
rm opensanctions_data/default_entities.ftm.json

git commit -m "remove opensanctions data 20260615"
git commit -m "add opensanctions default data 20260630-1330"
