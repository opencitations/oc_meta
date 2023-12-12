import os
import json
import redis
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

def process_file(args):
    file_path, redis_host, redis_port, db_number, omid_db_number = args
    r = redis.Redis(host=redis_host, port=redis_port, db=db_number)
    r_omid = redis.Redis(host=redis_host, port=redis_port, db=omid_db_number)
    doi_data = {}

    with open(file_path, 'r', encoding='utf8') as file:
        data = json.load(file)
        for item in data['items']:
            doi = str(item['DOI'])
            omid = r_omid.get(f'doi:{doi.lower()}')
            if omid:
                doi_data[f'omid:{omid.decode()}'] = 'https://api.crossref.org/snapshots/monthly/2023/09/all.json.tar.gz'
                if len(doi_data) >= 1000:
                    r.mset(doi_data)
                    doi_data = {}

    if doi_data:
        r.mset(doi_data)

def load_json_to_redis(folder_path, redis_host='localhost', redis_port=6379, db_number=14, omid_db_number=10, workers=cpu_count()):
    json_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.json')]
    num_processes = min(len(json_files), workers)
    with Pool(processes=num_processes) as pool:
        tasks = [(file, redis_host, redis_port, db_number, omid_db_number) for file in json_files]
        list(tqdm(pool.imap_unordered(process_file, tasks), total=len(tasks), desc="Caricamento", unit="file"))

    print("Caricamento completato.")

folder_path = '/srv/data/arcangelo/crossref-data-2023-09'
load_json_to_redis(folder_path, workers=64)