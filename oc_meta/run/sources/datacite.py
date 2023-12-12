import os
import json
import redis
from tqdm import tqdm
from multiprocessing import Pool, cpu_count


def load_json_to_redis(file_path, redis_host='localhost', redis_port=6379, db_number=14, omid_db_number=10):
    r = redis.Redis(host=redis_host, port=redis_port, db=db_number)
    r_omid = redis.Redis(host=redis_host, port=redis_port, db=omid_db_number)

    with open(file_path, 'r', encoding='utf-8') as file:
        doi_data = {}
        for line in file:
            item = json.loads(line)
            doi = item.get('id', '')
            omid_key = f'doi:{doi.lower()}'
            omid = r_omid.get(omid_key)
            if omid:
                omid_decoded = omid.decode()
                omid_record_key = f'omid:{omid_decoded}'
                if not r.exists(omid_record_key):
                    doi_data[omid_record_key] = 'https://archive.org/details/datacite_dump_20211022'
                    if len(doi_data) >= 1000:
                        r.mset(doi_data)
                        doi_data = {}
        if doi_data:
            r.mset(doi_data)
    print("Caricamento completato.")

folder_path = '/srv/data/arcangelo/datacite_dump_20211022/datacite_dump_20211022.json'
load_json_to_redis(folder_path)