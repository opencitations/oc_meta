import os
import csv
import redis
from tqdm import tqdm
from multiprocessing import Pool, cpu_count


def load_csv_to_redis(file_path, redis_host='localhost', redis_port=6379, db_number=14, omid_db_number=10):
    r = redis.Redis(host=redis_host, port=redis_port, db=db_number)
    r_omid = redis.Redis(host=redis_host, port=redis_port, db=omid_db_number)

    while True:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                pmid_data = {}
                for row in reader:
                    pmid = row.get('pmid', '')
                    omid_key = f'pmid:{pmid}'
                    omid = r_omid.get(omid_key)
                    if omid:
                        omid_decoded = omid.decode()
                        omid_record_key = f'omid:{omid_decoded}'
                        if not r.exists(omid_record_key):
                            pmid_data[omid_record_key] = 'Pubmed'
                            if len(pmid_data) >= 1000:
                                r.mset(pmid_data)
                                pmid_data = {}
                if pmid_data:
                    r.mset(pmid_data)
            break
        except csv.Error:
            # Incrementa il limite della dimensione del campo e riprova.
            max_field_size = csv.field_size_limit()
            csv.field_size_limit(max_field_size * 2)

    print("Caricamento completato.")

folder_path = '/srv/data/arcangelo/pubmed/icite_metadata.csv'
load_csv_to_redis(folder_path)