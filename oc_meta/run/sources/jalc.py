import os
import csv
import redis
from tqdm import tqdm

def process_file(file_path, r, r_omid):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        id_data = {}
        for row in reader:
            ids = row.get('id', '').split()
            for id in ids:
                omid = r_omid.get(id)
                if omid:
                    omid_decoded = omid.decode()
                    omid_record_key = f'omid:{omid_decoded}'
                    if not r.exists(omid_record_key):
                        id_data[omid_record_key] = 'https://api.japanlinkcenter.org/'
                        if len(id_data) >= 1000:
                            r.mset(id_data)
                            id_data = {}
        if id_data:
            r.mset(id_data)

def load_csv_to_redis(folder_path, redis_host='localhost', redis_port=6379, db_number=14, omid_db_number=10):
    r = redis.Redis(host=redis_host, port=redis_port, db=db_number)
    r_omid = redis.Redis(host=redis_host, port=redis_port, db=omid_db_number)
    file_paths = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]
    for file_path in file_paths:
        process_file(file_path, r, r_omid)
    print("Caricamento completato.")

folder_path = '/home/arcangelo/meta_input_joci'
load_csv_to_redis(folder_path)