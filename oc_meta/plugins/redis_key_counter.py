import redis
from tqdm import tqdm
import multiprocessing

redis_client = redis.StrictRedis(host='localhost', port=6379, db=14)

keys = redis_client.keys('*')

count = 0

progress_bar = tqdm(total=len(keys), desc="Conteggio chiavi")

for key in keys:
    value = redis_client.get(key)
    if value.decode('utf-8') == 'https://api.japanlinkcenter.org/':
        count += 1
    progress_bar.update(1)
progress_bar.close()

print(f"Numero di chiavi con il valore 'https://api.japanlinkcenter.org/': {count}")