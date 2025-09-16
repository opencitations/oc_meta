import argparse
import os
import zipfile
from multiprocessing import Pool, cpu_count

from oc_meta.run.gen_info_dir import (get_prefix, get_resource_number,
                                      get_short_name)
from rdflib import ConjunctiveGraph
from rdflib.namespace import PROV, RDF
from redis import Redis
from tqdm import tqdm


def process_zip_file(args):
    zip_file, redis_host, redis_port, redis_db = args
    redis_client = Redis(host=redis_host, port=redis_port, db=redis_db)
    missing_entities = []

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            with zip_ref.open(file_name) as entity_file:
                g = ConjunctiveGraph()
                g.parse(data=entity_file.read(), format='json-ld')
                
                for s, p, o in g.triples((None, RDF.type, PROV.Entity)):
                    prov_entity_uri = str(s)
                    entity_uri = prov_entity_uri.split('/prov/se/')[0]
                    supplier_prefix = get_prefix(entity_uri)
                    short_name = get_short_name(entity_uri)
                    resource_number = get_resource_number(entity_uri)
                    
                    expected_key = f"{short_name}:{supplier_prefix}:{resource_number}:se"
                    
                    if not redis_client.exists(expected_key):
                        print(f"\nEntità mancante trovata:")
                        print(f"URI: {entity_uri}")
                        print(f"Prov URI: {prov_entity_uri}")
                        print(f"Chiave Redis attesa: {expected_key}")
                        print("---")

                        missing_entities.append({
                            "URI": entity_uri,
                            "Prov URI": prov_entity_uri,
                            "Chiave Redis attesa": expected_key
                        })

    return missing_entities

def explore_provenance_files(root_path, redis_host, redis_port, redis_db):
    prov_zip_files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(root_path) 
                      for f in filenames if f.endswith('.zip') and 'prov' in dp]

    args_list = [(zip_file, redis_host, redis_port, redis_db) for zip_file in prov_zip_files]
    
    num_processes = cpu_count()  # Usa tutti i core disponibili
    with Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.imap(process_zip_file, args_list), total=len(args_list), desc="Processing provenance zip files"))

    all_missing_entities = [item for sublist in results for item in sublist]

    print(f"\nTotale entità mancanti trovate: {len(all_missing_entities)}")

def main():
    parser = argparse.ArgumentParser(description="Verifica la presenza di entità di provenance in Redis.")
    parser.add_argument("directory", type=str, help="Il percorso della directory da esplorare")
    parser.add_argument("--redis-host", type=str, default="localhost", help="L'host del server Redis")
    parser.add_argument("--redis-port", type=int, default=6379, help="La porta del server Redis")
    parser.add_argument("--redis-db", type=int, default=6, help="Il numero del database Redis da utilizzare")
    args = parser.parse_args()

    explore_provenance_files(args.directory, args.redis_host, args.redis_port, args.redis_db)

if __name__ == "__main__":
    main()