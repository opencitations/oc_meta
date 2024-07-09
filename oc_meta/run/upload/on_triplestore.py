import argparse
import os
import json
from SPARQLWrapper import SPARQLWrapper, POST
from tqdm import tqdm
from oc_meta.run.split_insert_and_delete import process_sparql_file

CACHE_FILE = 'ts_upload_cache.json'
FAILED_QUERIES_FILE = 'failed_queries.txt'

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r', encoding='utf8') as cache_file:
            return set(json.load(cache_file))
    return set()

def save_cache(processed_files):
    with open(CACHE_FILE, 'w', encoding='utf8') as cache_file:
        json.dump(list(processed_files), cache_file)

def save_failed_query_file(filename):
    with open(FAILED_QUERIES_FILE, 'a', encoding='utf8') as failed_file:
        failed_file.write(f"{filename}\n")

def execute_sparql_update(endpoint, query):
    try:
        sparql = SPARQLWrapper(endpoint)
        sparql.setMethod(POST)
        sparql.setQuery(query)
        response = sparql.queryAndConvert()
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

def generate_sparql_queries(quads_to_add, quads_to_remove, batch_size):
    queries = []

    if quads_to_add:
        for i in range(0, len(quads_to_add), batch_size):
            insert_query = 'INSERT DATA {\n'
            batch = quads_to_add[i:i+batch_size]
            for graph in set(q[-1] for q in batch):
                insert_query += f'  GRAPH {graph} {{\n'
                for quad in batch:
                    if quad[-1] == graph:
                        insert_query += '    ' + ' '.join(quad[:-1]) + ' .\n'
                insert_query += '  }\n'
            insert_query += '}\n'
            queries.append(insert_query)

    if quads_to_remove:
        for i in range(0, len(quads_to_remove), batch_size):
            delete_query = 'DELETE DATA {\n'
            batch = quads_to_remove[i:i+batch_size]
            for graph in set(q[-1] for q in batch):
                delete_query += f'  GRAPH {graph} {{\n'
                for quad in batch:
                    if quad[-1] == graph:
                        delete_query += '    ' + ' '.join(quad[:-1]) + ' .\n'
                delete_query += '  }\n'
            delete_query += '}\n'
            queries.append(delete_query)

    return queries

def split_queries(file_path, batch_size):
    quads_to_add, quads_to_remove = process_sparql_file(file_path)
    return generate_sparql_queries(quads_to_add, quads_to_remove, batch_size)

def upload_sparql_updates(endpoint, folder, batch_size):
    processed_files = load_cache()
    failed_files = []

    all_files = [f for f in os.listdir(folder) if f.endswith('.sparql')]
    files_to_process = [f for f in all_files if f not in processed_files]
    for file in tqdm(files_to_process, desc="Processing files"):
        file_path = os.path.join(folder, file)
        queries = split_queries(file_path, batch_size)
        for query in queries:
            success = execute_sparql_update(endpoint, query)
            if not success:
                save_failed_query_file(file)
                break
        else:
            processed_files.add(file)
            save_cache(processed_files)
    
    if failed_files:
        print("Files with failed queries:")
        for file in failed_files:
            print(file)

def main():
    parser = argparse.ArgumentParser(description='Execute SPARQL update queries on a triple store.')
    parser.add_argument('endpoint', type=str, help='Endpoint URL of the triple store')
    parser.add_argument('folder', type=str, help='Path to the folder containing SPARQL update query files')
    parser.add_argument('--batch_size', type=int, default=10, help='Number of quadruples to include in a batch (default: 10)')

    args = parser.parse_args()

    batch_size = args.batch_size

    upload_sparql_updates(args.endpoint, args.folder, batch_size)

if __name__ == "__main__":
    main()