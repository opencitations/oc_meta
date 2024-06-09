import argparse
import os
import json
from SPARQLWrapper import SPARQLWrapper, POST
from tqdm import tqdm

CACHE_FILE = 'ts_upload_cache.json'

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r', encoding='utf8') as cache_file:
            return set(json.load(cache_file))
    return set()

def save_cache(processed_files):
    with open(CACHE_FILE, 'w', encoding='utf8') as cache_file:
        json.dump(list(processed_files), cache_file)

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

def split_queries(content):
    content = content + " ;"
    queries = content.split(". } } ;")
    queries = [query.strip() + ". } }" for query in queries if query.strip()]
    return queries

def main():
    parser = argparse.ArgumentParser(description='Execute SPARQL update queries on a triple store.')
    parser.add_argument('endpoint', type=str, help='Endpoint URL of the triple store')
    parser.add_argument('folder', type=str, help='Path to the folder containing SPARQL update query files')
    parser.add_argument('--batch_size', type=int, default=10, help='Number of queries to include in a batch (default: 10)')

    args = parser.parse_args()

    processed_files = load_cache()
    failed_files = []

    all_files = [f for f in os.listdir(args.folder) if f.endswith('.txt')]
    files_to_process = [f for f in all_files if f not in processed_files]

    for file in tqdm(files_to_process, desc="Processing files"):
        file_path = os.path.join(args.folder, file)
        with open(file_path, 'r', encoding='utf8') as query_file:
            content = query_file.read()
            queries = split_queries(content)
            batch_size = args.batch_size
            for i in range(0, len(queries), batch_size):
                batch_queries = ";\n".join(queries[i:i + batch_size])
                success = execute_sparql_update(args.endpoint, batch_queries)
                if not success:
                    print(f"Failed to execute batch starting from query {i} in file {file}")
                    failed_files.append(file)
                    break
            else:
                processed_files.add(file)
                save_cache(processed_files)
    
    if failed_files:
        print("Files with failed queries:")
        for file in failed_files:
            print(file)

if __name__ == "__main__":
    main()
