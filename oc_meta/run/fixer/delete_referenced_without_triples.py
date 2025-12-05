import argparse
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

from oc_meta.plugins.editor import MetaEditor
from sparqlite import SPARQLClient
from tqdm import tqdm
from yaml import safe_load

def query_and_delete_triples(uri, meta_config, resp_agent, stop_file, endpoint):
    if os.path.exists(stop_file):
        return "stopped"

    meta_editor = MetaEditor(meta_config=meta_config, resp_agent=resp_agent)

    query = f"""
    SELECT ?s ?p
    WHERE {{
        ?s ?p <{uri}> .
    }}
    """
    with SPARQLClient(endpoint, max_retries=3, backoff_factor=5) as client:
        results = client.query(query)

    for result in results['results']['bindings']:
        s = result['s']['value']
        p = result['p']['value']
        if p not in {'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://purl.org/spar/datacite/usesIdentifierScheme', 'http://purl.org/spar/pro/withRole'}:
            meta_editor.delete(res=s, property=p, object=uri)

    return "deleted"

def single_process_deletion(uris, meta_config, resp_agent, stop_file, endpoint):    
    with tqdm(total=len(uris), desc="Deleting triples for URIs") as pbar:
        for uri in uris:
            if os.path.exists(stop_file):
                print("Stop file detected. Halting further deletions.")
                break
            query_and_delete_triples(uri, meta_config, resp_agent, stop_file, endpoint)
            pbar.update(1)

def multi_process_deletion(uris, meta_config, resp_agent, stop_file, endpoint):
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(query_and_delete_triples, uri, meta_config, resp_agent, stop_file, endpoint): uri for uri in uris}
        
        with tqdm(total=len(uris), desc="Deleting triples for URIs") as pbar:
            for future in as_completed(futures):
                result = future.result()
                if result == "stopped":
                    print("Stop file detected. Halting further deletions.")
                    break
                pbar.update(1)

def main():
    parser = argparse.ArgumentParser(description="Delete triples for URIs using MetaEditor.")
    parser.add_argument("json_file", type=str, help="Path to the JSON file containing URIs")
    parser.add_argument("meta_config", type=str, help="Path to the MetaEditor configuration file")
    parser.add_argument("resp_agent", type=str, help="Responsible agent URI")
    parser.add_argument("--stop_file", type=str, default=".stop_deletions", help="Path to the stop file for graceful termination")
    parser.add_argument("--multiprocessing", action="store_true", help="Use multiprocessing for deletions")

    args = parser.parse_args()

    if os.path.exists(args.stop_file):
        os.remove(args.stop_file)

    with open(args.json_file, 'r') as file:
        uris = json.load(file)

    with open(args.meta_config, 'r', encoding='utf8') as f:
        meta_config = safe_load(f)
    endpoint = meta_config['triplestore_url']

    if args.multiprocessing:
        multi_process_deletion(uris, args.meta_config, args.resp_agent, args.stop_file, endpoint)
    else:
        single_process_deletion(uris, args.meta_config, args.resp_agent, args.stop_file, endpoint)

if __name__ == "__main__":
    main()