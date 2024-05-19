import argparse
import json
from oc_meta.plugins.editor import MetaEditor
from tqdm import tqdm
import os
from concurrent.futures import ProcessPoolExecutor, as_completed


def delete_uri(uri, meta_config, resp_agent, stop_file):
    if os.path.exists(stop_file):
        return "stopped"
    
    meta_editor = MetaEditor(meta_config=meta_config, resp_agent=resp_agent)
    print(f"Deleting entity: {uri}")
    meta_editor.delete(res=uri, property=None, object=None)
    return "deleted"


def single_process_deletion(uris, meta_config, resp_agent, stop_file):
    meta_editor = MetaEditor(meta_config=meta_config, resp_agent=resp_agent)
    
    with tqdm(total=len(uris), desc="Deleting URIs") as pbar:
        for uri in uris:
            if os.path.exists(stop_file):
                print("Stop file detected. Halting further deletions.")
                break
            print(f"Deleting entity: {uri}")
            meta_editor.delete(res=uri, property=None, object=None)
            pbar.update(1)


def multi_process_deletion(uris, meta_config, resp_agent, stop_file):
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(delete_uri, uri, meta_config, resp_agent, stop_file): uri for uri in uris}
        
        with tqdm(total=len(uris), desc="Deleting URIs") as pbar:
            for future in as_completed(futures):
                result = future.result()
                if result == "stopped":
                    print("Stop file detected. Halting further deletions.")
                    break
                pbar.update(1)


def main():
    parser = argparse.ArgumentParser(description="Delete URIs using MetaEditor.")
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

    if args.multiprocessing:
        multi_process_deletion(uris, args.meta_config, args.resp_agent, args.stop_file)
    else:
        single_process_deletion(uris, args.meta_config, args.resp_agent, args.stop_file)


if __name__ == "__main__":
    main()