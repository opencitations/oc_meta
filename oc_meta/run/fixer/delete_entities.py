import argparse
import json
from oc_meta.plugins.editor import MetaEditor
from tqdm import tqdm
import os


def main():
    parser = argparse.ArgumentParser(description="Delete URIs using MetaEditor.")
    parser.add_argument("json_file", type=str, help="Path to the JSON file containing URIs")
    parser.add_argument("meta_config", type=str, help="Path to the MetaEditor configuration file")
    parser.add_argument("resp_agent", type=str, help="Responsible agent URI")
    parser.add_argument("--stop_file", type=str, default=".stop_deletions", help="Path to the stop file for graceful termination")

    args = parser.parse_args()

    if os.path.exists(args.stop_file):
        os.remove(args.stop_file)

    with open(args.json_file, 'r') as file:
        uris = json.load(file)
    
    meta_editor = MetaEditor(meta_config=args.meta_config, resp_agent=args.resp_agent)
    
    for uri in tqdm(uris, desc="Deleting URIs"):
        if os.path.exists(args.stop_file):
            print("Stop file detected. Halting further deletions.")
            break
        
        print(f"Deleting entity: {uri}")

        meta_editor.delete(res=uri, property=None, object=None)

if __name__ == "__main__":
    main()