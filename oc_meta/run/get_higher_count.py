import argparse
import json
import os
import zipfile

from tqdm import tqdm


def process_file(filepath, folder_name, number):
    max_id = -1
    try:
        with zipfile.ZipFile(filepath, 'r') as archive:
            for name in archive.namelist():
                with archive.open(name) as f:
                    content = json.loads(f.read())
                    items = content[0]['@graph']
                    for item in items:
                        url = item.get(f'@{folder_name}', '')
                        prefix = f"https://w3id.org/oc/meta/{folder_name}/06{number}0"
                        id_value = url[len(prefix):]
                        current_id = int(id_value)
                        max_id = max(max_id, current_id)
    except zipfile.BadZipFile:
        print(f"Warning: {filepath} is not a valid ZIP file. Skipping...")
    return max_id

def find_max_id(base_path, folder_name, number):
    folder_path = os.path.join(base_path, folder_name, '06' + str(number) + '0')
    all_files = [os.path.join(dirpath, file) 
                 for dirpath, _, filenames in os.walk(folder_path) 
                 for file in filenames 
                 if file.endswith('.zip') and 'prov' not in dirpath.split(os.sep)]
    
    max_ids = [process_file(file, folder_name, number) for file in tqdm(all_files, desc="Processing files")]
    return max(max_ids)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find the max id in the .jsonld files")
    parser.add_argument("base_path", help="Path to the root directory")
    parser.add_argument("folder_name", help="Name of the starting folder (ar, br, ra, re, id)")
    parser.add_argument("number", type=int, help="Integer to replace in '06[number]0'")
    args = parser.parse_args()
    result = find_max_id(args.base_path, args.folder_name, args.number)
    print(f"\nThe maximum id value for the given parameters is: {result}")
