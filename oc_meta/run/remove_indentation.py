import argparse
import multiprocessing
import os
import zipfile
from tqdm import tqdm
import json

def process_file(file_path):
    if file_path.endswith('.lock'):
        os.remove(file_path)
    elif file_path.endswith('.zip'):
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                with zipfile.ZipFile(file_path + '.tmp', 'w') as new_zip:
                    for file in zip_ref.infolist():
                        with zip_ref.open(file) as f:
                            if file.filename.endswith('.json'):
                                content = json.load(f)
                                new_content = json.dumps(content, indent=None).encode('utf-8')
                                new_zip.writestr(file, new_content)
                            else:
                                new_zip.writestr(file, f.read())
        except zipfile.BadZipFile:
            os.remove(file_path)
            print(f"Warning: '{file_path}' is not a zip file or is corrupted and will be removed.")
        os.replace(file_path + '.tmp', file_path)

def walk_and_process(root_dir):
    file_paths = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            file_paths.append(os.path.join(root, file))

    with multiprocessing.Pool() as pool:
        list(tqdm(pool.imap(process_file, file_paths), total=len(file_paths)))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Remove .lock files and rewrite .zip-contained JSONs without indentation.')
    parser.add_argument('root_dir', type=str, help='Root directory to start processing')
    
    args = parser.parse_args()
    
    walk_and_process(args.root_dir)
