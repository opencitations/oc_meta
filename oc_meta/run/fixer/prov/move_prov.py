import argparse
import os
from multiprocessing import Pool
from tqdm import tqdm
import shutil

def parse_arguments():
    parser = argparse.ArgumentParser(description="Copy files from source to destination preserving the folder structure.")
    parser.add_argument("source", help="Source directory path")
    parser.add_argument("destination", help="Destination directory path")
    return parser.parse_args()

def prepare_directory_structure(args):
    source_root, dest_root, path = args
    dest_path = path.replace(source_root, dest_root, 1)
    if not os.path.exists(dest_path):
        os.makedirs(dest_path, exist_ok=True)
    return dest_path

def copy_file(args):
    source_file, dest_root, source_root = args
    dest_file = source_file.replace(source_root, dest_root, 1)
    shutil.copy2(source_file, dest_file)
    return dest_file

def find_items(source, dest):
    dirs_to_create = []
    files_to_copy = []
    for root, dirs, files in os.walk(source):
        if 'prov' in root.split(os.path.sep):
            dirs_to_create.append((source, dest, root))
            for file in files:
                source_file = os.path.join(root, file)
                files_to_copy.append((source_file, dest, source))
    return dirs_to_create, files_to_copy

def main():
    args = parse_arguments()
    source = args.source
    destination = args.destination
    
    dirs_to_create, files_to_copy = find_items(source, destination)

    with Pool() as pool:
        print("Creazione delle cartelle...")
        for _ in tqdm(pool.imap_unordered(prepare_directory_structure, dirs_to_create), total=len(dirs_to_create), desc="Creazione cartelle"):
            pass

        print("\nCopia dei file...")
        for _ in tqdm(pool.imap_unordered(copy_file, files_to_copy), total=len(files_to_copy), desc="Copia file"):
            pass

if __name__ == "__main__":
    main()