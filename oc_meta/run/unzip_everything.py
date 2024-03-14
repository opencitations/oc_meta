import argparse
import os
import zipfile
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

def extract_zip(args):
    file_path, input_path, output_path = args
    root = os.path.dirname(file_path)
    output_dir = root.replace(input_path, output_path, 1)
    try:
        os.makedirs(output_dir, exist_ok=True)  # exist_ok=True ignora l'errore se la cartella esiste già
    except FileExistsError:
        pass  # Se la cartella è stata creata da un altro processo nel frattempo, ignora l'errore
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        for member in zip_ref.namelist():
            extracted_path = os.path.join(output_dir, member)
            if not extracted_path.startswith(os.path.commonpath([output_dir])):
                raise Exception("Path traversal attempt detected")
            zip_ref.extract(member, output_dir)

def find_and_extract(input_path, output_path, data_only=False):
    args_list = []
    for root, dirs, files_in_dir in os.walk(input_path):
        for file in files_in_dir:
            if file.endswith(".zip"):
                file_path = os.path.join(root, file)
                path_parts = os.path.normpath(file_path).split(os.sep)
                if data_only and 'prov' not in path_parts:
                    args_list.append((file_path, input_path, output_path))
                elif not data_only:
                    args_list.append((file_path, input_path, output_path))
    
    # Utilizzo di multiprocessing per l'estrazione parallela
    pool = Pool(processes=cpu_count())
    for _ in tqdm(pool.imap_unordered(extract_zip, args_list), total=len(args_list), desc="Estrazione file in corso"):
        pass
    pool.close()
    pool.join()

def main():
    parser = argparse.ArgumentParser(description="Estrai file JSON da archivi ZIP in parallelo.")
    parser.add_argument("input_path", type=str, help="Percorso della cartella di input.")
    parser.add_argument("output_path", type=str, help="Percorso della cartella di output.")
    parser.add_argument("--data_only", action="store_true", help="Estrai file solo da sottocartelle diverse da 'prov'.")

    args = parser.parse_args()

    # Assicurazione che output_path termini con os.sep
    if not args.output_path.endswith(os.sep):
        args.output_path += os.sep

    find_and_extract(args.input_path, args.output_path, args.data_only)

if __name__ == "__main__":
    main()