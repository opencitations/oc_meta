import argparse
import gzip
import os
import zipfile
from pebble import ProcessPool
from concurrent.futures import TimeoutError
from tqdm import tqdm
import rdflib


def count_triples_in_file(compression_type, file_path, data_format):
    triple_count = 0
    try:
        if compression_type == 'zip':
            with zipfile.ZipFile(file_path, 'r') as z:
                jsonld_filename = [f for f in z.namelist()][0]
                with z.open(jsonld_filename) as f:
                    file_content = f.read().decode('utf-8')
        elif compression_type == 'gz':
            with gzip.open(file_path, 'rt') as f:
                file_content = f.read()
        elif compression_type in ['json', 'ttl']:
            with open(file_path, 'r', encoding='utf8') as f:
                file_content = f.read()
        g = rdflib.Dataset(default_union=True)
        g.parse(data=file_content, format=data_format)
        triple_count += len(g)
    except Exception as e:
        print(f"Error processing file in {file_path}: {e}")
    return triple_count

def process_directory(directory, compression_type, prov_only, data_only, data_format):
    if compression_type not in ['zip', 'gz', 'json', 'ttl']:
        raise ValueError("Unsupported compression type.")

    files = []
    for root, dirs, files_in_dir in os.walk(directory):
        for file in files_in_dir:
            if file.endswith('.' + compression_type):
                file_path = os.path.join(root, file)
                path_parts = os.path.normpath(file_path).split(os.sep)                
                if prov_only and 'prov' in path_parts:
                    files.append(file_path)
                elif data_only and 'prov' not in path_parts:
                    files.append(file_path)
                elif not prov_only and not data_only:
                    files.append(file_path)

    total_triples = 0
    with ProcessPool() as pool:
        future_results = []
        for file_path in files:
            future = pool.schedule(count_triples_in_file, args=(compression_type, file_path, data_format))
            future_results.append(future)

        for future in tqdm(future_results, desc="Processing files", unit="file"):
            try:
                result = future.result()
                total_triples += result
            except TimeoutError as error:
                print(f"Operation took longer than expected: {error}")
            except Exception as error:
                print(f"Error during execution: {error}")

    return total_triples

def main():
    parser = argparse.ArgumentParser(description="Conta le triple RDF nei file compressi in una directory.")
    parser.add_argument('directory', type=str, help='Directory da esplorare')
    parser.add_argument('compression_type', type=str, choices=['zip', 'gz', 'json', 'ttl'], help='Type of file compression (zip, gz, json, or nt)')
    parser.add_argument('data_format', type=str, choices=['json-ld', 'nquads', 'turtle'], help='Data format of the files (json-ld, nquads, or nt)')
    parser.add_argument('--prov_only', action='store_true', help='Conta solo nelle sottocartelle di nome prov', default=False)
    parser.add_argument('--data_only', action='store_true', help='Conta solo nelle sottocartelle di nome diverso da prov', default=False)

    args = parser.parse_args()

    total_triples = process_directory(args.directory, args.compression_type, args.prov_only, args.data_only, args.data_format)
    print(f"Numero totale di triple RDF: {total_triples}")

if __name__ == "__main__":
    main()