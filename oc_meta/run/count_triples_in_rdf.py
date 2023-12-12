import argparse
import gzip
import os
import zipfile
import ijson
from pebble import ProcessPool
from concurrent.futures import TimeoutError
from multiprocessing import Manager
from tqdm import tqdm
import json
import rdflib


def count_triples_in_file(compression_type, file_path, jsonld_filename):
    triple_count = 0
    try:
        if compression_type == 'zip':
            with zipfile.ZipFile(file_path, 'r') as z:
                with z.open(jsonld_filename) as f:
                    file_content = f.read().decode('utf-8')
        elif compression_type == 'gz':
            with gzip.open(file_path, 'rt') as f:
                file_content = f.read()
        elif compression_type == 'json':
            with open(file_path, 'r') as f:
                file_content = f.read()

        g = rdflib.ConjunctiveGraph()
        g.parse(data=file_content, format='json-ld')
        triple_count += len(g)
    except Exception as e:
        print(f"Error processing file {jsonld_filename} in {file_path}: {e}")
    return triple_count

def process_directory(directory, compression_type):
    if compression_type not in ['zip', 'gz', 'json']:
        raise ValueError("Unsupported compression type.")

    files = [os.path.join(root, file)
             for root, dirs, files in os.walk(directory)
             for file in files if file.endswith('.' + compression_type)]

    total_triples = 0
    with ProcessPool() as pool:
        future_results = []
        for file_path in files:
            if compression_type == 'zip':
                with zipfile.ZipFile(file_path, 'r') as z:
                    jsonld_files = [f for f in z.namelist() if f.endswith('.json')]
                    for jsonld_file in jsonld_files:
                        future = pool.schedule(count_triples_in_file, args=(compression_type, file_path, jsonld_file))
                        future_results.append(future)
            elif compression_type in ['gz', 'json']:
                future = pool.schedule(count_triples_in_file, args=(compression_type, file_path, file_path))
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
    parser.add_argument('compression_type', type=str, choices=['zip', 'gz', 'json'], help='Type of file compression (zip, gz, or json)')
    args = parser.parse_args()

    total_triples = process_directory(args.directory, args.compression_type)
    print(f"Numero totale di triple RDF: {total_triples}")

if __name__ == "__main__":
    main()