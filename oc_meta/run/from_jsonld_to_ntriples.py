import argparse
import os
import zipfile
from rdflib import ConjunctiveGraph
from tqdm import tqdm
import multiprocessing
from multiprocessing import Manager
import hashlib

def parse_args():
    parser = argparse.ArgumentParser(description='Process JSON-LD files from zip archives with multiprocessing.')
    parser.add_argument('input_folder', type=str, help='Path to the input folder')
    parser.add_argument('--prov', action='store_true', help='Include files from folders named "prov"')
    parser.add_argument('--only_prov', action='store_true', help='Process files only from folders named "prov"')
    parser.add_argument('output_folder', type=str, help='Path to the output folder')
    parser.add_argument('--format', choices=['triples', 'quads'], default='triples', help='Output format: triples or quads (default: triples)')
    return parser.parse_args()

def generate_unique_output_filename(zip_path, json_filename, output_format):
    hash_digest = hashlib.sha256(zip_path.encode()).hexdigest()[:10]
    extension = 'nq' if output_format == 'quads' else 'ttl'
    unique_filename = f"{hash_digest}_{json_filename.replace('.json', '')}.{extension}"
    return unique_filename

def process_zip_file(args):
    zip_path, output_folder, output_format, queue = args
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.filename.endswith('.json'):
                with zip_ref.open(file_info) as json_file:
                    json_file_data = json_file.read()
                    process_jsonld_data(json_file_data, file_info.filename, zip_path, output_folder, output_format)
    queue.put(1)  # Signal completion of one ZIP file

def process_jsonld_data(json_data, original_filename, zip_path, output_folder, output_format):
    g = ConjunctiveGraph()
    g.parse(data=json_data, format='json-ld')
    unique_output_filename = generate_unique_output_filename(zip_path, original_filename, output_format)
    output_file_path = os.path.join(output_folder, unique_output_filename)
    # Serializziamo in N-Triples o N-Quads
    g.serialize(format='nquads' if output_format == 'quads' else 'nt', destination=output_file_path, encoding='utf-8')

def main():
    args = parse_args()
    if not os.path.exists(args.output_folder):
        os.makedirs(args.output_folder, exist_ok=True)
    zip_files = []
    for root, dirs, files in os.walk(args.input_folder, topdown=True):
        if args.only_prov and 'prov' not in root:
            continue
        if not args.prov and 'prov' in root:
            continue
        for file in files:
            if file.endswith('.zip'):
                zip_files.append(os.path.join(root, file))

    with Manager() as manager:
        queue = manager.Queue()
        with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
            pool.map_async(process_zip_file, [(zip_path, args.output_folder, args.format, queue) for zip_path in zip_files])
            pool.close()

            pbar = tqdm(total=len(zip_files), desc='Overall Progress', unit='zip')
            for _ in range(len(zip_files)):
                queue.get()  # Wait for a task to complete
                pbar.update(1)  # Update progress bar
            pbar.close()

if __name__ == '__main__':
    main()