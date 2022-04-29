import csv
import io
import os
from tqdm import tqdm
from zipfile import ZipFile


def get_dois_from_coci(coci_dir:str, output_file_path:str, verbose:bool=False) -> None:
    dois_found = set()
    if os.path.exists(output_file_path):
        if verbose:
            print(f'[INFO: coci_process] Looking for previously stored DOIs')
        with open(output_file_path, 'r', encoding='utf8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                dois_found.add(row['doi'])
        if verbose:
            print(f'[INFO: coci_process] {len(dois_found)} DOIs found')
    if verbose:
        pbar = tqdm(total=get_files_count(coci_dir))
    for fold, _, files in os.walk(coci_dir):
        for file in files:
            if file.endswith('.csv'):
                output_csv = list()
                data = csv.DictReader(open(os.path.join(fold, file), 'r', encoding='utf8'))
                process_data(data, dois_found, output_csv)
                store_data(output_file_path, output_csv)
                if verbose:
                    pbar.update()
            elif file.endswith('.zip'):
                with ZipFile(os.path.join(coci_dir, file), 'r') as archive:
                    for name in archive.namelist():
                        output_csv = list()
                        with archive.open(name) as infile:
                            data = csv.DictReader(io.TextIOWrapper(infile, 'utf-8'))
                            process_data(data, dois_found, output_csv)
                            store_data(output_file_path, output_csv)
                            if verbose:
                                pbar.update()
    if verbose:
        pbar.close()
        print(f'[INFO: COCI_PROCESS] {len(dois_found)} DOIs stored')

def process_data(data:csv.DictReader, dois_found:set, output_csv:list) -> None:
    for row in data:
        citing = row['citing']
        cited = row['cited']
        if citing not in dois_found:
            output_csv.append({'doi':row['citing']})
            dois_found.add(citing)
        if cited not in dois_found:
            output_csv.append({'doi':row['cited']})
            dois_found.add(cited)

def store_data(output_file_path:str, output_csv:str):
    with open(output_file_path, 'a', encoding='utf8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['doi'])
        writer.writeheader()
        writer.writerows(output_csv)

def get_files_count(coci_dir:str) -> int:
    if any(file.endswith('.csv') for  _, _, files in os.walk(coci_dir) for file in files):
        file_count = sum(len(files) for _, _, files in os.walk(coci_dir))
    elif any(file.endswith('.zip') for  _, _, files in os.walk(coci_dir) for file in files):
        file_count = 0
        for _, _, files in os.walk(coci_dir):
            for file in files:
                with ZipFile(os.path.join(coci_dir, file), 'r') as archive:
                    file_count += len(archive.namelist())
    return file_count

