from oc_meta.lib.file_manager import write_csv, pathoo
from tqdm import tqdm
from zipfile import ZipFile
import csv
import io
import os


def get_ids_from_citations(citations_dir:str, output_dir:str, threshold:int=10000, verbose:bool=False) -> None:
    '''
    This script extracts the identifiers of the citing and cited documents from citation data organized in the CSV format accepted by OpenCitations.

    :params citations_dir: the directory containing the citations files, either in CSV or ZIP format
    :type citations_dir: str
    :params output_dir: directory of the output CSV files
    :type output_dir: str
    :params verbose: show a loading bar, elapsed time and estimated time
    :type verbose: bool
    :returns: None
    '''
    threshold = 10000 if threshold is None else int(threshold)
    if not any(file.endswith('.csv') or file.endswith('.zip') for  _, _, files in os.walk(citations_dir) for file in files):
        raise RuntimeError('I did not find CSV or ZIP files in the given directory')
    ids_found = set()
    if os.path.isdir(output_dir):
        if verbose:
            print(f'[INFO: get_ids_from_citations_data] Looking for previously stored IDs')
        for filename in os.listdir(output_dir):
            with open(os.path.join(output_dir, filename), 'r', encoding='utf8', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ids_found.add(row['id'])
        if verbose:
            print(f'[INFO: get_ids_from_citations] {len(ids_found)} IDs found')
    else:
        pathoo(output_dir)
    if verbose:
        pbar = tqdm(total=get_files_count(citations_dir))
    file_counter = 1
    output_csv = list()
    len_ids_found = len(ids_found)
    for fold, _, files in os.walk(citations_dir):
        for file in files:
            cur_file = file_counter + len_ids_found
            if file.endswith('.csv'):
                data = csv.DictReader(open(os.path.join(fold, file), 'r', encoding='utf8'))
                process_data(data, ids_found, output_csv)
                if file_counter % threshold == 0:
                    write_csv(path=os.path.join(output_dir, f'{cur_file-threshold+1}-{cur_file}.csv'), datalist=output_csv)
                    output_csv = list()
                if verbose:
                    pbar.update()
                file_counter += 1
            elif file.endswith('.zip'):
                with ZipFile(os.path.join(citations_dir, file), 'r') as archive:
                    for name in archive.namelist():
                        cur_file = file_counter + len_ids_found
                        with archive.open(name) as infile:
                            data = csv.DictReader(io.TextIOWrapper(infile, 'utf-8'))
                            process_data(data, ids_found, output_csv)
                            if file_counter % threshold == 0:
                                write_csv(path=os.path.join(output_dir, f'{cur_file-threshold+1}-{cur_file}.csv'), datalist=output_csv)
                                output_csv = list()
                            if verbose:
                                pbar.update()
                            file_counter += 1
    if output_csv:
        write_csv(path=os.path.join(output_dir, f'{cur_file + 1 - (cur_file % threshold)}-{cur_file}.csv'), datalist=output_csv)
    if verbose:
        pbar.close()
        print(f'[INFO: get_ids_from_citations] {len(ids_found)} IDs stored')

def process_data(data:csv.DictReader, ids_found:set, output_csv:list) -> None:
    for row in data:
        citing = row['citing']
        cited = row['cited']
        if citing not in ids_found:
            output_csv.append({'id':row['citing']})
            ids_found.add(citing)
        if cited not in ids_found:
            output_csv.append({'id':row['cited']})
            ids_found.add(cited)

def get_files_count(citations_dir:str) -> int:
    if any(file.endswith('.csv') for  _, _, files in os.walk(citations_dir) for file in files):
        file_count = sum(len(files) for _, _, files in os.walk(citations_dir))
    elif any(file.endswith('.zip') for  _, _, files in os.walk(citations_dir) for file in files):
        file_count = 0
        for _, _, files in os.walk(citations_dir):
            for file in files:
                with ZipFile(os.path.join(citations_dir, file), 'r') as archive:
                    file_count += len(archive.namelist())
    return file_count