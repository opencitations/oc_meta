#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import os
from argparse import ArgumentParser
from functools import partial
from multiprocessing import cpu_count
from sys import platform
from typing import List, Tuple

from oc_idmanager import DOIManager
from pebble import ProcessFuture, ProcessPool
from tqdm import tqdm

from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.file_manager import call_api, get_csv_data, pathoo, write_csv
from oc_meta.lib.jsonmanager import get_all_files, load_json
from oc_meta.plugins.metadata_manager import MetadataManager
from oc_meta.run.meta_process import chunks


def extract_dois_from_dump(crossref_json_dir:str, output_dir:str, max_workers:int) -> None:
    all_files, targz_fd = get_all_files(crossref_json_dir, cache_filepath=None)
    all_files = [file for file in all_files 
        if not os.path.exists(
            os.path.join(output_dir, 'crossref',
                os.path.basename(file).replace('.json', '').replace('.gz', '') + '.csv'))
        and not os.path.exists(
            os.path.join(output_dir, 'reference',
                os.path.basename(file).replace('.json', '').replace('.gz', '') + '.csv'))
        ]
    doi_manager = DOIManager(data=dict(), use_api_service=False)
    print('[INFO] Extracting DOIs from the Crossref dump')
    pbar = tqdm(total=len(all_files))
    if platform.startswith('linux') or platform == 'darwin':
        os.sched_setaffinity(0, set(range(0, max_workers)))
    with ProcessPool(max_workers=max_workers) as executor:
        for filename in all_files:
            future:ProcessFuture = executor.schedule(
                function=get_dois, 
                args=(filename, targz_fd, doi_manager))
            future.add_done_callback(partial(task_done, output_dir, filename, pbar))
    pbar.close()

def get_dois(filepath:str, targz_fd, doi_manager:DOIManager) -> Tuple[List[dict], List[dict]]:
    source_data = load_json(filepath, targz_fd)
    file_dois = list()
    dois_in_references = list()
    if source_data:
        for item in source_data['items']:
            citing = doi_manager.normalise(item.get('DOI'))
            if citing is not None:
                file_dois.append({'id': citing})
                if 'reference' in item:
                    for reference in item['reference']:
                        cited = doi_manager.normalise(reference.get('DOI'))
                        if cited is not None:
                            dois_in_references.append({'id': cited})
    return file_dois, dois_in_references

def task_done(output_dir:str, filename:str, pbar:tqdm, task_output:ProcessFuture) -> None:
    filename = os.path.basename(filename).replace('.json', '').replace('.gz', '') + '.csv'
    crossref, ref_dois = task_output.result()
    crossref_dir = os.path.join(output_dir, 'crossref')
    ref_dir = os.path.join(output_dir, 'reference')
    write_csv(path=os.path.join(crossref_dir, filename), datalist=crossref)
    write_csv(path=os.path.join(ref_dir, filename), datalist=ref_dois)
    pbar.update()

def generate_set_of_crossref_dois(crossref_dois_dir:str) -> set:
    print('[INFO] Storing Crossref DOIs in memory')
    files = os.listdir(crossref_dois_dir)
    pbar = tqdm(total=len(files))
    crossref_dois = set()
    for file in files:
        crossref_dois.update(CSVManager.load_csv_column_as_set(os.path.join(crossref_dois_dir, file), 'id'))
        pbar.update()
    pbar.close()
    return crossref_dois

def get_ref_dois_not_in_crossref(crossref_dois:set, ref_dir:str) -> set:
    print('[INFO] Getting the set of references DOIs not in Crossref')
    files = os.listdir(ref_dir)
    pbar = tqdm(total=len(files))
    ref_not_in_crossref = set()
    for i, file in enumerate(files):
        ref_not_in_crossref.update(CSVManager.load_csv_column_as_set(os.path.join(ref_dir, file), 'id'))
        if i % 1000 == 0:
            ref_not_in_crossref = ref_not_in_crossref.difference(crossref_dois)
        pbar.update()
    pbar.close()
    ref_not_in_crossref = ref_not_in_crossref.difference(crossref_dois)
    return ref_not_in_crossref

def store_dois_not_in_crossref(ref_not_in_crossref:set, output_dir:str) -> None:
    output_dir = os.path.join(output_dir, 'dois_not_in_crossref')
    counter = 1
    threshold = 100000
    for chunk in chunks(list(ref_not_in_crossref), threshold):
        path = os.path.join(output_dir, f'{counter}-{counter+len(chunk)-1}.csv')
        datalist = [{'id': doi} for doi in chunk]
        write_csv(path, datalist)
        counter += len(chunk)

def extract_metadata(output_dir:str, orcid_doi_filepath:str):
    dois_not_in_crossref_dir = os.path.join(output_dir, 'dois_not_in_crossref')
    base_output_dir = os.path.join(dois_not_in_crossref_dir, 'metadata_extracted')
    processed_dois = {
        row['id'] for dirpath, _, filenames in os.walk(base_output_dir) 
            for filename in filenames 
                for row in get_csv_data(os.path.join(dirpath, filename))}
    print(len(processed_dois))
    for filename in os.listdir(dois_not_in_crossref_dir):
        dois = CSVManager.load_csv_column_as_set(os.path.join(dois_not_in_crossref_dir, filename), 'id').difference(processed_dois)
        for doi in dois:
            doi_manager = DOIManager()
            api_response = call_api(url=f'{doi_manager._api_unknown}{doi}', headers=doi_manager._headers)
            metadata_manager = MetadataManager(metadata_provider = "unknown", api_response = api_response, orcid_doi_filepath = orcid_doi_filepath)
            metadata: dict = metadata_manager.extract_metadata()
            if not metadata.get('id'):
                metadata['id'] = doi
            registration_agency = metadata['ra']
            metadata.pop('valid', None); metadata.pop('ra', None)
            locals()[f'{registration_agency}_counter'] = 0
            ra_output_dir = os.path.join(base_output_dir, registration_agency)
            pathoo(ra_output_dir)
            output_path = os.path.join(ra_output_dir, f"{locals()[f'{registration_agency}_counter']}.csv")
            if os.path.exists(output_path):
                if len(get_csv_data(output_path)) == 10000:
                    locals()[f'{registration_agency}_counter'] += 1
            output_path = os.path.join(ra_output_dir, f"{locals()[f'{registration_agency}_counter']}.csv")
            write_csv(output_path, [metadata], method='a')

if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('meta_process.py', description='This script runs the OCMeta data processing workflow')
    arg_parser.add_argument('-c', '--crossref_json_dir', dest='crossref_json_dir', required=True, help='Crossref json files directory')
    arg_parser.add_argument('-o', '--output', dest='output_dir', required=True, help='Directory of the output CSV files to store Crossref and citations DOIS and lower memory requirements')
    arg_parser.add_argument('-or', '--orcid', dest='orcid_doi_filepath', required=False, help='DOI-ORCID index filepath, to enrich data')
    arg_parser.add_argument('-m', '--max_workers', dest='max_workers', required=False, default=cpu_count(), type=int, help='Max workers')
    arg_parser.add_argument('-w', '--wanted', dest='wanted_dois_filepath', required=False, default=None, help='A CSV filepath containing what DOI to process, not mandatory')
    args = arg_parser.parse_args()
    if not os.path.exists(os.path.join(args.output_dir, 'dois_not_in_crossref')):
        extract_dois_from_dump(args.crossref_json_dir, args.output_dir, args.max_workers)
        crossref_dois = generate_set_of_crossref_dois(os.path.join(args.output_dir, 'crossref'))
        ref_not_in_crossref = get_ref_dois_not_in_crossref(crossref_dois, os.path.join(args.output_dir, 'reference'))
        wanted_dois = CSVManager.load_csv_column_as_set(args.wanted_dois_filepath, 'id') if args.wanted_dois_filepath else None
        if wanted_dois:
            ref_not_in_crossref = ref_not_in_crossref.intersection(wanted_dois)
        store_dois_not_in_crossref(ref_not_in_crossref, args.output_dir)
    extract_metadata(args.output_dir, args.orcid_doi_filepath)