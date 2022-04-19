#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019 Silvio Peroni <essepuntato@gmail.com>
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021 Simone Persiani <iosonopersia@gmail.com>
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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


from argparse import ArgumentParser
from datetime import datetime
from itertools import cycle
from meta.lib.csvmanager import CSVManager
from meta.lib.file_manager import get_data, normalize_path, pathoo, suppress_stdout, init_cache, sort_files
from meta.plugins.multiprocess.resp_agents_creator import RespAgentsCreator
from meta.plugins.multiprocess.resp_agents_curator import RespAgentsCurator
from meta.scripts.creator import Creator
from meta.scripts.curator import Curator
from multiprocessing import Pool
from oc_ocdm import Storer
from oc_ocdm.prov import ProvSet
from time_agnostic_library.support import generate_config_file
from tqdm import tqdm
from typing import Set, Tuple
import csv
import multiprocessing
import os
import yaml


class MetaProcess:
    def __init__(self, config:str):
        with open(config, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        # Mandatory settings
        self.triplestore_url = settings['triplestore_url']
        self.input_csv_dir = normalize_path(settings['input_csv_dir'])
        self.base_output_dir = normalize_path(settings['base_output_dir'])
        self.resp_agent = settings['resp_agent']
        self.info_dir = os.path.join(self.base_output_dir, 'info_dir')
        self.output_csv_dir = os.path.join(self.base_output_dir, 'csv')
        self.output_rdf_dir = os.path.join(self.base_output_dir, f'rdf{os.sep}')
        self.indexes_dir = os.path.join(self.base_output_dir, 'indexes')
        self.cache_path = os.path.join(self.base_output_dir, 'cache.txt')
        # Optional settings
        self.base_iri = settings['base_iri']
        self.context_path = settings['context_path']
        self.dir_split_number = settings['dir_split_number']
        self.items_per_file = settings['items_per_file']
        self.default_dir = settings['default_dir']
        self.rdf_output_in_chunks = settings['rdf_output_in_chunks']
        self.source = settings['source']
        self.valid_dois_cache = CSVManager() if bool(settings['use_doi_api_service']) == True else None
        self.workers_number = int(settings['workers_number'])
        supplier_prefix:str = settings['supplier_prefix']
        self.supplier_prefix = supplier_prefix[:-1] if supplier_prefix.endswith('0') else supplier_prefix
        self.verbose = settings['verbose']
        # Time-Agnostic_library integration
        self.time_agnostic_library_config = os.path.join(os.path.dirname(config), 'time_agnostic_library_config.json')
        if not os.path.exists(self.time_agnostic_library_config):
            generate_config_file(config_path=self.time_agnostic_library_config, dataset_urls=[self.triplestore_url], dataset_dirs=list(),
                provenance_urls=settings['provenance_endpoints'], provenance_dirs=list(), 
                blazegraph_full_text_search=settings['blazegraph_full_text_search'], graphdb_connector_name=settings['graphdb_connector_name'], 
                cache_endpoint=settings['cache_endpoint'], cache_update_endpoint=settings['cache_update_endpoint'])

    def prepare_folders(self) -> Set[str]:
        completed = init_cache(self.cache_path)
        files_in_input_csv_dir = {filename for filename in os.listdir(self.input_csv_dir) if filename.endswith('.csv')}
        files_to_be_processed = sort_files(files_in_input_csv_dir.difference(completed))
        for dir in [self.output_csv_dir, self.indexes_dir, self.output_rdf_dir]:
            pathoo(dir)
        if self.rdf_output_in_chunks:
            data_dir = os.path.join(self.output_rdf_dir, 'data' + os.sep)
            prov_dir = os.path.join(self.output_rdf_dir, 'prov' + os.sep)
            for dir in [prov_dir, data_dir]:
                pathoo(dir)
        csv.field_size_limit(128)
        return files_to_be_processed

    def curate_and_create(self, filename:str, worker_number:int=None, resp_agents_only:bool=False) -> Tuple[Storer, Storer, str]:
        filepath = os.path.join(self.input_csv_dir, filename)
        data = get_data(filepath)
        supplier_prefix = f'{self.supplier_prefix}0' if worker_number is None else f'{self.supplier_prefix}{str(worker_number)}0'
        # Curator
        self.info_dir = os.path.join(self.info_dir, supplier_prefix) if worker_number else self.info_dir
        curator_info_dir = os.path.join(self.info_dir, 'curator' + os.sep)
        if resp_agents_only:
            curator_obj = RespAgentsCurator(data=data, ts=self.triplestore_url, prov_config=self.time_agnostic_library_config, info_dir=curator_info_dir, base_iri=self.base_iri, prefix=supplier_prefix)
        else:
            curator_obj = Curator(data=data, ts=self.triplestore_url, prov_config=self.time_agnostic_library_config, info_dir=curator_info_dir, base_iri=self.base_iri, prefix=supplier_prefix, valid_dois_cache=self.valid_dois_cache)
        name = f"{datetime.now().strftime('%Y-%m-%dT%H_%M_%S_%f')}_{supplier_prefix}"
        curator_obj.curator(filename=name, path_csv=self.output_csv_dir, path_index=self.indexes_dir)
        # Creator
        creator_info_dir = os.path.join(self.info_dir, 'creator' + os.sep)
        if resp_agents_only:
            creator_obj = RespAgentsCreator(
                data=curator_obj.data, endpoint=self.triplestore_url, base_iri=self.base_iri, info_dir=creator_info_dir, supplier_prefix=supplier_prefix, resp_agent=self.resp_agent, ra_index=curator_obj.index_id_ra, preexisting_entities=curator_obj.preexisting_entities)
        else:
            creator_obj = Creator(
                data=curator_obj.data, endpoint=self.triplestore_url, base_iri=self.base_iri, info_dir=creator_info_dir, supplier_prefix=supplier_prefix, resp_agent=self.resp_agent, ra_index=curator_obj.index_id_ra, 
                br_index=curator_obj.index_id_br, re_index_csv=curator_obj.re_index, ar_index_csv=curator_obj.ar_index, vi_index=curator_obj.VolIss, preexisting_entities=curator_obj.preexisting_entities)
        creator = creator_obj.creator(source=self.source)
        # Provenance
        prov = ProvSet(creator, self.base_iri, creator_info_dir, wanted_label=False)
        prov.generate_provenance()
        # Storer
        res_storer = Storer(creator, context_map={}, dir_split=self.dir_split_number, n_file_item=self.items_per_file, default_dir=self.default_dir, output_format='json-ld')
        prov_storer = Storer(prov, context_map={}, dir_split=self.dir_split_number, n_file_item=self.items_per_file, output_format='json-ld')
        with suppress_stdout():
            self.store_data_and_prov(res_storer, prov_storer, filename)
        return filename
    
    def store_data_and_prov(self, res_storer:Storer, prov_storer:Storer, filename:str) -> None:
        if self.rdf_output_in_chunks:
            filename_without_csv = filename[:-4]
            f = os.path.join(self.output_rdf_dir, 'data', filename_without_csv + '.json')
            lock.acquire()
            res_storer.store_graphs_in_file(f, self.context_path)
            lock.release()
            res_storer.upload_all(self.triplestore_url, self.output_rdf_dir, batch_size=100)
            f_prov = os.path.join(self.output_rdf_dir, 'prov', filename_without_csv + '.json')
            prov_storer.store_graphs_in_file(f_prov, self.context_path)
        else:
            lock.acquire()
            res_storer.store_all(base_dir=self.output_rdf_dir, base_iri=self.base_iri, context_path=self.context_path)
            prov_storer.store_all(self.output_rdf_dir, self.base_iri, self.context_path)
            lock.release()
            res_storer.upload_all(triplestore_url=self.triplestore_url, base_dir=self.output_rdf_dir, batch_size=100)

def run_meta_process(meta_process:MetaProcess, resp_agents_only:bool=False) -> None:
    files_to_be_processed = meta_process.prepare_folders()
    pbar = tqdm(total=len(files_to_be_processed)) if meta_process.verbose else None
    max_workers = meta_process.workers_number
    if max_workers == 0:
        workers = list(range(1, os.cpu_count()))
    elif max_workers == 1:
        workers = [None]
    else:
        multiples_of_ten = {i for i in range(1, max_workers+1) if int(i) % 10 == 0}
        workers = [i for i in range(1, max_workers+len(multiples_of_ten)+1) if i not in multiples_of_ten]
    l = multiprocessing.Lock()
    with Pool(processes=max_workers, initializer=init_lock, initargs=(l,), maxtasksperchild=1) as executor:
        futures = [executor.apply_async(func=meta_process.curate_and_create, args=(file_to_be_processed, worker_number, resp_agents_only)) for file_to_be_processed, worker_number in zip(files_to_be_processed, cycle(workers))]
        for future in futures:
            processed_file = future.get()
            with open(meta_process.cache_path, 'a', encoding='utf-8') as aux_file:
                aux_file.write(processed_file + '\n')
            pbar.update() if pbar else None
    os.remove(meta_process.cache_path)
    pbar.close() if pbar else None

def init_lock(l):
    global lock
    lock = l

if __name__ == '__main__':
    arg_parser = ArgumentParser('meta_process.py', description='This script runs the OCMeta data processing workflow')
    arg_parser.add_argument('-c', '--config', dest='config', required=True, help='Configuration file directory')
    args = arg_parser.parse_args()
    meta_process = MetaProcess(config=args.config)
    run_meta_process(meta_process=meta_process)