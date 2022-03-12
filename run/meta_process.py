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
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from meta.lib.csvmanager import CSVManager
from meta.lib.file_manager import get_data, normalize_path, pathoo, suppress_stdout
from meta.plugins.multiprocess.resp_agents_creator import RespAgentsCreator
from meta.plugins.multiprocess.resp_agents_curator import RespAgentsCurator
from meta.scripts.creator import Creator
from meta.scripts.curator import Curator
from oc_ocdm import Storer
from oc_ocdm.prov import ProvSet
from time_agnostic_library.support import generate_config_file
from tqdm import tqdm
from typing import Set, Tuple
import csv
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
                provenance_urls=settings['provenance_endpoints'], provenance_dirs=settings['provenance_dirs'], 
                blazegraph_full_text_search=settings['blazegraph_full_text_search'], cache_triplestore_url=settings['cache_triplestore_url'])

    def prepare_folders(self) -> Set[str]:
        if not os.path.exists(self.cache_path):
            pathoo(self.cache_path)
            completed = set()
        else:
            with open(self.cache_path, 'r', encoding='utf-8') as cache_file:
                completed = {line.rstrip('\n') for line in cache_file}
        files_in_input_csv_dir = {filename for filename in os.listdir(self.input_csv_dir) if filename.endswith('.csv')}
        files_to_be_processed = files_in_input_csv_dir.difference(completed)
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
        supplier_prefix = self.supplier_prefix if worker_number is None else f'{self.supplier_prefix}{str(worker_number)}0'
        # Curator
        self.info_dir = os.path.join(self.info_dir, supplier_prefix) if worker_number else self.info_dir
        curator_info_dir = os.path.join(self.info_dir, 'curator' + os.sep)
        if resp_agents_only:
            curator_obj = RespAgentsCurator(data=data, ts=self.triplestore_url, prov_config=self.time_agnostic_library_config, info_dir=curator_info_dir, base_iri=self.base_iri, prefix=supplier_prefix)
        else:
            curator_obj = Curator(data=data, ts=self.triplestore_url, prov_config=self.time_agnostic_library_config, info_dir=curator_info_dir, base_iri=self.base_iri, prefix=supplier_prefix, valid_dois_cache=self.valid_dois_cache)
        name = f"{datetime.now().strftime('%Y-%m-%dT%H_%M_%S')}_{supplier_prefix}"
        curator_obj.curator(filename=name, path_csv=self.output_csv_dir, path_index=self.indexes_dir)
        # Creator
        creator_info_dir = os.path.join(self.info_dir, 'creator' + os.sep)
        if resp_agents_only:
            creator_obj = RespAgentsCreator(
                data=curator_obj.data, base_iri=self.base_iri, info_dir=creator_info_dir, supplier_prefix=supplier_prefix, resp_agent=self.resp_agent, ra_index=curator_obj.index_id_ra)
        else:
            creator_obj = Creator(
                data=curator_obj.data, base_iri=self.base_iri, info_dir=creator_info_dir, supplier_prefix=supplier_prefix, resp_agent=self.resp_agent, ra_index=curator_obj.index_id_ra, 
                br_index=curator_obj.index_id_br, re_index_csv=curator_obj.re_index, ar_index_csv=curator_obj.ar_index, vi_index=curator_obj.VolIss)
        creator = creator_obj.creator(source=self.source)
        # Provenance
        prov = ProvSet(creator, self.base_iri, creator_info_dir, wanted_label=False)
        prov.generate_provenance()
        # Storer
        res_storer = Storer(creator, context_map={}, dir_split=self.dir_split_number, n_file_item=self.items_per_file, default_dir=self.default_dir, output_format='json-ld')
        prov_storer = Storer(prov, context_map={}, dir_split=self.dir_split_number, n_file_item=self.items_per_file, output_format='json-ld')
        return res_storer, prov_storer, filename
    
    def store_data_and_prov(self, res_storer:Storer, prov_storer:Storer, filename:str) -> None:
        if self.rdf_output_in_chunks:
            filename_without_csv = filename[:-4]
            f = os.path.join(self.output_rdf_dir, 'data', filename_without_csv + '.json')
            res_storer.store_graphs_in_file(f, self.context_path)
            res_storer.upload_all(self.triplestore_url, self.output_rdf_dir, batch_size=100)
            f_prov = os.path.join(self.output_rdf_dir, 'prov', filename_without_csv + '.json')
            prov_storer.store_graphs_in_file(f_prov, self.context_path)
        else:
            res_storer.upload_and_store(self.output_rdf_dir, self.triplestore_url, self.base_iri, self.context_path, batch_size=100)
            prov_storer.store_all(self.output_rdf_dir, self.base_iri, self.context_path)
    
def run_meta_process(meta_process:MetaProcess, resp_agents_only:bool=False) -> None:
    files_to_be_processed = meta_process.prepare_folders()
    pbar = tqdm(total=len(files_to_be_processed)) if meta_process.verbose else None
    max_workers = meta_process.workers_number
    multiples_of_ten = {i for i in range(max_workers) if i % 10 == 0}
    workers = [i for i in range(max_workers+len(multiples_of_ten)) if i not in multiples_of_ten]
    while len(files_to_be_processed) > 0:
        with ProcessPoolExecutor(max_workers = max_workers) as executor:
            results = [executor.submit(meta_process.curate_and_create, filename, worker_number, resp_agents_only) for filename, worker_number in zip(files_to_be_processed, workers)]
            for f in as_completed(results):
                res_storer, prov_storer, processed_file = f.result()
                with suppress_stdout():
                    meta_process.store_data_and_prov(res_storer=res_storer, prov_storer=prov_storer, filename=processed_file)
                files_to_be_processed.remove(processed_file)
                with open(meta_process.cache_path, 'a', encoding='utf-8') as aux_file:
                    aux_file.write(processed_file + '\n')
                pbar.update() if pbar else None
    os.remove(meta_process.cache_path)
    pbar.close() if pbar else None

if __name__ == '__main__':
    arg_parser = ArgumentParser('meta_process.py', description='This script runs the OCMeta data processing workflow')
    arg_parser.add_argument('-c', '--config', dest='config', required=True, help='Configuration file directory')
    args = arg_parser.parse_args()
    meta_process = MetaProcess(config=args.config)
    run_meta_process(meta_process=meta_process)