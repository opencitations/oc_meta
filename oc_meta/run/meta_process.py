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
from filelock import FileLock
from itertools import cycle
from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.file_manager import get_data, normalize_path, pathoo, suppress_stdout, init_cache, sort_files, zipit
from oc_meta.plugins.multiprocess.resp_agents_creator import RespAgentsCreator
from oc_meta.plugins.multiprocess.resp_agents_curator import RespAgentsCurator
from oc_meta.scripts.creator import Creator
from oc_meta.scripts.curator import Curator
from oc_ocdm import Storer
from oc_ocdm.prov import ProvSet
from pathlib import Path
from pebble import ProcessPool, ProcessFuture
from sys import platform
from time_agnostic_library.support import generate_config_file
from typing import List, Set, Tuple
import csv
import os
import yaml


class MetaProcess:
    def __init__(self, config:str):
        with open(config, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        self.config = config
        # Mandatory settings
        self.triplestore_url = settings['triplestore_url']
        self.input_csv_dir = normalize_path(settings['input_csv_dir'])
        self.base_output_dir = normalize_path(settings['base_output_dir'])
        self.resp_agent = settings['resp_agent']
        self.info_dir = os.path.join(self.base_output_dir, 'info_dir')
        self.output_csv_dir = os.path.join(self.base_output_dir, 'csv')
        self.distinct_output_dirs = True if settings['base_output_dir'] != settings['output_rdf_dir'] else False
        self.output_rdf_dir = normalize_path(settings['output_rdf_dir']) + os.sep + 'rdf' + os.sep
        self.indexes_dir = os.path.join(self.base_output_dir, 'indexes')
        self.cache_path = os.path.join(self.base_output_dir, 'cache.txt')
        self.errors_path = os.path.join(self.base_output_dir, 'errors.txt')
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

    def curate_and_create(self, filename:str, cache_path:str, errors_path:str, worker_number:int=None, resp_agents_only:bool=False) -> Tuple[dict, str, str, str]:
        if os.path.exists(os.path.join(self.base_output_dir, '.stop')):
            return {'message': 'skip'}, cache_path, errors_path, filename
        try:
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
            name = f"{filename.replace('.csv', '')}_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}"
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
            return {'message': 'success'}, cache_path, errors_path, filename
        except Exception as e:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(e).__name__, e.args)
            return {'message': message}, cache_path, errors_path, filename
    
    def store_data_and_prov(self, res_storer:Storer, prov_storer:Storer, filename:str) -> None:
        if self.rdf_output_in_chunks:
            filename_without_csv = filename[:-4]
            f = os.path.join(self.output_rdf_dir, 'data', filename_without_csv + '.json')
            res_storer.store_graphs_in_file(f, self.context_path)
            res_storer.upload_all(self.triplestore_url, self.output_rdf_dir, batch_size=100)
            f_prov = os.path.join(self.output_rdf_dir, 'prov', filename_without_csv + '.json')
            prov_storer.store_graphs_in_file(f_prov, self.context_path)
        else:
            res_storer.store_all(base_dir=self.output_rdf_dir, base_iri=self.base_iri, context_path=self.context_path)
            prov_storer.store_all(self.output_rdf_dir, self.base_iri, self.context_path)
            res_storer.upload_all(triplestore_url=self.triplestore_url, base_dir=self.output_rdf_dir, batch_size=100)
    
    def save_data(self):
        output_dirname = f"meta_output_{datetime.now().strftime('%Y-%m-%dT%H_%M_%S_%f')}.zip"
        dirs_to_zip = [self.base_output_dir, self.output_rdf_dir] if self.distinct_output_dirs else [self.base_output_dir]
        zipit(dirs_to_zip, output_dirname)

def run_meta_process(meta_process:MetaProcess, resp_agents_only:bool=False) -> None:
    delete_lock_files(base_dir=meta_process.base_output_dir)
    files_to_be_processed = meta_process.prepare_folders()
    max_workers = meta_process.workers_number
    if max_workers == 0:
        workers = list(range(1, os.cpu_count()))
    elif max_workers == 1:
        workers = [None]
    else:
        multiples_of_ten = {i for i in range(1, max_workers+1) if int(i) % 10 == 0}
        workers = [i for i in range(1, max_workers+len(multiples_of_ten)+1) if i not in multiples_of_ten]
    is_unix = platform in {'linux', 'linux2', 'darwin'}
    generate_gentle_buttons(meta_process.base_output_dir, meta_process.config, is_unix)
    files_chunks = chunks(list(files_to_be_processed), 3000) if is_unix else [files_to_be_processed]
    for files_chunk in files_chunks:
        with ProcessPool(max_workers=max_workers, max_tasks=1) as executor:
            for file_to_be_processed, worker_number in zip(files_chunk, cycle(workers)):
                future:ProcessFuture = executor.schedule(
                    function=meta_process.curate_and_create, 
                    args=(file_to_be_processed, meta_process.cache_path, meta_process.errors_path, worker_number, resp_agents_only)) 
                future.add_done_callback(task_done) 
        delete_lock_files(base_dir=meta_process.base_output_dir)
        if is_unix and not os.path.exists(os.path.join(meta_process.base_output_dir, '.stop')):
            meta_process.save_data()
    if os.path.exists(meta_process.cache_path) and not os.path.exists(os.path.join(meta_process.base_output_dir, '.stop')):
        os.rename(meta_process.cache_path, meta_process.cache_path.replace('.txt', f'_{datetime.now().strftime("%Y-%m-%dT%H_%M_%S_%f")}.txt'))
    if not is_unix:
        delete_lock_files(base_dir=meta_process.base_output_dir)

def task_done(task_output:ProcessFuture) -> None:
    message, cache_path, errors_path, filename = task_output.result()
    if message['message'] == 'skip':
        pass
    elif message['message'] == 'success':
        if not os.path.exists(cache_path):
            with open(cache_path, 'w', encoding='utf-8') as aux_file:
                aux_file.write(filename + '\n')
        else:
            with open(cache_path, 'r', encoding='utf-8') as aux_file:
                cache_data = aux_file.read().splitlines()
                cache_data.append(filename)
                data_sorted = sorted(cache_data, key=lambda filename: int(filename.replace('.csv', '')), reverse=False)
            with open(cache_path, 'w', encoding='utf-8') as aux_file:
                aux_file.write('\n'.join(data_sorted))
    else:
        with open(errors_path, 'a', encoding='utf-8') as aux_file:
            aux_file.write(f'{filename}: {message["message"]}' + '\n')

def chunks(lst:list, n:int) -> List[list]:
    '''Yield successive n-sized chunks from lst.'''
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def delete_lock_files(base_dir:list) -> None:
    for dirpath, _, filenames in os.walk(base_dir):
        for filename in filenames:
            if filename.endswith('.lock'):
                os.remove(os.path.join(dirpath, filename))

def generate_gentle_buttons(dir:str, config:str, is_unix:bool):
    if os.path.exists(os.path.join(dir, '.stop')):
        os.remove(os.path.join(dir, '.stop'))
    ext = 'sh' if is_unix else 'bat'
    with open (f'gently_run.{ext}', 'w') as rsh:
        rsh.write(f'python -m oc_meta.lib.stopper -t "{dir}" --remove\npython -m oc_meta.run.meta_process -c {config}')
    with open (f'gently_stop.{ext}', 'w') as rsh:
        rsh.write(f'python -m oc_meta.lib.stopper -t "{dir}" --add')


if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('meta_process.py', description='This script runs the OCMeta data processing workflow')
    arg_parser.add_argument('-c', '--config', dest='config', required=True, help='Configuration file directory')
    args = arg_parser.parse_args()
    meta_process = MetaProcess(config=args.config)
    run_meta_process(meta_process=meta_process)