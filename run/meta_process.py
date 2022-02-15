from oc_ocdm import Storer
from oc_ocdm.prov import ProvSet

from meta.lib.file_manager import get_data, normalize_path, pathoo
from meta.scripts.creator import Creator
from meta.scripts.curator import Curator
from meta.lib.csvmanager import CSVManager

from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import repeat
from datetime import datetime
from argparse import ArgumentParser
from math import ceil
import yaml
import os
import csv
from tqdm import tqdm
from typing import Set, Union

class MetaProcess:
    def __init__(self, config:str):
        with open(config, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        self.triplestore_url = settings['triplestore_url']
        self.info_dir = normalize_path(settings['info_dir'])
        self.resp_agent = settings['resp_agent']
        self.input_csv_dir = normalize_path(settings['input_csv_dir'])
        self.output_csv_dir = normalize_path(settings['output_csv_dir'])
        output_rdf_dir = normalize_path(settings['output_rdf_dir'])
        self.output_rdf_dir = f'{output_rdf_dir}/' if output_rdf_dir[-1] != '/' else output_rdf_dir
        self.indexes_dir = normalize_path(settings['indexes_dir'])
        self.cache_path = normalize_path(settings['cache_path'])

        self.base_iri = settings['base_iri']
        self.context_path = settings['context_path']
        self.dir_split_number = settings['dir_split_number']
        self.items_per_file = settings['items_per_file']
        self.default_dir = normalize_path(settings['default_dir'])
        self.rdf_output_in_chunks = settings['rdf_output_in_chunks']
        self.supplier_prefix = settings['supplier_prefix']
        self.source = settings['source']
        self.valid_dois_cache = CSVManager() if bool(settings['supplier_prefix']) == True else None
        self.run_in_multiprocess = bool(settings['run_in_multiprocess'])
        self.verbose = settings['verbose']

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
            prov_dir = os.path.join(self.output_rdf_dir, 'prov' + os.sep)
            nt_dir = os.path.join(self.output_rdf_dir, 'nt' + os.sep)
            for dir in [prov_dir, nt_dir]:
                pathoo(dir)
        csv.field_size_limit(128)
        return files_to_be_processed

    def run_meta_process(self, filename:str, worker_number:int=None) -> None:
        filepath = os.path.join(self.input_csv_dir, filename)
        data = get_data(filepath)
        if worker_number:
            worker_number = worker_number + 1 if worker_number % 10 == 0 else worker_number
        supplier_prefix = self.supplier_prefix if worker_number is None else f'{self.supplier_prefix}{str(worker_number)}0'
        # Curator
        info_dir = os.path.join(self.info_dir, supplier_prefix) if worker_number else self.info_dir
        curator_info_dir = os.path.join(info_dir, 'curator' + os.sep)
        curator_obj = Curator(data=data, ts=self.triplestore_url, info_dir=curator_info_dir, base_iri=self.base_iri, prefix=supplier_prefix, valid_dois_cache=self.valid_dois_cache)
        name = datetime.now().strftime('%Y-%m-%dT%H_%M_%S') + supplier_prefix
        curator_obj.curator(filename=name, path_csv=self.output_csv_dir, path_index=self.indexes_dir)
        # Creator
        creator_info_dir = os.path.join(info_dir, 'creator' + os.sep)
        creator_obj = Creator(
            data=curator_obj.data, base_iri=self.base_iri, info_dir=creator_info_dir, supplier_prefix=supplier_prefix, resp_agent=self.resp_agent, ra_index=curator_obj.index_id_ra, 
            br_index=curator_obj.index_id_br, re_index_csv=curator_obj.re_index, ar_index_csv=curator_obj.ar_index, vi_index=curator_obj.VolIss)
        creator = creator_obj.creator(source=self.source)
        # Provenance
        prov = ProvSet(creator, self.base_iri, creator_info_dir, wanted_label=False)
        prov.generate_provenance()
        # Storer
        res_storer = Storer(creator, context_map={}, dir_split=self.dir_split_number, n_file_item=self.items_per_file, default_dir=self.default_dir, output_format='nt11')
        prov_storer = Storer(prov, context_map={}, dir_split=self.dir_split_number, n_file_item=self.items_per_file, output_format='nquads')
        if self.rdf_output_in_chunks:
            filename_without_csv = filename[:-4]
            f = os.path.join(self.output_rdf_dir, 'nt', filename_without_csv + '.nt')
            res_storer.store_graphs_in_file(f, self.context_path)
            res_storer.upload_all(self.triplestore_url, self.output_rdf_dir, batch_size=100)
            # Provenance
            f_prov = os.path.join(self.output_rdf_dir, 'prov', filename_without_csv + '.nq')
            prov_storer.store_graphs_in_file(f_prov, self.context_path)
        else:
            res_storer.upload_and_store(self.output_rdf_dir, self.triplestore_url, self.base_iri, self.context_path, batch_size=100)
            prov_storer.store_all(self.output_rdf_dir, self.base_iri, self.context_path)
        with open(self.cache_path, 'a', encoding='utf-8') as aux_file:
            aux_file.write(filename + '\n')


if __name__ == '__main__':
    arg_parser = ArgumentParser('meta_process.py', description='This script runs the OCMeta data processing workflow')
    arg_parser.add_argument('-c', '--config', dest='config', required=True, help='Configuration file directory')
    args = arg_parser.parse_args()
    meta_process = MetaProcess(config=args.config)
    # Start the Meta process
    files_to_be_processed = meta_process.prepare_folders()
    max_workers = os.cpu_count()
    supplier_prefix = meta_process.supplier_prefix
    pbar = tqdm(total=len(files_to_be_processed)) if meta_process.verbose else None
    if meta_process.run_in_multiprocess:
        counter = len(files_to_be_processed)
        workers = list()
        while counter:
            multiples_of_ten = {i for i in range(max_workers) if i % 10 == 0}
            valid_workers = [i for i in range(max_workers+len(multiples_of_ten)) if i not in multiples_of_ten]
            workers.extend(valid_workers)
            counter -=1
        with ProcessPoolExecutor() as executor:
            results = [executor.submit(meta_process.run_meta_process, filename, worker_number) for filename, worker_number in zip(files_to_be_processed, workers)]
            for f in as_completed(results):
                pbar.update() if pbar else None
                print(f.result())
    else:
        for filename in files_to_be_processed:
            meta_process.run_meta_process(filename)
            pbar.update() if pbar else None
    pbar.close() if pbar else None