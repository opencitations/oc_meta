from oc_ocdm import Storer
from oc_ocdm.prov import ProvSet

from meta.lib.file_manager import *
from meta.scripts.creator import *
from meta.scripts.curator import *
from datetime import datetime
from argparse import ArgumentParser
import yaml
import os
import csv
from tqdm import tqdm


class MetaProcess:
    def __init__(self, config:str):
        with open(config, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        self.triplestore_url = settings['triplestore_url']
        self.info_dir = settings['info_dir']
        self.resp_agent = settings['resp_agent']
        self.input_csv_dir = settings['input_csv_dir']
        self.output_csv_dir = settings['output_csv_dir']
        self.base_dir = settings['output_rdf_dir']
        self.indexes_dir = settings['indexes_dir']
        self.cache_path = settings['cache_path']

        self.base_iri = settings['base_iri']
        self.context_path = settings['context_path']
        self.dir_split_number = settings['dir_split_number']
        self.items_per_file = settings['items_per_file']
        self.default_dir = settings['default_dir']
        self.rdf_output_in_chunks = settings['rdf_output_in_chunks']
        self.supplier_prefix = settings['supplier_prefix']
        self.source = settings['source']
        self.verbose = settings['verbose']

    def process(self) -> None:
        if not os.path.exists(self.cache_path):
            pathoo(self.cache_path)
            completed = set()
        else:
            with open(self.cache_path, 'r', encoding='utf-8') as cache_file:
                completed = {line.rstrip('\n') for line in cache_file}
        if self.verbose:
            pbar = tqdm(total=len(os.listdir(self.input_csv_dir)))
        prov_dir = os.path.join(self.base_dir, 'prov' + os.sep)
        nt_dir = os.path.join(self.base_dir, 'nt' + os.sep)
        for dir in [self.output_csv_dir, self.indexes_dir, self.base_dir, prov_dir, nt_dir]:
            pathoo(dir)
        csv.field_size_limit(128)
        for filename in os.listdir(self.input_csv_dir):
            if filename.endswith('.csv') and filename not in completed:
                filepath = os.path.join(self.input_csv_dir, filename)
                data = get_data(filepath)
                # Curator
                curator_info_dir = os.path.join(self.info_dir, 'curator' + os.sep)
                curator_obj = Curator(data, self.triplestore_url, info_dir=curator_info_dir, base_iri=self.base_iri, prefix=self.supplier_prefix)
                name = datetime.now().strftime('%Y-%m-%dT%H_%M_%S')
                curator_obj.curator(filename=name, path_csv=self.output_csv_dir, path_index=self.indexes_dir)
                # Creator
                creator_info_dir = os.path.join(self.info_dir, 'creator' + os.sep)
                creator_obj = Creator(
                    data=curator_obj.data, 
                    base_iri=self.base_iri, 
                    info_dir=creator_info_dir, 
                    supplier_prefix=self.supplier_prefix, 
                    resp_agent=self.resp_agent,
                    ra_index=curator_obj.index_id_ra, 
                    br_index=curator_obj.index_id_br, 
                    re_index_csv=curator_obj.re_index,
                    ar_index_csv=curator_obj.ar_index, 
                    vi_index=curator_obj.VolIss)
                creator = creator_obj.creator(source=self.source)
                # Provenance
                prov = ProvSet(creator, self.base_iri, creator_info_dir, wanted_label=False)
                prov.generate_provenance()
                # Storer
                res_storer = Storer(creator,
                                    context_map={},
                                    dir_split=self.dir_split_number,
                                    n_file_item=self.items_per_file,
                                    default_dir=self.default_dir,
                                    output_format='nt11')
                prov_storer = Storer(prov,
                                    context_map={},
                                    dir_split=self.dir_split_number,
                                    n_file_item=self.items_per_file,
                                    output_format='nquads')
                self.store_json_ld(creator)
                if self.rdf_output_in_chunks:
                    filename_without_csv = filename[:-4]
                    f = os.path.join(self.base_dir, 'nt', filename_without_csv + '.nt')
                    res_storer.store_graphs_in_file(f, self.context_path)
                    res_storer.upload_all(self.triplestore_url, self.base_dir, batch_size=100)
                    # Provenance
                    f_prov = os.path.join(prov_dir, filename_without_csv + '.nq')
                    prov_storer.store_graphs_in_file(f_prov, self.context_path)
                else:
                    res_storer.upload_and_store(self.base_dir, self.triplestore_url, self.base_iri, self.context_path, batch_size=100)
                    prov_storer.store_all(self.base_dir, self.base_iri, self.context_path)
                with open(self.cache_path, 'a', encoding='utf-8') as aux_file:
                    aux_file.write(filename + '\n')
            if self.verbose:
                pbar.update(1)
        if self.verbose:
            pbar.close()

    def store_json_ld(self, creator:Creator) -> None:
        json_ld_storer = Storer(creator,
            context_map={},
            dir_split=self.dir_split_number,
            n_file_item=self.items_per_file,
            default_dir=self.default_dir,
            output_format='json-ld')
        base_dir = os.path.join(self.base_dir, 'jsonld/')
        json_ld_storer.store_all(base_dir=base_dir, base_iri=self.base_iri, context_path=self.context_path)



if __name__ == '__main__':
    arg_parser = ArgumentParser('meta_process.py', description='This script runs OCMeta data processing workflow')
    arg_parser.add_argument('-c', '--config', dest='config', required=True,
                            help='Configuration file directory')
    args = arg_parser.parse_args()
    meta_process = MetaProcess(args.config)
    meta_process.process()