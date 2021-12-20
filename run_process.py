from typing import List

from oc_ocdm import Storer
from oc_ocdm.prov import ProvSet

from meta.scripts.creator import *
from meta.scripts.curator import *
from meta.lib.conf import base_iri, context_path, info_dir, triplestore_url, \
    base_dir, dir_split_number, items_per_file, default_dir, rdf_output_in_chunks, supplier_prefix
from datetime import datetime
from argparse import ArgumentParser
from csv import DictReader
import os
from tqdm import tqdm


def process(crossref_csv_dir:str, csv_dir:str, index_dir:str, auxiliary_path:str, source=None, verbose:bool=False) -> None:
    if not os.path.exists(auxiliary_path):
        open(auxiliary_path, 'wt', encoding='utf-8')
        completed = set()
    else:
        with open(auxiliary_path, "r") as aux_file:
            completed = {line.rstrip('\n') for line in aux_file}
    if verbose:
        pbar = tqdm(total=len(os.listdir(crossref_csv_dir)))
    pathoo(csv_dir)
    pathoo(index_dir)
    pathoo(base_dir)
    prov_dir = os.path.join(base_dir, 'prov' + os.sep)
    pathoo(prov_dir)
    for filename in os.listdir(crossref_csv_dir):
        if filename.endswith(".csv") and filename not in completed:
            filepath = os.path.join(crossref_csv_dir, filename)
            data = list(DictReader(open(filepath, 'r', encoding='utf8'), delimiter=','))
            # Curator
            curator_info_dir = os.path.join(info_dir, 'curator' + os.sep)
            curator_obj = Curator(data, triplestore_url, info_dir=curator_info_dir, prefix=supplier_prefix)
            name = datetime.now().strftime("%Y-%m-%dT%H_%M_%S")
            curator_obj.curator(filename=name, path_csv=csv_dir, path_index=index_dir)
            # Creator
            creator_info_dir = os.path.join(info_dir, 'creator' + os.sep)
            creator_obj = Creator(curator_obj.data, base_iri, creator_info_dir, supplier_prefix,
                                  curator_obj.index_id_ra, curator_obj.index_id_br, curator_obj.re_index,
                                  curator_obj.ar_index, curator_obj.VolIss)
            creator = creator_obj.creator(source=source)
            # Provenance
            prov = ProvSet(creator, base_iri, creator_info_dir, wanted_label=False)
            prov.generate_provenance()
            # Storer
            res_storer = Storer(creator,
                                context_map={},
                                dir_split=dir_split_number,
                                n_file_item=items_per_file,
                                default_dir=default_dir,
                                output_format='nt11')
            prov_storer = Storer(prov,
                                 context_map={},
                                 dir_split=dir_split_number,
                                 n_file_item=items_per_file,
                                 output_format='nquads')
            if rdf_output_in_chunks:
                filename_without_csv = filename[:-4]
                f = os.path.join(base_dir, filename_without_csv + ".nt")
                res_storer.store_graphs_in_file(f, context_path)
                res_storer.upload_all(triplestore_url, base_dir, batch_size=100)

                # Provenance
                f_prov = os.path.join(prov_dir, filename_without_csv + '.nq')
                prov_storer.store_graphs_in_file(f_prov, context_path)
            else:
                res_storer.upload_and_store(
                    base_dir, triplestore_url, base_iri, context_path, batch_size=100)
                prov_storer.store_all(
                    base_dir, base_iri, context_path)
            with open(auxiliary_path, "a", encoding='utf-8') as aux_file:
                aux_file.write(filename + "\n")
        if verbose:
            pbar.update(1)
    if verbose:
        pbar.close()

def pathoo(path):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))


if __name__ == "__main__":
    arg_parser = ArgumentParser("run_process.py", description="This script runs OCMeta data processing workflow")
    arg_parser.add_argument("-c", "--crossref", dest="crossref_csv_dir", required=True,
                            help="Csv files directory")
    arg_parser.add_argument("-out", "--output", dest="csv_dir", required=True,
                            help="Directory where cleaned CSV will be stored")
    arg_parser.add_argument("-i", "--ind", dest="index_dir", required=True,
                            help="Directory where cleaned indices will be stored")
    arg_parser.add_argument("-a", "--aux", dest="auxiliary_path", required=True,
                            help="Txt file containing processed CSV list filepath")
    arg_parser.add_argument("-s", "--src", dest="source", required=False,
                            help="Data source, not mandatory")
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')
    args = arg_parser.parse_args()
    process(args.crossref_csv_dir, args.csv_dir, args.index_dir, args.auxiliary_path, source=args.source, verbose=args.verbose)
