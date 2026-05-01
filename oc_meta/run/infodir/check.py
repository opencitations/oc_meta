# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import multiprocessing
import os
import zipfile

from oc_ocdm.counter_handler.filesystem_counter_handler import FilesystemCounterHandler
from oc_ocdm.support import get_prefix, get_resource_number, get_short_name
from rdflib import Dataset, URIRef
from rdflib.namespace import PROV, RDF
from rich_argparse import RichHelpFormatter
from tqdm import tqdm

from oc_meta.lib.file_manager import collect_zip_files

SUPPLIER_PREFIX = "060"


def process_zip_file(args):
    zip_file, info_dir = args
    info_dir_with_prefix = os.path.join(info_dir, SUPPLIER_PREFIX) + os.sep
    counter_handler = FilesystemCounterHandler(info_dir=info_dir_with_prefix, supplier_prefix=SUPPLIER_PREFIX)
    missing_entities = []

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            with zip_ref.open(file_name) as entity_file:
                g = Dataset(default_union=True)
                g.parse(data=entity_file.read(), format='json-ld')

                for s, p, o in g.triples((None, RDF.type, PROV.Entity)):
                    prov_entity_uri = str(s)
                    entity_uri = prov_entity_uri.split('/prov/se/')[0]
                    entity_uri_ref = URIRef(entity_uri)
                    supplier_prefix = get_prefix(entity_uri_ref)
                    short_name = get_short_name(entity_uri_ref)
                    resource_number = get_resource_number(entity_uri_ref)

                    counter_value = counter_handler.read_counter(
                        entity_short_name=short_name,
                        prov_short_name="se",
                        identifier=resource_number,
                        supplier_prefix=supplier_prefix,
                    )

                    if counter_value == 0:
                        print(f"\nMissing entity:")
                        print(f"URI: {entity_uri}")
                        print(f"Prov URI: {prov_entity_uri}")
                        print("---")

                        missing_entities.append({
                            "URI": entity_uri,
                            "Prov URI": prov_entity_uri,
                        })

    return missing_entities

def explore_provenance_files(root_path, info_dir):
    prov_zip_files = collect_zip_files(root_path, only_prov=True)

    args_list = [(zip_file, info_dir) for zip_file in prov_zip_files]

    ctx = multiprocessing.get_context('forkserver')
    with ctx.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = list(tqdm(pool.imap(process_zip_file, args_list), total=len(args_list), desc="Processing provenance zip files"))

    all_missing_entities = [item for sublist in results for item in sublist]

    print(f"\nTotal missing entities: {len(all_missing_entities)}")

def main():
    parser = argparse.ArgumentParser(
        description="Verify provenance entities have matching counter file entries.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("directory", type=str, help="Path to the RDF directory to scan")
    parser.add_argument("info_dir", type=str, help="Base directory for counter files")
    args = parser.parse_args()

    explore_provenance_files(args.directory, args.info_dir)

if __name__ == "__main__":
    main()
