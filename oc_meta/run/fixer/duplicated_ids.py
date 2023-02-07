#!python
# Copyright 2023, Arcangelo Massari <arcangelo.massari@unibo.it>, Arianna Moretti <arianna.moretti4@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import os
from argparse import ArgumentParser

import yaml
from pebble import ProcessFuture, ProcessPool
from rdflib import URIRef
from tqdm import tqdm

from oc_meta.plugins.editor import MetaEditor
from oc_meta.plugins.fixer.merge_duplicated_ids import (extract_identifiers,
                                                        process_archive)


def task_done(task_output:ProcessFuture) -> None:
    output = task_output.result()
    if output:
        print(f'To be merged: {output}')
        TO_BE_MERGED.extend(output)
    PBAR.update()

if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('diplicated_ids.py', description='Merge duplicated ids for the entity type specified')
    arg_parser.add_argument('-e', '--entity_type', dest='entity_type', required=True, choices=['ra', 'br'], help='An entity type abbreviation')
    arg_parser.add_argument('-c', '--meta_config', dest='meta_config', required=True, help='OpenCitations Meta configuration file location')
    arg_parser.add_argument('-r', '--resp_agent', dest='resp_agent', required=True, help='Your ORCID URL')
    args = arg_parser.parse_args()
    with open(args.meta_config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    rdf_dir = os.path.join(settings['output_rdf_dir'], 'rdf') + os.sep
    rdf_entity_dir = os.path.join(rdf_dir, args.entity_type) + os.sep
    zip_output_rdf = settings['zip_output_rdf']
    file_extension = '.zip' if zip_output_rdf else '.json'
    filepaths = [os.path.join(fold, file) for fold, _, files in os.walk(rdf_entity_dir) for file in files if file.endswith(file_extension) and os.path.basename(fold) != 'prov']
    PBAR = tqdm(total=len(filepaths))
    TO_BE_MERGED = list()
    rdf_dir = os.path.join(settings['output_rdf_dir'], 'rdf') + os.sep
    dir_split_number = settings['dir_split_number']
    items_per_file = settings['items_per_file']
    meta_config = args.meta_config
    resp_agent = settings['resp_agent']
    memory = dict()
    with ProcessPool(max_tasks=1) as executor:
        for filepath in filepaths:
            future:ProcessFuture = executor.schedule(
                function=process_archive, 
                args=(filepath, extract_identifiers, memory, rdf_dir, dir_split_number, items_per_file, zip_output_rdf, memory, meta_config, resp_agent)) 
            future.add_done_callback(task_done)
    PBAR.close()
    meta_editor = MetaEditor(meta_config, resp_agent)
    already_merged = set()
    pbar = tqdm(len(TO_BE_MERGED))
    for couple in TO_BE_MERGED:
        if couple[1] not in already_merged:
            meta_editor.merge(URIRef(couple[0]), URIRef(couple[1]))
            already_merged.add(couple[1])
        pbar.update()
    pbar.close()