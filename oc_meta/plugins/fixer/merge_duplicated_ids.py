#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2023 Arcangelo Massari <arcangelo.massari@unibo.it>
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

import yaml

from oc_meta.plugins.csv_generator.csv_generator import (find_file,
                                                         process_archive)


def find_duplicated_ids_in_entity_type(filepath: str, meta_config: str, entity_type_abbr: str, resp_agent: str, zip_output_rdf: bool):
    with open(meta_config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    rdf_dir = os.path.join(settings['output_rdf_dir'], 'rdf', entity_type_abbr) + os.sep
    dir_split_number = settings['dir_split_number']
    items_per_file = settings['items_per_file']
    memory = dict()
    for dirpath, _, filenames in os.walk(rdf_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            identifiers = process_archive(filepath, extract_identifiers)
            print(identifiers)

def extract_identifiers(data: list):
    for graph in data:
        graph_data = graph['@graph']
        for entity in graph_data:
            entity_ids = list()
            if 'http://purl.org/spar/datacite/hasIdentifier' in entity:
                identifiers = entity['http://purl.org/spar/datacite/hasIdentifier']
                for identifier in identifiers:
                    entity_ids.append(identifier['@id'])
