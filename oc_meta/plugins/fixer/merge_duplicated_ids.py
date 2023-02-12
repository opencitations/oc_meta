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

from typing import Tuple

from oc_meta.plugins.csv_generator.csv_generator import (find_file,
                                                         process_archive,
                                                         process_id)


def extract_identifiers(data: list, filepath: str, rdf_dir:str, dir_split_number: int, items_per_file: int, zip_output_rdf: bool, memory: dict, meta_config: str, resp_agent: str) -> Tuple[str, list]:
    to_be_merged = list()
    for graph in data:
        graph_data = graph['@graph']
        for entity in graph_data:
            ids_mapping = dict()
            entity_full_ids = list()
            if 'http://purl.org/spar/datacite/hasIdentifier' in entity:
                identifiers = entity['http://purl.org/spar/datacite/hasIdentifier']
                for identifier in identifiers:
                    id_uri = identifier['@id']
                    id_path = find_file(rdf_dir, dir_split_number, items_per_file, id_uri, zip_output_rdf)
                    id_full = process_archive(id_path, process_id, memory, id_uri, meta_config, resp_agent, id_path, memory)
                    if id_full in entity_full_ids:
                        prev_id_uri = next(prev_uri for prev_uri, prev_full in ids_mapping.items() if prev_full == id_full)
                        to_be_merged.append(tuple(sorted([prev_id_uri, id_uri])))
                    entity_full_ids.append(id_full)
                    ids_mapping[id_uri] = id_full
    return filepath, to_be_merged