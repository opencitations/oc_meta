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

from oc_meta.plugins.fixer.merge_duplicated_ids import \
    find_duplicated_ids_in_entity_type

if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('diplicated_ids.py', description='Merge duplicated ids for the entity type specified')
    arg_parser.add_argument('-e', '--entity_type', dest='entity_type', required=True, choices=['ra', 'br'], help='An entity type abbreviation')
    arg_parser.add_argument('-c', '--c', dest='config', required=True, help='OpenCitations Meta configuration file location')
    arg_parser.add_argument('-r', '--resp_agent', dest='resp_agent', required=True, help='Your ORCID URL')
    args = arg_parser.parse_args()
    with open(args.config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    rdf_dir = os.path.join(settings['output_rdf_dir'], 'rdf') + os.sep
    zip_output_rdf = settings['zip_output_rdf']
    find_duplicated_ids_in_entity_type(rdf_dir, args.config, args.entity_type, args.resp_agent, zip_output_rdf)