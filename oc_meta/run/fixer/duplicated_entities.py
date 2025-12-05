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

from argparse import ArgumentParser

import yaml
from oc_meta.lib.file_manager import get_csv_data
from oc_meta.plugins.editor import MetaEditor
from rdflib import URIRef
from sparqlite import SPARQLClient
from tqdm import tqdm

if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('duplicated_entities.py', description='Merge duplicated entities havng the same identifiers')
    arg_parser.add_argument('-i', '--csv', dest='csv_filepath', required=True, help='The path to a CSV file containing the list of entities to be merged. The CSV has two columns, "id" and "omid". The "id" column contains an identifier, while the "omid" column contains a list of omids separated by semicolon')
    arg_parser.add_argument('-e', '--entity_type', dest='entity_type', required=True, choices=['ra', 'br'], help='An entity type abbreviation')
    arg_parser.add_argument('-c', '--meta_config', dest='meta_config', required=True, help='OpenCitations Meta configuration file location')
    arg_parser.add_argument('-r', '--resp_agent', dest='resp_agent', required=True, help='Your ORCID URL')
    args = arg_parser.parse_args()
    with open(args.meta_config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    entity_type = args.entity_type
    csv_data = get_csv_data(args.csv_filepath)
    pbar = tqdm(total=len(csv_data))
    meta_editor = MetaEditor(meta_config=args.meta_config, resp_agent=args.resp_agent)
    with SPARQLClient(settings['triplestore_url'], max_retries=3, backoff_factor=5) as client:
        for entity in csv_data:
            identifier = entity['id']
            id_components = identifier.split(':', maxsplit=1)
            id_scheme = id_components[0]
            literal_value = id_components[1]
            omids = sorted([f'https://w3id.org/oc/meta/{omid}' for omid in entity['omid'].split('; ')])
            for omid in list(omids):
                query = f'''
                    PREFIX datacite: <http://purl.org/spar/datacite/>
                    PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
                    ASK {{
                        <{omid}> datacite:hasIdentifier ?identifier.
                        ?identifier datacite:usesIdentifierScheme datacite:{id_scheme};
                                    literal:hasLiteralValue "{literal_value}".
                    }}
                '''
                result = client.query(query)['boolean']
                if not result:
                    print(f'The identifier {identifier} is not associated with the omid {omid}')
                    omids.remove(omid)
            if len(omids) > 1:
                to_be_merged = [(omids[0], other) for other in omids[1:]]
                for merge_couple in to_be_merged:
                    meta_editor.merge(URIRef(merge_couple[0]), URIRef(merge_couple[1]))
            pbar.update()
    pbar.close()