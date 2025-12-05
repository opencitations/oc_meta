#!python
# Copyright 2023, Arcangelo Massari <arcangelo.massari@unibo.it>
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

from oc_meta.lib.csvmanager import CSVManager
from oc_ocdm.graph.graph_entity import GraphEntity
from pebble import ProcessFuture, ProcessPool
from sparqlite import SPARQLClient
from tqdm import tqdm


def task_done(task_output:ProcessFuture) -> None:
    identifier, it_exists = task_output.result()
    if not it_exists:
        print(identifier)
    PBAR.update()

def ask_for_id(identifier: str, endpoint: str) -> bool:
    query = f'''
        ASK {{
            ?br <{GraphEntity.iri_has_identifier}> ?identifier.
            ?identifier <{GraphEntity.iri_uses_identifier_scheme}> <{GraphEntity.iri_doi}>;
                        <{GraphEntity.iri_has_literal_value}> "{identifier}".
        }}
    '''
    with SPARQLClient(endpoint, max_retries=3, backoff_factor=5) as client:
        it_exists = client.query(query)['boolean']
    return identifier, it_exists

if __name__ == '__main__':
    arg_parser = ArgumentParser('check_meta_output.py')
    arg_parser.add_argument('-i', '--ids', dest='ids', required=False,
                            help='A CSV filepath containing what ids must be checked')
    arg_parser.add_argument('-t', '--ts', dest='ts', required=False,
                            help='The triplestore endpoint URL')
    args = arg_parser.parse_args()
    ids_set = list(CSVManager.load_csv_column_as_set(args.ids, 'id'))
    PBAR = tqdm(total=len(ids_set))
    print('Starting the queries')
    with ProcessPool() as executor:
        for identifier in ids_set:
            future:ProcessFuture = executor.schedule(
                function=ask_for_id,
                args=(identifier, args.ts))
            future.add_done_callback(task_done)
    PBAR.close()

