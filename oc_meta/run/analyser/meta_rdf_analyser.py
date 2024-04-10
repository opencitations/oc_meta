import argparse
import os
import multiprocessing
from zipfile import ZipFile
import rdflib
from tqdm import tqdm
import functools

def explore_dir(root_dir, entity_type):
    """Esplora ricorsivamente per trovare i file .zip validi, 
    escludendo se.zip e scegliendo la directory corretta in base al tipo di entità."""
    sub_dir = 'br' if entity_type == 'bibliographic_resources' else 'ar'
    target_path = os.path.join(root_dir, sub_dir)
    # Itera sui file nella directory target e nelle sue sottodirectory
    for root, _, files in os.walk(target_path):
        for filename in files:
            # Seleziona i file .zip escludendo 'se.zip'
            if filename.endswith('.zip') and filename != 'se.zip':
                yield os.path.join(root, filename)

def load_and_query_zip(zip_path, entity_type):
    """Carica e interroga il contenuto del file zip senza decomprimerlo."""
    g = rdflib.ConjunctiveGraph()
    with ZipFile(zip_path) as myzip:
        with myzip.open(myzip.namelist()[0]) as myfile:
            g.parse(myfile, format='json-ld')
    if entity_type == 'bibliographic_resources':
        query = """
        SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE {
            ?s a <http://purl.org/spar/fabio/Expression> .
        }
        """
    elif entity_type == 'authors':
        query = """
        prefix pro: <http://purl.org/spar/pro/>
        SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE {
            ?s a pro:RoleInTime;
            pro:withRole pro:author.
        }
        """
    elif entity_type == 'editors':
        query = """
        prefix pro: <http://purl.org/spar/pro/>
        SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE {
            ?s a pro:RoleInTime;
            pro:withRole pro:editor.
        }
        """
    elif entity_type == 'publishers':
        query = """
        prefix pro: <http://purl.org/spar/pro/>
        SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE {
            ?s a pro:RoleInTime;
            pro:withRole pro:publisher.
        }
        """
    for row in g.query(query):
        return int(row[0])

def main(root_dir, entity_type):
    zip_files = list(explore_dir(root_dir, entity_type))
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        func = functools.partial(load_and_query_zip, entity_type=entity_type)
        results = list(tqdm(pool.imap(func, zip_files), total=len(zip_files)))
    total_count = sum(results)
    print(f"Totale entità di tipo Expression: {total_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Conta le risorse bibliografiche e gli autori, editori, o pubblicatori.')
    parser.add_argument('root_dir', type=str, help='La root directory da esplorare.')
    parser.add_argument('entity_type', type=str, choices=['bibliographic_resources', 'authors', 'editors', 'publishers'], 
                        help='Il tipo di entità da contare.')
    args = parser.parse_args()
    main(args.root_dir, args.entity_type)
