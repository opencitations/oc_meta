import argparse
import gzip
from pathlib import Path
import rdflib
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

def load_and_search(file_path, entity_uri):
    """
    Load a .jsonld.gz file into an RDF graph and search for the entity URI.

    Parameters:
    - file_path: The path to the .jsonld.gz file.
    - entity_uri: The URI of the entity to search for.

    Returns:
    - The file path if the entity is found, otherwise None.
    """
    graph = rdflib.ConjunctiveGraph()
    with gzip.open(file_path, 'rt', encoding='utf-8') as file:
        graph.parse(file, format='json-ld')

    query = """
    ASK {
        <""" + entity_uri + """> ?p ?o .
    }
    """
    if graph.query(query).askAnswer:
        return str(file_path)
    return None

def main(directory, entity_uri):
    """
    Search for an entity URI in .jsonld.gz files within the specified directory using multiprocessing.

    Parameters:
    - directory: The path to the directory containing .jsonld.gz files.
    - entity_uri: The URI of the entity to search for.
    """
    path = Path(directory)
    files = [str(file_path) for file_path in path.glob('*.jsonld.gz')]
    
    # Determine the number of processes to use
    num_processes = min(multiprocessing.cpu_count(), len(files))

    found_files = []
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        results = list(tqdm(executor.map(load_and_search, files, [entity_uri]*len(files)), total=len(files), desc="Processing files"))

    # Filter out None results
    found_files = [result for result in results if result]

    if found_files:
        print("Entity found in the following files:")
        for file in found_files:
            print(file)
    else:
        print("Entity not found in any file.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search for an entity URI in .jsonld.gz files with multiprocessing.")
    parser.add_argument("directory", type=str, help="The path to the directory containing .jsonld.gz files.")
    parser.add_argument("entity_uri", type=str, help="The URI of the entity to search for.")
    args = parser.parse_args()

    main(args.directory, args.entity_uri)