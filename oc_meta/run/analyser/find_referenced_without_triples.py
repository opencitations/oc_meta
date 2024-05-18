import argparse
import zipfile
from pathlib import Path
from rdflib import ConjunctiveGraph, URIRef
from tqdm import tqdm
import json
import multiprocessing

def process_zip_file(zip_path):
    """ Process a single zip file, read JSON-LD contents and return sets of subjects and objects. """
    graph = ConjunctiveGraph()
    subjects = set()
    objects = set()

    with zipfile.ZipFile(zip_path, 'r') as zipf:
        for file in zipf.namelist():
            if file.endswith('.json'):
                with zipf.open(file) as f:
                    data = f.read()
                    graph.parse(data=data, format='json-ld')

    for s, p, o in graph:
        if isinstance(s, URIRef):
            subjects.add(s)
        if isinstance(o, URIRef):
            objects.add(o)
    
    return subjects, objects

def main(directory):
    """ Main function to process all zip files in the given directory excluding 'se.zip'. """
    path = Path(directory)
    all_zip_files = [f for f in path.rglob('*.zip') if f.name != 'se.zip']

    with multiprocessing.Pool() as pool:
        results = list(tqdm(pool.imap(process_zip_file, all_zip_files), total=len(all_zip_files)))

    all_subjects = set()
    all_objects = set()

    for subjects, objects in results:
        all_subjects.update(subjects)
        all_objects.update(objects)

    # Subjects that are referenced but have no triples (are not subjects in any triple)
    referenced_without_triples = all_objects - all_subjects

    # Save the referenced subjects without triples to a JSON file
    with open('referenced_without_triples.json', 'w') as f:
        json.dump(list(referenced_without_triples), f)

    print(f"Referenced subjects without triples saved to 'referenced_without_triples.json'.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process .zip files to find RDF subjects that are referenced but have no triples.')
    parser.add_argument('--directory', type=str, help='Directory to scan for .zip files')
    args = parser.parse_args()
    
    main(args.directory)