import argparse
import zipfile
from pathlib import Path
from rdflib import ConjunctiveGraph, URIRef
from tqdm import tqdm
import json
import multiprocessing

def process_zip_file(zip_path):
    """ Process a single zip file, read JSON-LD contents and return sets of subjects, objects, and outgoing subjects. """
    graph = ConjunctiveGraph()
    subjects = set()
    objects = set()
    outgoing_subjects = set()  # Subjects with outgoing edges to URIRefs

    with zipfile.ZipFile(zip_path, 'r') as zipf:
        for file in zipf.namelist():
            if file.endswith('.json'):
                with zipf.open(file) as f:
                    data = f.read()
                    graph.parse(data=data, format='json-ld')

    for s, p, o in graph:
        subjects.add(s)
        if isinstance(o, URIRef):
            objects.add(o)
            outgoing_subjects.add(s)
    
    return subjects-outgoing_subjects, objects

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

    full_orphans = all_subjects - all_objects

    # Save the full orphans to a JSON file
    with open('full_orphans.json', 'w') as f:
        json.dump(list(full_orphans), f)

    print(f"Full orphans saved to 'full_orphans.json'.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process .zip files to find full orphan RDF subjects.')
    parser.add_argument('--directory', type=str, help='Directory to scan for .zip files')
    args = parser.parse_args()
    
    main(args.directory)