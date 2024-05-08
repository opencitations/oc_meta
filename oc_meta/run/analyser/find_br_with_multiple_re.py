import argparse
import zipfile
from pathlib import Path
from rdflib import ConjunctiveGraph, URIRef
from tqdm import tqdm
import json
import multiprocessing

def process_zip_file(zip_path):
    """ Process a single zip file, read JSON-LD contents and check for multiple 'embodiment' properties. """
    graph = ConjunctiveGraph()
    targeted_subjects = {}

    with zipfile.ZipFile(zip_path, 'r') as zipf:
        for file in zipf.namelist():
            if file.endswith('.json'):
                with zipf.open(file) as f:
                    data = f.read()
                    graph.parse(data=data, format='json-ld')

    embodiment_uri = URIRef("http://purl.org/vocab/frbr/core#embodiment")
    for subject in graph.subjects(unique=True):
        embodiments = list(graph.objects(subject, embodiment_uri, unique=True))
        if len(embodiments) > 1:
            targeted_subjects[str(subject)] = [str(emb) for emb in embodiments]

    return targeted_subjects

def main(directory):
    """ Main function to process all zip files in the given directory excluding 'se.zip'. """
    path = Path(directory)
    all_zip_files = [f for f in path.rglob('*.zip') if f.name != 'se.zip']

    with multiprocessing.Pool() as pool:
        results = list(tqdm(pool.imap(process_zip_file, all_zip_files), total=len(all_zip_files)))

    all_targeted_subjects = {}

    for subjects in results:
        all_targeted_subjects.update(subjects)

    # Save the subjects with multiple embodiments to a JSON file
    with open('targeted_subjects.json', 'w') as f:
        json.dump(all_targeted_subjects, f)

    print(f"Subjects with multiple embodiments saved to 'targeted_subjects.json'.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process .zip files to extract subjects with multiple "embodiment" properties.')
    parser.add_argument('--directory', type=str, required=True, help='Directory to scan for .zip files')
    args = parser.parse_args()
    
    main(args.directory)