import argparse
from multiprocessing import Pool, Manager
import os
from pathlib import Path
import zipfile
from tqdm import tqdm
from rdflib import ConjunctiveGraph, URIRef
import json
from filelock import FileLock
import os
from rdflib.namespace import PROV, RDF
from collections import defaultdict

def init_worker(zip_files_list, lock):
    global zip_files
    global zip_files_lock
    
    zip_files = zip_files_list
    zip_files_lock = lock
    
def init_queue(queue):
    global task_queue
    task_queue = queue

def load_graph_from_zip(zip_path):
    graph = ConjunctiveGraph()
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.json'):
                    with zip_ref.open(file_info.filename) as file:
                        data = file.read().decode('utf-8')
                        graph.parse(data=data, format='json-ld')
    except Exception as e:
        print(f"Error processing {zip_path}: {e}")
    return graph

def find_zip_files(directory):
    zip_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file != 'se.zip' and file.endswith('.zip'):
                zip_files.append(os.path.join(root, file))
    return zip_files

def check_provenance_entity(prov_graph, subject):
    errors = []
    error_types = defaultdict(int)
    entity_uri = URIRef(str(subject) + '/prov/se/1')
    if (entity_uri, None, None) not in prov_graph:
        error_message = f"Provenance entity not found for {entity_uri}"
        errors.append(error_message)
        error_types["Provenance entity not found"] += 1
    required_predicates = [
        (PROV.generatedAtTime, "generatedAtTime"),
        (PROV.specializationOf, "specializationOf", subject),
        (PROV.wasAttributedTo, "wasAttributedTo"),
        (RDF.type, 'type', PROV.Entity)
    ]
    for predicate, label, *expected_value in required_predicates:
        if expected_value:
            if not (entity_uri, predicate, expected_value[0]) in prov_graph:
                error_message = f"{label} property not found or incorrect for {entity_uri}"
                errors.append(error_message)
                error_types[f"{label} property error"] += 1
        else:
            if not (entity_uri, predicate, None) in prov_graph:
                error_message = f"{label} property missing for {entity_uri}"
                errors.append(error_message)
                error_types[f"{label} property missing"] += 1
    return errors, dict(error_types)

def process_zip_file(zip_path):
    local_error_stats = defaultdict(int)  # Dizionario locale per gli errori
    local_error_details = defaultdict(list)  # Dettagli degli errori per ogni entità

    # Carica il grafo principale dal file ZIP
    main_graph = load_graph_from_zip(zip_path)

    # Determina il percorso del file di provenienza e caricalo se esiste
    prov_graph = None
    base_path = Path(zip_path).parent
    prov_folder = base_path.joinpath(Path(zip_path).stem, 'prov')
    prov_zip_path = prov_folder.joinpath('se.zip')
    
    if prov_zip_path.exists():
        prov_graph = load_graph_from_zip(prov_zip_path)
    else:
        local_error_stats["Provenance file not found"] += 1

    if prov_graph:
        for subject in main_graph.subjects(unique=True):
            prov_errors, error_types = check_provenance_entity(prov_graph, subject)
            if prov_errors:
                local_error_details[str(subject)].extend(prov_errors)
                for error_type in error_types:
                    local_error_stats[error_type] += 1

    # Segnala il completamento del task
    task_queue.put(1)
    return dict(local_error_stats), dict(local_error_details)

def main(directory, error_log_path):
    temp_file_path = "zip_tasks.tmp"

    if os.path.exists(temp_file_path):
        print("Loading tasks from the temporary file...")
        with open(temp_file_path, 'r') as temp_file:
            zip_files = [line.strip() for line in temp_file]
    else:
        print("Scanning directory for ZIP files...")
        zip_files = find_zip_files(directory)

        # Salvataggio dei tasks nel file temporaneo
        with open(temp_file_path, 'w') as temp_file:
            for zip_path in zip_files:
                temp_file.write(f"{zip_path}\n")

    
    with Manager() as manager:
        queue = manager.Queue()
        results = []
        with Pool(initializer=init_queue, initargs=(queue,)) as pool:
            for zip_path in zip_files:
                result = pool.apply_async(process_zip_file, args=(zip_path,))
                results.append(result)
            
            pool.close()

            pbar = tqdm(total=len(zip_files))
            completed_tasks = 0
            while completed_tasks < len(zip_files):
                queue.get()
                completed_tasks += 1
                pbar.update(1)
            pbar.close()

            pool.join()

        aggregated_errors = defaultdict(int)
        aggregated_error_details = defaultdict(list)
        for result in results:
            local_errors, local_error_details = result.get()
            for error_type, count in local_errors.items():
                aggregated_errors[error_type] += count
            for entity, errors in local_error_details.items():
                aggregated_error_details[entity].extend(errors)

        with open(error_log_path, 'w') as summary_file:
            json.dump({"errors": dict(aggregated_errors), "details": dict(aggregated_error_details)}, summary_file, indent=4)

    # os.remove(temp_file_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process ZIP files excluding 'prov' folders and log errors with file lock.")
    parser.add_argument("directory", type=str, help="The path to the directory to analyze.")
    parser.add_argument("error_log_path", type=str, help="The path to the error log file.")
    args = parser.parse_args()
    
    main(args.directory, args.error_log_path)