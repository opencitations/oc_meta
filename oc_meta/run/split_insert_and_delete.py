import os
import glob
import argparse
from tqdm import tqdm
from rdflib.plugins.sparql.parser import parseUpdate
from multiprocessing import Pool, cpu_count
from rdflib import Literal

# Function to process a file and extract quadruples to add and remove using rdflib
def process_sparql_file(filepath):
    quads_to_add = []
    quads_to_remove = []
    
    with open(filepath, 'r', encoding='utf-8') as file:
        query = file.read()
        try:
            parsed_query = parseUpdate(query)
            
            for update in parsed_query['request']:
                if update.name == 'InsertData':
                    for quadsNotTriples in update.quads.quadsNotTriples:
                        graph = quadsNotTriples.term
                        for quad in quadsNotTriples.triples:
                            subject, predicate, obj = quad
                            subject_str = f'<{subject}>'
                            predicate_str = f'<{predicate}>'
                            if 'string' in obj:
                                object_literal = Literal(lexical_or_value=obj.string, datatype=obj.datatype) if obj.datatype else Literal(lexical_or_value=obj.string)
                                object_str = object_literal.n3()
                            else:
                                object_str = f'<{obj}>'
                            quads_to_add.append((subject_str, predicate_str, object_str, f'<{graph}>'))
                elif update.name == 'DeleteData':
                    for quadsNotTriples in update.quads.quadsNotTriples:
                        graph = quadsNotTriples.term
                        for quad in quadsNotTriples.triples:
                            subject, predicate, obj = quad
                            subject_str = f'<{subject}>'
                            predicate_str = f'<{predicate}>'
                            if 'string' in obj:
                                object_literal = Literal(lexical_or_value=obj.string, datatype=obj.datatype) if obj.datatype else Literal(lexical_or_value=obj.string)
                                object_str = object_literal.n3()
                            else:
                                object_str = f'<{obj}>'
                            quads_to_remove.append((subject_str, predicate_str, object_str, f'<{graph}>'))
        except Exception as e:
            print(query, e)

    return quads_to_add, quads_to_remove

# Function to save quadruples in batches
def save_quads(quads, batch_size, prefix):
    os.makedirs(prefix, exist_ok=True)
    for i in tqdm(range(0, len(quads), batch_size), desc="Saving quads"):
        batch = quads[i:i+batch_size]
        with open(f'{prefix}/{i//batch_size + 1}.txt', 'w') as file:
            for quad in batch:
                file.write(' '.join(quad) + ' .\n')

# Function to save delete data queries in batches
def save_delete_data_queries(queries, batch_size, prefix):
    os.makedirs(prefix, exist_ok=True)
    for i in tqdm(range(0, len(queries), batch_size), desc="Saving delete queries"):
        batch = queries[i:i+batch_size]
        with open(f'{prefix}/{i//batch_size + 1}.txt', 'w') as file:
            file.write('DELETE DATA {\n')
            for graph in set(q[-1] for q in batch):
                file.write(f'  GRAPH {graph} {{\n')
                for query in batch:
                    if query[-1] == graph:
                        file.write('    ' + ' '.join(query[:-1]) + ' .\n')
                file.write('  }\n')
            file.write('}\n')

# Worker function for multiprocessing
def worker(filepath):
    return process_sparql_file(filepath)

# Main function to process all .sparql files in the directory using multiprocessing
def main(directory, output_directory):
    all_quads_to_add = []
    all_quads_to_remove = []
    
    filepaths = glob.glob(os.path.join(directory, '*.sparql'))
    
    # Create a pool of processes
    num_processes = cpu_count()
    pool = Pool(processes=num_processes)

    # Create a tqdm progress bar
    with tqdm(total=len(filepaths), desc="Processing files") as pbar:
        # Collect results from all processes
        for quads_add, quads_remove in pool.imap_unordered(worker, filepaths):
            all_quads_to_add.extend(quads_add)
            all_quads_to_remove.extend(quads_remove)
            pbar.update()

    # Close the pool
    pool.close()
    pool.join()

    # Create output subdirectories
    quads_to_add_dir = os.path.join(output_directory, 'quads_to_add')
    delete_data_queries_dir = os.path.join(output_directory, 'delete_data_queries')

    # Save quadruples to add in batches of 10,000
    save_quads(all_quads_to_add, 10000, quads_to_add_dir)
    
    # Save delete data queries in batches of 1,000
    save_delete_data_queries(all_quads_to_remove, 1000, delete_data_queries_dir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process .sparql files and extract quadruples.')
    parser.add_argument('directory', type=str, help='The directory containing .sparql files')
    parser.add_argument('output_directory', type=str, help='The directory to save the output files')

    args = parser.parse_args()
    main(args.directory, args.output_directory)