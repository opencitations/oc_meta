import argparse
import multiprocessing
import os
import time
from rdflib import ConjunctiveGraph, Literal, URIRef
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm
import queue

def query_triplestore(endpoint, offset, limit, max_retries=5):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(f"""
        SELECT ?g ?s ?p ?o WHERE {{
            GRAPH ?g {{ ?s ?p ?o }}
        }} LIMIT {limit} OFFSET {offset}
    """)
    sparql.setReturnFormat(JSON)
    for attempt in range(max_retries):
        try:
            results = sparql.query().convert()
            return results
        except Exception as e:
            print(f"Errore nella query: {e}, tentativo {attempt + 1} di {max_retries}")
            time.sleep(2 ** attempt)  # Backoff esponenziale

def count_triples(endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery("""
        SELECT (COUNT(*) as ?count) WHERE {
            GRAPH ?g { ?s ?p ?o }
        }
    """)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    try:
        count = int(results["results"]["bindings"][0]["count"]["value"])
        return count
    except (IndexError, KeyError, ValueError):
        print("Errore nel calcolo del numero totale di triple")
        return 0

def convert_to_graph(results):
    g = ConjunctiveGraph()
    for result in results["results"]["bindings"]:
        graph_uri = URIRef(result["g"]["value"])
        s = URIRef(result["s"]["value"])
        p = URIRef(result["p"]["value"])
        o_value = result["o"]["value"]
        if result["o"]["type"] == "uri":
            o = URIRef(o_value)
        else:
            datatype = result["o"].get("datatype")
            if datatype:
                o = Literal(o_value, datatype=URIRef(datatype))
            else:
                o = Literal(o_value)
        g.add((s, p, o, graph_uri))
    return g

def process_task(endpoint, output_folder, page_size, offset, file_count, progress_queue):
    results = query_triplestore(endpoint, offset, page_size)
    if results["results"]["bindings"]:
        graph = convert_to_graph(results)
        output_filename = os.path.join(output_folder, f"output_{file_count}.jsonld")
        with open(output_filename, 'w', encoding='utf-8') as f:
            jsonld_data = graph.serialize(format='json-ld')
            f.write(jsonld_data)
    progress_queue.put(1)

def progress_monitor(total_tasks, progress_queue):
    with tqdm(total=total_tasks, desc="Downloading and Saving Triples") as pbar:
        completed_tasks = 0
        while completed_tasks < total_tasks:
            completed_tasks += progress_queue.get()
            pbar.update(1)

def process_worker(endpoint, output_folder, page_size, task_queue, progress_queue):
    while not task_queue.empty():
        try:
            offset, file_count = task_queue.get_nowait()
            process_task(endpoint, output_folder, page_size, offset, file_count, progress_queue)
        except queue.Empty:
            break

def main(endpoint, output_folder, page_size, n_processes):
    os.makedirs(output_folder, exist_ok=True)

    total_triples = count_triples(endpoint)
    num_pages = (total_triples + page_size - 1) // page_size

    manager = multiprocessing.Manager()
    progress_queue = manager.Queue()
    task_queue = manager.Queue()

    for file_count in range(num_pages):
        offset = file_count * page_size
        task_queue.put((offset, file_count))

    monitor = multiprocessing.Process(target=progress_monitor, args=(num_pages, progress_queue))
    monitor.start()

    processes = [multiprocessing.Process(target=process_worker, args=(endpoint, output_folder, page_size, task_queue, progress_queue)) for _ in range(n_processes)]

    for proc in processes:
        proc.start()

    for proc in processes:
        proc.join()

    monitor.join()  # Assicurati che il monitor del progresso termini correttamente


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download triples from a Blazegraph triplestore and save as JSON-LD.")
    parser.add_argument("--endpoint", required=True, help="SPARQL endpoint URL")
    parser.add_argument("--output", required=True, help="Folder path to save output JSON-LD files")
    parser.add_argument("--pagesize", type=int, default=10000, help="Number of triples per page (default: 1000)")
    parser.add_argument("--nprocesses", type=int, default=4, help="Number of processes to use (default: 4)")

    args = parser.parse_args()
    main(args.endpoint, args.output, args.pagesize, args.nprocesses)