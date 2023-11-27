import argparse
import multiprocessing
import os

from rdflib import ConjunctiveGraph, Literal, URIRef
from SPARQLWrapper import JSON, SPARQLWrapper


def query_triplestore(endpoint, offset, limit):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(f"""
        SELECT ?g ?s ?p ?o WHERE {{
            GRAPH ?g {{ ?s ?p ?o }}
        }} LIMIT {limit} OFFSET {offset}
    """)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results

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

def process_task(endpoint, output_folder, page_size, offset, file_count):
    results = query_triplestore(endpoint, offset, page_size)
    if results["results"]["bindings"]:
        graph = convert_to_graph(results)
        output_filename = os.path.join(output_folder, f"output_{file_count}.jsonld")
        with open(output_filename, 'w', encoding='utf-8') as f:
            jsonld_data = graph.serialize(format='json-ld')
            f.write(jsonld_data)

def main(endpoint, output_folder, page_size, n_processes):
    os.makedirs(output_folder, exist_ok=True)

    total_triples = count_triples(endpoint)
    num_pages = (total_triples + page_size - 1) // page_size
    pages_per_process = (num_pages + n_processes - 1) // n_processes

    processes = []
    for i in range(n_processes):
        start_offset = i * pages_per_process * page_size
        for j in range(pages_per_process):
            offset = start_offset + j * page_size
            file_count = i * pages_per_process + j
            if file_count < num_pages:
                proc = multiprocessing.Process(target=process_task, args=(endpoint, output_folder, page_size, offset, file_count))
                processes.append(proc)
                proc.start()

    for proc in processes:
        proc.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download triples from a Blazegraph triplestore and save as JSON-LD.")
    parser.add_argument("--endpoint", required=True, help="SPARQL endpoint URL")
    parser.add_argument("--output", required=True, help="Folder path to save output JSON-LD files")
    parser.add_argument("--pagesize", type=int, default=1000, help="Number of triples per page (default: 1000)")
    parser.add_argument("--nprocesses", type=int, default=4, help="Number of processes to use (default: 4)")

    args = parser.parse_args()
    main(args.endpoint, args.output, args.pagesize, args.nprocesses)