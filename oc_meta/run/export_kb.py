import argparse
import multiprocessing
from multiprocessing import Process, Queue

import os
import time
from rdflib import ConjunctiveGraph, Literal, URIRef
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm
import queue
from multiprocessing import Manager
from pebble import ProcessPool

def count_total_triples(endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery("""
        SELECT (COUNT(*) AS ?totalTriples) WHERE {
            ?s ?p ?o .
        }
    """)
    sparql.setReturnFormat(JSON)
    try:
        results = sparql.query().convert()
        total_triples = int(results["results"]["bindings"][0]["totalTriples"]["value"])
        return total_triples
    except Exception as e:
        print(f"Errore nella query: {e}")
        return None

def query_triplestore_by_class(endpoint, class_uri, uri_prefix, supplier_prefix, offset, limit):
    sparql = SPARQLWrapper(endpoint)
    full_uri_prefix = f"{uri_prefix}/{supplier_prefix}"
    sparql.setQuery(f"""
        SELECT ?g ?s ?p ?o WHERE {{
            GRAPH ?g {{
                ?s a <{class_uri}>.
                FILTER (STRSTARTS(STR(?s), "{full_uri_prefix}")).
                ?s ?p ?o.
            }}
        }} LIMIT {limit} OFFSET {offset}
    """)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results, bool(results["results"]["bindings"])

def convert_to_graph(results):
    g = ConjunctiveGraph()
    triple_count = 0
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
        triple_count += 1
    return g, triple_count

def generate_supplier_prefixes():
    prefixes = []
    for i in range(1, 10000):
        if i % 10 != 0:
            prefix = f"06{i}0"
            prefixes.append(prefix)
    return prefixes

def process_task(class_uri, uri_prefix, supplier_prefixes, endpoint, output_folder, page_size, progress_queue):
    for supplier_prefix in supplier_prefixes:
        output_folder_prefix = os.path.join(output_folder, class_uri.split('/')[-1], supplier_prefix)
        os.makedirs(output_folder_prefix, exist_ok=True)
        file_count = 0
        offset = 0
        while True:
            results, has_data = query_triplestore_by_class(endpoint, class_uri, uri_prefix, supplier_prefix, offset, page_size)
            if has_data:
                graph, triple_count = convert_to_graph(results)
                output_filename = os.path.join(output_folder_prefix, f"{class_uri.split('/')[-1]}_{supplier_prefix}_output_{file_count}.jsonld")
                with open(output_filename, 'w', encoding='utf-8') as f:
                    jsonld_data = graph.serialize(format='json-ld', indent=None, ensure_ascii=False)
                    f.write(jsonld_data)
                progress_queue.put(triple_count)
            else:
                break
            offset += page_size
            file_count += 1
    progress_queue.put(None)

def progress_monitor(total_triples, progress_queue, class_uris):
    pbar = tqdm(total=total_triples)
    completed_processes = 0
    while completed_processes < len(class_uris):
        update = progress_queue.get()
        if update is None:
            completed_processes += 1
        else:
            pbar.update(update)
    pbar.close()

def main(endpoint, output_folder, page_size):
    class_uris = [
        "http://purl.org/spar/fabio/Expression",
        "http://purl.org/spar/fabio/Manifestation",
        "http://xmlns.com/foaf/0.1/Agent",
        "http://purl.org/spar/datacite/Identifier",
        "http://purl.org/spar/pro/RoleInTime"
    ]

    uri_prefixes = {
        "http://purl.org/spar/fabio/Expression": "https://w3id.org/oc/meta/br",
        "http://purl.org/spar/fabio/Manifestation": "https://w3id.org/oc/meta/re",
        "http://xmlns.com/foaf/0.1/Agent": "https://w3id.org/oc/meta/ra",
        "http://purl.org/spar/datacite/Identifier": "https://w3id.org/oc/meta/id",
        "http://purl.org/spar/pro/RoleInTime": "https://w3id.org/oc/meta/ar"
    }

    supplier_prefixes = generate_supplier_prefixes()

    total_triples = count_total_triples(endpoint)
    results = []

    total_triples = count_total_triples(endpoint)
    if total_triples is None:
        return

    with Manager() as manager:
        progress_queue = manager.Queue()
        monitor = Process(target=progress_monitor, args=(total_triples, progress_queue, class_uris))
        monitor.start()

        with ProcessPool() as pool:
            for class_uri in class_uris:
                uri_prefix = uri_prefixes[class_uri]
                pool.schedule(process_task, args=(class_uri, uri_prefix, supplier_prefixes, endpoint, output_folder, page_size, progress_queue))

        monitor.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download triples from a Blazegraph triplestore and save as JSON-LD.")
    parser.add_argument("--endpoint", required=True, help="SPARQL endpoint URL")
    parser.add_argument("--output", required=True, help="Folder path to save output JSON-LD files")
    parser.add_argument("--pagesize", type=int, default=10000, help="Number of triples per page (default: 1000)")

    args = parser.parse_args()
    main(args.endpoint, args.output, args.pagesize)