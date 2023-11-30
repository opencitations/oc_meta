import argparse
import multiprocessing
import os
import time
from rdflib import ConjunctiveGraph, Literal, URIRef
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm
import queue

def query_triplestore_by_class(endpoint, class_uri, offset, limit, max_retries=5):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(f"""
        SELECT ?g ?s ?p ?o WHERE {{
            GRAPH ?g {{?s a <{class_uri}> ;
               ?p ?o .}}
        }} LIMIT {limit} OFFSET {offset}
    """)
    sparql.setReturnFormat(JSON)
    for attempt in range(max_retries):
        try:
            results = sparql.query().convert()
            return results, bool(results["results"]["bindings"])
        except Exception as e:
            print(f"Errore nella query: {e}, tentativo {attempt + 1} di {max_retries}")
            time.sleep(2 ** attempt)

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

def process_task(class_uri, endpoint, output_folder, page_size):
    output_folder = os.path.join(output_folder, class_uri.split('/')[-1])
    os.makedirs(output_folder, exist_ok=True)

    file_count = 0
    offset = 0
    while True:
        results, has_data = query_triplestore_by_class(endpoint, class_uri, offset, page_size)
        if has_data:
            graph = convert_to_graph(results)
            output_filename = os.path.join(output_folder, f"{class_uri.split('/')[-1]}_output_{file_count}.jsonld")
            with open(output_filename, 'w', encoding='utf-8') as f:
                jsonld_data = graph.serialize(format='json-ld')
                f.write(jsonld_data)
        else:
            break
        offset += page_size
        file_count += 1

def main(endpoint, output_folder, page_size):
    class_uris = [
        "http://purl.org/spar/fabio/Expression",
        "http://purl.org/spar/fabio/Manifestation",
        "http://xmlns.com/foaf/0.1/Agent",
        "http://purl.org/spar/datacite/Identifier",
        "http://purl.org/spar/pro/RoleInTime"
    ]

    pool = multiprocessing.Pool(processes=len(class_uris))

    for class_uri in class_uris:
        print(f"Processing class: {class_uri}")
        pool.apply_async(process_task, (class_uri, endpoint, output_folder, page_size))

    pool.close()
    pool.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download triples from a Blazegraph triplestore and save as JSON-LD.")
    parser.add_argument("--endpoint", required=True, help="SPARQL endpoint URL")
    parser.add_argument("--output", required=True, help="Folder path to save output JSON-LD files")
    parser.add_argument("--pagesize", type=int, default=10000, help="Number of triples per page (default: 1000)")

    args = parser.parse_args()
    main(args.endpoint, args.output, args.pagesize)