import argparse
import os

from oc_meta.plugins.editor import MetaEditor
from rdflib import URIRef
from sparqlite import SPARQLClient
from tqdm import tqdm

def query_publishers(endpoint_url, output_file):
    if os.path.isfile(output_file):
        with open(output_file, 'r') as file:
            publishers = [line.strip() for line in file]
    else:
        sparql_query = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT DISTINCT ?pub
        WHERE {
            ?pub a foaf:Agent;
                foaf:name ?name.
            FILTER CONTAINS(?name, ";").
        }
        """

        with SPARQLClient(endpoint_url, max_retries=3, backoff_factor=5, timeout=3600) as client:
            results = client.query(sparql_query)

        publishers = [result["pub"]["value"] for result in results["results"]["bindings"]]
        with open(output_file, 'w') as file:
            for pub in publishers:
                print(pub)
                file.write(pub + '\n')
    return publishers

def update_publishers_names(endpoint_url, publishers, config_path, resp_agent):
    meta_editor = MetaEditor(meta_config=config_path, resp_agent=URIRef(resp_agent))

    with SPARQLClient(endpoint_url, max_retries=3, backoff_factor=5, timeout=3600) as client:
        for pub in tqdm(publishers, desc="Updating publisher names"):
            query = f"""
                SELECT ?old_value
                WHERE {{
                    <{pub}> foaf:name ?old_value.
                }}"""
            results = client.query(query)
            old_value = results["results"]["bindings"][0]["old_value"]["value"]
            new_value = old_value.replace(";", "")
            meta_editor.update_property(res=URIRef(pub), property="has_name", new_value=new_value)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query SPARQL endpoint for publishers whose name contains a semicolon, work with MetaEditor on the URIs.")
    parser.add_argument("endpoint", help="SPARQL endpoint URL")
    parser.add_argument("output_file", help="Path to the output file where URIs will be saved")
    parser.add_argument("meta_config_path", help="Path to the MetaEditor configuration file")
    parser.add_argument("resp_agent", help="Responsible agent for MetaEditor operations")
    args = parser.parse_args()

    publishers = query_publishers(args.endpoint, args.output_file)
    update_publishers_names(args.endpoint, publishers, args.meta_config_path, args.resp_agent)