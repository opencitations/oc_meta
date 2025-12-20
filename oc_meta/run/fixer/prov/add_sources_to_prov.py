import argparse
import os

import redis
from sparqlite import SPARQLClient

def create_redis_connection():
    return redis.Redis(host='localhost', port=6379, db=14)

def extract_omid(entity):
    parts = entity.split('/')
    return f"omid:{parts[-2]}/{parts[-1]}", parts[-2]

def search_omid_in_redis(r, omid):
    result = r.get(omid)
    return result.decode() if result else None

def run_sparql_query(sparql_endpoint, entity_id, entity_class):
    with SPARQLClient(sparql_endpoint, max_retries=3, backoff_factor=5, timeout=3600) as client:
        if entity_class == "id":
            query = f"""
                PREFIX datacite: <http://purl.org/spar/datacite/>
                SELECT ?br WHERE {{
                    <{entity_id}> ^datacite:hasIdentifier ?br.
                }}
            """
        elif entity_class == "ar":
            query = f"""
                PREFIX pro: <http://purl.org/spar/pro/>
                SELECT ?br WHERE {{
                    <{entity_id}> ^pro:isDocumentContextFor ?br.
                }}
            """
        elif entity_class == "ra":
            query = f"""
                PREFIX pro: <http://purl.org/spar/pro/>
                SELECT ?br WHERE {{
                    <{entity_id}> ^pro:isHeldBy/isDocumentContextFor ?br.
                }}
            """
        elif entity_class == "re":
            query = f"""
                PREFIX frbr: <http://purl.org/vocab/frbr/core#>
                SELECT ?br WHERE {{
                    <{entity_id}> ^frbr:embodiment ?br.
                }}
            """
        elif entity_class == "br":
            query = f"""
                SELECT ?type WHERE {{
                    <{entity_id}> a ?type.
                }}
            """
            results = client.query(query)
            for result in results["results"]["bindings"]:
                br_type = result["type"]["value"]
            if br_type in {'http://purl.org/spar/fabio/JournalIssue', 'http://purl.org/spar/fabio/JournalVolume', 'http://purl.org/spar/fabio/Journal'}:
                query = f"""
                    PREFIX fabio: <http://purl.org/spar/fabio/>
                    PREFIX frbr: <http://purl.org/vocab/frbr/core#>
                    SELECT ?br WHERE {{
                        <{entity_id}> ^frbr:partOf* ?br.
                        ?br a fabio:JournalArticle.
                    }}
                """
                results = client.query(query)
                for result in results["results"]["bindings"]:
                    return result["br"]["value"]
            else:
                query = f"""
                    PREFIX fabio: <http://purl.org/spar/fabio/>
                    PREFIX frbr: <http://purl.org/vocab/frbr/core#>
                    SELECT ?br WHERE {{
                        <{entity_id}> ^frbr:partOf ?br.
                    }}
                """
                results = client.query(query)
                for result in results["results"]["bindings"]:
                    return result["br"]["value"]
        results = client.query(query)
        for result in results["results"]["bindings"]:
            return result["br"]["value"]

def explore_folder(folder_path, sparql_endpoint, r):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file == 'se.zip':
                file_path = os.path.join(root, file)
                print(file_path)
                # with zipfile.ZipFile(file_path, 'r') as zip_ref:
                #     for zip_info in zip_ref.infolist():
                #         with zip_ref.open(zip_info) as f:
                #             data = json.loads(f.read().decode())
                #             for graph in data:
                #                 for entity in graph['@graph']:
                #                     entity_id = entity['http://www.w3.org/ns/prov#specializationOf'][0]['@id']
                #                     if 'http://www.w3.org/ns/prov#hadPrimarySource' not in entity:
                #                         omid, entity_class = extract_omid(entity_id)
                #                         primary_source = search_omid_in_redis(r, omid)
                #                         if primary_source is None:
                #                             sparql_result = run_sparql_query(sparql_endpoint, entity_id, entity_class)
                #                             if sparql_result:
                #                                 br, _ = extract_omid(sparql_result)
                #                                 primary_source = search_omid_in_redis(r, br)
                                            

def main():
    parser = argparse.ArgumentParser(description="Explore folder for se.zip files and display their contents.")
    parser.add_argument("folder_path", type=str, help="Path of the folder to explore")
    parser.add_argument("sparql_endpoint", type=str, help="SPARQL endpoint URL")
    args = parser.parse_args()
    r = create_redis_connection()
    explore_folder(args.folder_path, args.sparql_endpoint, r)

if __name__ == "__main__":
    main()