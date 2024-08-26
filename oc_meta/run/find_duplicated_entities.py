import argparse
import csv
from SPARQLWrapper import SPARQLWrapper, JSON

def execute_sparql_query(endpoint_url, query):
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()

def find_surviving_entities(results):
    merge_map = {}
    for result in results["results"]["bindings"]:
        entity1 = result["entity1"]["value"]
        entity2 = result["entity2"]["value"]

        if entity1 not in merge_map:
            merge_map[entity1] = entity1
        if entity2 not in merge_map:
            merge_map[entity2] = entity2

        merge_map[entity2] = find_final_entity(merge_map, entity1)

    # Costruisce una mappa finale
    final_entities = {}
    for entity, merged_into in merge_map.items():
        final_entity = find_final_entity(merge_map, merged_into)
        if final_entity not in final_entities:
            final_entities[final_entity] = []
        final_entities[final_entity].append(entity)

    return final_entities

def find_final_entity(merge_map, entity):
    visited = set()
    while merge_map[entity] != entity:
        if entity in visited:
            break
        visited.add(entity)
        entity = merge_map[entity]
    return entity

def write_results_to_csv(final_entities, csv_file_path):
    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['surviving_entity', 'merged_entities'])

        for surviving_entity, merged_entities in final_entities.items():
            filtered_entities = [e for e in merged_entities if e != surviving_entity]
            writer.writerow([surviving_entity, '; '.join(filtered_entities)])

def main():
    parser = argparse.ArgumentParser(description='Run a SPARQL query and save results to CSV.')
    parser.add_argument('endpoint', type=str, help='SPARQL endpoint URL')
    parser.add_argument('csv_path', type=str, help='Path to the CSV file to save results')
    parser.add_argument('entity_type', type=str, choices=['id', 'br', 'ra'], help='Type of entity: "id" or "br"')
    args = parser.parse_args()

    if args.entity_type == 'id':
        sparql_query = """
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            SELECT ?entity1 ?entity2 {
                ?entity1 datacite:usesIdentifierScheme ?scheme;
                    literal:hasLiteralValue ?literal.
                ?entity2 datacite:usesIdentifierScheme ?scheme;
                    literal:hasLiteralValue ?literal.
                FILTER(?entity1 != ?entity2 )
            }    
        """
    elif args.entity_type == 'ra':
        sparql_query = """
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT ?entity1 ?entity2 {
                ?entity1 datacite:hasIdentifier ?id;
                        a foaf:Agent.
                ?entity2 datacite:hasIdentifier ?id;
                        a foaf:Agent.
                FILTER(?entity1 != ?entity2 )
            }    
        """

    results = execute_sparql_query(args.endpoint, sparql_query)
    final_entities = find_surviving_entities(results)
    write_results_to_csv(final_entities, args.csv_path)

if __name__ == '__main__':
    main()