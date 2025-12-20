import argparse
import csv

from sparqlite import SPARQLClient
from tqdm import tqdm

def get_entity_type(entity_url):
    parts = entity_url.split('/')
    if 'oc' in parts and 'meta' in parts:
        try:
            return parts[parts.index('meta') + 1]
        except IndexError:
            return None
    return None

def query_sparql(client, entity):
    query = f"""
    PREFIX fabio: <http://purl.org/spar/fabio/>
    PREFIX frbr: <http://purl.org/vocab/frbr/core#>
    SELECT ?type ?partOfType
    WHERE {{
        <{entity}> a ?type .
        FILTER (?type != fabio:Expression)
        OPTIONAL {{
            <{entity}> frbr:partOf ?container .
            ?container a ?partOfType .
            FILTER (?partOfType != fabio:Expression)
        }}
    }}
    """
    return client.query(query)

def process_csv(input_csv, output_csv, endpoint_url):
    with open(input_csv, mode='r', newline='', encoding='utf-8') as infile, \
         open(output_csv, mode='w', newline='', encoding='utf-8') as outfile:

        reader = list(csv.DictReader(infile))
        writer = csv.DictWriter(outfile, fieldnames=['entity', 'type', 'partOfType'])
        writer.writeheader()

        with SPARQLClient(endpoint_url, max_retries=3, backoff_factor=5, timeout=3600) as client:
            for row in tqdm(reader, desc='Processing rows', unit='row'):
                entity = row['surviving_entity']
                if get_entity_type(entity) == 'br':
                    results = query_sparql(client, entity)
                    for result in results["results"]["bindings"]:
                        writer.writerow({
                            'entity': entity,
                            'type': result["type"]["value"],
                            'partOfType': result["partOfType"]["value"] if "partOfType" in result else None
                        })

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find the containment chain of bibliographic resources')
    parser.add_argument('input_csv', help='The path to the input CSV file')
    parser.add_argument('output_csv', help='The path to the output CSV file')
    parser.add_argument('endpoint_url', help='The SPARQL endpoint URL')
    args = parser.parse_args()

    process_csv(args.input_csv, args.output_csv, args.endpoint_url)