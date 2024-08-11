import pandas as pd
import os
import argparse
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm
from retrying import retry


class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, item):
        if item not in self.parent:
            self.parent[item] = item
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, item1, item2):
        root1 = self.find(item1)
        root2 = self.find(item2)
        if root1 != root2:
            self.parent[root2] = root1


def load_csv(file_path):
    return pd.read_csv(file_path)

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def query_sparql(endpoint, uri):
    sparql = SPARQLWrapper(endpoint)
    query = f"""
    SELECT ?subject WHERE {{
        ?subject ?predicate <{uri}> .
    }}
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    
    subjects = [result['subject']['value'] for result in results['results']['bindings']]
    return subjects

def get_all_related_entities(endpoint, uris):
    related_entities = set(uris)
    for uri in uris:
        subjects = query_sparql(endpoint, uri)
        related_entities.update(subjects)
    return related_entities

def group_entities(df, endpoint):
    uf = UnionFind()
    
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing rows"):
        surviving_entity = row['surviving_entity']
        merged_entities = row['merged_entities'].split("; ")

        all_entities = [surviving_entity] + merged_entities
        
        all_related_entities = get_all_related_entities(endpoint, all_entities)
        
        for entity in all_related_entities:
            uf.union(surviving_entity, entity)

    grouped_data = {}
    for index, row in df.iterrows():
        surviving_entity = row['surviving_entity']
        group_id = uf.find(surviving_entity)
        
        if group_id not in grouped_data:
            grouped_data[group_id] = []
        
        grouped_data[group_id].append(row)

    for group_id in grouped_data:
        grouped_data[group_id] = pd.DataFrame(grouped_data[group_id])

    return grouped_data

def save_grouped_entities(grouped_data, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    for key, df in grouped_data.items():
        output_file = os.path.join(output_dir, f"{key.split('/')[-1]}.csv")
        if len(df) > 1:
            print(f"File with multiple rows: {output_file}")
        df.to_csv(output_file, index=False)

def main():
    parser = argparse.ArgumentParser(description='Process CSV and group entities based on SPARQL queries.')
    parser.add_argument('csv_file_path', type=str, help='Path to the input CSV file')
    parser.add_argument('output_dir', type=str, help='Directory to save the output files')
    parser.add_argument('sparql_endpoint', type=str, help='SPARQL endpoint URL')

    args = parser.parse_args()
    
    df = load_csv(args.csv_file_path)
    grouped_entities = group_entities(df, args.sparql_endpoint)
    save_grouped_entities(grouped_entities, args.output_dir)

if __name__ == "__main__":
    main()