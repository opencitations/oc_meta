import argparse
import csv
import os

import pandas as pd
from retrying import retry
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm


class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, item):
        path = [item]
        while item in self.parent and self.parent[item] != item:
            item = self.parent[item]
            path.append(item)
            if len(path) > 1000:  # Limite arbitrario per evitare loop infiniti
                print(f"Warning: Long path detected: {' -> '.join(path)}")
                break
        for p in path:
            self.parent[p] = item
        return item

    def union(self, item1, item2):
        root1 = self.find(item1)
        root2 = self.find(item2)
        if root1 != root2:
            self.parent[root2] = root1


def load_csv(file_path):
    return pd.read_csv(file_path)


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def query_sparql(endpoint, uri, query_type):
    sparql = SPARQLWrapper(endpoint)
    
    if query_type == 'subjects':
        query = f"""
        SELECT ?subject WHERE {{
            ?subject ?predicate <{uri}> .
        }}
        """
    elif query_type == 'objects':
        query = f"""
        SELECT ?object WHERE {{
            <{uri}> ?predicate ?object .
            ?object ?p ?o .
        }}
        """
    
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    
    if query_type == 'subjects':
        return [result['subject']['value'] for result in results['results']['bindings']]
    elif query_type == 'objects':
        return [result['object']['value'] for result in results['results']['bindings']]


def get_all_related_entities(endpoint, uris):
    related_entities = set(uris)
    for uri in uris:
        subjects = query_sparql(endpoint, uri, 'subjects')
        objects = query_sparql(endpoint, uri, 'objects')
        related_entities.update(subjects)
        related_entities.update(objects)
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

    # Converti le liste di righe in DataFrame
    for group_id in grouped_data:
        grouped_data[group_id] = pd.DataFrame(grouped_data[group_id])

    return grouped_data


def optimize_groups(grouped_data, target_size=50):
    """
    Ottimizza i gruppi combinando quelli singoli mantenendo separate le entità interconnesse.
    
    Args:
        grouped_data (dict): Dizionario di DataFrame raggruppati
        target_size (int): Dimensione minima target per ogni gruppo
    
    Returns:
        dict: Dizionario ottimizzato dei DataFrame raggruppati
    """
    # Separa i gruppi in singoli e multipli
    single_groups = {k: v for k, v in grouped_data.items() if len(v) == 1}
    multi_groups = {k: v for k, v in grouped_data.items() if len(v) > 1}
    
    # Se non ci sono gruppi singoli, restituisci i gruppi originali
    if not single_groups:
        return grouped_data
    
    # Crea nuovi gruppi combinando quelli singoli
    combined_groups = {}
    single_items = list(single_groups.items())
    
    # Combina i gruppi singoli in gruppi della dimensione target
    current_group = []
    current_key = None
    
    for key, df in single_items:
        if len(current_group) == 0:
            current_key = key
            
        current_group.append(df)
        
        if len(current_group) >= target_size:
            combined_groups[current_key] = pd.concat(current_group, ignore_index=True)
            current_group = []
    
    # Gestisci eventuali gruppi rimanenti
    if current_group:
        if len(current_group) == 1 and multi_groups:
            # Se è rimasto un gruppo singolo e ci sono gruppi multipli,
            # aggiungiamo il gruppo singolo al gruppo multiplo più piccolo
            smallest_multi = min(multi_groups.items(), key=lambda x: len(x[1]))
            multi_groups[smallest_multi[0]] = pd.concat(
                [smallest_multi[1]] + current_group, 
                ignore_index=True
            )
        else:
            # Altrimenti lo manteniamo come gruppo separato
            combined_groups[current_key] = pd.concat(current_group, ignore_index=True)
    
    # Unisci i gruppi multipli originali con i nuovi gruppi combinati
    return {**multi_groups, **combined_groups}


def save_grouped_entities(grouped_data, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    for key, df in grouped_data.items():
        output_file = os.path.join(output_dir, f"{key.split('/')[-1]}.csv")
        print(f"Saving group with {len(df)} rows to {output_file}")
        
        try:
            df.to_csv(output_file, index=False)
        except AttributeError as e:
            print(f"Error saving file {output_file}: {str(e)}")
            try:
                df.to_csv(output_file, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
                print(f"Successfully saved using alternative method: {output_file}")
            except Exception as alt_e:
                print(f"Alternative method also failed: {str(alt_e)}")
        except Exception as e:
            print(f"Unexpected error saving file {output_file}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description='Process CSV and group entities based on SPARQL queries.')
    parser.add_argument('csv_file_path', type=str, help='Path to the input CSV file')
    parser.add_argument('output_dir', type=str, help='Directory to save the output files')
    parser.add_argument('sparql_endpoint', type=str, help='SPARQL endpoint URL')
    parser.add_argument('--min_group_size', type=int, default=50, 
                      help='Minimum target size for groups (default: 10)')

    args = parser.parse_args()
    
    df = load_csv(args.csv_file_path)
    print(f"Loaded CSV file with {len(df)} rows")
    
    grouped_entities = group_entities(df, args.sparql_endpoint)
    print(f"Initially grouped entities into {len(grouped_entities)} groups")
    
    optimized_groups = optimize_groups(grouped_entities, args.min_group_size)
    print(f"Optimized into {len(optimized_groups)} groups")
    
    save_grouped_entities(optimized_groups, args.output_dir)
    print("Finished saving grouped entities")


if __name__ == "__main__":
    main()