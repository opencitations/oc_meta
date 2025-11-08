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
        if item not in self.parent:
            self.parent[item] = item
            return item

        path = []
        current = item
        visited = set()

        while current != self.parent[current]:
            if current in visited:
                raise ValueError(f"Cycle detected in union-find structure at {current}")
            visited.add(current)
            path.append(current)
            current = self.parent[current]

        for node in path:
            self.parent[node] = current

        return current

    def union(self, item1, item2):
        root1 = self.find(item1)
        root2 = self.find(item2)
        if root1 != root2:
            self.parent[root2] = root1


def load_csv(file_path):
    df = pd.read_csv(file_path)
    required_columns = ["surviving_entity", "merged_entities"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"CSV file missing required columns: {missing_columns}")
    return df


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def query_sparql_batch(endpoint, uris, batch_size=10):
    """
    Query SPARQL for related entities in batches.

    Args:
        endpoint: SPARQL endpoint URL
        uris: List of URIs to query
        batch_size: Number of URIs to process in a single query

    Returns:
        Set of all related entities
    """
    related_entities = set()
    sparql = SPARQLWrapper(endpoint)

    for i in range(0, len(uris), batch_size):
        batch_uris = uris[i:i + batch_size]

        subject_clauses = []
        object_clauses = []

        for uri in batch_uris:
            subject_clauses.append(f"{{?entity ?p <{uri}>}}")
            object_clauses.append(f"{{<{uri}> ?p ?entity}}")

        query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?entity WHERE {{
                {{
                    {' UNION '.join(subject_clauses + object_clauses)}
                }}
                ?entity ?p2 ?o2 .
                FILTER (?p != rdf:type)
                FILTER (?p != datacite:usesIdentifierScheme)
                FILTER (?p != pro:withRole)
            }}
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        for result in results['results']['bindings']:
            if result['entity']['type'] == 'uri':
                related_entities.add(result['entity']['value'])

    return related_entities


def get_all_related_entities(endpoint, uris, batch_size=10):
    """
    Get all related entities for a list of URIs using batch queries.

    Args:
        endpoint: SPARQL endpoint URL
        uris: List of URIs to query
        batch_size: Number of URIs to process in a single query

    Returns:
        Set of all related entities including input URIs
    """
    related_entities = set(uris)
    batch_results = query_sparql_batch(endpoint, uris, batch_size)
    related_entities.update(batch_results)
    return related_entities


def group_entities(df, endpoint):
    uf = UnionFind()
    rows_list = []

    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing rows"):
        surviving_entity = row['surviving_entity']
        merged_entities = row['merged_entities'].split("; ")

        all_entities = [surviving_entity] + merged_entities

        all_related_entities = get_all_related_entities(endpoint, all_entities)

        for entity in all_related_entities:
            uf.union(surviving_entity, entity)

        rows_list.append(row)

    grouped_data = {}
    for row in rows_list:
        surviving_entity = row['surviving_entity']
        group_id = uf.find(surviving_entity)

        if group_id not in grouped_data:
            grouped_data[group_id] = []

        grouped_data[group_id].append(row)

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