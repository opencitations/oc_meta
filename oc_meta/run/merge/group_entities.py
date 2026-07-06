# SPDX-FileCopyrightText: 2024-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import csv
import os

import pandas as pd
import yaml
from rich_argparse import RichHelpFormatter
from tqdm import tqdm

from oc_meta.lib.file_manager import find_rdf_file
from oc_meta.run.merge.closure import compute_related_closure
from oc_meta.run.merge.csv_utils import parse_merged_entities


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


def group_entities(df, endpoint, dir_split=10000, items_per_file=1000, zip_output=True):
    """
    Group entities based on RDF connections and file range conflicts.

    Args:
        df: DataFrame with columns 'surviving_entity' and 'merged_entities'
        endpoint: SPARQL endpoint URL
        dir_split: Directory split number (default: 1000)
        items_per_file: Items per file (default: 1000)
        zip_output: Whether files are zipped (default: True)

    Returns:
        Dict of group_id -> DataFrame with grouped rows
    """
    uf = UnionFind()
    rows_list = []

    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing rows"):
        surviving_entity = row["surviving_entity"]
        merged_entities = parse_merged_entities(row["merged_entities"])

        all_entities = [surviving_entity] + merged_entities

        # Union over the full merge closure so that merges touching a shared
        # entity (including cascaded containers) land in the same group.
        all_related_entities = compute_related_closure(endpoint, all_entities)
        for entity in all_related_entities:
            uf.union(surviving_entity, entity)

        # Union for file range conflicts (only for IDs being merged, not related entities)
        for entity in all_entities:
            entity_file = find_rdf_file(
                entity, "", dir_split, items_per_file, zip_output
            )
            uf.union(surviving_entity, f"FILE:{entity_file}")

        rows_list.append(row)

    grouped_data = {}
    for row in rows_list:
        surviving_entity = row["surviving_entity"]
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
                [smallest_multi[1]] + current_group, ignore_index=True
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
                df.to_csv(
                    output_file,
                    index=False,
                    encoding="utf-8",
                    quoting=csv.QUOTE_NONNUMERIC,
                )
                print(f"Successfully saved using alternative method: {output_file}")
            except Exception as alt_e:
                print(f"Alternative method also failed: {str(alt_e)}")
        except Exception as e:
            print(f"Unexpected error saving file {output_file}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(
        description="Process CSV and group entities based on SPARQL queries.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("csv_file_path", type=str, help="Path to the input CSV file")
    parser.add_argument(
        "output_dir", type=str, help="Directory to save the output files"
    )
    parser.add_argument(
        "meta_config", type=str, help="Path to meta configuration YAML file"
    )
    parser.add_argument(
        "--min_group_size",
        type=int,
        default=50,
        help="Minimum target size for groups (default: 50)",
    )

    args = parser.parse_args()

    with open(args.meta_config, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    sparql_endpoint = config["triplestore_url"]
    dir_split = config["dir_split_number"]
    items_per_file = config["items_per_file"]
    zip_output = config["zip_output_rdf"]

    df = load_csv(args.csv_file_path)
    print(f"Loaded CSV file with {len(df)} rows")
    print(
        f"Configuration: dir_split={dir_split}, items_per_file={items_per_file}, zip_output={zip_output}"
    )

    grouped_entities = group_entities(
        df, sparql_endpoint, dir_split, items_per_file, zip_output
    )
    print(f"Initially grouped entities into {len(grouped_entities)} groups")

    optimized_groups = optimize_groups(grouped_entities, args.min_group_size)
    print(f"Optimized into {len(optimized_groups)} groups")

    save_grouped_entities(optimized_groups, args.output_dir)
    print("Finished saving grouped entities")


if __name__ == "__main__":
    main()
