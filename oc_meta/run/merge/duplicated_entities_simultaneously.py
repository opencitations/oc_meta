import argparse
import concurrent.futures
import csv
import os
from typing import List, Dict

from oc_meta.plugins.editor import MetaEditor
from oc_ocdm.graph import GraphSet
from rdflib import URIRef
from tqdm import tqdm


class EntityMerger:
    def __init__(self, meta_config: str, resp_agent: str, entity_types: List[str], stop_file_path: str, workers: int):
        self.meta_config = meta_config
        self.resp_agent = resp_agent
        self.entity_types = entity_types
        self.stop_file_path = stop_file_path
        self.workers = workers

    @staticmethod
    def get_entity_type(entity_url: str) -> str:
        parts = entity_url.split('/')
        if 'oc' in parts and 'meta' in parts:
            try:
                return parts[parts.index('meta') + 1]
            except IndexError:
                return None
        return None

    @staticmethod
    def read_csv(csv_file: str) -> List[Dict]:
        data = []
        with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if 'Done' not in row:
                    row['Done'] = 'False'
                data.append(row)
        return data

    @staticmethod
    def write_csv(csv_file: str, data: List[Dict]):
        fieldnames = data[0].keys()
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow(row)

    @staticmethod
    def count_csv_rows(csv_file: str) -> int:
        with open(csv_file, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f) - 1  # Subtract 1 to exclude the header row

    def process_file(self, csv_file: str) -> str:
        """Process a single CSV file with its own MetaEditor instance"""
        data = self.read_csv(csv_file)
        meta_editor = MetaEditor(self.meta_config, self.resp_agent, save_queries=True)
        modified = False

        # Create a GraphSet for the current file
        g_set = GraphSet(meta_editor.base_iri, custom_counter_handler=meta_editor.counter_handler)

        for row in data:
            if os.path.exists(self.stop_file_path):
                break

            entity_type = self.get_entity_type(row['surviving_entity'])
            if row.get('Done') != 'True' and entity_type in self.entity_types:
                surviving_entity = URIRef(row['surviving_entity'])
                merged_entities = row['merged_entities'].split('; ')

                for merged_entity in merged_entities:
                    merged_entity = merged_entity.strip()
                    try:
                        meta_editor.merge(g_set, surviving_entity, URIRef(merged_entity))
                    except ValueError:
                        continue

                row['Done'] = 'True'
                modified = True
        if modified:
            meta_editor.save(g_set)
            self.write_csv(csv_file, data)

        return csv_file

    def process_folder(self, csv_folder: str):
        """Process all CSV files in a folder using parallel processing"""
        if os.path.exists(self.stop_file_path):
            os.remove(self.stop_file_path)

        csv_files = [os.path.join(csv_folder, file) 
                    for file in os.listdir(csv_folder) 
                    if file.endswith('.csv')]

        # Filter CSV files based on number of rows and workers
        if self.workers > 4:
            csv_files = [file for file in csv_files 
                        if self.count_csv_rows(file) <= 10000]

        with concurrent.futures.ProcessPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(self.process_file, csv_file): csv_file 
                for csv_file in csv_files
            }

            for future in tqdm(concurrent.futures.as_completed(futures), 
                             total=len(futures), 
                             desc="Overall Progress"):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing file: {e}")


def main():
    parser = argparse.ArgumentParser(description="Merge entities from CSV files in a folder.")
    parser.add_argument('csv_folder', type=str, 
                       help="Path to the folder containing CSV files")
    parser.add_argument('meta_config', type=str, 
                       help="Meta configuration string")
    parser.add_argument('resp_agent', type=str, 
                       help="Responsible agent string")
    parser.add_argument('--entity_types', nargs='+', 
                       default=['ra', 'br', 'id'], 
                       help="Types of entities to merge (ra, br, id)")
    parser.add_argument('--stop_file', type=str, 
                       default="stop.out", 
                       help="Path to the stop file")
    parser.add_argument('--workers', type=int, 
                       default=4, 
                       help="Number of parallel workers")
    
    args = parser.parse_args()

    merger = EntityMerger(
        meta_config=args.meta_config,
        resp_agent=args.resp_agent,
        entity_types=args.entity_types,
        stop_file_path=args.stop_file,
        workers=args.workers
    )
    
    merger.process_folder(args.csv_folder)


if __name__ == "__main__":
    main()