import os
import csv
import argparse
from tqdm import tqdm
from oc_meta.core.curator import is_a_valid_row

def read_csv(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        return [row for row in csv.DictReader(file)]

def get_all_citation_ids(citation_folder):
    citation_ids = set()
    citation_files = [f for f in os.listdir(citation_folder) if os.path.isfile(os.path.join(citation_folder, f)) and f.endswith('.csv')]
    
    for file_name in tqdm(citation_files, desc="Reading citation files"):
        file_path = os.path.join(citation_folder, file_name)
        citations = read_csv(file_path)
        for citation in citations:
            citation_ids.add(citation['citing'])
            citation_ids.add(citation['cited'])
    return citation_ids

def validate_metadata(metadata_folder, citation_ids):
    metadata_files = [f for f in os.listdir(metadata_folder) if os.path.isfile(os.path.join(metadata_folder, f)) and f.endswith('.csv')]
    
    for file_name in tqdm(metadata_files, desc="Validating metadata files"):
        file_path = os.path.join(metadata_folder, file_name)
        metadata = read_csv(file_path)
        for row in tqdm(metadata, desc=f"Processing {file_name}", leave=False):
            if not is_a_valid_row(row):
                ids = row['id'].split()
                if not any(id in citation_ids for id in ids):
                    print(f"Invalid row without citations found: {row}")

def main():
    parser = argparse.ArgumentParser(description="Validate metadata CSV files.")
    parser.add_argument('metadata_folder', type=str, help="Path to the metadata CSV folder.")
    parser.add_argument('citation_folder', type=str, help="Path to the citation CSV folder.")
    args = parser.parse_args()
    
    citation_ids = get_all_citation_ids(args.citation_folder)
    validate_metadata(args.metadata_folder, citation_ids)

if __name__ == "__main__":
    main()