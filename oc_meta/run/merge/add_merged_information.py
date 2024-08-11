import argparse
import csv
import os
from tqdm import tqdm

def read_main_csv(main_csv):
    """Reads the main CSV file into a dictionary for quick lookup."""
    lookup_dict = {}
    with open(main_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            lookup_dict[row['surviving_entity']] = row['Done']
    return lookup_dict

def update_csv_files(directory, lookup_dict):
    """Updates CSV files in the directory with the Done status from the lookup dictionary."""
    for filename in tqdm(os.listdir(directory)):
        if filename.endswith(".csv"):
            filepath = os.path.join(directory, filename)
            update_csv_file(filepath, lookup_dict)

def update_csv_file(filepath, lookup_dict):
    """Updates a single CSV file with the Done status from the lookup dictionary."""
    updated_rows = []
    with open(filepath, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        fieldnames = reader.fieldnames
        if 'Done' not in fieldnames:
            fieldnames.append('Done')
        
        for row in reader:
            if row['surviving_entity'] in lookup_dict:
                row['Done'] = lookup_dict[row['surviving_entity']]
            else:
                row['Done'] = 'False'
            updated_rows.append(row)

    with open(filepath, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

def main():
    parser = argparse.ArgumentParser(description="Update CSV files with Done status based on the main CSV file.")
    parser.add_argument('main_csv', help="Path to the main CSV file")
    parser.add_argument('directory', help="Directory containing CSV files to be updated")
    args = parser.parse_args()

    lookup_dict = read_main_csv(args.main_csv)
    update_csv_files(args.directory, lookup_dict)

if __name__ == '__main__':
    main()