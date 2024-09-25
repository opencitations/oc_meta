import argparse
import csv
from typing import Set, List

def read_csv(file_path: str) -> Set[str]:
    entities = set()
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        for row in reader:
            surviving_entity = row[0]
            merged_entities = row[1].split('; ')
            entities.add(surviving_entity)
            entities.update(merged_entities)
    return entities

def find_differences(file_a: str, file_b: str) -> tuple[List[str], List[str]]:
    entities_a = read_csv(file_a)
    entities_b = read_csv(file_b)
    
    in_a_not_b = list(entities_a - entities_b)
    in_b_not_a = list(entities_b - entities_a)
    
    return in_a_not_b, in_b_not_a

def main():
    parser = argparse.ArgumentParser(description="Find differences between two CSV files.")
    parser.add_argument("file_a", help="Path to the first CSV file")
    parser.add_argument("file_b", help="Path to the second CSV file")
    args = parser.parse_args()

    in_a_not_b, in_b_not_a = find_differences(args.file_a, args.file_b)

    print("Entities in A but not in B:")
    for entity in in_a_not_b:
        print(entity)

    print("\nEntities in B but not in A:")
    for entity in in_b_not_a:
        print(entity)

if __name__ == "__main__":
    main()