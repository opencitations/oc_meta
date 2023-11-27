import argparse
import csv
import os
import re

import psycopg2


def get_csv_data(filepath:str):
    if not os.path.splitext(filepath)[1].endswith('.csv'):
        return list()
    field_size_changed = False
    cur_field_size = 128
    data = list()
    while not data:
        try:
            with open(filepath, 'r', encoding='utf8') as f:
                data = list(csv.DictReader(f, delimiter=','))
        except csv.Error:
            cur_field_size *= 2
            csv.field_size_limit(cur_field_size)
            field_size_changed = True
    if field_size_changed:
        csv.field_size_limit(128)
    return data

def extract_omids(text, field, regex_pattern):
    identifiers = []
    if field == 'id':
        identifiers.extend([identifier for identifier in text.split() if not identifier.startswith('omid:')])
    elif field in {'author', 'editor', 'publisher'}:
        agents = text.split('; ')
        for agent in agents:
            if re.match(regex_pattern, agent):
                identifiers.extend([identifier for identifier in re.match(regex_pattern, agent).group(2).split() if not identifier.startswith('omid:')])
    elif field in 'venue':
        if re.match(regex_pattern, text):
            identifiers.extend([identifier for identifier in re.match(regex_pattern, text).group(2).split() if not identifier.startswith('omid:')])
    return identifiers

def save_to_database(omids, source, connection):
    with connection.cursor() as cursor:
        for omid in omids:
            cursor.execute("INSERT INTO omid_table (omid, source) VALUES (%s, %s)", (omid, source))
    connection.commit()

def main():
    parser = argparse.ArgumentParser(description='Process CSV files in a directory.')
    parser.add_argument('directory', type=str, help='Directory containing CSV files')
    args = parser.parse_args()

    regex_pattern = r'\s*(.*?)\s*\[\s*((?:[^\s]+:[^\s]+)?(?:\s+[^\s]+:[^\s]+)*)\s*\]'
    connection = psycopg2.connect("dbname='oc_meta' user='postgres' password='Permesiva1!'")

    for filename in os.listdir(args.directory):
        if filename.endswith('.csv'):
            reader = get_csv_data(os.path.join(args.directory, filename))
            for row in reader:
                for field in ['id', 'author', 'editor', 'publisher', 'venue']:
                    omids = extract_omids(row[field], field, regex_pattern)
                    print(omids)
                    # save_to_database(omids, "Crossref", connection)

    connection.close()

if __name__ == "__main__":
    main()