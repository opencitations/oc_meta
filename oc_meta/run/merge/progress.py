import argparse
import csv
import os

def get_entity_type(entity_url):
    parts = entity_url.split('/')
    if 'oc' in parts and 'meta' in parts:
        try:
            return parts[parts.index('meta') + 1]
        except IndexError:
            return None
    return None

def process_csv(csv_file, entities_types_total, entities_types_done):
    with open(csv_file, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            entities = [row['surviving_entity']] + row['merged_entities'].split('; ')
            for entity in entities:
                entity_type = get_entity_type(entity)
                if entity_type:
                    if entity_type not in entities_types_total:
                        entities_types_total[entity_type] = set()
                    entities_types_total[entity_type].add(entity)
                    if row['Done'].strip().lower() == 'true':
                        if entity_type not in entities_types_done:
                            entities_types_done[entity_type] = set()
                        entities_types_done[entity_type].add(entity)

def main():
    parser = argparse.ArgumentParser(description="Conta le entità uniche per tipo, evitando duplicati tra file e cartella, con 'Done'=True.")
    parser.add_argument("csv_path", help="Percorso al file CSV di input")
    parser.add_argument("folder_path", help="Percorso alla cartella contenente altri file CSV")
    args = parser.parse_args()

    entities_types_total = {}
    entities_types_done = {}

    # Processa il file CSV principale
    process_csv(args.csv_path, entities_types_total, entities_types_done)

    # Processa tutti i file nella cartella specificata
    if os.path.exists(args.folder_path):
        for filename in os.listdir(args.folder_path):
            file_path = os.path.join(args.folder_path, filename)
            process_csv(file_path, entities_types_total, entities_types_done)

    # Stampa i risultati
    for entity_type, entities in entities_types_total.items():
        total = len(entities)
        done = len(entities_types_done.get(entity_type, set()))
        print(f"Tipo di entità: {entity_type}, Totale entità uniche: {total}, Con 'Done'=True: {done}")

if __name__ == "__main__":
    main()