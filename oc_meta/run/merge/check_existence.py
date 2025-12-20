import argparse
import csv
import os
from typing import Dict, Set

from sparqlite import SPARQLClient
from tqdm import tqdm


def get_entity_existence(client: SPARQLClient, entity: str) -> bool:
    """
    Verifica l'esistenza di un'entità tramite query SPARQL
    """
    query = f"""
    ASK {{
        <{entity}> ?p ?o
    }}
    """

    try:
        result = client.query(query)
        return result.get('boolean', False)
    except Exception as e:
        print(f"Errore nella verifica dell'entità {entity}: {str(e)}")
        return False

def process_csv(file_path: str, endpoint_url: str) -> Dict[str, Set[str]]:
    """
    Processa un singolo file CSV e restituisce le entità non done
    """
    entities_status = {
        'existing': set(),
        'missing': set()
    }

    with SPARQLClient(endpoint_url, max_retries=3, backoff_factor=5, timeout=3600) as client:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Controlla solo le righe con Done=false
                if not row.get('Done') or row.get('Done').strip().lower() == 'false':
                    # Prendi le entità dalla seconda colonna
                    entities = row[list(row.keys())[1]].split('; ')
                    for entity in entities:
                        entity = entity.strip()
                        if entity:
                            # Verifica l'esistenza dell'entità
                            if get_entity_existence(client, entity):
                                entities_status['existing'].add(entity)
                            else:
                                entities_status['missing'].add(entity)

    return entities_status

def main():
    parser = argparse.ArgumentParser(description="Verifica l'esistenza delle entità non done tramite query SPARQL")
    parser.add_argument("folder_path", help="Percorso alla cartella contenente i file CSV")
    parser.add_argument("endpoint_url", help="URL dell'endpoint SPARQL")
    args = parser.parse_args()

    total_stats = {
        'existing': set(),
        'missing': set()
    }

    # Processa tutti i file nella cartella
    if os.path.exists(args.folder_path):
        csv_files = [f for f in os.listdir(args.folder_path) if f.endswith('.csv')]
        for filename in tqdm(csv_files, desc="Processando i file"):
            file_path = os.path.join(args.folder_path, filename)
            file_stats = process_csv(file_path, args.endpoint_url)
            
            # Aggiorna le statistiche totali
            total_stats['existing'].update(file_stats['existing'])
            total_stats['missing'].update(file_stats['missing'])
            
    # Stampa statistiche finali
    print("\nStatistiche Totali:")
    print(f"Totale entità uniche esistenti: {len(total_stats['existing'])}")
    print(f"Totale entità uniche mancanti: {len(total_stats['missing'])}")
    print(f"Percentuale di entità esistenti: {(len(total_stats['existing']) / (len(total_stats['existing']) + len(total_stats['missing'])) * 100):.2f}%")

    # Opzionalmente, stampa le entità mancanti
    if total_stats['missing']:
        print("\nEntità mancanti:")
        for entity in sorted(total_stats['missing']):
            print(entity)

if __name__ == "__main__":
    main()