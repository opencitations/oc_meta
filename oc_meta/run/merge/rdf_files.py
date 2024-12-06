#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import zipfile
from collections import defaultdict

import rdflib


def merge_file_set(identifier, file_paths, zip_output_rdf):
    """
    Unisce i file RDF parziali in un unico file finale.
    identifier: percorso base (senza estensione) per il file risultante
    file_paths: lista di file parziali da unire
    zip_output_rdf: booleano, True se i file devono essere uniti in un .zip
    """
    try:
        merged_graph = rdflib.ConjunctiveGraph()
        base_file_path = f"{identifier}.json" if not zip_output_rdf else f"{identifier}.zip"

        # Se esiste già un file base (finale), lo carica per continuare la fusione
        if os.path.exists(base_file_path):
            if zip_output_rdf:
                with zipfile.ZipFile(base_file_path, 'r') as zipf:
                    with zipf.open(zipf.namelist()[0], 'r') as json_file:
                        base_graph_data = json.load(json_file)
                        merged_graph.parse(data=json.dumps(base_graph_data), format='json-ld')
            else:
                with open(base_file_path, 'r', encoding='utf-8') as json_file:
                    base_graph_data = json.load(json_file)
                    merged_graph.parse(data=json.dumps(base_graph_data), format='json-ld')

        # Unisce tutti i file parziali
        for file_path in file_paths:
            if zip_output_rdf:
                with zipfile.ZipFile(file_path, 'r') as zipf:
                    with zipf.open(zipf.namelist()[0], 'r') as json_file:
                        graph_data = json.load(json_file)
                        merged_graph.parse(data=json.dumps(graph_data), format='json-ld')
            else:
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    graph_data = json.load(json_file)
                    merged_graph.parse(data=json.dumps(graph_data), format='json-ld')

        # Salva il file finale
        if zip_output_rdf:
            with zipfile.ZipFile(base_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.writestr(f"{os.path.basename(identifier)}.json", 
                              merged_graph.serialize(format='json-ld', indent=None, ensure_ascii=False).encode('utf-8'))
        else:
            with open(base_file_path, 'w', encoding='utf-8') as f:
                f.write(merged_graph.serialize(format='json-ld', indent=None, ensure_ascii=False))

        # Rimuove i file parziali
        for file_path in file_paths:
            os.remove(file_path)

        print(f"Fusione completata per: {base_file_path}")
    except Exception as e:
        print(f"Errore durante la fusione dei file per {identifier}: {e}")


def find_unmerged_files(base_dir):
    """
    Esplora ricorsivamente base_dir e individua i file RDF parziali non uniti.
    Restituisce una struttura dati che mappa l'identificatore base ai file parziali.
    """
    files_to_merge = defaultdict(list)

    for root, dirs, files in os.walk(base_dir):
        # Separiamo i file .json e .zip (si assume uno dei due formati)
        json_files = [f for f in files if f.endswith('.json')]
        zip_files = [f for f in files if f.endswith('.zip')]

        # Controllo JSON
        for file in json_files:
            if '_' in file:  # File parziale
                number_part = file.split('_')[0]
                identifier = os.path.join(root, number_part)
                files_to_merge[identifier].append(os.path.join(root, file))

        # Controllo ZIP
        for file in zip_files:
            if '_' in file:  # File parziale
                number_part = file.split('_')[0]
                identifier = os.path.join(root, number_part)
                files_to_merge[identifier].append(os.path.join(root, file))

    # Filtra solo quelli che effettivamente hanno più di un file parziale o che non hanno il file finale
    final_candidates = {}
    for identifier, partial_files in files_to_merge.items():
        # Controlliamo se esiste già il file finale
        zip_output_rdf = False
        base_json = f"{identifier}.json"
        base_zip = f"{identifier}.zip"
        if any(pf.endswith('.zip') for pf in partial_files):
            zip_output_rdf = True

        final_file = base_zip if zip_output_rdf else base_json
        if not os.path.exists(final_file):
            # Se non esiste il file finale o mancano fusioni
            final_candidates[identifier] = (partial_files, zip_output_rdf)
        else:
            # Potremmo controllare se ci sono file parziali malgrado il finale esista già
            # In tal caso, possiamo decidere di fonderli lo stesso.
            # Qui, per semplicità, se ci sono parziali residui anche se c'è un finale, lo includiamo.
            final_candidates[identifier] = (partial_files, zip_output_rdf)

    return final_candidates


def main():
    parser = argparse.ArgumentParser(description='Trova e fonde file RDF non fusi.')
    parser.add_argument('base_dir', help='La cartella di base da cui partire per la ricerca di file non fusi.')
    args = parser.parse_args()

    anomalies = find_unmerged_files(args.base_dir)
    if not anomalies:
        print("Nessuna anomalia trovata.")
    else:
        print("Anomalie individuate! Inizio della fusione dei file:")
        for identifier, (partial_files, zip_output_rdf) in anomalies.items():
            print(f" - Fusione in corso per: {identifier}")
    #         merge_file_set(identifier, partial_files, zip_output_rdf)


if __name__ == "__main__":
    main()