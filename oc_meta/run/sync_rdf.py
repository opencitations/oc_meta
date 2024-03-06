import argparse
import os
import zipfile
from rdflib import ConjunctiveGraph, plugin
from rdflib.serializer import Serializer

def process_zip_file(zip_path):
    """Processa un singolo file zip e carica il suo contenuto JSON-LD in un grafo RDF."""
    with zipfile.ZipFile(zip_path, 'r') as z:
        for file_info in z.infolist():
            if file_info.filename.endswith('.json'):
                with z.open(file_info) as file:
                    jsonld_content = file.read()
                    graph = ConjunctiveGraph()
                    graph.parse(data=jsonld_content, format='json-ld')
                    for entity in graph.subjects(unique=True):
                        print(entity)

def walk_directory(directory):
    """Itera su tutti i file nelle directory e sottodirectory, escludendo 'prov'."""
    for root, dirs, files in os.walk(directory):
        # Escludi directory chiamate 'prov'
        dirs[:] = [d for d in dirs if d != 'prov']
        for file in files:
            if file.endswith('.zip'):
                zip_path = os.path.join(root, file)
                process_zip_file(zip_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=str, help="La directory da processare.")
    
    args = parser.parse_args()
    
    walk_directory(args.directory)
    
    # Qui puoi fare operazioni con il grafo, come stamparlo o salvarlo
    print(graph.serialize(format='turtle').decode("utf-8"))