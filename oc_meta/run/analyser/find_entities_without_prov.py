import argparse
import os
import zipfile
from rdflib import Graph, URIRef, Dataset
from tqdm import tqdm

def count_data_files(input_folder):
    count = 0
    for root, dirs, files in os.walk(input_folder):
        if "prov" in dirs:
            dirs.remove("prov")
        count += sum(1 for f in files if f.endswith('.zip'))
    return count

def check_provenance(input_folder, output_file):
    total_files = count_data_files(input_folder)
    entities_without_provenance = []

    with tqdm(total=total_files, desc="Processing files") as pbar:
        for root, dirs, files in os.walk(input_folder):
            if "prov" in dirs:
                dirs.remove("prov")
            
            for file in files:
                if file.endswith('.zip'):
                    zip_path = os.path.join(root, file)
                    entities_to_check = set()
                    
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        for json_file in zip_ref.namelist():
                            if json_file.endswith('.json'):
                                with zip_ref.open(json_file) as f:
                                    g = Graph()
                                    g.parse(f, format='json-ld')
                                    
                                    for s in g.subjects():
                                        if isinstance(s, URIRef):
                                            entities_to_check.add(str(s))
                    
                    # Costruisci il percorso del file di provenance
                    prov_folder = os.path.join(os.path.dirname(zip_path), 'prov')
                    prov_file = os.path.join(prov_folder, 'se.zip')
                    
                    if os.path.exists(prov_file):
                        with zipfile.ZipFile(prov_file, 'r') as prov_zip:
                            ds = Dataset()
                            for prov_json in prov_zip.namelist():
                                if prov_json.endswith('.json'):
                                    with prov_zip.open(prov_json) as prov_f:
                                        ds.parse(prov_f, format='json-ld')
                            
                            for entity_id in entities_to_check:
                                prov_graph_id = f"{entity_id}/prov/"
                                if URIRef(prov_graph_id) not in ds.graphs():
                                    entities_without_provenance.append(entity_id)
                    else:
                        entities_without_provenance.extend(entities_to_check)
                
                pbar.update(1)

    # Salva le entità senza provenance su file
    with open(output_file, 'w') as f:
        for entity in entities_without_provenance:
            f.write(f"{entity}\n")

def main():
    parser = argparse.ArgumentParser(description="Controlla la provenance delle entità RDF in file ZIP JSON-LD.")
    parser.add_argument("input_folder", help="Cartella di input contenente i file ZIP JSON-LD")
    parser.add_argument("output_file", help="File di output per le entità senza provenance")
    args = parser.parse_args()
    
    check_provenance(args.input_folder, args.output_file)
    print(f"Entità senza provenance salvate in: {args.output_file}")

if __name__ == "__main__":
    main()