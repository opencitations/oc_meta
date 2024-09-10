import argparse
import gzip
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from tqdm import tqdm

def search_in_file(file_path, search_string):
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line_number, line in enumerate(f, 1):
                if search_string in line:
                    return file_path, line_number, line.strip()
    except Exception as e:
        print(f"Errore durante la lettura del file {file_path}: {e}")
    return None

def main():
    parser = argparse.ArgumentParser(description="Cerca una stringa in file .nq.gz")
    parser.add_argument("folder", help="Percorso della cartella contenente i file .nq.gz")
    parser.add_argument("search_string", help="Stringa da cercare nei file")
    args = parser.parse_args()

    folder_path = Path(args.folder)
    search_string = args.search_string

    if not folder_path.is_dir():
        print(f"Il percorso specificato non è una cartella valida: {folder_path}")
        return

    nq_files = list(folder_path.glob("*.nq.gz"))

    if not nq_files:
        print(f"Nessun file .nq.gz trovato nella cartella: {folder_path}")
        return

    print(f"Ricerca di '{search_string}' in {len(nq_files)} file...")

    results = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(search_in_file, file, search_string) for file in nq_files]
        for future in tqdm(as_completed(futures), total=len(nq_files), desc="Progresso", unit="file"):
            result = future.result()
            if result:
                results.append(result)

    if results:
        print(f"\nLa stringa '{search_string}' è stata trovata nei seguenti file:")
        for file_path, line_number, line in results:
            print(f"\nFile: {file_path}")
            print(f"Riga numero: {line_number}")
            print(f"Contenuto: {line}")
    else:
        print(f"\nLa stringa '{search_string}' non è stata trovata in nessun file.")

if __name__ == "__main__":
    main()