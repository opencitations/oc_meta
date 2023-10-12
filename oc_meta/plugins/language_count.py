import csv
import os
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

import pycountry
from langdetect import detect
from tqdm import tqdm

cartella = "E:/meta_csv_output"  # Sostituisci con il percorso della tua cartella

def get_csv_data(filepath: str) -> list:
    if not os.path.splitext(filepath)[1].endswith('.csv'):
        return []

    cur_field_size = 128
    field_size_changed = False
    data = []

    while not data:
        try:
            with open(filepath, 'r', encoding='utf8') as data_initial:
                valid_data = (line.replace('\0', '').replace('\n', '') for line in data_initial)
                data = list(csv.DictReader(valid_data, delimiter=','))
        except csv.Error:
            cur_field_size *= 2
            csv.field_size_limit(cur_field_size)
            field_size_changed = True

    if field_size_changed:
        csv.field_size_limit(128)
    return data

def get_full_language_name(lang_code):
    try:
        return pycountry.languages.get(alpha_2=lang_code).name
    except AttributeError:
        return lang_code  # Se il codice non viene trovato, restituisci il codice originale

def process_file(file):
    data = get_csv_data(os.path.join(cartella, file))
    local_lingue = []

    for riga in data:
        try:
            title = riga['title']
            lingua = detect(title)
            full_lang_name = get_full_language_name(lingua)
            local_lingue.append(full_lang_name)
        except:
            pass

    return local_lingue

def main():
    # Crea una lista dei file csv nella cartella
    file_csv = [f for f in os.listdir(cartella) if f.endswith('.csv')]

    results = []

    totale_file = len(file_csv)

    with ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(process_file, file_csv), total=totale_file, desc="Elaborazione files"))

    lingue = [lang for sublist in results for lang in sublist]

    # Conta quante volte ogni lingua è stata rilevata
    conteggio = Counter(lingue)

    # Salva i risultati in un file CSV
    with open('E:/lingue_in_meta.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Lingua', 'Conteggio'])
        for lingua, count in conteggio.items():
            writer.writerow([lingua, count])
    print("Elaborazione completata e risultati salvati in risultati.csv!")\

if __name__ == '__main__':
    main()