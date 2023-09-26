import os
import sys


def trova_file_mancanti(file_txt):
    elenco_file = []
    file_mancanti = []

    # Legge il file txt e crea l'elenco dei file
    with open(file_txt, "r") as file:
        elenco_file = file.read().splitlines()

    # Estrae i numeri dai nomi dei file e li converte in interi
    numeri_file = [int(os.path.splitext(nome_file)[0]) for nome_file in elenco_file]

    # Trova i file mancanti nella sequenza numerica
    ultimo_numero = max(numeri_file)
    for numero in range(1, ultimo_numero + 1):
        if numero not in numeri_file:
            file_mancanti.append(str(numero) + ".csv")

    return file_mancanti

# Verifica se è stato specificato il nome del file txt come argomento
if len(sys.argv) < 2:
    print("Usage: python script.py file.txt")
    sys.exit(1)

# Ottiene il nome del file txt dalla riga di comando
file_txt = sys.argv[1]

# Trova i file mancanti nella sequenza numerica
file_mancanti = trova_file_mancanti(file_txt)

# Stampa l'elenco dei file mancanti
if file_mancanti:
    print("File mancanti:")
    for file in file_mancanti:
        print(file)
else:
    print("Nessun file mancante nella sequenza numerica.")