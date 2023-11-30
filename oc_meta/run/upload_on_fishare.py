import requests
import os 

# Sostituire con il tuo token API e l'ID dell'articolo
API_TOKEN = '861ecbf7449c7581100739bdaf17a7f318beada2563e8f94dc4d2168c825bd05697cdc90533a027b8210741dc9a8959f64aa2055e4a50441fb6e8fe1b1c4f543'
ARTICLE_ID = '21747536'

# Lista dei file da caricare
files_to_upload = ['/home/arcangelo/meta_output_current/rdf/ar.zip']

# Endpoint base di Figshare
BASE_URL = 'https://api.figshare.com/v2/account/articles'

def create_file(article_id, file_name, file_path):
    """ Crea un file nell'articolo su Figshare. """
    url = f"{BASE_URL}/{article_id}/files"
    headers = {'Authorization': f'token {API_TOKEN}'}
    data = {'name': file_name, 'size': os.path.getsize(file_path)}
    response = requests.post(url, headers=headers, json=data)
    return response.json()

def upload_file(upload_url, file_path):
    """ Carica un file all'URL specificato. """
    headers = {'Authorization': f'token {API_TOKEN}'}
    with open(file_path, 'rb') as file_data:
        requests.put(upload_url, headers=headers, data=file_data, verify=False)

def main():
    for file_path in files_to_upload:
        file_name = os.path.basename(file_path)
        print(f"Creazione del file {file_name} in Figshare...")
        file_info = create_file(ARTICLE_ID, file_name, file_path)

        if 'location' in file_info:
            upload_url = file_info['location']
            print(f"Caricamento di {file_name} a Figshare...")
            upload_file(upload_url, file_path)
            print(f"{file_name} caricato con successo.")
        else:
            print(f"Errore nella creazione del file {file_name}: {file_info}")

if __name__ == '__main__':
    main()