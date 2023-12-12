import os
import zipfile

def zip_individual_files_in_folder(folder_path):
    for foldername, subfolders, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(foldername, filename)
            zip_filename = os.path.join(foldername, f"{filename}.zip")
            
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(file_path, os.path.basename(file_path))

if __name__ == "__main__":
    folder_to_zip  = '/srv/data/arcangelo/export_test/rdf/'  # Inserisci il percorso della cartella che vuoi zippare
    zip_individual_files_in_folder(folder_to_zip)
    print(f"File zippati individualmente nella cartella di origine.")