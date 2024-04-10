import argparse
import os
import zipfile
import lzma
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

def process_file(zip_info_tuple):
    input_dir, output_dir, root, file = zip_info_tuple
    zip_path = os.path.join(root, file)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for zip_info in zip_ref.infolist():
            with zip_ref.open(zip_info) as zip_file:
                file_content = zip_file.read()
                
                relative_path = os.path.relpath(root, input_dir)
                output_path = os.path.join(output_dir, relative_path)
                os.makedirs(output_path, exist_ok=True)
                
                lzma_file_path = os.path.join(output_path, zip_info.filename + '.xz')
                
                with lzma.open(lzma_file_path, 'w') as lzma_file:
                    lzma_file.write(file_content)

def zip_to_lzma(input_dir, output_dir):
    # Trova tutti i file zip nella directory di input
    zip_files = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith('.zip'):
                zip_files.append((input_dir, output_dir, root, file))
    
    # Imposta il numero di processi pari al numero di CPU
    pool = Pool(processes=cpu_count())
    
    # Processa i file con una barra di progresso
    for _ in tqdm(pool.imap_unordered(process_file, zip_files), total=len(zip_files), desc="Processing files"):
        pass

    pool.close()
    pool.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert ZIP files to LZMA using multiprocessing")
    parser.add_argument("input_dir", help="Input directory containing ZIP files")
    parser.add_argument("output_dir", help="Output directory to save LZMA files")
    args = parser.parse_args()

    zip_to_lzma(args.input_dir, args.output_dir)
