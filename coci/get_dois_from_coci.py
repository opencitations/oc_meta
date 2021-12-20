import csv, os
from tqdm import tqdm
from argparse import ArgumentParser


def get_dois_from_coci(coci_dir:str, output_file_path:str, verbose:bool=False) -> None:
    dois_found = set()
    if os.path.exists(output_file_path):
        with open(output_file_path, 'r', encoding='utf8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                dois_found.add(row['doi'])
    if verbose:
        file_count = sum(len(files) for _, _, files in os.walk(coci_dir))
        pbar = tqdm(total=file_count)
    for fold, _, files in os.walk(coci_dir):
        for file in files:
            output_csv = list()
            if file.endswith('.csv'):
                data = csv.DictReader(open(os.path.join(fold, file), 'r', encoding='utf8'))
                for row in data:
                    citing = row['citing']
                    cited = row['cited']
                    if citing not in dois_found:
                        output_csv.append({'doi':row['citing']})
                        dois_found.add(citing)
                    if cited not in dois_found:
                        output_csv.append({'doi':row['cited']})
                        dois_found.add(cited)
                with open(output_file_path, 'a', encoding='utf8', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=['doi'])
                    writer.writeheader()
                    writer.writerows(output_csv)
            pbar.update(1)
    pbar.close()


if __name__ == '__main__':
    arg_parser = ArgumentParser('run_preprocess.py', description='This script create csv files from Crossref json,'
                                                                 ' enriching them thanks to an doi-orcid index')
    arg_parser.add_argument('-c', '--coci', dest='coci_dir', required=True,
                            help='COCI files directory')
    arg_parser.add_argument('-out', '--output', dest='output_file_path', required=True,
                            help='Path of the output CSV file')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')
    args = arg_parser.parse_args()
    get_dois_from_coci(coci_dir=args.coci_dir, output_file_path=args.output_file_path, verbose=args.verbose)