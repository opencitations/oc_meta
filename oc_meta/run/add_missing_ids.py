import os
from argparse import ArgumentParser

from tqdm import tqdm

from oc_meta.lib.file_manager import get_csv_data, write_csv


def add_missing_ids(input_dir: str, output_dir: str):
    pbar = tqdm(total=len(os.listdir(input_dir)))
    for filename in os.listdir(input_dir):
        data = get_csv_data(os.path.join(input_dir, filename))
        news_rows = []
        for row in data:
            if row['is_citing_in_meta']:
                news_rows.append({'id': row['citing'], 'title': '', 'author': '', 'editor': '', 'publisher': '', 'volume': '', 'venue': '', 'issue': '', 'pub_date': '', 'page': '', 'type': ''})
            elif row['is_cited_in_meta']:
                news_rows.append({'id': row['cited'], 'title': '', 'author': '', 'editor': '', 'publisher': '', 'volume': '', 'venue': '', 'issue': '', 'pub_date': '', 'page': '', 'type': ''})
        output_filepath = os.path.join(output_dir, filename)
        write_csv(output_filepath, news_rows)
        pbar.update()
    pbar.close()
            

if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('add_missing_ids.py', description='This script outputs Mtea input CSV files starting from CSVs containing missing identifiers in Meta')
    arg_parser.add_argument('-c', '--csv', dest='input_dir', required=True,
                            help='The directory containing the input CSV files')
    arg_parser.add_argument('-o', '--output', dest='output_dir', required=True,
                            help='The output directory')
    args = arg_parser.parse_args()
    add_missing_ids(args.input_dir, args.output_dir)