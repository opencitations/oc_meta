from __future__ import annotations

import os
from argparse import ArgumentParser

from fakeredis import FakeStrictRedis
from oc_ds_converter.datasource.redis import RedisDataSource
from tqdm import tqdm

from oc_meta.lib.file_manager import get_csv_data, write_csv


def discard_existing_res(csv_filepath: str, output_filepath: str, r: FakeStrictRedis|RedisDataSource = FakeStrictRedis()):
    all_identifiers = list()
    r = r if isinstance(r, FakeStrictRedis) else r._r
    pbar = tqdm(total=len(os.listdir(csv_filepath)))
    if not os.path.exists(output_filepath):
        os.makedirs(output_filepath)
    for filename in os.listdir(csv_filepath):
        data = get_csv_data(os.path.join(csv_filepath, filename))
        for row in data:
            all_identifiers.extend(row['id'].split())
        identifiers_in_r = r.mget(all_identifiers)
        preexisting_identifiers = {identifier for i, identifier in enumerate(all_identifiers) if identifiers_in_r[i]}
        output_data = list()
        for row in data:
            if not all(identifier in preexisting_identifiers for identifier in row['id'].split()):
                output_data.append(row)
        counter = 1
        cur_output_filepath = os.path.join(output_filepath, f'{str(counter)}.csv')
        counter += 1
        write_csv(cur_output_filepath, output_data)
        pbar.update()
    pbar.close()


if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('discard_existing_res.py', description='This script removes rows from metadata CSV tables if such rows refer to bibliographic resources that are in OpenCitations Meta')
    arg_parser.add_argument('-c', '--csv', dest='csv_filepath', required=True,
                            help='The directory containing the metadata CSV files')
    arg_parser.add_argument('-o', '--output', dest='output_filepath', required=True,
                                help='The directory to store the output CSV files')
    arg_parser.add_argument('-t', '--testing', dest='testing', required=False, action='store_true',
                                help='Use fakeredis')
    args = arg_parser.parse_args()
    r = FakeStrictRedis() if args.testing else RedisDataSource('DB-META-BR')
    discard_existing_res(args.csv_filepath, args.output_filepath, r)