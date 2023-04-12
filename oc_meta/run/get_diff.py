#!python
# Copyright 2022, Arcangelo Massari <arcangelo.massari@unibo.it>, Arianna Moretti <arianna.moretti4@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import os
from argparse import ArgumentParser

from tqdm import tqdm

from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.file_manager import get_csv_data, write_csv


def get_diff(meta_csv_path: str, new: str, destination: str) -> None:
    new_ids_set = CSVManager.load_csv_column_as_set(new, 'id')
    meta_ids_set = extract_ids_from_meta(meta_csv_path)
    diff = new_ids_set.difference(meta_ids_set)
    datalist = [{'id': id} for id in diff]
    if datalist:
        write_csv(destination, datalist)

def extract_ids_from_meta(meta_csv_path: str) -> set:
    meta_ids_set = set()
    filepaths = os.listdir(meta_csv_path)
    pbar = tqdm(total=len(filepaths))
    for filepath in filepaths:
        data = get_csv_data(os.path.join(meta_csv_path, filepath))
        for row in data:
            meta_ids_set.update([identifier.replace('doi:', '') for identifier in row['id'].split() if not identifier.startswith('meta:')])
        pbar.update()
    pbar.close()
    return meta_ids_set


if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('get_diff.py', description='Get a list of identifiers in the second list but not in the first one')
    arg_parser.add_argument('-m', '--meta', dest='meta_csv_path', required=True, help='OpenCitations Meta CSV output path')
    arg_parser.add_argument('-w', '--wanted', dest='wanted', required=True, help='The path to a list of identifiers i CSV format')
    arg_parser.add_argument('-d', '--destination', dest='destination', required=True, help='The output CSV path')
    args = arg_parser.parse_args()
    get_diff(args.meta_csv_path, args.wanted, args.destination)