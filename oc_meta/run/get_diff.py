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


from argparse import ArgumentParser

from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.file_manager import write_csv


def get_diff(old: str, new: str, destination: str) -> None:
    old_ids_set = CSVManager.load_csv_column_as_set(old, 'id')
    new_ids_set = CSVManager.load_csv_column_as_set(new, 'id')
    diff = new_ids_set.difference(old_ids_set)
    datalist = [{'id': id} for id in diff]
    if datalist:
        write_csv(destination, datalist)


if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('get_diff.py', description='Get a list of identifiers in the second list but not in the first one')
    arg_parser.add_argument('-o', '--old', dest='old', required=True, help='The oldest list CSV path')
    arg_parser.add_argument('-n', '--new', dest='new', required=True, help='The newest list CSV path')
    arg_parser.add_argument('-d', '--destination', dest='destination', required=True, help='The output CSV path')
    args = arg_parser.parse_args()
    get_diff(args.old, args.new, args.destination)