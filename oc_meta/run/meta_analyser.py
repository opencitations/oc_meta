#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import json
from argparse import ArgumentParser

from oc_meta.plugins.analyser import OCMetaCounter


if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('meta_analyser.py', description='This script allows performing various analyses on the OC Meta output CSVs')
    arg_parser.add_argument('-c', '--csv_dir', dest='csv_dir', required=True, help='The directory containing the OC Meta output CSVs')
    arg_parser.add_argument('-w', '--what', dest='what', required=True, choices=['authors', 'editors', 'publishers', 'venues', 'types', 'years'], help='What to count')
    arg_parser.add_argument('-b', '--by', dest='by_what', required=False, default=None, choices=['publication', 'venue'], help="This parameter is only useful for the 'top' operation. It allows entities specified in 'what' to be ordered by other entities ('by what'). For example, sorting journals by number of publications")
    arg_parser.add_argument('-n', '--number', dest='number', required=False, default=None, type=int, help="This parameter is only useful for the 'top' operation. Once the entities are sorted, only the first N are returned. If N is not specified, all entities are returned")
    args = arg_parser.parse_args()
    analyser = OCMetaCounter(args.csv_dir)
    if args.what and not args.by_what:
        result = analyser.count(what=args.what)
    elif args.what and args.by_what:
        result = analyser.get_top(what=args.what, by_what=args.by_what, number=args.number)
    if isinstance(result, int):
        with open('analysis.txt', 'w', encoding='utf-8') as f:
            f.write(str(result))
    elif isinstance(result, list):
        result = [(meta, {k: list(v) if isinstance(v, set) else v for k, v in data.items()}) for meta, data in result]
        with open('analysis.json', 'w', encoding='utf-8') as outfile:
            json.dump(result, outfile)