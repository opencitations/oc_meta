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


from argparse import ArgumentParser

from oc_meta.plugins.csv_generator.csv_generator import generate_csv

if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('csv_generator.py', description='This script generates output CSVs from the OpenCitations Meta RDF dump')
    arg_parser.add_argument('-r', '--rdf', dest='rdf_dir', required=True,
                            help='RDF files root directory')
    arg_parser.add_argument('-d', '--dir_split_number', dest='dir_split_number', required=True,
                            help="Number of RDF files per folder. dir_split_number's value must be multiple of items_per_file's value")
    arg_parser.add_argument('-i', '--items_per_file', dest='items_per_file', required=True,
                            help="Number of RDF items per file")
    arg_parser.add_argument('-o', '--output_dir', dest='output_dir', required=True,
                            help='The output directory where the CSV files will be stores')
    arg_parser.add_argument('-t', '--threshold', dest='threshold', required=True, default=5000, type=int,
                            help='How many lines the CSV output must contain')
    args = arg_parser.parse_args()
    generate_csv(args.rdf_dir, args.dir_split_number, args.items_per_file, args.output_dir, args.threshold)