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

import os
from argparse import ArgumentParser

import redis
import yaml

from oc_meta.plugins.csv_generator_lite.csv_generator_lite import generate_csv

if __name__ == '__main__':
    parser = ArgumentParser('csv_generator_lite.py', 
                          description='Generate CSV files from OpenCitations Meta RDF dump')
    parser.add_argument('-c', '--config', required=True, 
                       help='OpenCitations Meta configuration file location')
    parser.add_argument('-o', '--output_dir', required=True,
                       help='Directory where CSV files will be stored')
    parser.add_argument('--redis-host', default='localhost',
                       help='Redis host (default: localhost)')
    parser.add_argument('--redis-port', type=int, default=6379,
                       help='Redis port (default: 6379)')
    parser.add_argument('--redis-db', type=int, default=2,
                       help='Redis database number (default: 2)')
    parser.add_argument('--clean', action='store_true',
                       help='Clear checkpoint file and Redis cache before starting')
    args = parser.parse_args()

    with open(args.config, encoding='utf-8') as f:
        settings = yaml.full_load(f)

    rdf_dir = os.path.join(settings['output_rdf_dir'], 'rdf')
    dir_split_number = settings['dir_split_number']
    items_per_file = settings['items_per_file']

    if args.clean:
        checkpoint_file = 'processed_br_files.txt'
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            print(f"Removed checkpoint file: {checkpoint_file}")
        redis_client = redis.Redis(
            host=args.redis_host, port=args.redis_port, db=args.redis_db
        )
        deleted = redis_client.delete('processed_omids')
        if deleted:
            print("Cleared Redis processed_omids cache")

    generate_csv(
        input_dir=rdf_dir,
        output_dir=args.output_dir,
        dir_split_number=dir_split_number,
        items_per_file=items_per_file,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db
    ) 