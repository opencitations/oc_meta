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

import yaml
from pebble import ProcessFuture, ProcessPool
from tqdm import tqdm

from oc_meta.plugins.fixer.ar_order import find_broken_roles


def task_done(task_output:ProcessFuture) -> None:
    PBAR.update()

if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('ar_order.py', description='Search for bibliographic resources where the order of roles is broken and correct this information')
    arg_parser.add_argument('-c', '--c', dest='config', required=True, help='OpenCitations Meta configuration file location')
    arg_parser.add_argument('-r', '--resp_agent', dest='resp_agent', required=True, help='Your ORCID URL')
    arg_parser.add_argument('-m', '--max_workers', dest='max_workers', required=False, default=1, type=int, help='Workers number')
    args = arg_parser.parse_args()
    with open(args.config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    rdf_dir = os.path.join(settings['output_rdf_dir'], 'rdf') + os.sep
    br_files = [os.path.join(fold, file) for fold, _, files in os.walk(os.path.join(rdf_dir, 'br')) for file in files if file.endswith('.zip') and os.path.basename(fold) != 'prov']
    PBAR = tqdm(total=len(br_files))
    with ProcessPool(max_workers=args.max_workers, max_tasks=1) as executor:
        for filepath in br_files:
            future:ProcessFuture = executor.schedule(
                function=find_broken_roles, 
                args=(filepath, args.config, args.resp_agent)) 
            future.add_done_callback(task_done)
    PBAR.close()