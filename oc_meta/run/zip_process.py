#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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
# SOFTWARE


from argparse import ArgumentParser
from oc_meta.lib.archive_manager import JsonArchiveManager


if __name__ == '__main__':
    arg_parser = ArgumentParser('zip_process.py', description='Zip JSON files individually in all directories starting from a specified root directory')
    arg_parser.add_argument('-s', '--source', dest='src_dir', required=True, help='The source directory')
    arg_parser.add_argument('-d', '--destination', dest='dst_dir', required=True, help='The destination directory')
    arg_parser.add_argument('-r', '--replace', dest='replace_files', action='store_true', default=False, required=False, help='Specify this argument if you want to replace the original unzipped files with their zipped versions')
    args = arg_parser.parse_args()
    json_archive_manager = JsonArchiveManager()
    json_archive_manager.compress_json_files_in_dir(src_dir=args.src_dir, dst_dir=args.dst_dir, replace_files=args.replace_files)