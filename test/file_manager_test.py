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
# SOFTWARE.


import json
import os
import unittest
from shutil import rmtree

from oc_meta.lib.file_manager import (read_zipped_json, rm_tmp_csv_files,
                                      unzip_files_in_dir, zip_files_in_dir)

BASE = os.path.join('test', 'file_manager')
UNZIPPED_DIR = os.path.join(BASE, 'unzipped_dir')
OUTPUT_DIR = os.path.join(BASE, 'output')
OUTPUT_DIR_1 = os.path.join(BASE, 'output_1')


class test_JsonArchiveManager(unittest.TestCase):
    def test_zip_jsons_files_in_dir(self):
        zip_files_in_dir(UNZIPPED_DIR, OUTPUT_DIR)
        for dirpath, _, filenames in os.walk(OUTPUT_DIR):
            for filename in filenames:
                json_data = read_zipped_json(os.path.join(dirpath, filename))
                original_path = os.path.join(
                    UNZIPPED_DIR, 
                    dirpath.replace(f'{OUTPUT_DIR}{os.sep}', ''))
                with open(os.path.join(original_path, filename.replace('.zip', '.json')), 'r', encoding='utf-8') as original_f:
                    original_json = json.load(original_f)
                    self.assertEqual(json_data, original_json)
        rmtree(OUTPUT_DIR)

    def test_unzip_files_in_dir(self):
        zip_files_in_dir(UNZIPPED_DIR, OUTPUT_DIR_1)
        unzip_files_in_dir(OUTPUT_DIR_1, OUTPUT_DIR_1)
        for dirpath, _, filenames in os.walk(OUTPUT_DIR_1):
            for filename in filenames:
                if os.path.splitext(filename)[1] == ".json":
                    with open(os.path.join(dirpath, filename), encoding='utf-8') as f:
                        json_data = json.load(f)
                    original_zip = read_zipped_json(os.path.join(dirpath, filename.replace('.json', '.zip')))
                    self.assertEqual(json_data, original_zip)
        rmtree(OUTPUT_DIR_1)
    
    # def test_rm_tmp_csv_files(self):
    #     csv_dir = os.path.join(BASE, 'csv')
    #     os.mkdir(csv_dir)
    #     files = ['0_2022-09-29T02-35-31', '0_2022-09-29T08-03-27', '2_2022-09-29T08-03-27', '4_2022-09-29T08-03-27', '4_2022-09-29T02-35-31']
    #     for file in files:
    #         fp = open(os.path.join(csv_dir, f'{file}.csv'), 'w')
    #         fp.close()
    #     rm_tmp_csv_files(csv_dir)
    #     output = os.listdir(csv_dir)
    #     expected_output = ['0_2022-09-29T08-03-27.csv', '2_2022-09-29T08-03-27.csv', '4_2022-09-29T08-03-27.csv']
    #     rmtree(csv_dir)
    #     self.assertEqual(output, expected_output)
        

if __name__ == '__main__': # pragma: no cover
    unittest.main()
