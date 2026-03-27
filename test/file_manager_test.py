#!/usr/bin/python

# Copyright (C) 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2022-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC


import os
import tempfile
import unittest
from shutil import rmtree

import orjson
from oc_meta.lib.file_manager import (get_csv_data, read_zipped_json, unzip_files_in_dir, zip_files_in_dir)

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
                with open(os.path.join(original_path, filename.replace('.zip', '.json')), 'rb') as original_f:
                    original_json = orjson.loads(original_f.read())
                    self.assertEqual(json_data, original_json)
        rmtree(OUTPUT_DIR)

    def test_unzip_files_in_dir(self):
        zip_files_in_dir(UNZIPPED_DIR, OUTPUT_DIR_1)
        unzip_files_in_dir(OUTPUT_DIR_1, OUTPUT_DIR_1)
        for dirpath, _, filenames in os.walk(OUTPUT_DIR_1):
            for filename in filenames:
                if os.path.splitext(filename)[1] == ".json":
                    with open(os.path.join(dirpath, filename), 'rb') as f:
                        json_data = orjson.loads(f.read())
                    original_zip = read_zipped_json(os.path.join(dirpath, filename.replace('.json', '.zip')))
                    self.assertEqual(json_data, original_zip)
        rmtree(OUTPUT_DIR_1)
    
class TestGetCsvData(unittest.TestCase):
    def test_get_csv_data_header_only(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,title\n")
            path = f.name
        try:
            result = get_csv_data(path, clean_data=False)
            self.assertEqual(result, [])
        finally:
            os.unlink(path)

    def test_get_csv_data_with_data(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,title\n")
            f.write('"doi:10.1234/test","Test Title"\n')
            path = f.name
        try:
            result = get_csv_data(path, clean_data=False)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["id"], "doi:10.1234/test")
        finally:
            os.unlink(path)


if __name__ == '__main__': # pragma: no cover
    unittest.main()
