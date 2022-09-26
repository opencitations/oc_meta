from oc_meta.lib.archive_manager import JsonArchiveManager
from pathlib import Path
from shutil import rmtree
import json
import os
import unittest

BASE = os.path.join('test', 'archive_manager')
UNZIPPED_DIR = os.path.join(BASE, 'unzipped_dir')
OUTPUT_DIR = os.path.join(BASE, 'output')


class test_JsonArchiveManager(unittest.TestCase):
    def test_compress_files_in_dir(self):
        archive_manager = JsonArchiveManager()
        archive_manager.compress_json_files_in_dir(UNZIPPED_DIR, OUTPUT_DIR)
        for dirpath, _, filenames in os.walk(OUTPUT_DIR):
            for filename in filenames:
                json_data = archive_manager.read_zipped_json(os.path.join(dirpath, filename))
                original_path = os.path.join(
                    UNZIPPED_DIR, 
                    dirpath.replace(f'{OUTPUT_DIR}{os.sep}', ''))
                with open(os.path.join(original_path, filename.replace('.zip', '')), 'r', encoding='utf-8') as original_f:
                    original_json = json.load(original_f)
                    self.assertEqual(json_data, original_json)
        rmtree(OUTPUT_DIR)


if __name__ == '__main__': # pragma: no cover
    unittest.main()
