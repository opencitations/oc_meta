#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2024 Arcangelo Massari <arcangelo.massari@unibo.it>
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
import unittest
import csv
import tempfile
import shutil
import redis
from oc_meta.run.meta.preprocess_input import process_csv_file, create_redis_connection

class TestPreprocessInput(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(dir='.')
        self.output_dir = tempfile.mkdtemp(dir='.')
        
        # Create Redis connection for testing (using DB 5)
        self.redis_client = redis.Redis(host='localhost', port=6379, db=5, decode_responses=True)
        
        # Add some test data to Redis
        self.redis_client.set('doi:10.1007/978-3-662-07918-8_3', '1')
        self.redis_client.set('doi:10.1016/0021-9991(73)90147-2', '1')
        self.redis_client.set('doi:10.1109/20.877674', '1')

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        shutil.rmtree(self.output_dir)
        # Clean up Redis test data
        self.redis_client.flushdb()
        self.redis_client.close()

    def test_process_real_metadata(self):
        """Test processing metadata with Redis lookup"""
        real_data_path = os.path.join(self.test_dir, 'real_metadata.csv')
        
        # These DOIs exist in our Redis test DB
        real_metadata = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.1007/978-3-662-07918-8_3","Influence of Dielectric Properties, State, and Electrodes on Electric Strength","Ushakov, Vasily Y.","2004","Insulation of High-Voltage Equipment [isbn:9783642058530 isbn:9783662079188]",,,"27-82","book chapter","Springer Science and Business Media LLC [crossref:297]",
"doi:10.1016/0021-9991(73)90147-2","Flux-corrected transport. I. SHASTA, a fluid transport algorithm that works","Boris, Jay P; Book, David L","1973-01","Journal of Computational Physics [issn:0021-9991]","11","1","38-69","journal article","Elsevier BV [crossref:78]",
"doi:10.1109/20.877674","An investigation of FEM-FCT method for streamer corona simulation","Woong-Gee Min, ; Hyeong-Seok Kim, ; Seok-Hyun Lee, ; Song-Yop Hahn, ","2000-07","IEEE Transactions on Magnetics [issn:0018-9464]","36","4","1280-1284","journal article","Institute of Electrical and Electronics Engineers (IEEE) [crossref:263]",'''
        
        with open(real_data_path, 'w', encoding='utf-8') as f:
            f.write(real_metadata)

        next_file_num = process_csv_file(real_data_path, self.output_dir, 0, redis_db=5)
        
        # Since all DOIs exist in Redis, no file should be created
        output_files = os.listdir(self.output_dir)
        self.assertEqual(len(output_files), 0)
        self.assertEqual(next_file_num, 0)

    def test_process_mixed_metadata(self):
        """Test processing metadata with both existing and non-existing DOIs in Redis"""
        mixed_data_path = os.path.join(self.test_dir, 'mixed_metadata.csv')
        
        # Mix of existing DOIs, non-existing DOIs and empty IDs
        mixed_metadata = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.1007/978-3-662-07918-8_3","Influence of Dielectric Properties","Author 1","2004","Venue 1",,,"27-82","book chapter","Publisher 1",
"","Spatial Distribution of Ion Current","Author 2","2012-01","Venue 2","27","1","380-390","journal article","Publisher 2",
"doi:10.INVALID/123456789","Invalid DOI","Author 3","1980-01-14","Venue 3","13","1","3-6","journal article","Publisher 3",'''

        with open(mixed_data_path, 'w', encoding='utf-8') as f:
            f.write(mixed_metadata)

        next_file_num = process_csv_file(mixed_data_path, self.output_dir, 0, redis_db=5)
        
        # Should create one file with rows having empty IDs or non-existing DOIs
        self.assertEqual(next_file_num, 1)
        output_files = os.listdir(self.output_dir)
        self.assertEqual(len(output_files), 1)
        
        output_file = os.path.join(self.output_dir, '0.csv')
        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            self.assertEqual(len(rows), 2)  # Should contain empty ID row and invalid DOI row
            self.assertTrue(any(row['id'] == '' for row in rows))
            self.assertTrue(any(row['id'] == 'doi:10.INVALID/123456789' for row in rows))

    def test_process_duplicate_rows(self):
        """Test that duplicate rows are properly filtered out"""
        test_data_path = os.path.join(self.test_dir, 'duplicate_data.csv')
        
        test_data = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.INVALID/123","Test Title","Test Author","2024","Test Venue","1","1","1-10","journal article","Test Publisher",
"doi:10.INVALID/123","Test Title","Test Author","2024","Test Venue","1","1","1-10","journal article","Test Publisher",
"doi:10.INVALID/456","Different Title","Other Author","2024","Test Venue","1","1","11-20","journal article","Test Publisher",
"doi:10.INVALID/123","Test Title","Test Author","2024","Test Venue","1","1","1-10","journal article","Test Publisher",
"doi:10.INVALID/456","Different Title","Other Author","2024","Test Venue","1","1","11-20","journal article","Test Publisher",'''

        with open(test_data_path, 'w', encoding='utf-8') as f:
            f.write(test_data)

        next_file_num = process_csv_file(test_data_path, self.output_dir, 0, redis_db=5)
        
        self.assertEqual(next_file_num, 1)
        output_files = os.listdir(self.output_dir)
        self.assertEqual(len(output_files), 1)
        
        output_file = os.path.join(self.output_dir, '0.csv')
        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            self.assertEqual(len(rows), 2)
            unique_ids = set(row['id'] for row in rows)
            self.assertEqual(len(unique_ids), 2)
            self.assertIn('doi:10.INVALID/123', unique_ids)
            self.assertIn('doi:10.INVALID/456', unique_ids)

if __name__ == '__main__':
    unittest.main() 