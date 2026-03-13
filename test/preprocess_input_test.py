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

import io
import os
import shutil
import tempfile
import unittest

import redis

from oc_meta.lib.file_manager import get_csv_data, write_csv
from oc_meta.run.meta.preprocess_input import (
    check_ids_existence,
    create_redis_connection,
    get_csv_files,
    print_processing_report,
    process_csv_file,
    ProcessingStats,
)


class TestPreprocessInput(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(dir=".")
        self.output_dir = tempfile.mkdtemp(dir=".")

        self.redis_client = redis.Redis(
            host="localhost", port=6381, db=5, decode_responses=True
        )

        self.redis_client.set("doi:10.1007/978-3-662-07918-8_3", "1")
        self.redis_client.set("doi:10.1016/0021-9991(73)90147-2", "1")
        self.redis_client.set("doi:10.1109/20.877674", "1")

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        shutil.rmtree(self.output_dir)
        self.redis_client.flushdb()
        self.redis_client.close()

    def test_create_redis_connection(self):
        client = create_redis_connection("localhost", 6381, db=5)
        client.set("test_key", "test_value")
        result = client.get("test_key")
        self.assertEqual(result, "test_value")
        client.delete("test_key")
        client.close()

    def test_check_ids_existence_all_exist(self):
        ids = "doi:10.1007/978-3-662-07918-8_3 doi:10.1016/0021-9991(73)90147-2"
        result = check_ids_existence(ids, self.redis_client)
        self.assertEqual(result, True)

    def test_check_ids_existence_some_missing(self):
        ids = "doi:10.1007/978-3-662-07918-8_3 doi:10.INVALID/123"
        result = check_ids_existence(ids, self.redis_client)
        self.assertEqual(result, False)

    def test_check_ids_existence_empty_ids(self):
        result = check_ids_existence("", self.redis_client)
        self.assertEqual(result, False)

    def test_get_csv_files(self):
        csv_path = os.path.join(self.test_dir, "test.csv")
        txt_path = os.path.join(self.test_dir, "test.txt")
        with open(csv_path, "w") as f:
            f.write("id,title\n")
        with open(txt_path, "w") as f:
            f.write("not a csv")

        result = get_csv_files(self.test_dir)
        self.assertEqual(result, [csv_path])

    def test_get_csv_files_invalid_dir(self):
        with self.assertRaises(ValueError):
            get_csv_files("/nonexistent/path")

    def test_process_real_metadata_redis(self):
        real_data_path = os.path.join(self.test_dir, "real_metadata.csv")

        real_metadata = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.1007/978-3-662-07918-8_3","Influence of Dielectric Properties, State, and Electrodes on Electric Strength","Ushakov, Vasily Y.","2004","Insulation of High-Voltage Equipment [isbn:9783642058530 isbn:9783662079188]",,,"27-82","book chapter","Springer Science and Business Media LLC [crossref:297]",
"doi:10.1016/0021-9991(73)90147-2","Flux-corrected transport. I. SHASTA, a fluid transport algorithm that works","Boris, Jay P; Book, David L","1973-01","Journal of Computational Physics [issn:0021-9991]","11","1","38-69","journal article","Elsevier BV [crossref:78]",
"doi:10.1109/20.877674","An investigation of FEM-FCT method for streamer corona simulation","Woong-Gee Min, ; Hyeong-Seok Kim, ; Seok-Hyun Lee, ; Song-Yop Hahn, ","2000-07","IEEE Transactions on Magnetics [issn:0018-9464]","36","4","1280-1284","journal article","Institute of Electrical and Electronics Engineers (IEEE) [crossref:263]",'''

        with open(real_data_path, "w", encoding="utf-8") as f:
            f.write(real_metadata)

        next_file_num, stats, pending_rows = process_csv_file(
            real_data_path,
            self.output_dir,
            0,
            self.redis_client,
        )

        output_files = os.listdir(self.output_dir)
        self.assertEqual(len(output_files), 0)
        self.assertEqual(next_file_num, 0)
        self.assertEqual(stats.processed_rows, 0)
        self.assertEqual(stats.existing_ids_rows, 3)
        self.assertEqual(len(pending_rows), 0)

    def test_process_mixed_metadata_redis(self):
        mixed_data_path = os.path.join(self.test_dir, "mixed_metadata.csv")

        mixed_metadata = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.1007/978-3-662-07918-8_3","Influence of Dielectric Properties","Author 1","2004","Venue 1",,,"27-82","book chapter","Publisher 1",
"","Spatial Distribution of Ion Current","Author 2","2012-01","Venue 2","27","1","380-390","journal article","Publisher 2",
"doi:10.INVALID/123456789","Invalid DOI","Author 3","1980-01-14","Venue 3","13","1","3-6","journal article","Publisher 3",'''

        with open(mixed_data_path, "w", encoding="utf-8") as f:
            f.write(mixed_metadata)

        next_file_num, stats, pending_rows = process_csv_file(
            mixed_data_path,
            self.output_dir,
            0,
            self.redis_client,
        )

        if pending_rows:
            output_file = os.path.join(self.output_dir, f"{next_file_num}.csv")
            write_csv(output_file, pending_rows)

        self.assertEqual(next_file_num, 0)
        output_files = os.listdir(self.output_dir)
        self.assertEqual(len(output_files), 1)

        self.assertEqual(stats.total_rows, 3)
        self.assertEqual(stats.existing_ids_rows, 1)
        self.assertEqual(stats.processed_rows, 2)
        self.assertEqual(len(pending_rows), 2)

        output_file = os.path.join(self.output_dir, "0.csv")
        rows = get_csv_data(output_file, clean_data=False)
        self.assertEqual(len(rows), 2)
        self.assertEqual(any(row["id"] == "" for row in rows), True)
        self.assertEqual(
            any(row["id"] == "doi:10.INVALID/123456789" for row in rows), True
        )

    def test_process_duplicate_rows(self):
        test_data_path = os.path.join(self.test_dir, "duplicate_data.csv")

        test_data = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.INVALID/123","Test Title","Test Author","2024","Test Venue","1","1","1-10","journal article","Test Publisher",
"doi:10.INVALID/123","Test Title","Test Author","2024","Test Venue","1","1","1-10","journal article","Test Publisher",
"doi:10.INVALID/456","Different Title","Other Author","2024","Test Venue","1","1","11-20","journal article","Test Publisher",
"doi:10.INVALID/123","Test Title","Test Author","2024","Test Venue","1","1","1-10","journal article","Test Publisher",
"doi:10.INVALID/456","Different Title","Other Author","2024","Test Venue","1","1","11-20","journal article","Test Publisher",'''

        with open(test_data_path, "w", encoding="utf-8") as f:
            f.write(test_data)

        next_file_num, stats, pending_rows = process_csv_file(
            test_data_path,
            self.output_dir,
            0,
            self.redis_client,
        )

        if pending_rows:
            output_file = os.path.join(self.output_dir, f"{next_file_num}.csv")
            write_csv(output_file, pending_rows)

        self.assertEqual(next_file_num, 0)
        self.assertEqual(stats.total_rows, 5)
        self.assertEqual(stats.duplicate_rows, 3)
        self.assertEqual(stats.processed_rows, 2)
        self.assertEqual(len(pending_rows), 2)

        output_files = os.listdir(self.output_dir)
        self.assertEqual(len(output_files), 1)

        output_file = os.path.join(self.output_dir, "0.csv")
        rows = get_csv_data(output_file, clean_data=False)

        self.assertEqual(len(rows), 2)
        unique_ids = set(row["id"] for row in rows)
        self.assertEqual(len(unique_ids), 2)
        self.assertIn("doi:10.INVALID/123", unique_ids)
        self.assertIn("doi:10.INVALID/456", unique_ids)

    def test_cross_file_deduplication_redis(self):
        file1_path = os.path.join(self.test_dir, "data1.csv")
        file1_data = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.INVALID/123","Test Title","Test Author","2024","Test Venue","1","1","1-10","journal article","Test Publisher",
"doi:10.INVALID/456","Different Title","Other Author","2024","Test Venue","1","1","11-20","journal article","Test Publisher",'''

        file2_path = os.path.join(self.test_dir, "data2.csv")
        file2_data = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.INVALID/123","Test Title","Test Author","2024","Test Venue","1","1","1-10","journal article","Test Publisher",
"doi:10.INVALID/789","New Title","New Author","2024","Test Venue","1","1","21-30","journal article","Test Publisher",'''

        with open(file1_path, "w", encoding="utf-8") as f:
            f.write(file1_data)
        with open(file2_path, "w", encoding="utf-8") as f:
            f.write(file2_data)

        seen_rows: set = set()
        pending_rows: list = []
        next_file_num, stats1, pending_rows = process_csv_file(
            file1_path,
            self.output_dir,
            0,
            self.redis_client,
            seen_rows=seen_rows,
            pending_rows=pending_rows,
        )
        next_file_num, stats2, pending_rows = process_csv_file(
            file2_path,
            self.output_dir,
            next_file_num,
            self.redis_client,
            seen_rows=seen_rows,
            pending_rows=pending_rows,
        )

        if pending_rows:
            output_file = os.path.join(self.output_dir, f"{next_file_num}.csv")
            write_csv(output_file, pending_rows)

        self.assertEqual(stats1.total_rows, 2)
        self.assertEqual(stats1.duplicate_rows, 0)
        self.assertEqual(stats1.processed_rows, 2)

        self.assertEqual(stats2.total_rows, 2)
        self.assertEqual(stats2.duplicate_rows, 1)
        self.assertEqual(stats2.processed_rows, 1)

        output_files = sorted(os.listdir(self.output_dir))
        self.assertEqual(len(output_files), 1)

        output_file = os.path.join(self.output_dir, "0.csv")
        rows = get_csv_data(output_file, clean_data=False)
        self.assertEqual(len(rows), 3)
        unique_ids = set(row["id"] for row in rows)
        self.assertEqual(len(unique_ids), 3)
        self.assertIn("doi:10.INVALID/123", unique_ids)
        self.assertIn("doi:10.INVALID/456", unique_ids)
        self.assertIn("doi:10.INVALID/789", unique_ids)

    def test_rows_per_file_splitting(self):
        data_path = os.path.join(self.test_dir, "many_rows.csv")
        rows = [
            "id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor"
        ]
        for i in range(10):
            rows.append(
                f'"doi:10.INVALID/{i}","Title {i}","Author","2024","Venue","1","1","1-10","journal article","Publisher",'
            )

        with open(data_path, "w", encoding="utf-8") as f:
            f.write("\n".join(rows))

        next_file_num, stats, pending_rows = process_csv_file(
            data_path,
            self.output_dir,
            0,
            self.redis_client,
            rows_per_file=3,
        )

        if pending_rows:
            output_file = os.path.join(self.output_dir, f"{next_file_num}.csv")
            write_csv(output_file, pending_rows)

        self.assertEqual(next_file_num, 3)
        self.assertEqual(stats.processed_rows, 10)
        output_files = sorted(os.listdir(self.output_dir))
        self.assertEqual(len(output_files), 4)

    def test_print_processing_report(self):
        from rich.console import Console

        stats = ProcessingStats()
        stats.total_rows = 100
        stats.duplicate_rows = 10
        stats.existing_ids_rows = 20
        stats.processed_rows = 70

        captured = io.StringIO()
        test_console = Console(file=captured, force_terminal=True)

        import oc_meta.run.meta.preprocess_input as module

        original_console = module.console
        module.console = test_console

        print_processing_report([stats], ["file1.csv"])

        module.console = original_console

        output = captured.getvalue()
        self.assertIn("100", output)
        self.assertIn("10", output)
        self.assertIn("20", output)
        self.assertIn("70", output)


if __name__ == "__main__":
    unittest.main()
