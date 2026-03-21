#!/usr/bin/python

# Copyright (C) 2024 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2025-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import io
import os
import shutil
import tempfile
import unittest

import redis

from oc_meta.lib.file_manager import get_csv_data
from oc_meta.run.meta.preprocess_input import (
    FileResult,
    ProcessingStats,
    check_ids_existence_batch,
    create_redis_connection,
    deduplicate_and_write,
    get_csv_files,
    print_processing_report,
    filter_existing_ids_from_file,
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

    def test_check_ids_existence_batch_all_exist(self):
        rows = [
            {"id": "doi:10.1007/978-3-662-07918-8_3 doi:10.1016/0021-9991(73)90147-2"}
        ]
        result = check_ids_existence_batch(rows, self.redis_client)
        self.assertEqual(result, [True])

    def test_check_ids_existence_batch_some_missing(self):
        rows = [{"id": "doi:10.1007/978-3-662-07918-8_3 doi:10.INVALID/123"}]
        result = check_ids_existence_batch(rows, self.redis_client)
        self.assertEqual(result, [False])

    def test_check_ids_existence_batch_empty_ids(self):
        rows = [{"id": ""}]
        result = check_ids_existence_batch(rows, self.redis_client)
        self.assertEqual(result, [False])

    def test_check_ids_existence_batch_multiple_rows(self):
        rows = [
            {"id": "doi:10.1007/978-3-662-07918-8_3"},
            {"id": "doi:10.INVALID/123"},
            {"id": ""},
        ]
        result = check_ids_existence_batch(rows, self.redis_client)
        self.assertEqual(result, [True, False, False])

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

    def test_filter_existing_ids_from_file_all_existing(self):
        real_data_path = os.path.join(self.test_dir, "real_metadata.csv")

        real_metadata = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.1007/978-3-662-07918-8_3","Influence of Dielectric Properties, State, and Electrodes on Electric Strength","Ushakov, Vasily Y.","2004","Insulation of High-Voltage Equipment [isbn:9783642058530 isbn:9783662079188]",,,"27-82","book chapter","Springer Science and Business Media LLC [crossref:297]",
"doi:10.1016/0021-9991(73)90147-2","Flux-corrected transport. I. SHASTA, a fluid transport algorithm that works","Boris, Jay P; Book, David L","1973-01","Journal of Computational Physics [issn:0021-9991]","11","1","38-69","journal article","Elsevier BV [crossref:78]",
"doi:10.1109/20.877674","An investigation of FEM-FCT method for streamer corona simulation","Woong-Gee Min, ; Hyeong-Seok Kim, ; Seok-Hyun Lee, ; Song-Yop Hahn, ","2000-07","IEEE Transactions on Magnetics [issn:0018-9464]","36","4","1280-1284","journal article","Institute of Electrical and Electronics Engineers (IEEE) [crossref:263]",'''

        with open(real_data_path, "w", encoding="utf-8") as f:
            f.write(real_metadata)

        result = filter_existing_ids_from_file(real_data_path, "localhost", 6381, 5)

        self.assertEqual(result.stats.total_rows, 3)
        self.assertEqual(result.stats.existing_ids_rows, 3)
        self.assertEqual(len(result.rows), 0)

    def test_filter_existing_ids_from_file_mixed(self):
        mixed_data_path = os.path.join(self.test_dir, "mixed_metadata.csv")

        mixed_metadata = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.1007/978-3-662-07918-8_3","Influence of Dielectric Properties","Author 1","2004","Venue 1",,,"27-82","book chapter","Publisher 1",
"","Spatial Distribution of Ion Current","Author 2","2012-01","Venue 2","27","1","380-390","journal article","Publisher 2",
"doi:10.INVALID/123456789","Invalid DOI","Author 3","1980-01-14","Venue 3","13","1","3-6","journal article","Publisher 3",'''

        with open(mixed_data_path, "w", encoding="utf-8") as f:
            f.write(mixed_metadata)

        result = filter_existing_ids_from_file(mixed_data_path, "localhost", 6381, 5)

        self.assertEqual(result.stats.total_rows, 3)
        self.assertEqual(result.stats.existing_ids_rows, 1)
        self.assertEqual(len(result.rows), 2)

    def test_deduplicate_and_write_with_duplicates(self):
        row1 = {"id": "doi:10.INVALID/123", "title": "Test Title"}
        row2 = {"id": "doi:10.INVALID/456", "title": "Different Title"}

        row1_hash = tuple(sorted(row1.items()))
        row2_hash = tuple(sorted(row2.items()))

        results = [
            FileResult(
                file_path="file1.csv",
                rows=[(row1_hash, row1), (row2_hash, row2)],
                stats=ProcessingStats(total_rows=2, existing_ids_rows=0),
            ),
            FileResult(
                file_path="file2.csv",
                rows=[(row1_hash, row1)],
                stats=ProcessingStats(total_rows=1, existing_ids_rows=0),
            ),
        ]

        total_stats = deduplicate_and_write(results, self.output_dir, rows_per_file=10)

        self.assertEqual(total_stats.total_rows, 3)
        self.assertEqual(total_stats.duplicate_rows, 1)
        self.assertEqual(total_stats.processed_rows, 2)

        output_files = os.listdir(self.output_dir)
        self.assertEqual(len(output_files), 1)

        rows = get_csv_data(os.path.join(self.output_dir, "0.csv"), clean_data=False)
        self.assertEqual(len(rows), 2)

    def test_deduplicate_and_write_file_splitting(self):
        rows_data = []
        for i in range(10):
            row = {"id": f"doi:10.INVALID/{i}", "title": f"Title {i}"}
            row_hash = tuple(sorted(row.items()))
            rows_data.append((row_hash, row))

        results = [
            FileResult(
                file_path="file.csv",
                rows=rows_data,
                stats=ProcessingStats(total_rows=10, existing_ids_rows=0),
            )
        ]

        total_stats = deduplicate_and_write(results, self.output_dir, rows_per_file=3)

        self.assertEqual(total_stats.processed_rows, 10)

        output_files = sorted(os.listdir(self.output_dir))
        self.assertEqual(len(output_files), 4)

    def test_cross_file_deduplication(self):
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

        result1 = filter_existing_ids_from_file(file1_path, "localhost", 6381, 5)
        result2 = filter_existing_ids_from_file(file2_path, "localhost", 6381, 5)

        total_stats = deduplicate_and_write(
            [result1, result2], self.output_dir, rows_per_file=10
        )

        self.assertEqual(total_stats.total_rows, 4)
        self.assertEqual(total_stats.duplicate_rows, 1)
        self.assertEqual(total_stats.processed_rows, 3)

        output_files = os.listdir(self.output_dir)
        self.assertEqual(len(output_files), 1)

        rows = get_csv_data(os.path.join(self.output_dir, "0.csv"), clean_data=False)
        self.assertEqual(len(rows), 3)

        unique_ids = set(row["id"] for row in rows)
        self.assertEqual(
            unique_ids,
            {"doi:10.INVALID/123", "doi:10.INVALID/456", "doi:10.INVALID/789"},
        )

    def test_print_processing_report(self):
        from rich.console import Console

        stats = ProcessingStats(
            total_rows=100,
            duplicate_rows=10,
            existing_ids_rows=20,
            processed_rows=70,
        )

        captured = io.StringIO()
        test_console = Console(file=captured, force_terminal=True)

        import oc_meta.run.meta.preprocess_input as module

        original_console = module.console
        module.console = test_console

        print_processing_report(stats, 1)

        module.console = original_console

        output = captured.getvalue()
        self.assertIn("100", output)
        self.assertIn("10", output)
        self.assertIn("20", output)
        self.assertIn("70", output)


if __name__ == "__main__":
    unittest.main()
