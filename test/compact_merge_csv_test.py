#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2025 Arcangelo Massari <arcangelo.massari@unibo.it>
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

import csv
import os
import tempfile
import unittest
from pathlib import Path

from oc_meta.run.merge.compact_output_csv import (
    process_merge_directory,
    process_merge_file,
)


class TestProcessMergeCSV(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test files
        self.test_dir = tempfile.mkdtemp()  # Using default system temp directory

        # Create a test CSV file
        self.test_csv_content = [
            ["surviving_entity", "merged_entities", "Done"],
            ["https://w3id.org/oc/meta/id/1", "https://w3id.org/oc/meta/id/2", "True"],
            ["https://w3id.org/oc/meta/id/3", "https://w3id.org/oc/meta/id/4", "False"],
            [
                "https://w3id.org/oc/meta/id/5",
                "https://w3id.org/oc/meta/id/6; https://w3id.org/oc/meta/id/7",
                "True",
            ],
        ]

        self.test_file = os.path.join(self.test_dir, "test.csv")
        with open(self.test_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(self.test_csv_content)

    def tearDown(self):
        # Clean up temporary files
        for file in Path(self.test_dir).glob("*.csv"):
            file.unlink()
        os.rmdir(self.test_dir)

    def test_process_merge_file(self):
        results = process_merge_file(self.test_file)

        # Should only include rows with Done=True
        self.assertEqual(len(results["valid_entries"]), 2)
        self.assertEqual(results["total_rows"], 3)  # 3 data rows (excluding header)

        # Check first result
        self.assertEqual(
            results["valid_entries"][0][0], "https://w3id.org/oc/meta/id/1"
        )
        self.assertEqual(
            results["valid_entries"][0][1], "https://w3id.org/oc/meta/id/2"
        )

        # Check second result with multiple merged entities
        self.assertEqual(
            results["valid_entries"][1][0], "https://w3id.org/oc/meta/id/5"
        )
        self.assertEqual(
            results["valid_entries"][1][1],
            "https://w3id.org/oc/meta/id/6; https://w3id.org/oc/meta/id/7",
        )

    def test_process_merge_directory(self):
        # Create a second test file
        second_file = os.path.join(self.test_dir, "test2.csv")
        with open(second_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(
                [
                    ["surviving_entity", "merged_entities", "Done"],
                    [
                        "https://w3id.org/oc/meta/id/8",
                        "https://w3id.org/oc/meta/id/9",
                        "True",
                    ],
                ]
            )

        output_file = os.path.join(self.test_dir, "output.csv")
        process_merge_directory(self.test_dir, output_file)

        # Check output file
        with open(output_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

            # Check header
            self.assertEqual(rows[0], ["surviving_entity", "merged_entities"])

            # Should have 3 rows total (2 from first file + 1 from second file)
            self.assertEqual(len(rows), 4)


if __name__ == "__main__":
    unittest.main()
