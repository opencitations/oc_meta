#!/usr/bin/python

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import io
import os
import shutil
import tempfile

import pytest
from oc_meta.lib.file_manager import get_csv_data
from oc_meta.run.meta.merge_csv import (
    get_csv_files,
    merge_csv_files,
    print_merge_report,
    resolve_output_path,
)


class TestMergeCsv:
    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        self.test_dir = tempfile.mkdtemp(dir=".")
        self.output_dir = tempfile.mkdtemp(dir=".")
        yield
        shutil.rmtree(self.test_dir)
        shutil.rmtree(self.output_dir)

    def test_get_csv_files_basic(self):
        csv1 = os.path.join(self.test_dir, "1.csv")
        txt = os.path.join(self.test_dir, "data.txt")

        with open(csv1, "w") as fp:
            fp.write("id,title\n")
        with open(txt, "w") as fp:
            fp.write("not a csv")

        result = get_csv_files(self.test_dir)

        assert len(result) == 1
        assert result[0].endswith(".csv")

    def test_get_csv_files_invalid_dir(self):
        with pytest.raises(ValueError) as ctx:
            get_csv_files("/nonexistent/path")
        assert "not a directory" in str(ctx.value)

    def test_resolve_output_path_csv_file(self):
        output = os.path.join(self.output_dir, "result.csv")
        result = resolve_output_path(output)
        assert result == output

    def test_resolve_output_path_csv_file_nested(self):
        output = os.path.join(self.output_dir, "nested", "subdir", "result.csv")
        result = resolve_output_path(output)
        assert result == output
        assert os.path.isdir(os.path.dirname(output))

    def test_resolve_output_path_folder(self):
        folder = os.path.join(self.output_dir, "output_folder")
        result = resolve_output_path(folder)
        assert result == os.path.join(folder, "merged.csv")
        assert os.path.isdir(folder)

    def test_merge_csv_files_empty_directory(self):
        total_rows, files_processed, fieldnames = merge_csv_files(
            self.test_dir, os.path.join(self.output_dir, "merged.csv")
        )
        assert total_rows == 0
        assert files_processed == 0
        assert fieldnames == []

    def test_merge_csv_files_single_file(self):
        csv_path = os.path.join(self.test_dir, "1.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("id,title\n")
            f.write('"doi:10.1234/test","Test Title"\n')
            f.write('"doi:10.5678/other","Other Title"\n')

        output_path = os.path.join(self.output_dir, "merged.csv")
        total_rows, files_processed, fieldnames = merge_csv_files(
            self.test_dir, output_path
        )

        assert total_rows == 2
        assert files_processed == 1
        assert fieldnames == ["id", "title"]

        rows = get_csv_data(output_path, clean_data=False)
        assert len(rows) == 2

    def test_merge_csv_files_multiple_files(self):
        csv1 = os.path.join(self.test_dir, "1.csv")
        csv2 = os.path.join(self.test_dir, "2.csv")

        with open(csv1, "w", encoding="utf-8") as f:
            f.write("id,title\n")
            f.write('"doi:10.1234/a","Title A"\n')

        with open(csv2, "w", encoding="utf-8") as f:
            f.write("id,title\n")
            f.write('"doi:10.1234/b","Title B"\n')

        output_path = os.path.join(self.output_dir, "result.csv")
        total_rows, files_processed, fieldnames = merge_csv_files(
            self.test_dir, output_path
        )

        assert total_rows == 2
        assert files_processed == 2

        rows = get_csv_data(output_path, clean_data=False)
        assert len(rows) == 2

    def test_merge_csv_files_with_long_fields(self):
        csv_path = os.path.join(self.test_dir, "1.csv")
        long_value = "x" * 200000

        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("id,content\n")
            f.write('"doi:10.1234/long","{}"\n'.format(long_value))

        output_path = os.path.join(self.output_dir, "merged.csv")
        total_rows, files_processed, _ = merge_csv_files(self.test_dir, output_path)

        assert total_rows == 1
        assert files_processed == 1

        rows = get_csv_data(output_path, clean_data=False)
        assert rows[0]["content"] == long_value

    def test_merge_csv_files_skips_empty_files(self):
        csv1 = os.path.join(self.test_dir, "1.csv")
        csv2 = os.path.join(self.test_dir, "2.csv")

        with open(csv1, "w", encoding="utf-8") as f:
            f.write("id,title\n")

        with open(csv2, "w", encoding="utf-8") as f:
            f.write("id,title\n")
            f.write('"doi:10.1234/test","Test"\n')

        output_path = os.path.join(self.output_dir, "merged.csv")
        total_rows, files_processed, fieldnames = merge_csv_files(
            self.test_dir, output_path
        )

        assert total_rows == 1
        assert files_processed == 2
        assert fieldnames == ["id", "title"]

    def test_print_merge_report(self):
        from rich.console import Console
        import oc_meta.run.meta.merge_csv as module

        captured = io.StringIO()
        test_console = Console(file=captured, force_terminal=True)
        original_console = module.console
        module.console = test_console

        print_merge_report(150, 5, "/output/merged.csv")

        module.console = original_console

        output = captured.getvalue()
        assert "150" in output
        assert "5" in output
