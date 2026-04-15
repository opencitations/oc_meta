#!/usr/bin/python

# Copyright (C) 2024 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2025-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import io
import os
import shutil
import tempfile

import pytest
import redis

from oc_meta.lib.file_manager import get_csv_data
from oc_meta.run.meta.preprocess_input import (
    FileResult,
    ProcessingStats,
    check_ids_existence_batch,
    check_ids_sparql,
    collect_rows_from_file,
    create_redis_connection,
    deduplicate_and_write,
    filter_existing_ids_from_file,
    filter_sparql_results,
    get_csv_files,
    print_processing_report,
)
from test.test_utils import SERVER, add_data_ts


DATA_FILE = os.path.abspath(
    os.path.join("test", "testcases", "ts", "check_results_data.nt")
)


class TestPreprocessInput:
    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        self.test_dir = tempfile.mkdtemp(dir=".")
        self.output_dir = tempfile.mkdtemp(dir=".")

        self.redis_client = redis.Redis(
            host="localhost", port=6381, db=5, decode_responses=True
        )

        self.redis_client.set("doi:10.1007/978-3-662-07918-8_3", "1")
        self.redis_client.set("doi:10.1016/0021-9991(73)90147-2", "1")
        self.redis_client.set("doi:10.1109/20.877674", "1")
        yield
        shutil.rmtree(self.test_dir)
        shutil.rmtree(self.output_dir)
        self.redis_client.flushdb()
        self.redis_client.close()

    def test_create_redis_connection(self):
        client = create_redis_connection("localhost", 6381, db=5)
        client.set("test_key", "test_value")
        result = client.get("test_key")
        assert result == "test_value"
        client.delete("test_key")
        client.close()

    def test_check_ids_existence_batch_all_exist(self):
        rows = [
            {"id": "doi:10.1007/978-3-662-07918-8_3 doi:10.1016/0021-9991(73)90147-2"}
        ]
        result = check_ids_existence_batch(rows, self.redis_client)
        assert result == [True]

    def test_check_ids_existence_batch_some_missing(self):
        rows = [{"id": "doi:10.1007/978-3-662-07918-8_3 doi:10.INVALID/123"}]
        result = check_ids_existence_batch(rows, self.redis_client)
        assert result == [False]

    def test_check_ids_existence_batch_empty_ids(self):
        rows = [{"id": ""}]
        result = check_ids_existence_batch(rows, self.redis_client)
        assert result == [False]

    def test_check_ids_existence_batch_multiple_rows(self):
        rows = [
            {"id": "doi:10.1007/978-3-662-07918-8_3"},
            {"id": "doi:10.INVALID/123"},
            {"id": ""},
        ]
        result = check_ids_existence_batch(rows, self.redis_client)
        assert result == [True, False, False]

    def test_get_csv_files(self):
        csv_path = os.path.join(self.test_dir, "test.csv")
        txt_path = os.path.join(self.test_dir, "test.txt")
        with open(csv_path, "w") as f:
            f.write("id,title\n")
        with open(txt_path, "w") as f:
            f.write("not a csv")

        result = get_csv_files(self.test_dir)
        assert result == [csv_path]

    def test_get_csv_files_invalid_dir(self):
        with pytest.raises(ValueError):
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

        assert result.stats.total_rows == 3
        assert result.stats.existing_ids_rows == 3
        assert len(result.rows) == 0

    def test_filter_existing_ids_from_file_mixed(self):
        mixed_data_path = os.path.join(self.test_dir, "mixed_metadata.csv")

        mixed_metadata = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.1007/978-3-662-07918-8_3","Influence of Dielectric Properties","Author 1","2004","Venue 1",,,"27-82","book chapter","Publisher 1",
"","Spatial Distribution of Ion Current","Author 2","2012-01","Venue 2","27","1","380-390","journal article","Publisher 2",
"doi:10.INVALID/123456789","Invalid DOI","Author 3","1980-01-14","Venue 3","13","1","3-6","journal article","Publisher 3",'''

        with open(mixed_data_path, "w", encoding="utf-8") as f:
            f.write(mixed_metadata)

        result = filter_existing_ids_from_file(mixed_data_path, "localhost", 6381, 5)

        assert result.stats.total_rows == 3
        assert result.stats.existing_ids_rows == 1
        assert len(result.rows) == 2

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

        assert total_stats.total_rows == 3
        assert total_stats.duplicate_rows == 1
        assert total_stats.processed_rows == 2

        output_files = os.listdir(self.output_dir)
        assert len(output_files) == 1

        rows = get_csv_data(os.path.join(self.output_dir, "0.csv"), clean_data=False)
        assert len(rows) == 2

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

        assert total_stats.processed_rows == 10

        output_files = sorted(os.listdir(self.output_dir))
        assert len(output_files) == 4

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

        assert total_stats.total_rows == 4
        assert total_stats.duplicate_rows == 1
        assert total_stats.processed_rows == 3

        output_files = os.listdir(self.output_dir)
        assert len(output_files) == 1

        rows = get_csv_data(os.path.join(self.output_dir, "0.csv"), clean_data=False)
        assert len(rows) == 3

        unique_ids = set(row["id"] for row in rows)
        assert unique_ids == {"doi:10.INVALID/123", "doi:10.INVALID/456", "doi:10.INVALID/789"}

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
        assert "100" in output
        assert "10" in output
        assert "20" in output
        assert "70" in output


class TestCollectRowsFromFile:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.test_dir = tempfile.mkdtemp(dir=".")
        yield
        shutil.rmtree(self.test_dir)

    def test_collect_rows_basic(self):
        csv_path = os.path.join(self.test_dir, "data.csv")
        csv_data = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.1234/test","Test Title","Author 1","2024","Venue 1","1","1","1-10","journal article","Publisher 1",
"doi:10.5678/other","Other Title","Author 2","2024","Venue 2","2","1","11-20","journal article","Publisher 2",'''

        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(csv_data)

        result = collect_rows_from_file(csv_path)

        assert result.file_path == csv_path
        assert result.stats.total_rows == 2
        assert result.stats.existing_ids_rows == 0
        assert len(result.rows) == 2

    def test_collect_rows_empty_file(self):
        csv_path = os.path.join(self.test_dir, "empty.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor\n")

        result = collect_rows_from_file(csv_path)

        assert result.stats.total_rows == 0
        assert len(result.rows) == 0


class TestCheckIdsSparql:

    @pytest.fixture(autouse=True)
    def _load_data(self):
        add_data_ts(SERVER, DATA_FILE)

    def test_single_found(self):
        result = check_ids_sparql({"doi:10.1234/test"}, SERVER, workers=1)
        assert result == {"doi:10.1234/test"}

    def test_single_not_found(self):
        result = check_ids_sparql({"doi:10.9999/notfound"}, SERVER, workers=1)
        assert result == set()

    def test_mixed_found_and_not_found(self):
        ids = {"doi:10.1234/test", "doi:10.9999/notfound", "orcid:0000-0001-2345-6789"}
        result = check_ids_sparql(ids, SERVER, workers=1)
        assert result == {"doi:10.1234/test", "orcid:0000-0001-2345-6789"}

    def test_multiple_schemas(self):
        ids = {"doi:10.1234/test", "orcid:0000-0001-2345-6789", "viaf:123456789", "pmid:111"}
        result = check_ids_sparql(ids, SERVER, workers=1)
        assert result == {"doi:10.1234/test", "orcid:0000-0001-2345-6789", "viaf:123456789", "pmid:111"}

    def test_empty_set(self):
        result = check_ids_sparql(set(), SERVER, workers=1)
        assert result == set()

    def test_all_not_found(self):
        ids = {"doi:10.9999/a", "doi:10.9999/b"}
        result = check_ids_sparql(ids, SERVER, workers=1)
        assert result == set()

    def test_duplicate_identifier(self):
        ids = {"doi:10.1234/duplicate"}
        result = check_ids_sparql(ids, SERVER, workers=1)
        assert result == {"doi:10.1234/duplicate"}

    def test_progress_callback(self):
        total = 0

        def callback(batch_size: int) -> None:
            nonlocal total
            total += batch_size

        ids = {"doi:10.1234/test", "orcid:0000-0001-2345-6789"}
        check_ids_sparql(ids, SERVER, workers=1, progress_callback=callback)
        assert total == 2


class TestFilterSparqlResults:

    def test_all_ids_found(self):
        row = {"id": "doi:10.1234/test", "title": "Test"}
        row_hash = tuple(sorted(row.items()))
        results = [
            FileResult(
                file_path="file.csv",
                rows=[(row_hash, row)],
                stats=ProcessingStats(total_rows=1),
            )
        ]

        filter_sparql_results(results, {"doi:10.1234/test"})

        assert results[0].stats.existing_ids_rows == 1
        assert len(results[0].rows) == 0

    def test_no_ids_found(self):
        row = {"id": "doi:10.9999/notfound", "title": "Test"}
        row_hash = tuple(sorted(row.items()))
        results = [
            FileResult(
                file_path="file.csv",
                rows=[(row_hash, row)],
                stats=ProcessingStats(total_rows=1),
            )
        ]

        filter_sparql_results(results, set())

        assert results[0].stats.existing_ids_rows == 0
        assert len(results[0].rows) == 1

    def test_partial_ids_found(self):
        row = {"id": "doi:10.1234/test doi:10.9999/notfound", "title": "Test"}
        row_hash = tuple(sorted(row.items()))
        results = [
            FileResult(
                file_path="file.csv",
                rows=[(row_hash, row)],
                stats=ProcessingStats(total_rows=1),
            )
        ]

        filter_sparql_results(results, {"doi:10.1234/test"})

        assert results[0].stats.existing_ids_rows == 0
        assert len(results[0].rows) == 1

    def test_empty_id_field(self):
        row = {"id": "", "title": "Test"}
        row_hash = tuple(sorted(row.items()))
        results = [
            FileResult(
                file_path="file.csv",
                rows=[(row_hash, row)],
                stats=ProcessingStats(total_rows=1),
            )
        ]

        filter_sparql_results(results, {"doi:10.1234/test"})

        assert results[0].stats.existing_ids_rows == 0
        assert len(results[0].rows) == 1

    def test_multiple_files_mixed(self):
        row1 = {"id": "doi:10.1234/test", "title": "Exists"}
        row2 = {"id": "doi:10.9999/new", "title": "New"}
        row3 = {"id": "doi:10.1234/test doi:10.1234/a", "title": "Both exist"}
        row1_hash = tuple(sorted(row1.items()))
        row2_hash = tuple(sorted(row2.items()))
        row3_hash = tuple(sorted(row3.items()))

        results = [
            FileResult(
                file_path="file1.csv",
                rows=[(row1_hash, row1), (row2_hash, row2)],
                stats=ProcessingStats(total_rows=2),
            ),
            FileResult(
                file_path="file2.csv",
                rows=[(row3_hash, row3)],
                stats=ProcessingStats(total_rows=1),
            ),
        ]

        filter_sparql_results(results, {"doi:10.1234/test", "doi:10.1234/a"})

        assert results[0].stats.existing_ids_rows == 1
        assert len(results[0].rows) == 1
        assert results[0].rows[0][1]["id"] == "doi:10.9999/new"

        assert results[1].stats.existing_ids_rows == 1
        assert len(results[1].rows) == 0


class TestSparqlIntegration:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.test_dir = tempfile.mkdtemp(dir=".")
        self.output_dir = tempfile.mkdtemp(dir=".")
        add_data_ts(SERVER, DATA_FILE)
        yield
        shutil.rmtree(self.test_dir)
        shutil.rmtree(self.output_dir)

    def test_full_sparql_pipeline(self):
        csv_path = os.path.join(self.test_dir, "data.csv")
        csv_data = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.1234/test","Existing Title","Author 1","2024","Venue 1","1","1","1-10","journal article","Publisher 1",
"doi:10.9999/notfound","New Title","Author 2","2024","Venue 2","2","1","11-20","journal article","Publisher 2",
"doi:10.1234/test doi:10.1234/a","Both Existing","Author 3","2024","Venue 3","3","1","21-30","journal article","Publisher 3",
"doi:10.1234/test doi:10.9999/notfound","Partial","Author 4","2024","Venue 4","4","1","31-40","journal article","Publisher 4",'''

        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(csv_data)

        result = collect_rows_from_file(csv_path)
        assert result.stats.total_rows == 4

        all_ids: set[str] = set()
        for _hash, row in result.rows:
            ids_str = row["id"]
            if ids_str:
                all_ids.update(ids_str.split())

        found_ids = check_ids_sparql(all_ids, SERVER, workers=1)

        assert "doi:10.1234/test" in found_ids
        assert "doi:10.1234/a" in found_ids
        assert "doi:10.9999/notfound" not in found_ids

        filter_sparql_results([result], found_ids)

        assert result.stats.existing_ids_rows == 2
        assert len(result.rows) == 2

        remaining_ids = {row["id"] for _hash, row in result.rows}
        assert remaining_ids == {
            "doi:10.9999/notfound",
            "doi:10.1234/test doi:10.9999/notfound",
        }

        total_stats = deduplicate_and_write([result], self.output_dir, rows_per_file=10)

        assert total_stats.processed_rows == 2
        assert total_stats.existing_ids_rows == 2

        output_files = os.listdir(self.output_dir)
        assert len(output_files) == 1

        rows = get_csv_data(os.path.join(self.output_dir, "0.csv"), clean_data=False)
        assert len(rows) == 2

    def test_sparql_cross_file_deduplication(self):
        file1_path = os.path.join(self.test_dir, "data1.csv")
        file1_data = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.9999/new1","New Title 1","Author 1","2024","Venue","1","1","1-10","journal article","Publisher",
"doi:10.1234/test","Existing","Author 2","2024","Venue","2","1","11-20","journal article","Publisher",'''

        file2_path = os.path.join(self.test_dir, "data2.csv")
        file2_data = '''id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
"doi:10.9999/new1","New Title 1","Author 1","2024","Venue","1","1","1-10","journal article","Publisher",
"doi:10.9999/new2","New Title 2","Author 3","2024","Venue","3","1","21-30","journal article","Publisher",'''

        with open(file1_path, "w", encoding="utf-8") as f:
            f.write(file1_data)
        with open(file2_path, "w", encoding="utf-8") as f:
            f.write(file2_data)

        result1 = collect_rows_from_file(file1_path)
        result2 = collect_rows_from_file(file2_path)
        results = [result1, result2]

        all_ids: set[str] = set()
        for result in results:
            for _hash, row in result.rows:
                ids_str = row["id"]
                if ids_str:
                    all_ids.update(ids_str.split())

        found_ids = check_ids_sparql(all_ids, SERVER, workers=1)
        filter_sparql_results(results, found_ids)

        total_stats = deduplicate_and_write(results, self.output_dir, rows_per_file=10)

        assert total_stats.total_rows == 4
        assert total_stats.existing_ids_rows == 1
        assert total_stats.duplicate_rows == 1
        assert total_stats.processed_rows == 2

        rows = get_csv_data(os.path.join(self.output_dir, "0.csv"), clean_data=False)
        assert len(rows) == 2
        output_ids = {row["id"] for row in rows}
        assert output_ids == {"doi:10.9999/new1", "doi:10.9999/new2"}


class TestSingleFileOutput:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.output_dir = tempfile.mkdtemp(dir=".")
        yield
        shutil.rmtree(self.output_dir)

    def test_single_file_to_directory(self):
        row1 = {"id": "doi:10.INVALID/1", "title": "Title 1"}
        row2 = {"id": "doi:10.INVALID/2", "title": "Title 2"}
        row1_hash = tuple(sorted(row1.items()))
        row2_hash = tuple(sorted(row2.items()))

        results = [
            FileResult(
                file_path="file.csv",
                rows=[(row1_hash, row1), (row2_hash, row2)],
                stats=ProcessingStats(total_rows=2),
            )
        ]

        total_stats = deduplicate_and_write(results, self.output_dir)

        assert total_stats.processed_rows == 2

        merged_path = os.path.join(self.output_dir, "merged.csv")
        assert os.path.exists(merged_path)

        rows = get_csv_data(merged_path, clean_data=False)
        assert len(rows) == 2

    def test_single_file_to_csv_path(self):
        row = {"id": "doi:10.INVALID/1", "title": "Title"}
        row_hash = tuple(sorted(row.items()))

        results = [
            FileResult(
                file_path="file.csv",
                rows=[(row_hash, row)],
                stats=ProcessingStats(total_rows=1),
            )
        ]

        csv_path = os.path.join(self.output_dir, "output.csv")
        total_stats = deduplicate_and_write(results, csv_path)

        assert total_stats.processed_rows == 1
        assert os.path.exists(csv_path)

        rows = get_csv_data(csv_path, clean_data=False)
        assert len(rows) == 1

    def test_single_file_no_splitting(self):
        rows_data = []
        for i in range(20):
            row = {"id": f"doi:10.INVALID/{i}", "title": f"Title {i}"}
            row_hash = tuple(sorted(row.items()))
            rows_data.append((row_hash, row))

        results = [
            FileResult(
                file_path="file.csv",
                rows=rows_data,
                stats=ProcessingStats(total_rows=20),
            )
        ]

        total_stats = deduplicate_and_write(results, self.output_dir)

        assert total_stats.processed_rows == 20

        output_files = os.listdir(self.output_dir)
        assert len(output_files) == 1

        rows = get_csv_data(os.path.join(self.output_dir, "merged.csv"), clean_data=False)
        assert len(rows) == 20
