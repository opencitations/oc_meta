#!/usr/bin/env python

# SPDX-FileCopyrightText: 2023-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import csv
import os
import shutil
import sqlite3
import tempfile
import zipfile
from contextlib import closing

import pytest

import orjson
from oc_meta.run.find.duplicates import (
    disk_identifier_duplicate_groups,
    find_duplicate_ids,
    process_identifier_chunk,
    process_identifier_zip_file,
)


def _identifier_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE identifiers (scheme TEXT NOT NULL, value TEXT NOT NULL, "
        "uri TEXT NOT NULL, PRIMARY KEY (scheme, value, uri)) WITHOUT ROWID"
    )
    return connection


class TestDuplicatedIds:
    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        self.test_dir = tempfile.mkdtemp()
        self.id_dir = os.path.join(self.test_dir, "id")
        os.makedirs(self.id_dir)
        self.temp_dir = tempfile.mkdtemp()

        self.test_rdf_with_duplicates = self._create_test_rdf_data()
        self.test_zip_paths = self._create_test_zip_files()

        yield

        shutil.rmtree(self.test_dir, ignore_errors=True)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_test_rdf_data(self):
        rdf_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/1",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/doi"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "10.1234/test1"
                        },
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/2",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/doi"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "10.1234/test1"
                        },
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/3",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/doi"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "10.1234/test2"
                        },
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/4",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/orcid"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "0000-0001-2345-6789"
                        },
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/5",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/orcid"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "0000-0001-2345-6789"
                        },
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/6",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/doi"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "10.1234/test3",
                            "@type": "http://www.w3.org/2001/XMLSchema#string",
                        },
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/7",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/doi"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "10.1234/test3"
                        },
                    }
                ]
            },
        ]
        return rdf_data

    def _create_test_zip_files(self):
        zip_paths = []
        for i in range(4):
            zip_path = os.path.join(self.id_dir, f"test_{i}.zip")
            with zipfile.ZipFile(zip_path, "w") as zf:
                start_idx = i * 2
                end_idx = min(start_idx + 2, len(self.test_rdf_with_duplicates))
                for j in range(start_idx, end_idx):
                    rdf_content = orjson.dumps(self.test_rdf_with_duplicates[j])
                    zf.writestr(f"rdf_{j}.json", rdf_content)
            zip_paths.append(zip_path)
        return zip_paths

    def test_process_zip_file(self):
        result = process_identifier_zip_file(self.test_zip_paths[0])

        assert isinstance(result, dict)
        assert len(result) > 0

        doi_key = ("http://purl.org/spar/datacite/doi", "10.1234/test1")
        assert doi_key in result
        assert "https://w3id.org/oc/meta/id/1" in result[doi_key]

    def test_process_chunk(self):
        with closing(_identifier_connection()) as connection:
            process_identifier_chunk(self.test_zip_paths[:2], connection)

            rows = connection.execute(
                "SELECT scheme, value, uri FROM identifiers"
            ).fetchall()

        assert (
            "http://purl.org/spar/datacite/doi",
            "10.1234/test1",
            "https://w3id.org/oc/meta/id/1",
        ) in rows

    def test_duplicate_groups(self):
        doi = "http://purl.org/spar/datacite/doi"
        with closing(_identifier_connection()) as connection:
            connection.executemany(
                "INSERT INTO identifiers VALUES (?, ?, ?)",
                [
                    (doi, "10.1234/test1", "https://w3id.org/oc/meta/id/3"),
                    (doi, "10.1234/test1", "https://w3id.org/oc/meta/id/1"),
                    (doi, "10.1234/test1", "https://w3id.org/oc/meta/id/2"),
                    (doi, "10.1234/test2", "https://w3id.org/oc/meta/id/4"),
                ],
            )

            groups = list(disk_identifier_duplicate_groups(connection))

        assert groups == [
            (
                "https://w3id.org/oc/meta/id/1",
                ["https://w3id.org/oc/meta/id/2", "https://w3id.org/oc/meta/id/3"],
            )
        ]

    def test_find_duplicate_ids(self):
        output_csv = os.path.join(self.temp_dir, "output.csv")

        find_duplicate_ids(self.test_dir, output_csv, chunk_size=2)

        assert os.path.exists(output_csv)

        with open(output_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) > 0

    def test_default_temp_dir_uses_output_csv_directory_and_is_removed(self, capsys):
        output_dir = os.path.join(self.test_dir, "output")
        os.makedirs(output_dir)
        output_csv = os.path.join(output_dir, "duplicates.csv")

        find_duplicate_ids(self.test_dir, output_csv, chunk_size=2)

        captured = capsys.readouterr()
        temp_dir_prefix = "Temporary files will be stored in: "
        temp_dir_lines = [
            line
            for line in captured.out.splitlines()
            if line.startswith(temp_dir_prefix)
        ]

        assert len(temp_dir_lines) == 1

        temp_dir = temp_dir_lines[0].removeprefix(temp_dir_prefix)
        assert os.path.dirname(temp_dir) == output_dir
        assert not os.path.exists(temp_dir)

    def test_chunking_behavior(self):
        output_csv = os.path.join(self.temp_dir, "output_chunked.csv")

        find_duplicate_ids(self.test_dir, output_csv, chunk_size=1)

        assert os.path.exists(output_csv)

        with open(output_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            for row in rows:
                assert "surviving_entity" in row
                assert "merged_entities" in row

    def test_datatype_normalization(self):
        output_csv = os.path.join(self.temp_dir, "output_datatype.csv")

        find_duplicate_ids(self.test_dir, output_csv, chunk_size=2)

        assert os.path.exists(output_csv)

        with open(output_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            id6_and_id7_merged = False
            for row in rows:
                all_entities = {row["surviving_entity"]} | set(
                    row["merged_entities"].split("; ")
                )

                if (
                    "https://w3id.org/oc/meta/id/6" in all_entities
                    and "https://w3id.org/oc/meta/id/7" in all_entities
                ):
                    id6_and_id7_merged = True
                    break

            assert id6_and_id7_merged, (
                "ID 6 (with xsd:string datatype) and ID 7 (without datatype) should be merged as duplicates"
            )
