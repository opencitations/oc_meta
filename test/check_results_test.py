#!/usr/bin/python

# SPDX-FileCopyrightText: 2025-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import csv
import json
import os
import zipfile

import orjson
import pytest
import yaml

from oc_meta.run.meta.check_results import (
    check_omids_existence,
    check_provenance_existence,
    find_file,
    parse_identifiers,
)
from test.test_utils import (
    PROV_SERVER,
    SERVER,
    add_data_ts
)

DATA_FILE = os.path.abspath(
    os.path.join("test", "testcases", "ts", "check_results_data.nt")
)
PROV_FILE = os.path.abspath(
    os.path.join("test", "testcases", "ts", "check_results_prov.nq")
)


class TestParseIdentifiers:

    def test_parse_single_identifier(self):
        result = parse_identifiers("doi:10.1234/test")
        assert result == [{'schema': 'doi', 'value': '10.1234/test'}]

    def test_parse_multiple_identifiers(self):
        result = parse_identifiers("doi:10.1234/test isbn:978-3-16-148410-0")
        assert result == [
            {'schema': 'doi', 'value': '10.1234/test'},
            {'schema': 'isbn', 'value': '978-3-16-148410-0'}
        ]

    def test_parse_identifier_with_colon_in_value(self):
        result = parse_identifiers("url:http://example.com:8080/path")
        assert result == [{'schema': 'url', 'value': 'http://example.com:8080/path'}]

    def test_parse_empty_string(self):
        assert parse_identifiers("") == []

    def test_parse_whitespace_only(self):
        assert parse_identifiers("   ") == []

    def test_parse_none(self):
        assert parse_identifiers(None) == []

    def test_parse_with_uppercase_schema(self):
        result = parse_identifiers("DOI:10.1234/test")
        assert result == [{'schema': 'doi', 'value': '10.1234/test'}]

    def test_parse_malformed_identifier(self):
        assert parse_identifiers("malformed") == []


class TestFindFile:

    def test_find_file_zip_format(self):
        uri = "https://w3id.org/oc/meta/br/0605"
        result = find_file("/base/rdf", 10000, 1000, uri, True)
        assert result == "/base/rdf/br/060/10000/1000.zip"

    def test_find_file_json_format(self):
        uri = "https://w3id.org/oc/meta/br/0605"
        result = find_file("/base/rdf", 10000, 1000, uri, False)
        assert result == "/base/rdf/br/060/10000/1000.json"

    def test_find_file_with_subfolder(self):
        uri = "https://w3id.org/oc/meta/br/06012345"
        result = find_file("/base/rdf", 10000, 1000, uri, True)
        assert result == "/base/rdf/br/060/20000/13000.zip"

    def test_find_file_different_entity_type(self):
        uri = "https://w3id.org/oc/meta/ra/0605"
        result = find_file("/base/rdf", 10000, 1000, uri, True)
        assert result == "/base/rdf/ra/060/10000/1000.zip"

    def test_find_file_invalid_uri(self):
        assert find_file("/base/rdf", 10000, 1000, "invalid-uri", True) is None

    def test_find_file_boundary_values(self):
        uri = "https://w3id.org/oc/meta/br/06010000"
        result = find_file("/base/rdf", 10000, 1000, uri, True)
        assert result == "/base/rdf/br/060/10000/10000.zip"


class TestCheckOMIDsExistence:

    @pytest.fixture(autouse=True)
    def _load_data(self):
        add_data_ts(SERVER, DATA_FILE)

    def test_single_identifier_found(self):
        identifiers = [{'schema': 'doi', 'value': '10.1234/test'}]
        result = check_omids_existence(identifiers, SERVER, workers=1)
        assert result == {'doi:10.1234/test': {'https://w3id.org/oc/meta/id/0601'}}

    def test_identifier_not_found(self):
        identifiers = [{'schema': 'doi', 'value': '10.9999/notfound'}]
        result = check_omids_existence(identifiers, SERVER, workers=1)
        assert result == {}

    def test_multiple_omids_for_identifier(self):
        identifiers = [{'schema': 'doi', 'value': '10.1234/duplicate'}]
        result = check_omids_existence(identifiers, SERVER, workers=1)
        assert result == {
            'doi:10.1234/duplicate': {
                'https://w3id.org/oc/meta/id/0602',
                'https://w3id.org/oc/meta/id/0603'
            }
        }

    def test_empty_identifiers_list(self):
        assert check_omids_existence([], SERVER, workers=1) == {}

    def test_multiple_schemas(self):
        identifiers = [
            {'schema': 'doi', 'value': '10.1234/test'},
            {'schema': 'orcid', 'value': '0000-0001-2345-6789'},
            {'schema': 'viaf', 'value': '123456789'},
        ]
        result = check_omids_existence(identifiers, SERVER, workers=1)
        assert result == {
            'doi:10.1234/test': {'https://w3id.org/oc/meta/id/0601'},
            'orcid:0000-0001-2345-6789': {'https://w3id.org/oc/meta/id/0604'},
            'viaf:123456789': {'https://w3id.org/oc/meta/id/0606'},
        }

    def test_mix_found_and_not_found(self):
        identifiers = [
            {'schema': 'doi', 'value': '10.1234/test'},
            {'schema': 'doi', 'value': '10.9999/notfound'},
        ]
        result = check_omids_existence(identifiers, SERVER, workers=1)
        assert result == {'doi:10.1234/test': {'https://w3id.org/oc/meta/id/0601'}}


class TestCheckProvenanceExistence:

    @pytest.fixture(autouse=True)
    def _load_prov(self):
        add_data_ts(PROV_SERVER, PROV_FILE)

    def test_provenance_exists(self):
        omids = ["https://w3id.org/oc/meta/id/0601"]
        result = check_provenance_existence(omids, PROV_SERVER, workers=1)
        assert result == {"https://w3id.org/oc/meta/id/0601": True}

    def test_provenance_not_exists(self):
        omids = ["https://w3id.org/oc/meta/id/0609"]
        result = check_provenance_existence(omids, PROV_SERVER, workers=1)
        assert result == {"https://w3id.org/oc/meta/id/0609": False}

    def test_multiple_omids_mixed_results(self):
        omids = [
            "https://w3id.org/oc/meta/id/0601",
            "https://w3id.org/oc/meta/id/0609",
            "https://w3id.org/oc/meta/id/0603",
        ]
        result = check_provenance_existence(omids, PROV_SERVER, workers=1)
        assert result == {
            "https://w3id.org/oc/meta/id/0601": True,
            "https://w3id.org/oc/meta/id/0609": False,
            "https://w3id.org/oc/meta/id/0603": True,
        }

    def test_empty_omids_list(self):
        assert check_provenance_existence([], PROV_SERVER, workers=1) == {}

    def test_all_have_provenance(self):
        omids = [
            f"https://w3id.org/oc/meta/id/060{i}" for i in range(1, 9)
        ]
        result = check_provenance_existence(omids, PROV_SERVER, workers=1)
        assert result == {omid: True for omid in omids}

    def test_none_have_provenance(self):
        omids = [
            "https://w3id.org/oc/meta/id/0609",
            "https://w3id.org/oc/meta/id/0610",
        ]
        result = check_provenance_existence(omids, PROV_SERVER, workers=1)
        assert result == {
            "https://w3id.org/oc/meta/id/0609": False,
            "https://w3id.org/oc/meta/id/0610": False,
        }


class TestMainIntegration:

    @pytest.fixture(autouse=True)
    def _load_stores(self):
        add_data_ts(SERVER, DATA_FILE)
        add_data_ts(PROV_SERVER, PROV_FILE)

    @staticmethod
    def _create_csv(dir_path: str, filename: str, rows: list[dict]) -> str:
        csv_path = os.path.join(dir_path, filename)
        fieldnames = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                full_row = {k: '' for k in fieldnames}
                full_row.update(row)
                writer.writerow(full_row)
        return csv_path

    @staticmethod
    def _create_rdf_zip(rdf_dir: str, entity_type: str, prefix: str, dir_split: int, file_split: int, *omid_uris: str):
        target_dir = os.path.join(rdf_dir, entity_type, prefix, str(dir_split))
        os.makedirs(target_dir, exist_ok=True)
        zip_path = os.path.join(target_dir, f"{file_split}.zip")

        content = orjson.dumps([{"@id": uri} for uri in omid_uris])
        with zipfile.ZipFile(zip_path, 'w') as z:
            z.writestr(f"{file_split}.json", content)
        return zip_path

    @staticmethod
    def _create_prov_zip(data_zip_path: str, *omid_uris: str):

        base_dir = os.path.dirname(data_zip_path)
        file_name = os.path.splitext(os.path.basename(data_zip_path))[0]
        prov_dir = os.path.join(base_dir, file_name, "prov")
        os.makedirs(prov_dir, exist_ok=True)
        content = orjson.dumps([
            {"@id": f"{uri}/prov/", "@graph": [{"@id": f"{uri}/prov/se/1"}]}
            for uri in omid_uris
        ])
        with zipfile.ZipFile(os.path.join(prov_dir, "se.zip"), 'w') as z:
            z.writestr("se.json", content)

    @staticmethod
    def _write_config(tmp_path, input_dir, output_dir) -> str:
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump({
            "input_csv_dir": str(input_dir),
            "output_rdf_dir": str(output_dir),
            "triplestore_url": SERVER,
            "provenance_triplestore_url": PROV_SERVER,
            "dir_split_number": 10000,
            "items_per_file": 1000,
            "zip_output_rdf": True,
        }))
        return str(config_path)

    @staticmethod
    def _run_main(config_path: str, output_path: str):
        from unittest.mock import patch
        with patch('sys.argv', ['check_results', config_path, output_path]):
            from oc_meta.run.meta.check_results import main
            main()

    @staticmethod
    def _run_main_expect_fail(config_path: str, output_path: str):
        from unittest.mock import patch
        with patch('sys.argv', ['check_results', config_path, output_path]):
            from oc_meta.run.meta.check_results import main
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_pass_status(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        rdf_dir = output_dir / "rdf"
        rdf_dir.mkdir(parents=True)

        self._create_csv(str(input_dir), "test.csv", [
            {"id": "doi:10.1234/test", "title": "Test"}
        ])

        omid_uri = "https://w3id.org/oc/meta/id/0601"
        zip_path = self._create_rdf_zip(str(rdf_dir), "id", "060", 10000, 1000, omid_uri)
        self._create_prov_zip(zip_path, omid_uri)

        config_path = self._write_config(tmp_path, input_dir, output_dir)
        output_path = str(tmp_path / "report.json")
        self._run_main(config_path, output_path)

        with open(output_path) as f:
            report = json.load(f)

        assert report["status"] == "PASS"
        assert report["total_files_processed"] == 1
        assert report["errors"] == []
        assert len(report["files"]) == 1
        assert report["files"][0]["file"] == "test.csv"
        assert report["files"][0]["total_rows"] == 1
        assert "identifiers_details" not in report["files"][0]
        assert "processed_omids" not in report["files"][0]

    def test_fail_status_missing_omid(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        rdf_dir = output_dir / "rdf"
        rdf_dir.mkdir(parents=True)

        self._create_csv(str(input_dir), "test.csv", [
            {"id": "doi:10.1234/missing", "title": "Missing"}
        ])

        config_path = self._write_config(tmp_path, input_dir, output_dir)
        output_path = str(tmp_path / "report.json")
        self._run_main_expect_fail(config_path, output_path)

        with open(output_path) as f:
            report = json.load(f)

        assert report["status"] == "FAIL"
        assert len(report["errors"]) == 1
        assert report["errors"][0]["type"] == "missing_omid"
        assert report["errors"][0]["schema"] == "doi"
        assert report["errors"][0]["value"] == "10.1234/missing"
        assert report["errors"][0]["row"] == 1
        assert report["errors"][0]["column"] == "id"

    def test_warning_multiple_omids(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        rdf_dir = output_dir / "rdf"
        rdf_dir.mkdir(parents=True)

        self._create_csv(str(input_dir), "file1.csv", [
            {"id": "doi:10.1234/duplicate", "title": "Dup1"}
        ])
        self._create_csv(str(input_dir), "file2.csv", [
            {"id": "doi:10.1234/duplicate", "title": "Dup2"}
        ])

        omid1 = "https://w3id.org/oc/meta/id/0602"
        omid2 = "https://w3id.org/oc/meta/id/0603"

        for omid_uri, file_num in [(omid1, 1000), (omid2, 2000)]:
            zip_path = self._create_rdf_zip(str(rdf_dir), "id", "060", 10000, file_num, omid_uri)
            self._create_prov_zip(zip_path, omid_uri)

        config_path = self._write_config(tmp_path, input_dir, output_dir)
        output_path = str(tmp_path / "report.json")
        self._run_main(config_path, output_path)

        with open(output_path) as f:
            report = json.load(f)

        assert report["status"] == "PASS"
        assert len(report["warnings"]) == 1
        assert report["warnings"][0]["type"] == "multiple_omids"
        assert report["warnings"][0]["identifier"] == "doi:10.1234/duplicate"
        assert report["warnings"][0]["omid_count"] == 2
        assert len(report["warnings"][0]["omids"]) == 2

    def test_rdf_dir_not_found(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()

        config_path = self._write_config(tmp_path, input_dir, tmp_path / "nonexistent")
        output_path = str(tmp_path / "report.json")

        from unittest.mock import patch
        with patch('sys.argv', ['check_results', config_path, output_path]):
            from oc_meta.run.meta.check_results import main
            main()

        assert not os.path.exists(output_path)

    def test_no_csv_files(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        rdf_dir = output_dir / "rdf"
        rdf_dir.mkdir(parents=True)

        config_path = self._write_config(tmp_path, input_dir, output_dir)
        output_path = str(tmp_path / "report.json")

        from unittest.mock import patch
        with patch('sys.argv', ['check_results', config_path, output_path]):
            from oc_meta.run.meta.check_results import main
            main()

        assert not os.path.exists(output_path)

    def test_author_and_editor_identifiers(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        rdf_dir = output_dir / "rdf"
        rdf_dir.mkdir(parents=True)

        self._create_csv(str(input_dir), "test.csv", [
            {
                "id": "doi:10.1234/test",
                "author": "Doe, John [orcid:0000-0001-2345-6789]; Smith, Jane [orcid:0000-0002-3456-7890]",
                "editor": "Brown, Bob [viaf:123456789]",
            }
        ])

        omid_uris = [
            "https://w3id.org/oc/meta/id/0601",
            "https://w3id.org/oc/meta/id/0604",
            "https://w3id.org/oc/meta/id/0605",
            "https://w3id.org/oc/meta/id/0606",
        ]
        zip_path = self._create_rdf_zip(str(rdf_dir), "id", "060", 10000, 1000, *omid_uris)
        self._create_prov_zip(zip_path, *omid_uris)

        config_path = self._write_config(tmp_path, input_dir, output_dir)
        output_path = str(tmp_path / "report.json")
        self._run_main(config_path, output_path)

        with open(output_path) as f:
            report = json.load(f)

        assert report["status"] == "PASS"
        assert report["summary"]["total_identifiers"] == 4
        assert report["summary"]["identifiers_with_omids"] == 4

    def test_omid_schema_identifiers(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        rdf_dir = output_dir / "rdf"
        rdf_dir.mkdir(parents=True)

        self._create_csv(str(input_dir), "test.csv", [
            {"id": "omid:https://w3id.org/oc/meta/br/0601 doi:10.1234/test", "title": "Test"}
        ])

        omid_uri = "https://w3id.org/oc/meta/id/0601"
        zip_path = self._create_rdf_zip(str(rdf_dir), "id", "060", 10000, 1000, omid_uri)
        self._create_prov_zip(zip_path, omid_uri)

        config_path = self._write_config(tmp_path, input_dir, output_dir)
        output_path = str(tmp_path / "report.json")
        self._run_main(config_path, output_path)

        with open(output_path) as f:
            report = json.load(f)

        assert report["summary"]["omid_schema_identifiers"] == 1
        assert report["summary"]["total_identifiers"] == 2
        assert report["summary"]["omids_without_provenance"] == 1

    def test_missing_provenance_error(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        rdf_dir = output_dir / "rdf"
        rdf_dir.mkdir(parents=True)

        self._create_csv(str(input_dir), "test.csv", [
            {"id": "doi:10.1234/noprov", "title": "Test"}
        ])

        omid_uri = "https://w3id.org/oc/meta/id/0609"
        zip_path = self._create_rdf_zip(str(rdf_dir), "id", "060", 10000, 1000, omid_uri)
        self._create_prov_zip(zip_path, omid_uri)

        config_path = self._write_config(tmp_path, input_dir, output_dir)
        output_path = str(tmp_path / "report.json")
        self._run_main_expect_fail(config_path, output_path)

        with open(output_path) as f:
            report = json.load(f)

        assert report["status"] == "FAIL"
        prov_errors = [e for e in report["errors"] if e["type"] == "missing_provenance"]
        assert len(prov_errors) == 1
        assert prov_errors[0]["omid"] == omid_uri

    def test_missing_prov_file_on_disk(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        rdf_dir = output_dir / "rdf"
        rdf_dir.mkdir(parents=True)

        self._create_csv(str(input_dir), "test.csv", [
            {"id": "doi:10.1234/test", "title": "Test"}
        ])

        omid_uri = "https://w3id.org/oc/meta/id/0601"
        self._create_rdf_zip(str(rdf_dir), "id", "060", 10000, 1000, omid_uri)
        # No prov zip on disk

        config_path = self._write_config(tmp_path, input_dir, output_dir)
        output_path = str(tmp_path / "report.json")
        self._run_main(config_path, output_path)

        with open(output_path) as f:
            report = json.load(f)

        assert report["summary"]["prov_graphs_missing"] == 1
        assert report["summary"]["data_graphs_found"] == 1

    def test_cache_hit_same_file(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        rdf_dir = output_dir / "rdf"
        rdf_dir.mkdir(parents=True)

        self._create_csv(str(input_dir), "test.csv", [
            {"id": "doi:10.1234/a pmid:111", "title": "Test"}
        ])

        omid1 = "https://w3id.org/oc/meta/id/0607"
        omid2 = "https://w3id.org/oc/meta/id/0608"
        zip_path = self._create_rdf_zip(str(rdf_dir), "id", "060", 10000, 1000, omid1, omid2)
        self._create_prov_zip(zip_path, omid1)

        config_path = self._write_config(tmp_path, input_dir, output_dir)
        output_path = str(tmp_path / "report.json")
        self._run_main(config_path, output_path)

        with open(output_path) as f:
            report = json.load(f)

        assert report["status"] == "PASS"
        assert report["summary"]["identifiers_with_omids"] == 2
        # Both OMIDs in same zip -> 1 file checked, both data found
        assert report["summary"]["data_graphs_found"] == 2
