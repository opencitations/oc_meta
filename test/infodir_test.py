# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import os
import tempfile

import orjson
import pytest
from rdflib import URIRef

from oc_meta.run.infodir.check import explore_provenance_files, load_counters
from oc_meta.run.infodir.gen import explore_directories
from oc_ocdm.counter_handler.filesystem_counter_handler import FilesystemCounterHandler
from oc_ocdm.support import get_count


class TestGenInfoDir:
    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        self.root_dir = os.path.join("test", "gen_info_dir", "rdf")
        self.info_dir = tempfile.mkdtemp()
        self.supplier_prefix = "0670"
        yield

    def test_explore_directories(self):
        info_dir_with_prefix = (
            os.path.join(self.info_dir, self.supplier_prefix) + os.sep
        )
        explore_directories(self.root_dir, self.info_dir)

        counter_handler = FilesystemCounterHandler(
            info_dir=info_dir_with_prefix, supplier_prefix=self.supplier_prefix
        )
        br_counter = counter_handler.read_counter("br", supplier_prefix="0670")
        assert br_counter == 386000

        prov_counter_101 = counter_handler.read_counter(
            entity_short_name="br",
            prov_short_name="se",
            identifier=int(get_count(URIRef("https://w3id.org/oc/meta/br/0670101"))),
            supplier_prefix="0670",
        )
        prov_counter_3 = counter_handler.read_counter(
            entity_short_name="br",
            prov_short_name="se",
            identifier=int(get_count(URIRef("https://w3id.org/oc/meta/br/06703"))),
            supplier_prefix="0670",
        )
        assert prov_counter_101 == 2
        assert prov_counter_3 == 1


def _write_counter_file(
    info_dir: str, prefix: str, filename: str, values: dict[int, int]
) -> None:
    prefix_dir = os.path.join(info_dir, prefix)
    os.makedirs(prefix_dir, exist_ok=True)
    max_line = max(values) if values else 0
    lines = []
    for i in range(1, max_line + 1):
        lines.append(str(values[i]) if i in values else "")
    file_path = os.path.join(prefix_dir, filename)
    with open(file_path, "w") as f:
        f.write("\n".join(lines) + "\n")


class TestCheckInfoDir:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.root_dir = os.path.join("test", "gen_info_dir", "rdf")
        self.info_dir = str(tmp_path / "info_dir")
        os.makedirs(self.info_dir)
        self.output_path = str(tmp_path / "report.json")

    def test_no_issues(self):
        _write_counter_file(self.info_dir, "0670", "info_file_br.txt", {1: 386000})
        _write_counter_file(self.info_dir, "0670", "prov_file_br.txt", {3: 1, 101: 2})
        explore_provenance_files(self.root_dir, self.info_dir, self.output_path)
        with open(self.output_path, "rb") as f:
            report = orjson.loads(f.read())
        assert report["total_zip_files"] == 1
        assert report["total_mismatched_entity_counters"] == 0
        assert report["total_mismatched_prov_counters"] == 0

    def test_mismatched_entity_counter(self):
        _write_counter_file(self.info_dir, "0670", "info_file_br.txt", {1: 50})
        _write_counter_file(self.info_dir, "0670", "prov_file_br.txt", {3: 1, 101: 2})
        explore_provenance_files(self.root_dir, self.info_dir, self.output_path)
        with open(self.output_path, "rb") as f:
            report = orjson.loads(f.read())
        assert report["total_mismatched_entity_counters"] == 1
        assert report["mismatched_entity_counters"][0]["prefix"] == "0670"
        assert report["mismatched_entity_counters"][0]["short_name"] == "br"
        assert report["mismatched_entity_counters"][0]["expected_min"] == 101
        assert report["mismatched_entity_counters"][0]["actual"] == 50
        assert report["total_mismatched_prov_counters"] == 0

    def test_mismatched_prov_counter(self):
        _write_counter_file(self.info_dir, "0670", "info_file_br.txt", {1: 386000})
        _write_counter_file(self.info_dir, "0670", "prov_file_br.txt", {3: 1, 101: 1})
        explore_provenance_files(self.root_dir, self.info_dir, self.output_path)
        with open(self.output_path, "rb") as f:
            report = orjson.loads(f.read())
        assert report["total_mismatched_entity_counters"] == 0
        assert report["total_mismatched_prov_counters"] == 1
        assert (
            report["mismatched_prov_counters"][0]["entity_uri"]
            == "https://w3id.org/oc/meta/br/0670101"
        )
        assert report["mismatched_prov_counters"][0]["expected"] == 2
        assert report["mismatched_prov_counters"][0]["actual"] == 1

    def test_all_issues_when_no_counters(self):
        _write_counter_file(self.info_dir, "0670", "info_file_br.txt", {1: 0})
        _write_counter_file(self.info_dir, "0670", "prov_file_br.txt", {1: 0})
        explore_provenance_files(self.root_dir, self.info_dir, self.output_path)
        with open(self.output_path, "rb") as f:
            report = orjson.loads(f.read())
        assert report["total_mismatched_entity_counters"] == 1
        assert report["mismatched_entity_counters"][0]["expected_min"] == 101
        assert report["total_mismatched_prov_counters"] == 2

    def test_load_counters(self):
        _write_counter_file(self.info_dir, "0670", "info_file_br.txt", {1: 386000})
        _write_counter_file(self.info_dir, "0670", "prov_file_br.txt", {3: 1, 101: 2})
        _write_counter_file(self.info_dir, "0670", "prov_file_ra.txt", {5: 3})
        entity_counters, prov_counters = load_counters(self.info_dir)
        assert entity_counters["0670"]["br"] == 386000
        assert prov_counters["0670"]["br"][2] == 1
        assert prov_counters["0670"]["br"][100] == 2
        assert prov_counters["0670"]["ra"][4] == 3
