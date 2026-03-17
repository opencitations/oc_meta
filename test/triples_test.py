import gzip
import shutil
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

from oc_meta.run.count import triples

SAMPLE_NQUADS = """<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> <http://example.org/g1> .
<http://example.org/s2> <http://example.org/p2> "value" <http://example.org/g1> .
"""

SAMPLE_NQUADS_WITH_COMMENTS = """# This is a comment
<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> <http://example.org/g1> .

# Another comment
<http://example.org/s2> <http://example.org/p2> "value" <http://example.org/g1> .
"""

SAMPLE_TRIG = """@prefix ex: <http://example.org/> .
ex:g1 {
    ex:s1 ex:p1 ex:o1 .
    ex:s2 ex:p2 "value" .
}
"""

SAMPLE_TURTLE = """@prefix ex: <http://example.org/> .
ex:s1 ex:p1 ex:o1 .
ex:s2 ex:p2 "value" .
"""

SAMPLE_JSONLD = """{
  "@context": {"ex": "http://example.org/"},
  "@graph": [
    {"@id": "ex:s1", "ex:p1": {"@id": "ex:o1"}},
    {"@id": "ex:s2", "ex:p2": "value"}
  ]
}"""


@pytest.fixture
def temp_dir():
    path = Path(tempfile.mkdtemp())
    yield path
    shutil.rmtree(path)


@pytest.fixture
def sample_files(temp_dir: Path):
    data_dir = temp_dir / "data"
    prov_dir = temp_dir / "prov"
    data_dir.mkdir()
    prov_dir.mkdir()

    nq_file = data_dir / "data.nq"
    nq_file.write_text(SAMPLE_NQUADS)

    prov_nq_file = prov_dir / "prov.nq"
    prov_nq_file.write_text(SAMPLE_NQUADS)

    gz_file = data_dir / "data.nq.gz"
    with gzip.open(gz_file, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_NQUADS)

    zip_file = data_dir / "data.zip"
    with zipfile.ZipFile(zip_file, "w") as z:
        z.writestr("inner.json", SAMPLE_JSONLD)

    ttl_file = data_dir / "data.ttl"
    ttl_file.write_text(SAMPLE_TURTLE)

    return temp_dir


class TestDiscoverFiles:
    def test_directory_not_exists(self, temp_dir: Path) -> None:
        with pytest.raises(ValueError, match="does not exist"):
            triples.discover_files(
                temp_dir / "nonexistent", "*.nq", False, False, False
            )

    def test_no_files_match(self, temp_dir: Path) -> None:
        result = triples.discover_files(temp_dir, "*.xyz", False, False, False)
        assert result == []

    def test_non_recursive(self, sample_files: Path) -> None:
        result = triples.discover_files(sample_files, "*.nq", False, False, False)
        assert result == []

    def test_recursive(self, sample_files: Path) -> None:
        result = triples.discover_files(sample_files, "*.nq", True, False, False)
        assert len(result) == 2
        names = [f.name for f in result]
        assert "data.nq" in names
        assert "prov.nq" in names

    def test_prov_only(self, sample_files: Path) -> None:
        result = triples.discover_files(sample_files, "*.nq", True, True, False)
        assert len(result) == 1
        assert result[0].name == "prov.nq"

    def test_data_only(self, sample_files: Path) -> None:
        result = triples.discover_files(sample_files, "*.nq", True, False, True)
        assert len(result) == 1
        assert result[0].name == "data.nq"

    def test_glob_pattern(self, sample_files: Path) -> None:
        result = triples.discover_files(sample_files, "*.ttl", True, False, False)
        assert len(result) == 1
        assert result[0].name == "data.ttl"

    def test_skips_directories(self, temp_dir: Path) -> None:
        subdir = temp_dir / "data.nq"
        subdir.mkdir()
        result = triples.discover_files(temp_dir, "*.nq", False, False, False)
        assert result == []


class TestCountInFile:
    def test_nquads_file(self, sample_files: Path) -> None:
        nq_file = sample_files / "data" / "data.nq"
        path, count, error = triples.count_in_file(nq_file, "nquads")
        assert str(nq_file) == path
        assert count == 2
        assert error is None

    def test_gzip_file(self, sample_files: Path) -> None:
        gz_file = sample_files / "data" / "data.nq.gz"
        path, count, error = triples.count_in_file(gz_file, "nquads")
        assert str(gz_file) == path
        assert count == 2
        assert error is None

    def test_zip_file(self, sample_files: Path) -> None:
        zip_file = sample_files / "data" / "data.zip"
        path, count, error = triples.count_in_file(zip_file, "json-ld")
        assert str(zip_file) == path
        assert count == 2
        assert error is None

    def test_turtle_file(self, sample_files: Path) -> None:
        ttl_file = sample_files / "data" / "data.ttl"
        path, count, error = triples.count_in_file(ttl_file, "turtle")
        assert str(ttl_file) == path
        assert count == 2
        assert error is None

    def test_corrupt_file(self, temp_dir: Path) -> None:
        corrupt_file = temp_dir / "corrupt.nq"
        corrupt_file.write_text("this is not valid nquads {{{}}")
        path, count, error = triples.count_in_file(corrupt_file, "nquads")
        assert str(corrupt_file) == path
        assert count == 0
        assert error is not None

    def test_nquads_with_comments_and_empty_lines(self, temp_dir: Path) -> None:
        nq_file = temp_dir / "comments.nq"
        nq_file.write_text(SAMPLE_NQUADS_WITH_COMMENTS)
        path, count, error = triples.count_in_file(nq_file, "nquads")
        assert str(nq_file) == path
        assert count == 2
        assert error is None

    def test_corrupt_gzip_file(self, temp_dir: Path) -> None:
        gz_file = temp_dir / "corrupt.nq.gz"
        with gzip.open(gz_file, "wt", encoding="utf-8") as f:
            f.write("invalid {{{")
        path, count, error = triples.count_in_file(gz_file, "nquads")
        assert str(gz_file) == path
        assert count == 0
        assert error is not None

    def test_gzip_with_comments(self, temp_dir: Path) -> None:
        gz_file = temp_dir / "comments.nq.gz"
        with gzip.open(gz_file, "wt", encoding="utf-8") as f:
            f.write(SAMPLE_NQUADS_WITH_COMMENTS)
        path, count, error = triples.count_in_file(gz_file, "nquads")
        assert str(gz_file) == path
        assert count == 2
        assert error is None

    def test_zip_with_nquads(self, temp_dir: Path) -> None:
        zip_file = temp_dir / "data.zip"
        with zipfile.ZipFile(zip_file, "w") as z:
            z.writestr("inner.nq", SAMPLE_NQUADS)
        path, count, error = triples.count_in_file(zip_file, "nquads")
        assert str(zip_file) == path
        assert count == 2
        assert error is None

    def test_trig_gzip_file(self, temp_dir: Path) -> None:
        gz_file = temp_dir / "data.trig.gz"
        with gzip.open(gz_file, "wt", encoding="utf-8") as f:
            f.write(SAMPLE_TRIG)
        path, count, error = triples.count_in_file(gz_file, "trig")
        assert str(gz_file) == path
        assert count == 2
        assert error is None


class TestProcessFiles:
    def test_basic_processing(self, sample_files: Path) -> None:
        files = [sample_files / "data" / "data.nq"]
        total, failures = triples.process_files(
            files, "nquads", 1, False, False, "quads"
        )
        assert total == 2
        assert failures == []

    def test_show_per_file(self, sample_files: Path, capsys) -> None:
        files = [sample_files / "data" / "data.nq"]
        total, failures = triples.process_files(
            files, "nquads", 1, True, False, "quads"
        )
        assert total == 2
        captured = capsys.readouterr()
        assert "data.nq" in captured.out
        assert ": 2" in captured.out

    def test_keep_going_false(self, temp_dir: Path) -> None:
        corrupt = temp_dir / "corrupt.nq"
        corrupt.write_text("invalid {{{")
        valid = temp_dir / "valid.nq"
        valid.write_text(SAMPLE_NQUADS)
        files = [corrupt, valid]
        total, failures = triples.process_files(
            files, "nquads", 1, False, False, "quads"
        )
        assert len(failures) == 1

    def test_keep_going_true(self, temp_dir: Path) -> None:
        corrupt = temp_dir / "corrupt.nq"
        corrupt.write_text("invalid {{{")
        valid = temp_dir / "valid.nq"
        valid.write_text(SAMPLE_NQUADS)
        files = [corrupt, valid]
        total, failures = triples.process_files(
            files, "nquads", 1, False, True, "quads"
        )
        assert total == 2
        assert len(failures) == 1

    def test_workers_none_uses_cpu_count(self, sample_files: Path) -> None:
        files = [sample_files / "data" / "data.nq"]
        total, failures = triples.process_files(
            files, "nquads", None, False, False, "quads"
        )
        assert total == 2

    def test_workers_zero_defaults_to_one(self, sample_files: Path) -> None:
        files = [sample_files / "data" / "data.nq"]
        with patch("oc_meta.run.count.triples.multiprocessing.cpu_count", return_value=0):
            total, failures = triples.process_files(
                files, "nquads", None, False, False, "quads"
            )
        assert total == 2

    def test_fast_jsonld(self, sample_files: Path) -> None:
        zip_file = sample_files / "data" / "data.zip"
        total, failures = triples.process_files(
            [zip_file], "json-ld", 1, False, False, "triples", fast=True
        )
        assert total == 2
        assert failures == []


class TestFastJsonLdCounting:
    def test_simple_graph(self) -> None:
        data = {
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:s1", "ex:p1": {"@id": "ex:o1"}},
                {"@id": "ex:s2", "ex:p2": "value"},
            ],
        }
        assert triples._count_jsonld_triples(data) == 2

    def test_multiple_types(self) -> None:
        data = {
            "@graph": [
                {"@id": "ex:s1", "@type": ["ex:Type1", "ex:Type2"], "ex:p1": "value"}
            ]
        }
        assert triples._count_jsonld_triples(data) == 3

    def test_single_type(self) -> None:
        data = {"@graph": [{"@id": "ex:s1", "@type": "ex:Type1", "ex:p1": "value"}]}
        assert triples._count_jsonld_triples(data) == 2

    def test_array_values(self) -> None:
        data = {"@graph": [{"@id": "ex:s1", "ex:p1": ["v1", "v2", "v3"]}]}
        assert triples._count_jsonld_triples(data) == 3

    def test_nested_blank_node(self) -> None:
        data = {
            "@graph": [{"@id": "ex:s1", "ex:p1": {"ex:nested_p": "nested_value"}}]
        }
        assert triples._count_jsonld_triples(data) == 2

    def test_value_object(self) -> None:
        data = {
            "@graph": [
                {"@id": "ex:s1", "ex:p1": {"@value": "typed", "@language": "en"}}
            ]
        }
        assert triples._count_jsonld_triples(data) == 1

    def test_list_container(self) -> None:
        data = {"@graph": [{"@id": "ex:s1", "ex:p1": {"@list": ["a", "b", "c"]}}]}
        assert triples._count_jsonld_triples(data) == 3

    def test_set_container(self) -> None:
        data = {"@graph": [{"@id": "ex:s1", "ex:p1": {"@set": ["a", "b"]}}]}
        assert triples._count_jsonld_triples(data) == 2

    def test_top_level_array(self) -> None:
        data = [
            {"@id": "ex:s1", "ex:p1": "v1"},
            {"@id": "ex:s2", "ex:p2": "v2"},
        ]
        assert triples._count_jsonld_triples(data) == 2

    def test_top_level_array_with_graphs(self) -> None:
        data = [
            {
                "@graph": [
                    {"@id": "ex:s1", "@type": ["ex:Type1"], "ex:p1": "v1"}
                ],
                "@id": "ex:graph1",
            },
            {
                "@graph": [
                    {"@id": "ex:s2", "ex:p2": {"@id": "ex:o2"}, "ex:p3": "v3"}
                ],
                "@id": "ex:graph2",
            },
        ]
        assert triples._count_jsonld_triples(data) == 4

    def test_single_object_without_graph(self) -> None:
        data = {"@id": "ex:s1", "ex:p1": "v1", "ex:p2": "v2"}
        assert triples._count_jsonld_triples(data) == 2

    def test_count_in_file_fast_plain(self, temp_dir: Path) -> None:
        json_file = temp_dir / "data.json"
        json_file.write_text(SAMPLE_JSONLD)
        path, count, error = triples.count_in_file(json_file, "json-ld", fast=True)
        assert count == 2
        assert error is None

    def test_count_in_file_fast_gzip(self, temp_dir: Path) -> None:
        gz_file = temp_dir / "data.json.gz"
        with gzip.open(gz_file, "wt", encoding="utf-8") as f:
            f.write(SAMPLE_JSONLD)
        path, count, error = triples.count_in_file(gz_file, "json-ld", fast=True)
        assert count == 2
        assert error is None

    def test_count_in_file_fast_zip(self, temp_dir: Path) -> None:
        zip_file = temp_dir / "data.zip"
        with zipfile.ZipFile(zip_file, "w") as z:
            z.writestr("inner.json", SAMPLE_JSONLD)
        path, count, error = triples.count_in_file(zip_file, "json-ld", fast=True)
        assert count == 2
        assert error is None

    def test_fast_flag_ignored_for_nquads(self, temp_dir: Path) -> None:
        nq_file = temp_dir / "data.nq"
        nq_file.write_text(SAMPLE_NQUADS)
        path, count, error = triples.count_in_file(nq_file, "nquads", fast=True)
        assert count == 2
        assert error is None
