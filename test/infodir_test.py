# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import zipfile
from pathlib import Path

import orjson
import pytest

from oc_meta.run.infodir._common import (
    SourceDataError,
    bounded_process_map,
)
from oc_meta.run.infodir.check import check_info_dir
from oc_meta.run.infodir.gen import generate_info_dir


def _identity(value: str) -> str:
    return value


def _write_zip(path: Path, member_name: str, entities: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [{"@graph": [{"@id": entity} for entity in entities]}]
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, orjson.dumps(payload))


def _write_source(
    root: Path,
    data_entities: list[str],
    provenance_entities: list[str],
) -> None:
    if data_entities:
        _write_zip(
            root / "br" / "0670" / "10000" / "1000.zip",
            "1000.json",
            data_entities,
        )
    if provenance_entities:
        _write_zip(
            root / "br" / "0670" / "10000" / "1000" / "prov" / "se.zip",
            "se.json",
            provenance_entities,
        )


def _generate(
    tmp_path: Path,
    data_entities: list[str],
    provenance_entities: list[str],
    max_examples: int = 100,
) -> tuple[Path, Path, dict[str, object]]:
    root = tmp_path / "rdf"
    info_dir = tmp_path / "info_dir"
    _write_source(root, data_entities, provenance_entities)
    report = generate_info_dir(
        str(root),
        str(info_dir),
        workers=1,
        max_examples=max_examples,
        report_path=str(tmp_path / "generation-report.json"),
    )
    return root, info_dir, report


def _check(
    tmp_path: Path,
    root: Path,
    info_dir: Path,
    max_examples: int = 100,
) -> tuple[dict[str, object], int]:
    return check_info_dir(
        str(root),
        str(info_dir),
        str(tmp_path / "check-report.json"),
        workers=1,
        max_examples=max_examples,
    )


def test_generate_and_check_aligned_info_dir(tmp_path):
    root, info_dir, generation_report = _generate(
        tmp_path,
        [
            "https://w3id.org/oc/meta/br/06701",
            "https://w3id.org/oc/meta/br/06703",
        ],
        [
            "https://w3id.org/oc/meta/br/06701/prov/se/1",
            "https://w3id.org/oc/meta/br/06701/prov/se/2",
            "https://w3id.org/oc/meta/br/06703/prov/se/1",
        ],
    )

    assert (info_dir / "0670" / "info_file_br.txt").read_text() == "3\n"
    assert (info_dir / "0670" / "prov_file_br.txt").read_text() == "2\n\n1\n"
    assert generation_report["status"] == "generated"
    assert set(generation_report) == {
        "timestamp",
        "status",
        "root_path",
        "info_dir",
        "source",
        "live_entities_without_provenance",
    }
    assert orjson.loads((tmp_path / "generation-report.json").read_bytes()) == (
        generation_report
    )
    assert generation_report["live_entities_without_provenance"] == {
        "total": 0,
        "examples": [],
        "truncated": False,
    }

    report, exit_code = _check(tmp_path, root, info_dir)
    assert exit_code == 0
    assert report["status"] == "aligned"
    assert set(report) == {
        "timestamp",
        "status",
        "root_path",
        "info_dir",
        "source",
        "entity_counter_mismatches",
        "provenance_counter_mismatches",
        "counter_file_errors",
        "live_entities_without_provenance",
    }
    assert report["entity_counter_mismatches"] == {
        "total": 0,
        "examples": [],
        "truncated": False,
    }
    assert report["provenance_counter_mismatches"] == {
        "total": 0,
        "examples": [],
        "truncated": False,
    }
    assert report["counter_file_errors"] == {
        "total": 0,
        "examples": [],
        "truncated": False,
    }


def test_deleted_highest_identifier_sets_entity_counter(tmp_path):
    root, info_dir, _ = _generate(
        tmp_path,
        ["https://w3id.org/oc/meta/br/06701"],
        [
            "https://w3id.org/oc/meta/br/06701/prov/se/1",
            "https://w3id.org/oc/meta/br/06705/prov/se/1",
            "https://w3id.org/oc/meta/br/06705/prov/se/2",
        ],
    )

    assert (info_dir / "0670" / "info_file_br.txt").read_text() == "5\n"
    assert (info_dir / "0670" / "prov_file_br.txt").read_text() == "1\n\n\n\n2\n"
    report, exit_code = _check(tmp_path, root, info_dir)
    assert exit_code == 0
    assert report["status"] == "aligned"


def test_missing_provenance_is_reported_as_warning(tmp_path):
    root, info_dir, generation_report = _generate(
        tmp_path,
        [
            "https://w3id.org/oc/meta/br/06701",
            "https://w3id.org/oc/meta/br/06702",
        ],
        ["https://w3id.org/oc/meta/br/06701/prov/se/1"],
    )

    expected_warning = {
        "total": 1,
        "examples": [
            {
                "entity_uri": "https://w3id.org/oc/meta/br/06702",
                "zip_file": str(root / "br" / "0670" / "10000" / "1000.zip"),
            }
        ],
        "truncated": False,
    }
    assert generation_report["status"] == "generated_with_warnings"
    assert generation_report["live_entities_without_provenance"] == expected_warning
    assert (info_dir / "0670" / "info_file_br.txt").read_text() == "2\n"
    assert (info_dir / "0670" / "prov_file_br.txt").read_text() == "1\n"

    report, exit_code = _check(tmp_path, root, info_dir)
    assert exit_code == 1
    assert report["status"] == "warnings"
    assert report["live_entities_without_provenance"] == expected_warning


def test_corrupt_zip_does_not_publish_info_dir(tmp_path):
    root = tmp_path / "rdf"
    corrupt_zip = root / "br" / "0670" / "10000" / "1000" / "prov" / "se.zip"
    corrupt_zip.parent.mkdir(parents=True)
    corrupt_zip.write_bytes(b"not a zip file")
    info_dir = tmp_path / "info_dir"

    with pytest.raises(SourceDataError, match="Invalid ZIP file"):
        generate_info_dir(str(root), str(info_dir), workers=1)

    assert info_dir.exists() is False
    assert list(tmp_path.glob(".info_dir.*.tmp")) == []


def test_existing_info_dir_is_unchanged(tmp_path):
    root = tmp_path / "rdf"
    _write_source(
        root,
        ["https://w3id.org/oc/meta/br/06701"],
        ["https://w3id.org/oc/meta/br/06701/prov/se/1"],
    )
    info_dir = tmp_path / "info_dir"
    info_dir.mkdir()
    marker = info_dir / "marker"
    marker.write_text("unchanged")

    with pytest.raises(FileExistsError, match="Info directory already exists"):
        generate_info_dir(str(root), str(info_dir), workers=1)

    assert list(info_dir.iterdir()) == [marker]
    assert marker.read_text() == "unchanged"


@pytest.mark.parametrize(
    ("entity_value", "relation"),
    [("2\n", "too_low"), ("4\n", "too_high")],
)
def test_check_detects_entity_counter_mismatch(tmp_path, entity_value, relation):
    root, info_dir, _ = _generate(
        tmp_path,
        ["https://w3id.org/oc/meta/br/06703"],
        ["https://w3id.org/oc/meta/br/06703/prov/se/1"],
    )
    (info_dir / "0670" / "info_file_br.txt").write_text(entity_value)

    report, exit_code = _check(tmp_path, root, info_dir)
    assert exit_code == 1
    assert report["status"] == "mismatched"
    assert report["entity_counter_mismatches"] == {
        "total": 1,
        "examples": [
            {
                "prefix": "0670",
                "short_name": "br",
                "expected": 3,
                "actual": int(entity_value),
                "relation": relation,
            }
        ],
        "truncated": False,
    }


@pytest.mark.parametrize(
    ("provenance_value", "relation"),
    [("1\n", "too_low"), ("3\n", "too_high")],
)
def test_check_detects_provenance_counter_mismatch(
    tmp_path, provenance_value, relation
):
    root, info_dir, _ = _generate(
        tmp_path,
        ["https://w3id.org/oc/meta/br/06701"],
        [
            "https://w3id.org/oc/meta/br/06701/prov/se/1",
            "https://w3id.org/oc/meta/br/06701/prov/se/2",
        ],
    )
    (info_dir / "0670" / "prov_file_br.txt").write_text(provenance_value)

    report, exit_code = _check(tmp_path, root, info_dir)
    assert exit_code == 1
    assert report["status"] == "mismatched"
    assert report["provenance_counter_mismatches"] == {
        "total": 1,
        "examples": [
            {
                "prefix": "0670",
                "short_name": "br",
                "resource_number": 1,
                "expected": 2,
                "actual": int(provenance_value),
                "relation": relation,
            }
        ],
        "truncated": False,
    }


def test_check_detects_trailing_provenance_line(tmp_path):
    root, info_dir, _ = _generate(
        tmp_path,
        ["https://w3id.org/oc/meta/br/06701"],
        ["https://w3id.org/oc/meta/br/06701/prov/se/1"],
    )
    path = info_dir / "0670" / "prov_file_br.txt"
    path.write_text("1\n\n")

    report, exit_code = _check(tmp_path, root, info_dir)
    assert exit_code == 1
    assert report["status"] == "mismatched"
    assert report["provenance_counter_mismatches"] == {
        "total": 0,
        "examples": [],
        "truncated": False,
    }
    assert report["counter_file_errors"] == {
        "total": 1,
        "examples": [
            {
                "path": str(path),
                "error": "invalid_provenance_counter_line_count",
                "expected": 1,
                "actual": 2,
            }
        ],
        "truncated": False,
    }


def test_check_detects_unexpected_provenance_counter(tmp_path):
    root, info_dir, _ = _generate(
        tmp_path,
        ["https://w3id.org/oc/meta/br/06701"],
        ["https://w3id.org/oc/meta/br/06701/prov/se/1"],
    )
    path = info_dir / "0670" / "prov_file_br.txt"
    path.write_text("1\n2\n")

    report, exit_code = _check(tmp_path, root, info_dir)
    assert exit_code == 1
    assert report["provenance_counter_mismatches"] == {
        "total": 1,
        "examples": [
            {
                "prefix": "0670",
                "short_name": "br",
                "resource_number": 2,
                "expected": 0,
                "actual": 2,
                "relation": "unexpected",
            }
        ],
        "truncated": False,
    }
    assert report["counter_file_errors"] == {
        "total": 1,
        "examples": [
            {
                "path": str(path),
                "error": "invalid_provenance_counter_line_count",
                "expected": 1,
                "actual": 2,
            }
        ],
        "truncated": False,
    }


def test_check_detects_missing_provenance_counter_file(tmp_path):
    root, info_dir, _ = _generate(
        tmp_path,
        ["https://w3id.org/oc/meta/br/06701"],
        ["https://w3id.org/oc/meta/br/06701/prov/se/1"],
    )
    path = info_dir / "0670" / "prov_file_br.txt"
    path.unlink()

    report, exit_code = _check(tmp_path, root, info_dir)
    assert exit_code == 1
    assert report["provenance_counter_mismatches"] == {
        "total": 1,
        "examples": [
            {
                "prefix": "0670",
                "short_name": "br",
                "resource_number": 1,
                "expected": 1,
                "actual": 0,
                "relation": "missing",
            }
        ],
        "truncated": False,
    }
    assert report["counter_file_errors"] == {
        "total": 1,
        "examples": [
            {
                "prefix": "0670",
                "short_name": "br",
                "error": "missing_provenance_counter_file",
            }
        ],
        "truncated": False,
    }


def test_check_detects_invalid_provenance_counter_value(tmp_path):
    root, info_dir, _ = _generate(
        tmp_path,
        ["https://w3id.org/oc/meta/br/06701"],
        ["https://w3id.org/oc/meta/br/06701/prov/se/1"],
    )
    path = info_dir / "0670" / "prov_file_br.txt"
    path.write_text("invalid\n")

    report, exit_code = _check(tmp_path, root, info_dir)
    assert exit_code == 1
    assert report["provenance_counter_mismatches"] == {
        "total": 0,
        "examples": [],
        "truncated": False,
    }
    assert report["counter_file_errors"] == {
        "total": 1,
        "examples": [
            {
                "path": str(path),
                "line": 1,
                "error": "invalid_provenance_counter_value",
                "value": "invalid",
            }
        ],
        "truncated": False,
    }


def test_check_detects_unexpected_counter_file(tmp_path):
    root, info_dir, _ = _generate(
        tmp_path,
        ["https://w3id.org/oc/meta/br/06701"],
        ["https://w3id.org/oc/meta/br/06701/prov/se/1"],
    )
    path = info_dir / "0670" / "prov_file_ra.txt"
    path.write_text("1\n")

    report, exit_code = _check(tmp_path, root, info_dir)
    assert exit_code == 1
    assert report["provenance_counter_mismatches"] == {
        "total": 1,
        "examples": [
            {
                "prefix": "0670",
                "short_name": "ra",
                "resource_number": 1,
                "expected": 0,
                "actual": 1,
                "relation": "unexpected",
            }
        ],
        "truncated": False,
    }
    assert report["counter_file_errors"] == {
        "total": 2,
        "examples": [
            {
                "path": str(path),
                "error": "unexpected_provenance_counter_file",
            },
            {
                "path": str(path),
                "error": "invalid_provenance_counter_line_count",
                "expected": 0,
                "actual": 1,
            },
        ],
        "truncated": False,
    }


def test_check_caps_examples_but_counts_all_warnings(tmp_path):
    root, info_dir, generation_report = _generate(
        tmp_path,
        [
            "https://w3id.org/oc/meta/br/06701",
            "https://w3id.org/oc/meta/br/06702",
            "https://w3id.org/oc/meta/br/06703",
        ],
        [],
        max_examples=1,
    )

    expected = {
        "total": 3,
        "examples": [
            {
                "entity_uri": "https://w3id.org/oc/meta/br/06701",
                "zip_file": str(root / "br" / "0670" / "10000" / "1000.zip"),
            }
        ],
        "truncated": True,
    }
    assert generation_report["live_entities_without_provenance"] == expected
    report, exit_code = _check(tmp_path, root, info_dir, max_examples=1)
    assert exit_code == 1
    assert report["live_entities_without_provenance"] == expected


def test_check_returns_scan_failed_for_corrupt_source(tmp_path):
    root = tmp_path / "rdf"
    corrupt_zip = root / "br" / "0670" / "10000" / "1000.zip"
    corrupt_zip.parent.mkdir(parents=True)
    corrupt_zip.write_bytes(b"not a zip file")
    info_dir = tmp_path / "info_dir"

    report, exit_code = _check(tmp_path, root, info_dir)

    assert exit_code == 2
    assert report["status"] == "scan_failed"
    assert set(report) == {
        "timestamp",
        "status",
        "root_path",
        "info_dir",
        "source",
    }
    assert report["source"] == {
        "data_zip_files": 0,
        "provenance_zip_files": 0,
        "data_entities": 0,
        "provenance_entities": 0,
        "error": f"Invalid ZIP file: {corrupt_zip}",
    }


def test_bounded_process_map_limits_consumed_paths():
    yielded: list[str] = []

    def paths():
        for index in range(6):
            assert index - len(yielded) <= 2
            yield str(index)

    for result in bounded_process_map(paths(), _identity, workers=1):
        yielded.append(result)

    assert sorted(yielded) == ["0", "1", "2", "3", "4", "5"]
