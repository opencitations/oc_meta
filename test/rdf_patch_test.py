# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import orjson
import pytest

from oc_meta.lib.rdf_patch import (
    AuditConfig,
    EntityFileLocator,
    load_audit_config,
    load_available_entities,
    load_entities,
    load_progress,
    read_json_object,
    save_progress,
    sha256,
    write_json,
)

BASE = "https://w3id.org/oc/meta/"
RA_1 = f"{BASE}ra/0601"
RA_2 = f"{BASE}ra/0602"


def _rdf_data() -> list[dict[str, object]]:
    return [
        {
            "@id": f"{BASE}ra/",
            "@graph": [
                {
                    "@id": RA_1,
                    "http://xmlns.com/foaf/0.1/name": [{"@value": "Ada Rossi"}],
                }
            ],
        }
    ]


def test_load_audit_config(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    config_path = tmp_path / "meta.yaml"
    config_path.write_text(
        f"output_rdf_dir: {output_dir}\n"
        "dir_split_number: 10000\n"
        "items_per_file: 1000\n"
        "zip_output_rdf: false\n",
        encoding="utf-8",
    )

    assert load_audit_config(str(config_path)) == AuditConfig(
        str(output_dir / "rdf"), 10000, 1000, False
    )


def test_load_entities_from_json_and_zip(tmp_path: Path) -> None:
    content = orjson.dumps(_rdf_data())
    json_path = tmp_path / "entities.json"
    json_path.write_bytes(content)
    zip_path = tmp_path / "entities.zip"
    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("entities.json", content)
    expected = {
        RA_1: {
            "@id": RA_1,
            "http://xmlns.com/foaf/0.1/name": [{"@value": "Ada Rossi"}],
        }
    }

    assert load_entities(str(json_path)) == expected
    assert load_entities(str(zip_path)) == expected


def test_load_available_entities_returns_only_present_uris(tmp_path: Path) -> None:
    locator = EntityFileLocator(str(tmp_path), 10000, 1000, False)
    data_path = Path(locator.path(RA_1))
    data_path.parent.mkdir(parents=True)
    data_path.write_bytes(orjson.dumps(_rdf_data()))

    assert load_available_entities({RA_1, RA_2}, locator, 1) == {
        RA_1: {
            "@id": RA_1,
            "http://xmlns.com/foaf/0.1/name": [{"@value": "Ada Rossi"}],
        }
    }


def test_json_hash_and_progress_files(tmp_path: Path) -> None:
    json_path = tmp_path / "nested" / "value.json"
    write_json(str(json_path), {"b": 2, "a": 1})

    assert json_path.read_bytes() == b'{\n  "a": 1,\n  "b": 2\n}\n'
    assert read_json_object(str(json_path)) == {"a": 1, "b": 2}

    hash_path = tmp_path / "hash.txt"
    hash_path.write_bytes(b"abc")
    assert sha256(str(hash_path)) == (
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )

    progress_path = tmp_path / "progress.json"
    save_progress(str(progress_path), "plan", "review", {"group-2", "group-1"})
    assert read_json_object(str(progress_path)) == {
        "completed_groups": ["group-1", "group-2"],
        "plan_sha256": "plan",
        "review_sha256": "review",
    }
    assert load_progress(str(progress_path), "plan", "review") == {
        "group-1",
        "group-2",
    }

    with pytest.raises(ValueError) as error:
        load_progress(str(progress_path), "changed", "review")
    assert str(error.value) == "Progress file belongs to a different correction plan"
