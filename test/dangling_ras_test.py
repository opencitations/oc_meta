# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from pathlib import Path
from zipfile import BadZipFile, ZipFile

import orjson
import pytest
import yaml

from oc_meta.lib.rdf_patch import (
    IS_DOCUMENT_CONTEXT_FOR,
    IS_HELD_BY,
    PROV_SPECIALIZATION_OF,
    EntityFileLocator,
    provenance_path,
)
from oc_meta.run.find.dangling_ras import find_dangling_ras


@pytest.mark.parametrize("zipped", [False, True])
@pytest.mark.parametrize("dangling", [False, True])
def test_scan_reports_missing_agents_and_context(tmp_path, zipped, dangling):
    base = "https://w3id.org/oc/meta/"
    ra = base + "ra/0601"
    absent = base + "ra/0602"
    ar = base + "ar/0601"
    br = base + "br/0601"
    rdf = tmp_path / "rdf"
    locator = EntityFileLocator(str(rdf), 10000, 1000, zipped)
    config = tmp_path / "config.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "output_rdf_dir": str(tmp_path),
                "dir_split_number": 10000,
                "items_per_file": 1000,
                "zip_output_rdf": zipped,
            }
        )
    )
    role = {"@id": ar, IS_HELD_BY: [{"@id": absent if dangling else ra}]}
    work = {"@id": br, IS_DOCUMENT_CONTEXT_FOR: [{"@id": ar}]}
    snapshot = {
        "@id": absent + "/prov/se/2",
        PROV_SPECIALIZATION_OF: [{"@id": absent}],
    }
    contents = {
        locator.path(ra): [{"@id": ra}],
        locator.path(ar): [role],
        locator.path(br): [work],
        provenance_path(locator.path(absent), zipped): [snapshot],
    }
    for name, entities in contents.items():
        path = Path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = orjson.dumps([{"@graph": entities}])
        if zipped:
            with ZipFile(path, "w") as archive:
                archive.writestr("data.json", data)
        else:
            path.write_bytes(data)
    before = {name: Path(name).read_bytes() for name in contents}
    report = tmp_path / "report.jsonl"
    summary = {
        key: int(dangling)
        for key in (
            "missing_ras",
            "dangling_references",
            "affected_ars",
            "affected_brs",
        )
    }
    assert find_dangling_ras(str(config), str(report)) == summary
    expected: list[dict[str, object]] = [{"summary": summary}]
    if dangling:
        expected.append(
            {
                "ra": absent,
                "ar": ar,
                "expected_ra_file": locator.path(absent),
                "role": role,
                "works": [work],
                "provenance": [snapshot],
            }
        )
    assert report.read_bytes() == b"".join(
        orjson.dumps(row) + b"\n" for row in expected
    )
    assert {name: Path(name).read_bytes() for name in contents} == before
    assert sorted(tmp_path.glob("oc_meta_dangling_ras_*")) == []

    Path(locator.path(ar)).write_bytes(b"broken")
    error = BadZipFile if zipped else orjson.JSONDecodeError
    with pytest.raises(error):
        find_dangling_ras(str(config), str(report))
    assert report.read_bytes() == b"".join(
        orjson.dumps(row) + b"\n" for row in expected
    )
    assert sorted(tmp_path.glob("oc_meta_dangling_ras_*")) == []
