# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import os
import zipfile
from collections import defaultdict

import orjson

from oc_meta.lib.file_manager import find_rdf_file
from oc_meta.run.meta import check_rdf_files as scr

BASE = "https://w3id.org/oc/meta/"
DIR_SPLIT = 10000
ITEMS = 1000


def _write_zip(path: str, graphs: list) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    name = os.path.splitext(os.path.basename(path))[0] + ".json"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(name, orjson.dumps(graphs))


def _write_entities(rdf_dir: str, entities: list[dict]) -> None:
    by_file: dict[str, list[dict]] = defaultdict(list)
    for entity in entities:
        path = find_rdf_file(entity["@id"], rdf_dir, DIR_SPLIT, ITEMS, zip_output=True)
        by_file[path].append(entity)
    for path, ents in by_file.items():
        _write_zip(path, [{"@graph": ents}])


def _write_provs(rdf_dir: str, omid_to_snaps: dict[str, list[dict]]) -> None:
    by_prov: dict[str, list[dict]] = defaultdict(list)
    for omid, snaps in omid_to_snaps.items():
        data_zip = find_rdf_file(
            BASE + omid, rdf_dir, DIR_SPLIT, ITEMS, zip_output=True
        )
        prov_zip = os.path.join(os.path.splitext(data_zip)[0], "prov", "se.zip")
        by_prov[prov_zip].extend(snaps)
    for prov_zip, snaps in by_prov.items():
        _write_zip(prov_zip, [{"@graph": snaps}])


def _br(omid, title, ar_uris, id_uri="id/0601"):
    return {
        "@id": BASE + omid,
        "@type": [
            "http://purl.org/spar/fabio/Expression",
            "http://purl.org/spar/fabio/Preprint",
        ],
        "http://purl.org/dc/terms/title": [
            {"@type": "http://www.w3.org/2001/XMLSchema#string", "@value": title}
        ],
        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
            {"@type": "http://www.w3.org/2001/XMLSchema#gYear", "@value": "2020"}
        ],
        "http://purl.org/spar/datacite/hasIdentifier": [{"@id": BASE + id_uri}],
        "http://purl.org/spar/pro/isDocumentContextFor": [
            {"@id": BASE + ar} for ar in ar_uris
        ],
    }


def _ar(omid, ra, has_next=None, role="author"):
    entity = {
        "@id": BASE + omid,
        "@type": ["http://purl.org/spar/pro/RoleInTime"],
        "http://purl.org/spar/pro/withRole": [
            {"@id": f"http://purl.org/spar/pro/{role}"}
        ],
        "http://purl.org/spar/pro/isHeldBy": [{"@id": BASE + ra}],
    }
    if has_next:
        entity["https://w3id.org/oc/ontology/hasNext"] = [{"@id": BASE + has_next}]
    return entity


def _ra(omid, family):
    return {
        "@id": BASE + omid,
        "@type": ["http://xmlns.com/foaf/0.1/Agent"],
        "http://xmlns.com/foaf/0.1/familyName": [
            {"@type": "http://www.w3.org/2001/XMLSchema#string", "@value": family}
        ],
    }


def _id(omid, scheme, value):
    return {
        "@id": BASE + omid,
        "@type": ["http://purl.org/spar/datacite/Identifier"],
        "http://purl.org/spar/datacite/usesIdentifierScheme": [
            {"@id": f"http://purl.org/spar/datacite/{scheme}"}
        ],
        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
            {"@type": "http://www.w3.org/2001/XMLSchema#string", "@value": value}
        ],
    }


def _snapshot(omid, number, invalidated=False):
    snap = {
        "@id": f"{BASE}{omid}/prov/se/{number}",
        "@type": ["http://www.w3.org/ns/prov#Entity"],
        "http://www.w3.org/ns/prov#specializationOf": [{"@id": BASE + omid}],
    }
    if invalidated:
        snap["http://www.w3.org/ns/prov#invalidatedAtTime"] = [
            {
                "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                "@value": "2024-01-01T00:00:00+00:00",
            }
        ]
    return snap


def _healthy_rdf(rdf_dir: str) -> None:
    _write_entities(
        rdf_dir,
        [
            _br("br/0601", "A Clean Title", ["ar/0601", "ar/0602"]),
            _ar("ar/0601", "ra/0601", has_next="ar/0602"),
            _ar("ar/0602", "ra/0602"),
            _ra("ra/0601", "Alpha"),
            _ra("ra/0602", "Beta"),
            _id("id/0601", "doi", "10.1/x"),
        ],
    )
    _write_provs(
        rdf_dir,
        {
            omid: [_snapshot(omid, 1)]
            for omid in (
                "br/0601",
                "ra/0601",
                "ra/0602",
                "id/0601",
                "ar/0601",
                "ar/0602",
            )
        },
    )


def _row():
    return {
        "id": "omid:br/0601 doi:10.1/x",
        "title": "A Clean Title",
        "author": "Alpha, A [omid:ra/0601]; Beta, B [omid:ra/0602]",
        "pub_date": "2020",
        "venue": "",
        "type": "preprint",
        "publisher": "",
        "editor": "",
    }


def _cache(rdf_dir: str) -> scr.EntityCache:
    return scr.EntityCache(rdf_dir, DIR_SPLIT, ITEMS)


def test_all_pass(tmp_path):
    rdf = str(tmp_path / "rdf")
    _healthy_rdf(rdf)
    result = scr.check_curated_row(_row(), 1, _cache(rdf), BASE)
    assert result.errors == []
    assert result.warnings == []
    assert result.counts["data_graph.failed"] == 0
    assert result.counts["hasnext.checked"] == 1
    assert result.counts["agents.failed"] == 0


def test_data_graph_missing(tmp_path):
    rdf = str(tmp_path / "rdf")
    _healthy_rdf(rdf)
    os.remove(find_rdf_file(BASE + "ra/0601", rdf, DIR_SPLIT, ITEMS, zip_output=True))
    result = scr.check_curated_row(_row(), 1, _cache(rdf), BASE)
    missing = {e["omid"] for e in result.errors if e["type"] == "data_graph_missing"}
    assert missing == {BASE + "ra/0601", BASE + "ra/0602"}
    assert result.counts["data_graph.failed"] == 2


def test_hasnext_cycle(tmp_path):
    rdf = str(tmp_path / "rdf")
    _write_entities(
        rdf,
        [
            _br("br/0601", "A Clean Title", ["ar/0601", "ar/0602"]),
            _ar("ar/0601", "ra/0601", has_next="ar/0602"),
            _ar("ar/0602", "ra/0602", has_next="ar/0601"),
            _ra("ra/0601", "Alpha"),
            _ra("ra/0602", "Beta"),
            _id("id/0601", "doi", "10.1/x"),
        ],
    )
    _write_provs(
        rdf,
        {
            omid: [_snapshot(omid, 1)]
            for omid in (
                "br/0601",
                "ra/0601",
                "ra/0602",
                "id/0601",
                "ar/0601",
                "ar/0602",
            )
        },
    )
    result = scr.check_curated_row(_row(), 1, _cache(rdf), BASE)
    assert result.counts["hasnext.failed"] == 1
    anomaly_types = {e["anomaly_type"] for e in result.errors}
    assert anomaly_types == {"cycle", "no_start_node"}


def test_provenance_missing(tmp_path):
    rdf = str(tmp_path / "rdf")
    _healthy_rdf(rdf)
    data_zip = find_rdf_file(BASE + "br/0601", rdf, DIR_SPLIT, ITEMS, zip_output=True)
    os.remove(os.path.join(os.path.splitext(data_zip)[0], "prov", "se.zip"))
    result = scr.check_curated_row(_row(), 1, _cache(rdf), BASE)
    missing = [e for e in result.errors if e["type"] == "provenance_missing"]
    assert [e["omid"] for e in missing] == [BASE + "br/0601"]
    assert result.counts["provenance.failed"] == 1


def test_provenance_invalidated_is_warning(tmp_path):
    rdf = str(tmp_path / "rdf")
    _healthy_rdf(rdf)
    _write_provs(
        rdf,
        {
            "ra/0601": [
                _snapshot("ra/0601", 1, invalidated=True),
                _snapshot("ra/0601", 2, invalidated=True),
            ],
            "ra/0602": [_snapshot("ra/0602", 1)],
        },
    )
    result = scr.check_curated_row(_row(), 1, _cache(rdf), BASE)
    assert result.errors == []
    assert result.warnings == [
        {"type": "provenance_invalidated", "omid": BASE + "ra/0601", "row": 1}
    ]
    assert result.counts["provenance.invalidated"] == 1


def test_metadata_mismatch(tmp_path):
    rdf = str(tmp_path / "rdf")
    _healthy_rdf(rdf)
    row = _row()
    row["pub_date"] = "1999"
    result = scr.check_curated_row(row, 1, _cache(rdf), BASE)
    mismatches = [e for e in result.errors if e["type"] == "metadata_mismatch"]
    assert len(mismatches) == 1
    assert mismatches[0]["subtype"] == "pub_date"
    assert mismatches[0]["csv"] == "1999"
    assert mismatches[0]["rdf"] == ["2020"]


def test_type_synonym_data_file_is_dataset(tmp_path):
    rdf = str(tmp_path / "rdf")
    br = _br("br/0601", "A Clean Title", ["ar/0601", "ar/0602"])
    br["@type"] = [
        "http://purl.org/spar/fabio/Expression",
        "http://purl.org/spar/fabio/DataFile",
    ]
    _write_entities(
        rdf,
        [
            br,
            _ar("ar/0601", "ra/0601", has_next="ar/0602"),
            _ar("ar/0602", "ra/0602"),
            _ra("ra/0601", "Alpha"),
            _ra("ra/0602", "Beta"),
            _id("id/0601", "doi", "10.1/x"),
        ],
    )
    _write_provs(
        rdf,
        {
            omid: [_snapshot(omid, 1)]
            for omid in (
                "br/0601",
                "ra/0601",
                "ra/0602",
                "id/0601",
                "ar/0601",
                "ar/0602",
            )
        },
    )
    row = _row()
    row["type"] = "data file"
    result = scr.check_curated_row(row, 1, _cache(rdf), BASE)
    assert [e for e in result.errors if e["type"] == "metadata_mismatch"] == []


def test_agent_order_mismatch(tmp_path):
    rdf = str(tmp_path / "rdf")
    _healthy_rdf(rdf)
    row = _row()
    row["author"] = "Beta, B [omid:ra/0602]; Alpha, A [omid:ra/0601]"
    result = scr.check_curated_row(row, 1, _cache(rdf), BASE)
    order_errors = [e for e in result.errors if e["type"] == "agent_order_mismatch"]
    assert len(order_errors) == 1
    assert order_errors[0]["csv"] == [BASE + "ra/0602", BASE + "ra/0601"]
    assert order_errors[0]["rdf"] == [BASE + "ra/0601", BASE + "ra/0602"]


def test_ar_without_agent_is_warning(tmp_path):
    rdf = str(tmp_path / "rdf")
    orphan = {
        "@id": BASE + "ar/0602",
        "@type": ["http://purl.org/spar/pro/RoleInTime"],
        "http://purl.org/spar/pro/withRole": [
            {"@id": "http://purl.org/spar/pro/author"}
        ],
    }
    _write_entities(
        rdf,
        [
            _br("br/0601", "A Clean Title", ["ar/0601", "ar/0602"]),
            _ar("ar/0601", "ra/0601", has_next="ar/0602"),
            orphan,
            _ra("ra/0601", "Alpha"),
            _id("id/0601", "doi", "10.1/x"),
        ],
    )
    _write_provs(
        rdf,
        {
            omid: [_snapshot(omid, 1)]
            for omid in ("br/0601", "ra/0601", "id/0601", "ar/0601", "ar/0602")
        },
    )
    row = _row()
    row["author"] = "Alpha, A [omid:ra/0601]"
    result = scr.check_curated_row(row, 1, _cache(rdf), BASE)
    assert result.errors == []
    assert result.warnings == [
        {
            "type": "ar_without_agent",
            "omid": BASE + "br/0601",
            "ar": BASE + "ar/0602",
            "role": "author",
            "row": 1,
        }
    ]
    assert result.counts["agents.failed"] == 0


def test_input_cross_check(tmp_path):
    rdf = str(tmp_path / "rdf")
    _healthy_rdf(rdf)
    scr._index = {"doi:10.1/x": BASE + "br/0601"}
    present = scr.check_input_row({"id": "doi:10.1/x", "title": "raw"}, 1, BASE)
    assert present.errors == []
    assert present.counts["input.checked"] == 1
    assert present.counts["input.failed"] == 0
    dropped = scr.check_input_row({"id": "doi:10.9/missing", "title": "raw"}, 2, BASE)
    assert dropped.errors == [
        {"type": "input_row_dropped", "identifiers": ["doi:10.9/missing"], "row": 2}
    ]
    scr._index = None
