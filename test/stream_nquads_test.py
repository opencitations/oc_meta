# Copyright 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import json
import zipfile
from pathlib import Path

import pytest
from rdflib import Dataset

from oc_meta.run.migration.stream_nquads import convert_zip_to_nquads

SAMPLE_DATA_JSONLD = json.dumps([
    {
        "@graph": [
            {
                "@id": "https://w3id.org/oc/meta/br/06790727",
                "@type": [
                    "http://purl.org/spar/fabio/JournalIssue",
                    "http://purl.org/spar/fabio/Expression",
                ],
                "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                    {"@type": "http://www.w3.org/2001/XMLSchema#string", "@value": "7322"}
                ],
                "http://purl.org/vocab/frbr/core#partOf": [
                    {"@id": "https://w3id.org/oc/meta/br/062203516555"}
                ],
            },
            {
                "@id": "https://w3id.org/oc/meta/br/06790671",
                "@type": ["http://purl.org/spar/fabio/Expression"],
                "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                    {"@type": "http://www.w3.org/2001/XMLSchema#string", "@value": "6"}
                ],
            },
        ],
        "@id": "https://w3id.org/oc/meta/br/",
    }
])

SAMPLE_PROV_JSONLD = json.dumps([
    {
        "@graph": [
            {
                "@id": "https://w3id.org/oc/meta/br/06790181/prov/se/1",
                "@type": ["http://www.w3.org/ns/prov#Entity"],
                "http://purl.org/dc/terms/description": [
                    {"@type": "http://www.w3.org/2001/XMLSchema#string",
                     "@value": "The entity 'https://w3id.org/oc/meta/br/06790181' has been created."}
                ],
                "http://www.w3.org/ns/prov#generatedAtTime": [
                    {"@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                     "@value": "2024-03-27T17:31:42+00:00"}
                ],
                "http://www.w3.org/ns/prov#specializationOf": [
                    {"@id": "https://w3id.org/oc/meta/br/06790181"}
                ],
            }
        ],
        "@id": "https://w3id.org/oc/meta/br/06790181/prov/",
    },
    {
        "@graph": [
            {
                "@id": "https://w3id.org/oc/meta/br/06790575/prov/se/1",
                "@type": ["http://www.w3.org/ns/prov#Entity"],
                "http://purl.org/dc/terms/description": [
                    {"@type": "http://www.w3.org/2001/XMLSchema#string",
                     "@value": "The entity 'https://w3id.org/oc/meta/br/06790575' has been created."}
                ],
            }
        ],
        "@id": "https://w3id.org/oc/meta/br/06790575/prov/",
    },
])


def _make_zip(tmp_path: Path, filename: str, content: str) -> str:
    zip_path = tmp_path / filename
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data.json", content)
    return str(zip_path)


def _extract_quads(nquads_bytes: bytes) -> set[tuple[str, str, str, str]]:
    graph = Dataset(default_union=True)
    graph.parse(data=nquads_bytes.decode("utf-8"), format="nquads")
    return {(str(s), str(p), str(o), str(g)) for s, p, o, g in graph.quads()}


class TestConvertZipToNquads:
    def test_data_zip(self, tmp_path: Path) -> None:
        zip_path = _make_zip(tmp_path, "1000.zip", SAMPLE_DATA_JSONLD)
        quads = _extract_quads(convert_zip_to_nquads(zip_path))

        assert quads == {
            (
                "https://w3id.org/oc/meta/br/06790727",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://purl.org/spar/fabio/JournalIssue",
                "https://w3id.org/oc/meta/br/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790727",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://purl.org/spar/fabio/Expression",
                "https://w3id.org/oc/meta/br/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790727",
                "http://purl.org/spar/fabio/hasSequenceIdentifier",
                "7322",
                "https://w3id.org/oc/meta/br/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790727",
                "http://purl.org/vocab/frbr/core#partOf",
                "https://w3id.org/oc/meta/br/062203516555",
                "https://w3id.org/oc/meta/br/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790671",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://purl.org/spar/fabio/Expression",
                "https://w3id.org/oc/meta/br/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790671",
                "http://purl.org/spar/fabio/hasSequenceIdentifier",
                "6",
                "https://w3id.org/oc/meta/br/",
            ),
        }

    def test_prov_zip(self, tmp_path: Path) -> None:
        zip_path = _make_zip(tmp_path, "se.zip", SAMPLE_PROV_JSONLD)
        quads = _extract_quads(convert_zip_to_nquads(zip_path))

        assert quads == {
            (
                "https://w3id.org/oc/meta/br/06790181/prov/se/1",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://www.w3.org/ns/prov#Entity",
                "https://w3id.org/oc/meta/br/06790181/prov/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790181/prov/se/1",
                "http://purl.org/dc/terms/description",
                "The entity 'https://w3id.org/oc/meta/br/06790181' has been created.",
                "https://w3id.org/oc/meta/br/06790181/prov/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790181/prov/se/1",
                "http://www.w3.org/ns/prov#generatedAtTime",
                "2024-03-27T17:31:42+00:00",
                "https://w3id.org/oc/meta/br/06790181/prov/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790181/prov/se/1",
                "http://www.w3.org/ns/prov#specializationOf",
                "https://w3id.org/oc/meta/br/06790181",
                "https://w3id.org/oc/meta/br/06790181/prov/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790575/prov/se/1",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://www.w3.org/ns/prov#Entity",
                "https://w3id.org/oc/meta/br/06790575/prov/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790575/prov/se/1",
                "http://purl.org/dc/terms/description",
                "The entity 'https://w3id.org/oc/meta/br/06790575' has been created.",
                "https://w3id.org/oc/meta/br/06790575/prov/",
            ),
        }

    def test_bad_zip_raises(self, tmp_path: Path) -> None:
        bad_zip = tmp_path / "bad.zip"
        bad_zip.write_bytes(b"not a zip file")

        with pytest.raises(zipfile.BadZipFile):
            convert_zip_to_nquads(str(bad_zip))

    def test_no_json_files_raises(self, tmp_path: Path) -> None:
        zip_path = tmp_path / "nojson.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("readme.txt", "no json here")

        with pytest.raises(StopIteration):
            convert_zip_to_nquads(str(zip_path))

    def test_invalid_json_raises(self, tmp_path: Path) -> None:
        zip_path = _make_zip(tmp_path, "invalid.zip", "not valid json {")

        with pytest.raises(json.JSONDecodeError):
            convert_zip_to_nquads(str(zip_path))
