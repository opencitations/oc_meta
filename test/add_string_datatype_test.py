import json
import zipfile
from pathlib import Path

import pytest
from rdflib import XSD, Dataset, Literal, URIRef

from oc_meta.run.patches.add_string_datatype import (
    collect_zip_files,
    needs_modification,
    process_batch,
    process_dataset,
    process_zip_file,
)


@pytest.fixture
def temp_dirs(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    yield input_dir, output_dir


def create_zip_file(path: Path, json_name: str, data: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(json_name, json.dumps(data, ensure_ascii=False))


def read_zip_file(path: Path) -> list:
    with zipfile.ZipFile(path, "r") as zf:
        json_name = zf.namelist()[0]
        with zf.open(json_name) as f:
            return json.load(f)


class TestCollectZipFiles:
    def test_finds_zip_files_recursively(self, temp_dirs):
        input_dir, _ = temp_dirs
        (input_dir / "br" / "0690" / "10000").mkdir(parents=True)
        (input_dir / "br" / "0690" / "10000" / "1000.zip").touch()
        (input_dir / "br" / "0690" / "10000" / "1000" / "prov").mkdir(parents=True)
        (input_dir / "br" / "0690" / "10000" / "1000" / "prov" / "se.zip").touch()
        (input_dir / "ra" / "0690" / "10000").mkdir(parents=True)
        (input_dir / "ra" / "0690" / "10000" / "1000.zip").touch()

        result = collect_zip_files(input_dir)

        assert len(result) == 3
        assert all(p.suffix == ".zip" for p in result)

    def test_empty_directory(self, temp_dirs):
        input_dir, _ = temp_dirs

        result = collect_zip_files(input_dir)

        assert result == []


class TestNeedsModification:
    def test_detects_untyped_value(self):
        data = {"@value": "test"}
        assert needs_modification(data) is True

    def test_ignores_typed_value(self):
        data = {"@value": "test", "@type": "http://www.w3.org/2001/XMLSchema#string"}
        assert needs_modification(data) is False

    def test_ignores_language_tagged_value(self):
        data = {"@value": "test", "@language": "en"}
        assert needs_modification(data) is False

    def test_detects_untyped_in_nested_list(self):
        data = [{"@graph": [{"prop": [{"@value": "untyped"}]}]}]
        assert needs_modification(data) is True

    def test_all_typed_returns_false(self):
        data = [
            {
                "@graph": [
                    {
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Title", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                        ]
                    }
                ]
            }
        ]
        assert needs_modification(data) is False

    def test_empty_data(self):
        assert needs_modification([]) is False
        assert needs_modification({}) is False

    def test_non_value_dict(self):
        data = {"@id": "http://example.org/entity"}
        assert needs_modification(data) is False


class TestProcessDataset:
    def test_adds_xsd_string_to_untyped_literals(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        pred = URIRef("http://purl.org/dc/terms/title")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, pred, Literal("Test Title")))

        modifications = process_dataset(ds)

        assert modifications == {"http://purl.org/dc/terms/title": 1}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.string

    def test_preserves_existing_datatype(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        pred = URIRef("http://prismstandard.org/namespaces/basic/2.0/publicationDate")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, pred, Literal("2020-01-15", datatype=XSD.date)))

        modifications = process_dataset(ds)

        assert modifications == {}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.date

    def test_preserves_language_tagged_literals(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        pred = URIRef("http://purl.org/dc/terms/title")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, pred, Literal("Titre en Francais", lang="fr")))

        modifications = process_dataset(ds)

        assert modifications == {}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.language == "fr"
                assert o.datatype is None

    def test_multiple_properties(self):
        ds = Dataset()
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g.add((subj, URIRef("http://purl.org/dc/terms/title"), Literal("Title")))
        g.add((subj, URIRef("http://purl.org/dc/terms/title"), Literal("Another Title")))
        ra_subj = URIRef("https://w3id.org/oc/meta/ra/0601")
        g.add((ra_subj, URIRef("http://xmlns.com/foaf/0.1/familyName"), Literal("Smith")))
        g.add((ra_subj, URIRef("http://xmlns.com/foaf/0.1/givenName"), Literal("John")))

        modifications = process_dataset(ds)

        assert modifications == {
            "http://purl.org/dc/terms/title": 2,
            "http://xmlns.com/foaf/0.1/familyName": 1,
            "http://xmlns.com/foaf/0.1/givenName": 1,
        }


class TestProcessZipFile:
    def test_processes_bibliographic_resource_data(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/06015",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle"
                        ],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#date", "@value": "2020-01-15"}
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Test Article Title"}
                        ],
                        "http://purl.org/spar/datacite/hasIdentifier": [
                            {"@id": "https://w3id.org/oc/meta/id/0601"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", br_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {"http://purl.org/dc/terms/title": 1}
        output_path = output_dir / "br" / "0690" / "10000" / "1000.zip"
        assert output_path.exists()
        result = read_zip_file(output_path)
        title_obj = result[0]["@graph"][0]["http://purl.org/dc/terms/title"][0]
        assert title_obj["@type"] == "http://www.w3.org/2001/XMLSchema#string"
        assert title_obj["@value"] == "Test Article Title"
        date_obj = result[0]["@graph"][0]["http://prismstandard.org/namespaces/basic/2.0/publicationDate"][0]
        assert date_obj["@type"] == "http://www.w3.org/2001/XMLSchema#date"

    def test_processes_responsible_agent_data(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        ra_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/ra/06015",
                        "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                        "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Peroni"}],
                        "http://xmlns.com/foaf/0.1/givenName": [{"@value": "Silvio"}],
                        "http://purl.org/spar/datacite/hasIdentifier": [
                            {"@id": "https://w3id.org/oc/meta/id/0602"}
                        ]
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/ra/06016",
                        "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "OpenCitations"}]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/ra/"
            }
        ]
        zip_path = input_dir / "ra" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", ra_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {
            "http://xmlns.com/foaf/0.1/familyName": 1,
            "http://xmlns.com/foaf/0.1/givenName": 1,
            "http://xmlns.com/foaf/0.1/name": 1,
        }
        output_path = output_dir / "ra" / "0690" / "10000" / "1000.zip"
        result = read_zip_file(output_path)
        ra_entity = next(e for e in result[0]["@graph"] if e["@id"] == "https://w3id.org/oc/meta/ra/06015")
        family_name = ra_entity["http://xmlns.com/foaf/0.1/familyName"][0]
        assert family_name["@type"] == "http://www.w3.org/2001/XMLSchema#string"

    def test_processes_identifier_data(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        id_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/0601",
                        "@type": ["http://purl.org/spar/datacite/Identifier"],
                        "http://purl.org/spar/datacite/usesIdentifierScheme": [
                            {"@id": "http://purl.org/spar/datacite/doi"}
                        ],
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                            {"@value": "10.1234/test.article"}
                        ]
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/id/0602",
                        "@type": ["http://purl.org/spar/datacite/Identifier"],
                        "http://purl.org/spar/datacite/usesIdentifierScheme": [
                            {"@id": "http://purl.org/spar/datacite/orcid"}
                        ],
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                            {"@value": "0000-0002-8420-0696"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/id/"
            }
        ]
        zip_path = input_dir / "id" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", id_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {
            "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": 2
        }

    def test_processes_resource_embodiment_data(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        re_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/re/0601",
                        "@type": ["http://purl.org/spar/fabio/Manifestation"],
                        "http://prismstandard.org/namespaces/basic/2.0/startingPage": [
                            {"@value": "123"}
                        ],
                        "http://prismstandard.org/namespaces/basic/2.0/endingPage": [
                            {"@value": "145"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/re/"
            }
        ]
        zip_path = input_dir / "re" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", re_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {
            "http://prismstandard.org/namespaces/basic/2.0/startingPage": 1,
            "http://prismstandard.org/namespaces/basic/2.0/endingPage": 1,
        }

    def test_processes_provenance_data(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        prov_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601/prov/se/1",
                        "@type": ["http://www.w3.org/ns/prov#Entity"],
                        "http://purl.org/dc/terms/description": [
                            {"@value": "The entity 'https://w3id.org/oc/meta/br/0601' has been created."}
                        ],
                        "http://www.w3.org/ns/prov#generatedAtTime": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#dateTime", "@value": "2022-07-28T15:38:17"}
                        ],
                        "http://www.w3.org/ns/prov#hadPrimarySource": [
                            {"@id": "https://api.crossref.org/"}
                        ],
                        "http://www.w3.org/ns/prov#specializationOf": [
                            {"@id": "https://w3id.org/oc/meta/br/0601"}
                        ],
                        "http://www.w3.org/ns/prov#wasAttributedTo": [
                            {"@id": "https://orcid.org/0000-0002-8420-0696"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/0601/prov/"
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0602/prov/se/1",
                        "@type": ["http://www.w3.org/ns/prov#Entity"],
                        "http://purl.org/dc/terms/description": [
                            {"@value": "The entity 'https://w3id.org/oc/meta/br/0602' has been created."}
                        ],
                        "http://www.w3.org/ns/prov#generatedAtTime": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#dateTime", "@value": "2022-07-28T15:40:00"}
                        ],
                        "http://www.w3.org/ns/prov#hadPrimarySource": [
                            {"@id": "https://api.crossref.org/"}
                        ],
                        "http://www.w3.org/ns/prov#specializationOf": [
                            {"@id": "https://w3id.org/oc/meta/br/0602"}
                        ],
                        "http://www.w3.org/ns/prov#wasAttributedTo": [
                            {"@id": "https://orcid.org/0000-0002-8420-0696"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/0602/prov/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000" / "prov" / "se.zip"
        create_zip_file(zip_path, "se.json", prov_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {"http://purl.org/dc/terms/description": 2}
        output_path = output_dir / "br" / "0690" / "10000" / "1000" / "prov" / "se.zip"
        assert output_path.exists()
        result = read_zip_file(output_path)
        desc = result[0]["@graph"][0]["http://purl.org/dc/terms/description"][0]
        assert desc["@type"] == "http://www.w3.org/2001/XMLSchema#string"
        datetime_val = result[0]["@graph"][0]["http://www.w3.org/ns/prov#generatedAtTime"][0]
        assert datetime_val["@type"] == "http://www.w3.org/2001/XMLSchema#dateTime"

    def test_processes_volume_issue_sequence_identifiers(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalVolume"
                        ],
                        "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                            {"@value": "42"}
                        ]
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/br/0602",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalIssue"
                        ],
                        "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                            {"@value": "7-8"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "2000.zip"
        create_zip_file(zip_path, "2000.json", br_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {"http://purl.org/spar/fabio/hasSequenceIdentifier": 2}

    def test_copies_unchanged_file(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle"
                        ],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#date", "@value": "2020-01-15"}
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#string", "@value": "Already Typed Title"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", br_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {}
        output_path = output_dir / "br" / "0690" / "10000" / "1000.zip"
        assert output_path.exists()

    def test_preserves_directory_structure(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [{"@graph": [], "@id": "https://w3id.org/oc/meta/br/"}]
        paths = [
            ("br", "0690", "10000", "1000.zip"),
            ("br", "0690", "10000", "1000", "prov", "se.zip"),
            ("ra", "0690", "10000", "1000.zip"),
            ("id", "0690", "20000", "2000.zip"),
            ("re", "0690", "30000", "3000.zip"),
            ("ar", "0690", "40000", "4000.zip"),
        ]
        for path_parts in paths:
            zip_path = input_dir / Path(*path_parts)
            json_name = path_parts[-1].replace(".zip", ".json")
            create_zip_file(zip_path, json_name, br_data)

        for path_parts in paths:
            zip_path = input_dir / Path(*path_parts)
            process_zip_file(zip_path, input_dir, output_dir)

        for path_parts in paths:
            output_path = output_dir / Path(*path_parts)
            assert output_path.exists(), f"Missing: {output_path}"

    def test_handles_multiple_date_formats(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#date", "@value": "2020-01-15"}
                        ]
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/br/0602",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#gYear", "@value": "2020"}
                        ]
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/br/0603",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#gYearMonth", "@value": "2020-05"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", br_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {}

    def test_handles_mixed_typed_and_untyped(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#date", "@value": "2020-01-15"}
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Untyped Title"}
                        ],
                        "http://purl.org/spar/datacite/hasIdentifier": [
                            {"@id": "https://w3id.org/oc/meta/id/0601"}
                        ],
                        "http://purl.org/vocab/frbr/core#embodiment": [
                            {"@id": "https://w3id.org/oc/meta/re/0601"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", br_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {"http://purl.org/dc/terms/title": 1}
        result = read_zip_file(output_dir / "br" / "0690" / "10000" / "1000.zip")
        entity = result[0]["@graph"][0]
        assert entity["http://purl.org/dc/terms/title"][0]["@type"] == "http://www.w3.org/2001/XMLSchema#string"
        assert entity["http://prismstandard.org/namespaces/basic/2.0/publicationDate"][0]["@type"] == "http://www.w3.org/2001/XMLSchema#date"


class TestProcessBatch:
    def test_processes_multiple_files(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data1 = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://purl.org/dc/terms/title": [{"@value": "Title 1"}]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        br_data2 = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0602",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://purl.org/dc/terms/title": [{"@value": "Title 2"}]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path1 = input_dir / "br" / "0690" / "10000" / "1000.zip"
        zip_path2 = input_dir / "br" / "0690" / "10000" / "2000.zip"
        create_zip_file(zip_path1, "1000.json", br_data1)
        create_zip_file(zip_path2, "2000.json", br_data2)

        batch = [
            (zip_path1, input_dir, output_dir),
            (zip_path2, input_dir, output_dir),
        ]
        results = process_batch(batch)

        assert len(results) == 2
        assert results[0] == {"http://purl.org/dc/terms/title": 1}
        assert results[1] == {"http://purl.org/dc/terms/title": 1}

    def test_empty_batch(self):
        results = process_batch([])
        assert results == []


class TestIntegrationFullStructure:
    def test_full_rdf_structure_with_all_entity_types(self, temp_dirs):
        input_dir, output_dir = temp_dirs

        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/06015",
                        "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#date", "@value": "2020-01-15"}
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "Test Article"}],
                        "http://purl.org/spar/datacite/hasIdentifier": [{"@id": "https://w3id.org/oc/meta/id/0601"}],
                        "http://purl.org/spar/pro/isDocumentContextFor": [{"@id": "https://w3id.org/oc/meta/ar/0601"}],
                        "http://purl.org/vocab/frbr/core#embodiment": [{"@id": "https://w3id.org/oc/meta/re/0601"}],
                        "http://purl.org/vocab/frbr/core#partOf": [{"@id": "https://w3id.org/oc/meta/br/06020"}]
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/br/06020",
                        "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalVolume"],
                        "http://purl.org/spar/fabio/hasSequenceIdentifier": [{"@value": "15"}]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        create_zip_file(input_dir / "br" / "0690" / "10000" / "1000.zip", "1000.json", br_data)

        ra_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/ra/0601",
                        "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                        "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Peroni"}],
                        "http://xmlns.com/foaf/0.1/givenName": [{"@value": "Silvio"}],
                        "http://purl.org/spar/datacite/hasIdentifier": [{"@id": "https://w3id.org/oc/meta/id/0602"}]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/ra/"
            }
        ]
        create_zip_file(input_dir / "ra" / "0690" / "10000" / "1000.zip", "1000.json", ra_data)

        id_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/0601",
                        "@type": ["http://purl.org/spar/datacite/Identifier"],
                        "http://purl.org/spar/datacite/usesIdentifierScheme": [{"@id": "http://purl.org/spar/datacite/doi"}],
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [{"@value": "10.1234/test"}]
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/id/0602",
                        "@type": ["http://purl.org/spar/datacite/Identifier"],
                        "http://purl.org/spar/datacite/usesIdentifierScheme": [{"@id": "http://purl.org/spar/datacite/orcid"}],
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [{"@value": "0000-0002-8420-0696"}]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/id/"
            }
        ]
        create_zip_file(input_dir / "id" / "0690" / "10000" / "1000.zip", "1000.json", id_data)

        re_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/re/0601",
                        "@type": ["http://purl.org/spar/fabio/Manifestation"],
                        "http://prismstandard.org/namespaces/basic/2.0/startingPage": [{"@value": "1"}],
                        "http://prismstandard.org/namespaces/basic/2.0/endingPage": [{"@value": "10"}]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/re/"
            }
        ]
        create_zip_file(input_dir / "re" / "0690" / "10000" / "1000.zip", "1000.json", re_data)

        ar_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/ar/0601",
                        "@type": ["http://purl.org/spar/pro/RoleInTime"],
                        "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/author"}],
                        "http://purl.org/spar/pro/isHeldBy": [{"@id": "https://w3id.org/oc/meta/ra/0601"}]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/ar/"
            }
        ]
        create_zip_file(input_dir / "ar" / "0690" / "10000" / "1000.zip", "1000.json", ar_data)

        prov_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/06015/prov/se/1",
                        "@type": ["http://www.w3.org/ns/prov#Entity"],
                        "http://purl.org/dc/terms/description": [{"@value": "The entity has been created."}],
                        "http://www.w3.org/ns/prov#generatedAtTime": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#dateTime", "@value": "2022-07-28T15:38:17"}
                        ],
                        "http://www.w3.org/ns/prov#specializationOf": [{"@id": "https://w3id.org/oc/meta/br/06015"}]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/06015/prov/"
            }
        ]
        create_zip_file(input_dir / "br" / "0690" / "10000" / "1000" / "prov" / "se.zip", "se.json", prov_data)

        zip_files = collect_zip_files(input_dir)
        total_modifications = {}
        for zf in zip_files:
            mods = process_zip_file(zf, input_dir, output_dir)
            for prop, count in mods.items():
                total_modifications[prop] = total_modifications.get(prop, 0) + count

        assert total_modifications == {
            "http://purl.org/dc/terms/title": 1,
            "http://purl.org/spar/fabio/hasSequenceIdentifier": 1,
            "http://xmlns.com/foaf/0.1/familyName": 1,
            "http://xmlns.com/foaf/0.1/givenName": 1,
            "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": 2,
            "http://prismstandard.org/namespaces/basic/2.0/startingPage": 1,
            "http://prismstandard.org/namespaces/basic/2.0/endingPage": 1,
            "http://purl.org/dc/terms/description": 1,
        }

        expected_outputs = [
            output_dir / "br" / "0690" / "10000" / "1000.zip",
            output_dir / "ra" / "0690" / "10000" / "1000.zip",
            output_dir / "id" / "0690" / "10000" / "1000.zip",
            output_dir / "re" / "0690" / "10000" / "1000.zip",
            output_dir / "ar" / "0690" / "10000" / "1000.zip",
            output_dir / "br" / "0690" / "10000" / "1000" / "prov" / "se.zip",
        ]
        for path in expected_outputs:
            assert path.exists()

    def test_multiple_supplier_prefixes(self, temp_dirs):
        input_dir, output_dir = temp_dirs

        for prefix in ["0690", "0610", "0670"]:
            br_data = [
                {
                    "@graph": [
                        {
                            "@id": f"https://w3id.org/oc/meta/br/{prefix}15",
                            "@type": ["http://purl.org/spar/fabio/Expression"],
                            "http://purl.org/dc/terms/title": [{"@value": f"Title from {prefix}"}]
                        }
                    ],
                    "@id": "https://w3id.org/oc/meta/br/"
                }
            ]
            create_zip_file(input_dir / "br" / prefix / "10000" / "1000.zip", "1000.json", br_data)

        zip_files = collect_zip_files(input_dir)
        total_count = 0
        for zf in zip_files:
            mods = process_zip_file(zf, input_dir, output_dir)
            total_count += sum(mods.values())

        assert total_count == 3
        for prefix in ["0690", "0610", "0670"]:
            assert (output_dir / "br" / prefix / "10000" / "1000.zip").exists()
