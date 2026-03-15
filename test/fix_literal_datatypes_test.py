import json
import zipfile
from pathlib import Path

import pytest
from rdflib import XSD, Dataset, Literal, URIRef

from oc_meta.run.patches.fix_literal_datatypes import (
    PUBLICATION_DATE_PREDICATE,
    PUBLICATION_DATE_PREDICATE_STR,
    VALID_DATE_TYPES_STR,
    _has_invalid_date_type,
    _has_untyped_literal,
    _worker_init,
    collect_zip_files,
    is_provenance_file,
    needs_modification,
    process_batch,
    process_dataset,
    process_zip_file,
)

XSD_STRING_STR = str(XSD.string)


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


class TestWorkerInit:
    def test_sets_signal_handler(self):
        _worker_init()


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


class TestIsProvenanceFile:
    def test_detects_prov_in_path(self):
        path = Path("/data/br/0690/10000/1000/prov/se.zip")
        assert is_provenance_file(path) is True

    def test_returns_false_for_non_prov_path(self):
        path = Path("/data/br/0690/10000/1000.zip")
        assert is_provenance_file(path) is False


class TestHasUntypedLiteral:
    def test_detects_untyped_value(self):
        data = {"@value": "test"}
        assert _has_untyped_literal(data) is True

    def test_ignores_typed_value(self):
        data = {"@value": "test", "@type": XSD_STRING_STR}
        assert _has_untyped_literal(data) is False

    def test_ignores_language_tagged_value(self):
        data = {"@value": "test", "@language": "en"}
        assert _has_untyped_literal(data) is False

    def test_detects_untyped_in_nested_list(self):
        data = [{"@graph": [{"prop": [{"@value": "untyped"}]}]}]
        assert _has_untyped_literal(data) is True

    def test_all_typed_returns_false(self):
        data = [
            {
                "@graph": [
                    {
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Title", "@type": XSD_STRING_STR}
                        ]
                    }
                ]
            }
        ]
        assert _has_untyped_literal(data) is False

    def test_empty_data(self):
        assert _has_untyped_literal([]) is False
        assert _has_untyped_literal({}) is False

    def test_non_value_dict(self):
        data = {"@id": "http://example.org/entity"}
        assert _has_untyped_literal(data) is False


class TestHasInvalidDateType:
    def test_detects_string_type(self):
        data = {PUBLICATION_DATE_PREDICATE_STR: [{"@value": "2020-01-15", "@type": XSD_STRING_STR}]}
        assert _has_invalid_date_type(data) is True

    def test_detects_other_invalid_type(self):
        data = {PUBLICATION_DATE_PREDICATE_STR: [{"@value": "2020", "@type": str(XSD.integer)}]}
        assert _has_invalid_date_type(data) is True

    def test_ignores_date_type(self):
        data = {PUBLICATION_DATE_PREDICATE_STR: [{"@value": "2020-01-15", "@type": str(XSD.date)}]}
        assert _has_invalid_date_type(data) is False

    def test_ignores_gyear_type(self):
        data = {PUBLICATION_DATE_PREDICATE_STR: [{"@value": "2020", "@type": str(XSD.gYear)}]}
        assert _has_invalid_date_type(data) is False

    def test_ignores_gyearmonth_type(self):
        data = {PUBLICATION_DATE_PREDICATE_STR: [{"@value": "2020-05", "@type": str(XSD.gYearMonth)}]}
        assert _has_invalid_date_type(data) is False

    def test_ignores_untyped_date(self):
        data = {PUBLICATION_DATE_PREDICATE_STR: [{"@value": "2020-01-15"}]}
        assert _has_invalid_date_type(data) is False

    def test_ignores_other_predicates(self):
        data = {"http://purl.org/dc/terms/title": [{"@value": "Title", "@type": XSD_STRING_STR}]}
        assert _has_invalid_date_type(data) is False

    def test_detects_in_nested_graph(self):
        data = [
            {
                "@graph": [
                    {
                        PUBLICATION_DATE_PREDICATE_STR: [
                            {"@value": "2020", "@type": XSD_STRING_STR}
                        ]
                    }
                ]
            }
        ]
        assert _has_invalid_date_type(data) is True

    def test_empty_data(self):
        assert _has_invalid_date_type([]) is False
        assert _has_invalid_date_type({}) is False


class TestNeedsModification:
    def test_detects_untyped_literal_non_prov(self):
        data = {"@value": "test"}
        assert needs_modification(data, is_prov=False) is True

    def test_detects_untyped_literal_prov(self):
        data = {"@value": "test"}
        assert needs_modification(data, is_prov=True) is True

    def test_detects_string_date_non_prov(self):
        data = {PUBLICATION_DATE_PREDICATE_STR: [{"@value": "2020", "@type": XSD_STRING_STR}]}
        assert needs_modification(data, is_prov=False) is True

    def test_ignores_string_date_prov(self):
        data = {PUBLICATION_DATE_PREDICATE_STR: [{"@value": "2020", "@type": XSD_STRING_STR}]}
        assert needs_modification(data, is_prov=True) is False

    def test_no_modification_needed(self):
        data = {
            "http://purl.org/dc/terms/title": [{"@value": "Title", "@type": XSD_STRING_STR}],
            PUBLICATION_DATE_PREDICATE_STR: [{"@value": "2020", "@type": str(XSD.gYear)}]
        }
        assert needs_modification(data, is_prov=False) is False

    def test_empty_data(self):
        assert needs_modification([], is_prov=False) is False
        assert needs_modification({}, is_prov=True) is False


class TestProcessDataset:
    def test_adds_xsd_string_to_untyped_literals(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        pred = URIRef("http://purl.org/dc/terms/title")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, pred, Literal("Test Title")))

        modifications = process_dataset(ds, is_prov=False)

        assert modifications == {"untyped:http://purl.org/dc/terms/title": 1}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.string

    def test_fixes_untyped_date_directly(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("2020-01-15")))

        modifications = process_dataset(ds, is_prov=False)

        assert modifications == {f"date:{XSD.date}": 1}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.date
                assert str(o) == "2020-01-15"

    def test_fixes_date_from_string(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("2020-01-15", datatype=XSD.string)))

        modifications = process_dataset(ds, is_prov=False)

        assert modifications == {f"date:{XSD.date}": 1}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.date
                assert str(o) == "2020-01-15"

    def test_fixes_year_date_from_string(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("2020", datatype=XSD.string)))

        modifications = process_dataset(ds, is_prov=False)

        assert modifications == {f"date:{XSD.gYear}": 1}

    def test_fixes_year_month_date_from_string(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("2020-05", datatype=XSD.string)))

        modifications = process_dataset(ds, is_prov=False)

        assert modifications == {f"date:{XSD.gYearMonth}": 1}

    def test_skips_date_fix_for_provenance(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("2020-01-15", datatype=XSD.string)))

        modifications = process_dataset(ds, is_prov=True)

        assert modifications == {}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.string

    def test_preserves_existing_datatype(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("2020-01-15", datatype=XSD.date)))

        modifications = process_dataset(ds, is_prov=False)

        assert modifications == {}

    def test_preserves_language_tagged_literals(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        pred = URIRef("http://purl.org/dc/terms/title")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, pred, Literal("Titre en Francais", lang="fr")))

        modifications = process_dataset(ds, is_prov=False)

        assert modifications == {}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.language == "fr"
                assert o.datatype is None

    def test_raises_on_invalid_date_format(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("invalid-date", datatype=XSD.string)))

        with pytest.raises(ValueError):
            process_dataset(ds, is_prov=False)

    def test_combined_untyped_and_date_fix(self):
        ds = Dataset()
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        title_pred = URIRef("http://purl.org/dc/terms/title")
        g.add((subj, title_pred, Literal("Test Title")))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("2020-01-15", datatype=XSD.string)))

        modifications = process_dataset(ds, is_prov=False)

        assert modifications == {
            "untyped:http://purl.org/dc/terms/title": 1,
            f"date:{XSD.date}": 1,
        }

    def test_multiple_properties(self):
        ds = Dataset()
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g.add((subj, URIRef("http://purl.org/dc/terms/title"), Literal("Title")))
        g.add((subj, URIRef("http://purl.org/dc/terms/title"), Literal("Another Title")))
        ra_subj = URIRef("https://w3id.org/oc/meta/ra/0601")
        g.add((ra_subj, URIRef("http://xmlns.com/foaf/0.1/familyName"), Literal("Smith")))

        modifications = process_dataset(ds, is_prov=False)

        assert modifications == {
            "untyped:http://purl.org/dc/terms/title": 2,
            "untyped:http://xmlns.com/foaf/0.1/familyName": 1,
        }


class TestProcessZipFile:
    def test_processes_bibliographic_resource_with_both_fixes(self, temp_dirs):
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
                            {"@type": XSD_STRING_STR, "@value": "2020-01-15"}
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Test Article Title"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", br_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {
            "untyped:http://purl.org/dc/terms/title": 1,
            f"date:{XSD.date}": 1,
        }
        output_path = output_dir / "br" / "0690" / "10000" / "1000.zip"
        assert output_path.exists()
        result = read_zip_file(output_path)
        title_obj = result[0]["@graph"][0]["http://purl.org/dc/terms/title"][0]
        assert title_obj["@type"] == XSD_STRING_STR
        date_obj = result[0]["@graph"][0]["http://prismstandard.org/namespaces/basic/2.0/publicationDate"][0]
        assert date_obj["@type"] == str(XSD.date)

    def test_processes_responsible_agent_data(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        ra_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/ra/06015",
                        "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                        "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Peroni"}],
                        "http://xmlns.com/foaf/0.1/givenName": [{"@value": "Silvio"}]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/ra/"
            }
        ]
        zip_path = input_dir / "ra" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", ra_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {
            "untyped:http://xmlns.com/foaf/0.1/familyName": 1,
            "untyped:http://xmlns.com/foaf/0.1/givenName": 1,
        }

    def test_processes_identifier_data(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        id_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/0601",
                        "@type": ["http://purl.org/spar/datacite/Identifier"],
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                            {"@value": "10.1234/test.article"}
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
            "untyped:http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": 1
        }

    def test_processes_provenance_untyped_but_not_dates(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        prov_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601/prov/se/1",
                        "@type": ["http://www.w3.org/ns/prov#Entity"],
                        "http://purl.org/dc/terms/description": [
                            {"@value": "The entity has been created."}
                        ],
                        "http://www.w3.org/ns/prov#generatedAtTime": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#dateTime", "@value": "2022-07-28T15:38:17"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/0601/prov/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000" / "prov" / "se.zip"
        create_zip_file(zip_path, "se.json", prov_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {"untyped:http://purl.org/dc/terms/description": 1}
        output_path = output_dir / "br" / "0690" / "10000" / "1000" / "prov" / "se.zip"
        assert output_path.exists()
        result = read_zip_file(output_path)
        desc = result[0]["@graph"][0]["http://purl.org/dc/terms/description"][0]
        assert desc["@type"] == XSD_STRING_STR
        datetime_val = result[0]["@graph"][0]["http://www.w3.org/ns/prov#generatedAtTime"][0]
        assert datetime_val["@type"] == "http://www.w3.org/2001/XMLSchema#dateTime"

    def test_copies_unchanged_file(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": str(XSD.date), "@value": "2020-01-15"}
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@type": XSD_STRING_STR, "@value": "Already Typed Title"}
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
            assert output_path.exists()

    def test_handles_year_date(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": XSD_STRING_STR, "@value": "2020"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", br_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {f"date:{XSD.gYear}": 1}
        result = read_zip_file(output_dir / "br" / "0690" / "10000" / "1000.zip")
        date_obj = result[0]["@graph"][0]["http://prismstandard.org/namespaces/basic/2.0/publicationDate"][0]
        assert date_obj["@type"] == str(XSD.gYear)

    def test_handles_year_month_date(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": XSD_STRING_STR, "@value": "2020-06"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", br_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {f"date:{XSD.gYearMonth}": 1}
        result = read_zip_file(output_dir / "br" / "0690" / "10000" / "1000.zip")
        date_obj = result[0]["@graph"][0]["http://prismstandard.org/namespaces/basic/2.0/publicationDate"][0]
        assert date_obj["@type"] == str(XSD.gYearMonth)


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
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": XSD_STRING_STR, "@value": "2020"}
                        ]
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
        assert results[0] == {"untyped:http://purl.org/dc/terms/title": 1}
        assert results[1] == {f"date:{XSD.gYear}": 1}

    def test_empty_batch(self):
        results = process_batch([])
        assert results == []


class TestIntegration:
    def test_full_rdf_structure_with_all_entity_types(self, temp_dirs):
        input_dir, output_dir = temp_dirs

        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/06015",
                        "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": XSD_STRING_STR, "@value": "2020-01-15"}
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "Test Article"}]
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
                        "http://xmlns.com/foaf/0.1/givenName": [{"@value": "Silvio"}]
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
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [{"@value": "10.1234/test"}]
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/id/0602",
                        "@type": ["http://purl.org/spar/datacite/Identifier"],
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

        prov_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/06015/prov/se/1",
                        "@type": ["http://www.w3.org/ns/prov#Entity"],
                        "http://purl.org/dc/terms/description": [{"@value": "The entity has been created."}],
                        "http://www.w3.org/ns/prov#generatedAtTime": [
                            {"@type": "http://www.w3.org/2001/XMLSchema#dateTime", "@value": "2022-07-28T15:38:17"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/06015/prov/"
            }
        ]
        create_zip_file(input_dir / "br" / "0690" / "10000" / "1000" / "prov" / "se.zip", "se.json", prov_data)

        zip_files = collect_zip_files(input_dir)
        total_modifications: dict[str, int] = {}
        for zf in zip_files:
            mods = process_zip_file(zf, input_dir, output_dir)
            for key, count in mods.items():
                total_modifications[key] = total_modifications.get(key, 0) + count

        assert total_modifications == {
            "untyped:http://purl.org/dc/terms/title": 1,
            "untyped:http://purl.org/spar/fabio/hasSequenceIdentifier": 1,
            "untyped:http://xmlns.com/foaf/0.1/familyName": 1,
            "untyped:http://xmlns.com/foaf/0.1/givenName": 1,
            "untyped:http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": 2,
            "untyped:http://prismstandard.org/namespaces/basic/2.0/startingPage": 1,
            "untyped:http://prismstandard.org/namespaces/basic/2.0/endingPage": 1,
            "untyped:http://purl.org/dc/terms/description": 1,
            f"date:{XSD.date}": 1,
        }

        expected_outputs = [
            output_dir / "br" / "0690" / "10000" / "1000.zip",
            output_dir / "ra" / "0690" / "10000" / "1000.zip",
            output_dir / "id" / "0690" / "10000" / "1000.zip",
            output_dir / "re" / "0690" / "10000" / "1000.zip",
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
                            "http://purl.org/dc/terms/title": [{"@value": f"Title from {prefix}"}],
                            "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                                {"@type": XSD_STRING_STR, "@value": "2020"}
                            ]
                        }
                    ],
                    "@id": "https://w3id.org/oc/meta/br/"
                }
            ]
            create_zip_file(input_dir / "br" / prefix / "10000" / "1000.zip", "1000.json", br_data)

        zip_files = collect_zip_files(input_dir)
        total_untyped = 0
        total_dates = 0
        for zf in zip_files:
            mods = process_zip_file(zf, input_dir, output_dir)
            for key, count in mods.items():
                if key.startswith("untyped:"):
                    total_untyped += count
                elif key.startswith("date:"):
                    total_dates += count

        assert total_untyped == 3
        assert total_dates == 3
        for prefix in ["0690", "0610", "0670"]:
            assert (output_dir / "br" / prefix / "10000" / "1000.zip").exists()
