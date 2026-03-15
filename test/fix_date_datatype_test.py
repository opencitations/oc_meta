import json
import zipfile
from pathlib import Path

import pytest
from rdflib import XSD, Dataset, Literal, URIRef

from oc_meta.run.patches.fix_date_datatype import (
    PUBLICATION_DATE_PREDICATE,
    PUBLICATION_DATE_PREDICATE_STR,
    _has_string_typed_date,
    _worker_init,
    collect_zip_files,
    is_provenance_file,
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


class TestWorkerInit:
    def test_sets_signal_handler(self):
        _worker_init()


class TestCollectZipFiles:
    def test_finds_zip_files_recursively(self, temp_dirs):
        input_dir, _ = temp_dirs
        (input_dir / "br" / "0690" / "10000").mkdir(parents=True)
        (input_dir / "br" / "0690" / "10000" / "1000.zip").touch()
        (input_dir / "br" / "0610" / "10000").mkdir(parents=True)
        (input_dir / "br" / "0610" / "10000" / "1000.zip").touch()

        result = collect_zip_files(input_dir)

        assert len(result) == 2
        assert all(p.suffix == ".zip" for p in result)

    def test_includes_provenance_directories(self, temp_dirs):
        input_dir, _ = temp_dirs
        (input_dir / "br" / "0690" / "10000").mkdir(parents=True)
        (input_dir / "br" / "0690" / "10000" / "1000.zip").touch()
        (input_dir / "br" / "0690" / "10000" / "1000" / "prov").mkdir(parents=True)
        (input_dir / "br" / "0690" / "10000" / "1000" / "prov" / "se.zip").touch()

        result = collect_zip_files(input_dir)

        assert len(result) == 2


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


class TestHasStringTypedDate:
    def test_detects_string_type(self):
        values = [{"@value": "2020-01-15", "@type": str(XSD.string)}]
        assert _has_string_typed_date(values) is True

    def test_ignores_date_type(self):
        values = [{"@value": "2020-01-15", "@type": str(XSD.date)}]
        assert _has_string_typed_date(values) is False

    def test_ignores_gyear_type(self):
        values = [{"@value": "2020", "@type": str(XSD.gYear)}]
        assert _has_string_typed_date(values) is False

    def test_empty_list(self):
        assert _has_string_typed_date([]) is False


class TestNeedsModification:
    def test_detects_publication_date_with_string_type(self):
        data = {
            PUBLICATION_DATE_PREDICATE_STR: [
                {"@value": "2020-01-15", "@type": str(XSD.string)}
            ]
        }
        assert needs_modification(data) is True

    def test_ignores_publication_date_with_correct_type(self):
        data = {
            PUBLICATION_DATE_PREDICATE_STR: [
                {"@value": "2020-01-15", "@type": str(XSD.date)}
            ]
        }
        assert needs_modification(data) is False

    def test_ignores_other_predicates_with_string_type(self):
        data = {
            "http://purl.org/dc/terms/title": [
                {"@value": "Test Title", "@type": str(XSD.string)}
            ]
        }
        assert needs_modification(data) is False

    def test_detects_in_nested_graph(self):
        data = [
            {
                "@graph": [
                    {
                        PUBLICATION_DATE_PREDICATE_STR: [
                            {"@value": "2020", "@type": str(XSD.string)}
                        ]
                    }
                ]
            }
        ]
        assert needs_modification(data) is True

    def test_ignores_nested_other_predicates(self):
        data = [
            {
                "@graph": [
                    {
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Title", "@type": str(XSD.string)}
                        ]
                    }
                ]
            }
        ]
        assert needs_modification(data) is False

    def test_correct_date_returns_false(self):
        data = [
            {
                "@graph": [
                    {
                        PUBLICATION_DATE_PREDICATE_STR: [
                            {"@value": "2020-01-15", "@type": str(XSD.date)}
                        ]
                    }
                ]
            }
        ]
        assert needs_modification(data) is False

    def test_empty_data(self):
        assert needs_modification([]) is False
        assert needs_modification({}) is False


class TestProcessDataset:
    def test_fixes_full_date_from_string(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("2020-01-15", datatype=XSD.string)))

        modifications = process_dataset(ds)

        assert modifications == {str(XSD.date): 1}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.date
                assert str(o) == "2020-01-15"

    def test_fixes_year_only_date_from_string(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("2020", datatype=XSD.string)))

        modifications = process_dataset(ds)

        assert modifications == {str(XSD.gYear): 1}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.gYear
                assert str(o) == "2020"

    def test_fixes_year_month_date_from_string(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("2020-05", datatype=XSD.string)))

        modifications = process_dataset(ds)

        assert modifications == {str(XSD.gYearMonth): 1}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.gYearMonth
                assert str(o) == "2020-05"

    def test_preserves_correct_date_datatype(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("2020-01-15", datatype=XSD.date)))

        modifications = process_dataset(ds)

        assert modifications == {}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.date

    def test_ignores_non_date_predicates(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        title_pred = URIRef("http://purl.org/dc/terms/title")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, title_pred, Literal("Test Title", datatype=XSD.string)))

        modifications = process_dataset(ds)

        assert modifications == {}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.string

    def test_skips_invalid_date_format(self):
        ds = Dataset()
        subj = URIRef("https://w3id.org/oc/meta/br/0601")
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((subj, PUBLICATION_DATE_PREDICATE, Literal("invalid-date", datatype=XSD.string)))

        modifications = process_dataset(ds)

        assert modifications == {}
        for s, p, o, ctx in ds.quads():
            if isinstance(o, Literal):
                assert o.datatype == XSD.string
                assert str(o) == "invalid-date"

    def test_multiple_dates(self):
        ds = Dataset()
        g = ds.graph(URIRef("https://w3id.org/oc/meta/br/"))
        g.add((
            URIRef("https://w3id.org/oc/meta/br/0601"),
            PUBLICATION_DATE_PREDICATE,
            Literal("2020-01-15", datatype=XSD.string)
        ))
        g.add((
            URIRef("https://w3id.org/oc/meta/br/0602"),
            PUBLICATION_DATE_PREDICATE,
            Literal("2019", datatype=XSD.string)
        ))
        g.add((
            URIRef("https://w3id.org/oc/meta/br/0603"),
            PUBLICATION_DATE_PREDICATE,
            Literal("2018-06", datatype=XSD.string)
        ))

        modifications = process_dataset(ds)

        assert modifications == {
            str(XSD.date): 1,
            str(XSD.gYear): 1,
            str(XSD.gYearMonth): 1,
        }


class TestProcessZipFile:
    def test_fixes_date_in_bibliographic_resource(self, temp_dirs):
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
                            {"@type": str(XSD.string), "@value": "2020-01-15"}
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@type": str(XSD.string), "@value": "Test Article Title"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", br_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {str(XSD.date): 1}
        output_path = output_dir / "br" / "0690" / "10000" / "1000.zip"
        assert output_path.exists()
        result = read_zip_file(output_path)
        date_obj = result[0]["@graph"][0]["http://prismstandard.org/namespaces/basic/2.0/publicationDate"][0]
        assert date_obj["@type"] == str(XSD.date)
        assert date_obj["@value"] == "2020-01-15"
        title_obj = result[0]["@graph"][0]["http://purl.org/dc/terms/title"][0]
        assert title_obj["@type"] == str(XSD.string)

    def test_fixes_year_only_date(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/06015",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": str(XSD.string), "@value": "2020"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", br_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {str(XSD.gYear): 1}
        result = read_zip_file(output_dir / "br" / "0690" / "10000" / "1000.zip")
        date_obj = result[0]["@graph"][0]["http://prismstandard.org/namespaces/basic/2.0/publicationDate"][0]
        assert date_obj["@type"] == str(XSD.gYear)
        assert date_obj["@value"] == "2020"

    def test_fixes_year_month_date(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/06015",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": str(XSD.string), "@value": "2020-06"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", br_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {str(XSD.gYearMonth): 1}
        result = read_zip_file(output_dir / "br" / "0690" / "10000" / "1000.zip")
        date_obj = result[0]["@graph"][0]["http://prismstandard.org/namespaces/basic/2.0/publicationDate"][0]
        assert date_obj["@type"] == str(XSD.gYearMonth)
        assert date_obj["@value"] == "2020-06"

    def test_copies_file_without_string_dates(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/06015",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": str(XSD.date), "@value": "2020-01-15"}
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

    def test_copies_file_without_publication_dates(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        ra_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/ra/0601",
                        "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                        "http://xmlns.com/foaf/0.1/familyName": [
                            {"@type": str(XSD.string), "@value": "Peroni"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/ra/"
            }
        ]
        zip_path = input_dir / "ra" / "0690" / "10000" / "1000.zip"
        create_zip_file(zip_path, "1000.json", ra_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {}
        output_path = output_dir / "ra" / "0690" / "10000" / "1000.zip"
        assert output_path.exists()

    def test_copies_provenance_files_without_processing(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        prov_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601/prov/se/1",
                        "@type": ["http://www.w3.org/ns/prov#Entity"],
                        "http://purl.org/dc/terms/description": [
                            {"@type": str(XSD.string), "@value": "Entity created."}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/0601/prov/"
            }
        ]
        zip_path = input_dir / "br" / "0690" / "10000" / "1000" / "prov" / "se.zip"
        create_zip_file(zip_path, "se.json", prov_data)

        modifications = process_zip_file(zip_path, input_dir, output_dir)

        assert modifications == {}
        output_path = output_dir / "br" / "0690" / "10000" / "1000" / "prov" / "se.zip"
        assert output_path.exists()
        result = read_zip_file(output_path)
        assert result == prov_data

    def test_preserves_directory_structure(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [{"@graph": [], "@id": "https://w3id.org/oc/meta/br/"}]
        paths = [
            ("br", "0690", "10000", "1000.zip"),
            ("br", "0610", "20000", "2000.zip"),
            ("ra", "0690", "10000", "1000.zip"),
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

    def test_handles_mixed_date_formats(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": str(XSD.string), "@value": "2020-01-15"}
                        ]
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/br/0602",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": str(XSD.string), "@value": "2019"}
                        ]
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/br/0603",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": str(XSD.date), "@value": "2018-06-15"}
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
            str(XSD.date): 1,
            str(XSD.gYear): 1,
        }


class TestProcessBatch:
    def test_processes_multiple_files(self, temp_dirs):
        input_dir, output_dir = temp_dirs
        br_data1 = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": str(XSD.string), "@value": "2020-01-15"}
                        ]
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
                            {"@type": str(XSD.string), "@value": "2019"}
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
        assert results[0] == {str(XSD.date): 1}
        assert results[1] == {str(XSD.gYear): 1}

    def test_empty_batch(self):
        results = process_batch([])
        assert results == []


class TestIntegration:
    def test_full_processing_with_multiple_files(self, temp_dirs):
        input_dir, output_dir = temp_dirs

        br_data1 = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/06015",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": str(XSD.string), "@value": "2020-01-15"}
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@type": str(XSD.string), "@value": "Title 1"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        create_zip_file(input_dir / "br" / "0690" / "10000" / "1000.zip", "1000.json", br_data1)

        br_data2 = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/06020",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": str(XSD.string), "@value": "2019"}
                        ]
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/br/06021",
                        "@type": ["http://purl.org/spar/fabio/Expression"],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@type": str(XSD.string), "@value": "2018-06"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/"
            }
        ]
        create_zip_file(input_dir / "br" / "0610" / "10000" / "1000.zip", "1000.json", br_data2)

        ra_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/ra/0601",
                        "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                        "http://xmlns.com/foaf/0.1/name": [
                            {"@type": str(XSD.string), "@value": "Test Author"}
                        ]
                    }
                ],
                "@id": "https://w3id.org/oc/meta/ra/"
            }
        ]
        create_zip_file(input_dir / "ra" / "0690" / "10000" / "1000.zip", "1000.json", ra_data)

        zip_files = collect_zip_files(input_dir)
        total_modifications = {}
        for zf in zip_files:
            mods = process_zip_file(zf, input_dir, output_dir)
            for dtype, count in mods.items():
                total_modifications[dtype] = total_modifications.get(dtype, 0) + count

        assert total_modifications == {
            str(XSD.date): 1,
            str(XSD.gYear): 1,
            str(XSD.gYearMonth): 1,
        }

        assert (output_dir / "br" / "0690" / "10000" / "1000.zip").exists()
        assert (output_dir / "br" / "0610" / "10000" / "1000.zip").exists()
        assert (output_dir / "ra" / "0690" / "10000" / "1000.zip").exists()
