# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import csv
import os
import tempfile

from oc_meta.run.meta.convert_citations import convert_citations


class TestConvertCitations:
    def test_converts_citations_across_multiple_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            meta_dir = os.path.join(tmpdir, "meta")
            cit_dir = os.path.join(tmpdir, "citations")
            out_dir = os.path.join(tmpdir, "output")
            os.makedirs(meta_dir)
            os.makedirs(cit_dir)

            with open(os.path.join(meta_dir, "meta_batch1.csv"), "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(
                    [
                        "id",
                        "title",
                        "author",
                        "pub_date",
                        "venue",
                        "volume",
                        "issue",
                        "page",
                        "type",
                        "publisher",
                        "editor",
                    ]
                )
                w.writerow(
                    [
                        "temp:urn:nbn:de:0168-ssoar-347295 omid:br/06019115517",
                        "Postmodernism",
                        "Ven, Bert Van De",
                        "2000",
                        "",
                        "",
                        "",
                        "",
                        "journal article",
                        "",
                        "",
                    ]
                )
                w.writerow(
                    [
                        "temp:gesis-ssoar-34729_b1 omid:br/06019115518",
                        "Title B1",
                        "Author B",
                        "1941",
                        "",
                        "",
                        "",
                        "",
                        "book chapter",
                        "",
                        "",
                    ]
                )

            with open(os.path.join(meta_dir, "meta_batch2.csv"), "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(
                    [
                        "id",
                        "title",
                        "author",
                        "pub_date",
                        "venue",
                        "volume",
                        "issue",
                        "page",
                        "type",
                        "publisher",
                        "editor",
                    ]
                )
                w.writerow(
                    [
                        "doi:10.14361/9783839434291 isbn:9783839434291 omid:br/0622049481",
                        "Digital Book",
                        "Schmidt, A",
                        "2015",
                        "",
                        "",
                        "",
                        "",
                        "book",
                        "",
                        "",
                    ]
                )
                w.writerow(
                    [
                        "openalex:W3205470745 omid:br/06019115524",
                        "OpenAlex Entry",
                        "Doe, J",
                        "2020",
                        "",
                        "",
                        "",
                        "",
                        "journal article",
                        "",
                        "",
                    ]
                )

            with open(
                os.path.join(cit_dir, "citations_part1.csv"), "w", newline=""
            ) as f:
                w = csv.writer(f)
                w.writerow(
                    [
                        "citing_id",
                        "citing_publication_date",
                        "cited_id",
                        "cited_publication_date",
                    ]
                )
                w.writerow(
                    [
                        "temp:urn:nbn:de:0168-ssoar-347295",
                        "2000",
                        "temp:gesis-ssoar-34729_b1",
                        "1941",
                    ]
                )
                w.writerow(
                    [
                        "temp:urn:nbn:de:0168-ssoar-347295",
                        "2000",
                        "doi:10.14361/9783839434291",
                        "2015",
                    ]
                )

            with open(
                os.path.join(cit_dir, "citations_part2.csv"), "w", newline=""
            ) as f:
                w = csv.writer(f)
                w.writerow(
                    [
                        "citing_id",
                        "citing_publication_date",
                        "cited_id",
                        "cited_publication_date",
                    ]
                )
                w.writerow(
                    ["openalex:W3205470745", "2020", "isbn:9783839434291", "2015"]
                )
                w.writerow(
                    ["temp:orphan_citing", "2020", "temp:gesis-ssoar-34729_b1", "1941"]
                )
                w.writerow(
                    [
                        "temp:urn:nbn:de:0168-ssoar-347295",
                        "2000",
                        "temp:orphan_cited",
                        "2020",
                    ]
                )

            convert_citations(meta_dir, cit_dir, out_dir)

            with open(os.path.join(out_dir, "citations_part1.csv"), newline="") as f:
                rows = list(csv.DictReader(f))
            assert rows == [
                {"citing": "omid:br/06019115517", "cited": "omid:br/06019115518"},
                {"citing": "omid:br/06019115517", "cited": "omid:br/0622049481"},
            ]

            with open(os.path.join(out_dir, "citations_part2.csv"), newline="") as f:
                rows = list(csv.DictReader(f))
            assert rows == [
                {"citing": "omid:br/06019115524", "cited": "omid:br/0622049481"},
            ]
