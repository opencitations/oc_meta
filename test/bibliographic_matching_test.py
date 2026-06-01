# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import json
import os
import unittest
from unittest.mock import patch

from oc_meta.lib.bibliographic_matching import (
    MATCHING_THRESHOLD,
    SPARSE_MATCHING_THRESHOLD,
    compute_matching_score,
    fetch_crossref_metadata,
    fetch_triplestore_metadata,
    is_sparse,
)
from test.test_utils import SERVER, add_data_ts, wait_for_triplestore

TS_DATA = os.path.abspath(
    os.path.join("test", "testcases", "ts", "massari_publications.nt")
).replace("\\", "/")
CROSSREF_DIR = os.path.join("test", "testcases", "crossref")

ARTICLE_URI = "https://w3id.org/oc/meta/br/06901"

QSS_META = {
    "title": "opencitations meta",
    "first_author_family": "massari",
    "first_author_given": "Arcangelo",
    "year": "2024",
    "venue": "quantitative science studies",
    "issn": "2641-3337",
    "volume": "5",
    "issue": "1",
    "start_page": "50",
    "end_page": "75",
}

MAILTO = "test@example.com"


def _load_crossref_fixture(name: str) -> dict:
    with open(os.path.join(CROSSREF_DIR, name)) as f:
        return json.load(f)


class TestBibliographicMatching(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not wait_for_triplestore(SERVER, max_wait=30):
            raise TimeoutError("Triplestore not ready after 30 seconds")
        add_data_ts(SERVER, TS_DATA)

    def test_fetch_triplestore_metadata(self):
        meta = fetch_triplestore_metadata(SERVER, ARTICLE_URI)
        self.assertEqual(meta, QSS_META)

    def test_compute_matching_score_match(self):
        ts_meta = fetch_triplestore_metadata(SERVER, ARTICLE_URI)
        fixture = _load_crossref_fixture("qss_a_00292.json")
        with patch(
            "oc_meta.lib.bibliographic_matching.call_api", return_value=fixture
        ):
            cr_meta = fetch_crossref_metadata("10.1162/qss_a_00292", {}, MAILTO)
        assert cr_meta is not None
        score = compute_matching_score(ts_meta, cr_meta)
        self.assertEqual(score, 40.0)
        self.assertGreaterEqual(score, MATCHING_THRESHOLD)

    def test_compute_matching_score_mismatch(self):
        ts_meta = fetch_triplestore_metadata(SERVER, ARTICLE_URI)
        fixture = _load_crossref_fixture("s11192-022-04367-w.json")
        with patch(
            "oc_meta.lib.bibliographic_matching.call_api", return_value=fixture
        ):
            cr_meta = fetch_crossref_metadata("10.1007/s11192-022-04367-w", {}, MAILTO)
        assert cr_meta is not None
        score = compute_matching_score(ts_meta, cr_meta)
        self.assertEqual(score, 7.8925858951175405)
        self.assertLess(score, MATCHING_THRESHOLD)

    def test_is_sparse_and_threshold(self):
        self.assertEqual(MATCHING_THRESHOLD, 25.0)
        self.assertEqual(SPARSE_MATCHING_THRESHOLD, 10.0)
        self.assertFalse(is_sparse(QSS_META))

        sparse_meta = {
            "title": "",
            "first_author_family": "",
            "first_author_given": "",
            "year": "2024",
            "venue": "",
            "issn": "",
            "volume": "5",
            "issue": "1",
            "start_page": "50",
            "end_page": "75",
        }
        self.assertTrue(is_sparse(sparse_meta))

        score = compute_matching_score(sparse_meta, sparse_meta)
        self.assertEqual(score, 14.0)
        self.assertGreaterEqual(score, SPARSE_MATCHING_THRESHOLD)
        self.assertLess(score, MATCHING_THRESHOLD)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
