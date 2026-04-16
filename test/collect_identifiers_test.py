#!/usr/bin/env python

# SPDX-FileCopyrightText: 2025 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

# -*- coding: utf-8 -*-

"""
Test script for collect_identifiers method with real CSV data.
This test verifies that all types of identifiers are correctly extracted.
"""

import csv
import os

import pytest

from oc_meta.core.curator import Curator
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler


class TestCollectIdentifiers:
    """Test suite for collect_identifiers method using real data."""

    curator: Curator

    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self, request: pytest.FixtureRequest) -> None:
        """Set up test fixtures with real CSV data."""
        assert request.cls is not None
        csv_path = os.path.join(os.path.dirname(__file__), 'test_data_collect_identifiers.csv')
        test_data = []

        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                test_data.append(row)

        ts_url = "http://localhost:9999/sparql"
        prov_config = "test_prov_config.yaml"

        counter_handler = RedisCounterHandler(
            host="localhost",
            port=6381,
            db=0
        )

        request.cls.curator = Curator(
            data=test_data,
            ts=ts_url,
            prov_config=prov_config,
            counter_handler=counter_handler,
            base_iri="https://w3id.org/oc/meta",
            prefix="060"
        )

    def test_collect_identifiers_structure(self):
        """Test that collect_identifiers returns the expected 3-tuple structure."""
        result = self.curator.collect_identifiers()

        assert len(result) == 3, "collect_identifiers should return 3 values"

        metavals, identifiers, vvis = result

        assert isinstance(metavals, set)
        assert isinstance(identifiers, set)
        assert isinstance(vvis, set)

    def test_identifiers_extraction(self):
        """Test that DOI, ISSN, ORCID, and Crossref identifiers are correctly extracted."""
        metavals, identifiers, vvis = self.curator.collect_identifiers()

        expected_dois = [
            'doi:10.17759/chp.2024200411',
            'doi:10.1016/j.molliq.2024.126787'
        ]

        for doi in expected_dois:
            assert doi in identifiers, f"DOI {doi} should be in identifiers"

        expected_orcid = 'orcid:0000-0002-7915-1367'
        assert expected_orcid in identifiers, f"ORCID {expected_orcid} should be in identifiers"

        expected_crossrefs = ['crossref:7555', 'crossref:78', 'crossref:3434']
        for crossref in expected_crossrefs:
            assert crossref in identifiers, f"Crossref {crossref} should be in identifiers"

        # Verify exact VVI values based on test data
        expected_vvis = {
            # Cultural-Historical Psychology volume 20, issue 4
            ('20', '4', '', ('issn:1816-5435', 'issn:2224-8935')),
            # Marmara University volume 30, issue 2
            ('30', '2', '', ('issn:2146-0590',)),
            # Journal of Environmental Chemical Engineering volume 13, issue 1
            ('13', '1', '', ('issn:2213-3437',)),
            # Radiology Case Reports volume 20, issue 3
            ('20', '3', '', ('issn:1930-0433',)),
            # Journal of Atmospheric volume 267, no issue
            ('267', '', '', ('issn:1364-6826',)),
            # Engineering Failure Analysis volume 169, no issue
            ('169', '', '', ('issn:1350-6307',)),
            # Construction and Building Materials volume 458, no issue
            ('458', '', '', ('issn:0950-0618',)),
            # Materials Science volume 188, no issue
            ('188', '', '', ('issn:1369-8001',)),
            # Journal of Molecular Liquids volume 419, no issue
            ('419', '', '', ('issn:0167-7322',))
        }

        assert isinstance(vvis, set), "VVIs should be a set"

        for vvi in vvis:
            assert len(vvi) == 4, f"Each VVI should have 4 elements: {vvi}"
            volume, issue, venue_metaid, venue_ids_tuple = vvi

            assert isinstance(volume, str), f"Volume should be string: {volume}"
            assert isinstance(issue, str), f"Issue should be string: {issue}"
            assert isinstance(venue_metaid, (str, type(None))), f"Venue metaid should be string or None: {venue_metaid}"
            assert isinstance(venue_ids_tuple, tuple), f"Venue IDs should be tuple: {venue_ids_tuple}"

        assert len(vvis) == len(expected_vvis), f"Expected {len(expected_vvis)} VVIs, got {len(vvis)}"

        for expected_vvi in expected_vvis:
            assert expected_vvi in vvis, f"Expected VVI {expected_vvi} should be present in collected VVIs"

        venue_identifiers = [
            'issn:1816-5435', 'issn:2224-8935', 'issn:2146-0590',
            'issn:2213-3437', 'issn:1930-0433', 'issn:1364-6826',
            'issn:1350-6307', 'issn:0950-0618', 'issn:1369-8001', 'issn:0167-7322'
        ]

        for venue_id in venue_identifiers:
            assert venue_id not in identifiers, f"Venue ID {venue_id} should not be in main identifiers"
