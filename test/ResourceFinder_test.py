# SPDX-FileCopyrightText: 2022-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import os

import pytest
from oc_meta.lib.finder import ResourceFinder
from oc_ocdm.graph import GraphEntity
from oc_ocdm.light_graph import RDFTerm
from rdflib import Literal, URIRef, RDF, XSD
from sparqlite import SPARQLClient
from test.test_utils import add_data_ts, reset_triplestore


def reset_server(server: str) -> None:
    reset_triplestore(server)


@pytest.fixture(scope="class")
def finder():
    ENDPOINT = 'http://127.0.0.1:8805?access-token=qlever_test_token'
    BASE_IRI = 'https://w3id.org/oc/meta/'
    REAL_DATA_FILE = os.path.join('test', 'testcases', 'ts', 'real_data.nt')
    finder = ResourceFinder(ENDPOINT, BASE_IRI)
    add_data_ts(server=ENDPOINT, data_path=REAL_DATA_FILE)
    finder.get_everything_about_res(metavals={'omid:br/2373', 'omid:br/2380', 'omid:br/2730', 'omid:br/2374', 'omid:br/4435', 'omid:br/4436', 'omid:br/4437', 'omid:br/4438', 'omid:br/0604750', 'omid:br/0605379', 'omid:br/0606696'}, identifiers={'doi:10.1001/.391', 'orcid:0000-0001-6994-8412'}, vvis=set())
    return finder


class TestResourceFinder:
    def test_retrieve_br_from_id(self, finder):
        value = '10.1001/.391'
        schema = 'doi'
        output = finder.retrieve_br_from_id(schema, value)
        expected_output = [(
            'br/2373',
            'Treatment Of Excessive Anticoagulation With Phytonadione (Vitamin K): A Meta-analysis',
            [('id/2239', 'doi:10.1001/.391')]
        )]
        assert output == expected_output

    def test_retrieve_br_from_id_multiple_ids(self, finder):
        value = '10.1001/.405'
        schema = 'doi'
        output = finder.retrieve_br_from_id(schema, value)
        expected_output = [(
            'br/2374',
            "Neutropenia In Human Immunodeficiency Virus Infection: Data From The Women's Interagency HIV Study",
            [('id/2240', 'doi:10.1001/.405'), ('id/5000', 'doi:10.1001/.406')]
        )]
        assert output == expected_output

    def test_retrieve_br_from_meta(self, finder):
        metaid = 'br/2373'
        output = finder.retrieve_br_from_meta(metaid)
        expected_output = ('Treatment Of Excessive Anticoagulation With Phytonadione (Vitamin K): A Meta-analysis', [('id/2239', 'doi:10.1001/.391')], True)
        assert output == expected_output

    def test_retrieve_br_from_meta_multiple_ids(self, finder):
        metaid = 'br/2374'
        output = finder.retrieve_br_from_meta(metaid)
        output = (output[0], set(output[1]))
        expected_output = ("Neutropenia In Human Immunodeficiency Virus Infection: Data From The Women's Interagency HIV Study", {('id/2240', 'doi:10.1001/.405'), ('id/5000', 'doi:10.1001/.406')})
        assert output == expected_output

    def test_retrieve_metaid_from_id(self, finder):
        schema = 'doi'
        value = '10.1001/.391'
        output = finder.retrieve_metaid_from_id(schema, value)
        expected_output = 'id/2239'
        assert output == expected_output

    def test_retrieve_ra_from_meta(self, finder):
        metaid = 'ra/3308'
        output = finder.retrieve_ra_from_meta(metaid)
        expected_output = ('Dezee, K. J.', [], True)
        assert output == expected_output

    def test_retrieve_ra_from_meta_with_orcid(self, finder):
        metaid = 'ra/4940'
        output = finder.retrieve_ra_from_meta(metaid)
        expected_output = ('Alarcon, Louis H.', [('id/4475', 'orcid:0000-0001-6994-8412')], True)
        assert output == expected_output

    def test_retrieve_ra_from_meta_if_publisher(self, finder):
        metaid = 'ra/3309'
        output = finder.retrieve_ra_from_meta(metaid)
        expected_output = ('American Medical Association (ama)', [('id/4274', 'crossref:10')], True)
        assert output == expected_output

    def test_retrieve_ra_from_id(self, finder):
        schema = 'orcid'
        value = '0000-0001-6994-8412'
        output = finder.retrieve_ra_from_id(schema, value)
        expected_output = [
            ('ra/1000000', 'Alarcon, Louis H.', [('id/4475', 'orcid:0000-0001-6994-8412')]),
            ('ra/4940', 'Alarcon, Louis H.', [('id/4475', 'orcid:0000-0001-6994-8412')])
        ]
        assert sorted(output) == expected_output

    def test_retrieve_ra_from_id_if_publisher(self, finder):
        schema = 'crossref'
        value = '10'
        output = finder.retrieve_ra_from_id(schema, value)
        expected_output = [('ra/3309', 'American Medical Association (ama)', [('id/4274', 'crossref:10')])]
        assert output == expected_output

    def test_retrieve_ra_sequence_from_br_meta(self, finder):
        metaid = 'br/2380'
        output = finder.retrieve_ra_sequence_from_br_meta(metaid, 'author')
        expected_output = [
            {'ar/5343': ('Hodge, James G.', [], 'ra/3316')},
            {'ar/5344': ('Anderson, Evan D.', [], 'ra/3317')},
            {'ar/5345': ('Kirsch, Thomas D.', [], 'ra/3318')},
            {'ar/5346': ('Kelen, Gabor D.', [('id/4278', 'orcid:0000-0002-3236-8286')], 'ra/3319')}
        ]
        assert output == expected_output

    def test_retrieve_re_from_br_meta(self, finder):
        metaid = 'br/2373'
        output = finder.retrieve_re_from_br_meta(metaid)
        expected_output = ('re/2011', '391-397')
        assert output == expected_output

    def test_retrieve_br_info_from_meta(self, finder):
        metaid = 'br/2373'
        output = finder.retrieve_br_info_from_meta(metaid)
        expected_output = {
            'pub_date': '2006-02-27',
            'type': 'journal article',
            'page': ('re/2011', '391-397'),
            'issue': '4',
            'volume': '166',
            'venue': 'Archives Of Internal Medicine [omid:br/4387 issn:0003-9926]'
        }
        assert output == expected_output

    def test_retrieve_ra_sequence_with_loop(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta handles circular references without infinite loops.

        In a circular loop, there's no natural start, so the algorithm picks the AR
        with the lowest number as the starting point.
        """
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9999')
        # Use AR numbers where 9992 < 9991 to verify sorting works correctly
        ar1_uri = URIRef(f'{base_iri}/ar/9992')
        ar2_uri = URIRef(f'{base_iri}/ar/9991')
        ra1_uri = URIRef(f'{base_iri}/ra/9981')
        ra2_uri = URIRef(f'{base_iri}/ra/9982')

        # Create a circular AR chain: AR1(9992) -> AR2(9991) -> AR1(9992) (loop)
        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_has_next, ar2_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_given_name, Literal('John'))
        finder.add_triple(ra1_uri, GraphEntity.iri_family_name, Literal('Doe'))

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar2_uri)
        finder.add_triple(ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar2_uri, GraphEntity.iri_is_held_by, ra2_uri)
        finder.add_triple(ar2_uri, GraphEntity.iri_has_next, ar1_uri)
        finder.add_triple(ra2_uri, GraphEntity.iri_given_name, Literal('Jane'))
        finder.add_triple(ra2_uri, GraphEntity.iri_family_name, Literal('Smith'))

        result = finder.retrieve_ra_sequence_from_br_meta('br/9999', 'author')

        assert len(result) == 2
        # The AR with lowest number (9991) should be first
        assert list(result[0].keys())[0] == 'ar/9991'
        assert list(result[1].keys())[0] == 'ar/9992'

    def test_retrieve_ra_sequence_with_self_reference(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta handles self-referencing AR"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9998')
        ar1_uri = URIRef(f'{base_iri}/ar/9981')
        ra1_uri = URIRef(f'{base_iri}/ra/9971')

        # Create AR that points to itself
        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_has_next, ar1_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_name, Literal('Test Publisher'))

        # This should return only 1 AR (ignoring self-reference)
        result = finder.retrieve_ra_sequence_from_br_meta('br/9998', 'author')

        # Should return exactly 1 AR
        assert len(result) == 1
        assert list(result[0].keys())[0] == 'ar/9981'

    def test_retrieve_ra_sequence_with_invalid_next(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta handles invalid 'next' references"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9997')
        ar1_uri = URIRef(f'{base_iri}/ar/9971')
        ar2_uri = URIRef(f'{base_iri}/ar/9972')
        ar_invalid_uri = URIRef(f'{base_iri}/ar/9999')
        ra1_uri = URIRef(f'{base_iri}/ra/9961')
        ra2_uri = URIRef(f'{base_iri}/ra/9962')

        # Create AR chain where AR1 -> AR_INVALID (doesn't exist) and AR2 is orphaned
        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_has_next, ar_invalid_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_name, Literal('Author One'))

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar2_uri)
        finder.add_triple(ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar2_uri, GraphEntity.iri_is_held_by, ra2_uri)
        finder.add_triple(ra2_uri, GraphEntity.iri_name, Literal('Author Two'))

        # Should return chain stopping at invalid reference
        result = finder.retrieve_ra_sequence_from_br_meta('br/9997', 'author')

        # Should return at least AR1 (stops at invalid next)
        # The method will find 2 start candidates and pick the longest chain
        assert len(result) >= 1
        ar_ids = [list(item.keys())[0] for item in result]
        assert 'ar/9971' in ar_ids

    def test_retrieve_ra_sequence_with_missing_is_held_by(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta handles AR without is_held_by gracefully"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9996')
        ar1_uri = URIRef(f'{base_iri}/ar/9961')

        # Create AR without is_held_by relationship (malformed data)
        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        # Missing: ar1_uri iri_is_held_by ra_uri

        # Should handle gracefully without crash
        try:
            result = finder.retrieve_ra_sequence_from_br_meta('br/9996', 'author')
            # If it doesn't crash, check result is reasonable (either empty or handles error)
            assert isinstance(result, list)
        except (KeyError, UnboundLocalError) as e:
            pytest.fail(f"Method crashed with missing is_held_by: {e}")

    def test_retrieve_ra_sequence_with_multiple_next_values(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta handles AR with multiple 'next' relationships"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9995')
        ar1_uri = URIRef(f'{base_iri}/ar/9951')
        ar2_uri = URIRef(f'{base_iri}/ar/9952')
        ar3_uri = URIRef(f'{base_iri}/ar/9953')
        ra1_uri = URIRef(f'{base_iri}/ra/9941')
        ra2_uri = URIRef(f'{base_iri}/ra/9942')
        ra3_uri = URIRef(f'{base_iri}/ra/9943')

        # Create AR1 with multiple 'next' relationships (data error)
        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_has_next, ar2_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_has_next, ar3_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_name, Literal('Author One'))

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar2_uri)
        finder.add_triple(ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar2_uri, GraphEntity.iri_is_held_by, ra2_uri)
        finder.add_triple(ra2_uri, GraphEntity.iri_name, Literal('Author Two'))

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar3_uri)
        finder.add_triple(ar3_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar3_uri, GraphEntity.iri_is_held_by, ra3_uri)
        finder.add_triple(ra3_uri, GraphEntity.iri_name, Literal('Author Three'))

        # Should handle multiple next values consistently (last one wins in current implementation)
        result = finder.retrieve_ra_sequence_from_br_meta('br/9995', 'author')

        # Should return a valid result without crashing
        assert isinstance(result, list)
        assert len(result) > 0

    def test_retrieve_ra_sequence_no_ars_for_role(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta returns empty list when no ARs exist for specified role"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9994')
        ar1_uri = URIRef(f'{base_iri}/ar/9941')
        ra1_uri = URIRef(f'{base_iri}/ra/9931')

        # Create BR with editor, but request author
        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_editor)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_name, Literal('Editor Name'))

        # Request author (should be empty)
        result = finder.retrieve_ra_sequence_from_br_meta('br/9994', 'author')

        assert result == []

    def test_retrieve_ra_sequence_single_ar_no_chain(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta handles single AR without 'next'"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9993')
        ar1_uri = URIRef(f'{base_iri}/ar/9931')
        ra1_uri = URIRef(f'{base_iri}/ra/9921')

        # Create single AR without next
        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_name, Literal('Single Author'))

        result = finder.retrieve_ra_sequence_from_br_meta('br/9993', 'author')

        assert len(result) == 1
        assert list(result[0].keys())[0] == 'ar/9931'

    def test_retrieve_ra_sequence_two_independent_chains(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta picks longest chain when multiple disconnected chains exist"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9992')

        # Chain 1: AR1 -> AR2 (length 2)
        ar1_uri = URIRef(f'{base_iri}/ar/9921')
        ar2_uri = URIRef(f'{base_iri}/ar/9922')
        ra1_uri = URIRef(f'{base_iri}/ra/9911')
        ra2_uri = URIRef(f'{base_iri}/ra/9912')

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_has_next, ar2_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_name, Literal('Author One'))

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar2_uri)
        finder.add_triple(ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar2_uri, GraphEntity.iri_is_held_by, ra2_uri)
        finder.add_triple(ra2_uri, GraphEntity.iri_name, Literal('Author Two'))

        # Chain 2: AR3 (length 1, disconnected)
        ar3_uri = URIRef(f'{base_iri}/ar/9923')
        ra3_uri = URIRef(f'{base_iri}/ra/9913')

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar3_uri)
        finder.add_triple(ar3_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar3_uri, GraphEntity.iri_is_held_by, ra3_uri)
        finder.add_triple(ra3_uri, GraphEntity.iri_name, Literal('Author Three'))

        result = finder.retrieve_ra_sequence_from_br_meta('br/9992', 'author')

        # Should return the longer chain (chain 1 with 2 elements)
        assert len(result) == 2
        ar_ids = [list(item.keys())[0] for item in result]
        assert 'ar/9921' in ar_ids
        assert 'ar/9922' in ar_ids

    def test_retrieve_ra_sequence_equal_length_chains_tiebreaker(self, finder):
        """Test that when multiple chains have equal length, the one starting with lowest AR number wins."""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9986')

        # Chain 1: AR 9935 -> AR 9936 (length 2, starts with higher number)
        ar1_uri = URIRef(f'{base_iri}/ar/9935')
        ar2_uri = URIRef(f'{base_iri}/ar/9936')
        ra1_uri = URIRef(f'{base_iri}/ra/9925')
        ra2_uri = URIRef(f'{base_iri}/ra/9926')

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_has_next, ar2_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_name, Literal('Author A'))

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar2_uri)
        finder.add_triple(ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar2_uri, GraphEntity.iri_is_held_by, ra2_uri)
        finder.add_triple(ra2_uri, GraphEntity.iri_name, Literal('Author B'))

        # Chain 2: AR 9933 -> AR 9934 (length 2, starts with lower number)
        ar3_uri = URIRef(f'{base_iri}/ar/9933')
        ar4_uri = URIRef(f'{base_iri}/ar/9934')
        ra3_uri = URIRef(f'{base_iri}/ra/9923')
        ra4_uri = URIRef(f'{base_iri}/ra/9924')

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar3_uri)
        finder.add_triple(ar3_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar3_uri, GraphEntity.iri_is_held_by, ra3_uri)
        finder.add_triple(ar3_uri, GraphEntity.iri_has_next, ar4_uri)
        finder.add_triple(ra3_uri, GraphEntity.iri_name, Literal('Author C'))

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar4_uri)
        finder.add_triple(ar4_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar4_uri, GraphEntity.iri_is_held_by, ra4_uri)
        finder.add_triple(ra4_uri, GraphEntity.iri_name, Literal('Author D'))

        result = finder.retrieve_ra_sequence_from_br_meta('br/9986', 'author')

        # Both chains have length 2, so the one starting with lowest AR number (9933) wins
        assert len(result) == 2
        assert list(result[0].keys())[0] == 'ar/9933'
        assert list(result[1].keys())[0] == 'ar/9934'

    def test_retrieve_ra_sequence_editor_role(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta works with editor role"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9991')
        ar1_uri = URIRef(f'{base_iri}/ar/9911')
        ra1_uri = URIRef(f'{base_iri}/ra/9901')

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_editor)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_name, Literal('Editor Name'))

        result = finder.retrieve_ra_sequence_from_br_meta('br/9991', 'editor')

        assert len(result) == 1
        assert list(result[0].keys())[0] == 'ar/9911'

    def test_retrieve_ra_sequence_publisher_role(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta works with publisher role"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9990')
        ar1_uri = URIRef(f'{base_iri}/ar/9901')
        ra1_uri = URIRef(f'{base_iri}/ra/9891')

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_publisher)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_name, Literal('Publisher Name'))

        result = finder.retrieve_ra_sequence_from_br_meta('br/9990', 'publisher')

        assert len(result) == 1
        assert list(result[0].keys())[0] == 'ar/9901'

    def test_retrieve_ra_sequence_three_node_loop(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta handles three-node circular loop"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9989')
        ar1_uri = URIRef(f'{base_iri}/ar/9891')
        ar2_uri = URIRef(f'{base_iri}/ar/9892')
        ar3_uri = URIRef(f'{base_iri}/ar/9893')
        ra1_uri = URIRef(f'{base_iri}/ra/9881')
        ra2_uri = URIRef(f'{base_iri}/ra/9882')
        ra3_uri = URIRef(f'{base_iri}/ra/9883')

        # Create circular loop: AR1 -> AR2 -> AR3 -> AR1
        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_has_next, ar2_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_name, Literal('Author One'))

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar2_uri)
        finder.add_triple(ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar2_uri, GraphEntity.iri_is_held_by, ra2_uri)
        finder.add_triple(ar2_uri, GraphEntity.iri_has_next, ar3_uri)
        finder.add_triple(ra2_uri, GraphEntity.iri_name, Literal('Author Two'))

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar3_uri)
        finder.add_triple(ar3_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar3_uri, GraphEntity.iri_is_held_by, ra3_uri)
        finder.add_triple(ar3_uri, GraphEntity.iri_has_next, ar1_uri)
        finder.add_triple(ra3_uri, GraphEntity.iri_name, Literal('Author Three'))

        result = finder.retrieve_ra_sequence_from_br_meta('br/9989', 'author')

        # Should return exactly 3 ARs (breaking loop)
        assert len(result) == 3
        ar_ids = [list(item.keys())[0] for item in result]
        assert 'ar/9891' in ar_ids
        assert 'ar/9892' in ar_ids
        assert 'ar/9893' in ar_ids

    def test_retrieve_ra_sequence_duplicate_ra(self, finder):
        """Test that retrieve_ra_sequence_from_br_meta returns both ARs when they point to same RA"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9988')
        ar1_uri = URIRef(f'{base_iri}/ar/9881')
        ar2_uri = URIRef(f'{base_iri}/ar/9882')
        ra1_uri = URIRef(f'{base_iri}/ra/9871')

        # Two ARs pointing to same RA (duplicate author)
        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar1_uri, GraphEntity.iri_is_held_by, ra1_uri)
        finder.add_triple(ar1_uri, GraphEntity.iri_has_next, ar2_uri)
        finder.add_triple(ra1_uri, GraphEntity.iri_name, Literal('Same Author'))

        finder.add_triple(br_uri, GraphEntity.iri_is_document_context_for, ar2_uri)
        finder.add_triple(ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author)
        finder.add_triple(ar2_uri, GraphEntity.iri_is_held_by, ra1_uri)

        result = finder.retrieve_ra_sequence_from_br_meta('br/9988', 'author')

        # Should return both ARs even though they reference same RA
        assert len(result) == 2
        # Both should reference RA ra/9871
        assert result[0][list(result[0].keys())[0]][2] == 'ra/9871'
        assert result[1][list(result[1].keys())[0]][2] == 'ra/9871'

    def test_retrieve_venue_from_local_graph(self, finder):
        """Test that retrieve_venue_from_local_graph correctly uses metaID with prefix."""
        base_iri = 'https://w3id.org/oc/meta'
        # Create a venue with a volume and an issue
        venue_uri = URIRef(f'{base_iri}/br/8001')
        volume_uri = URIRef(f'{base_iri}/br/8002')
        issue_uri = URIRef(f'{base_iri}/br/8003')

        # Venue
        finder.add_triple(venue_uri, GraphEntity.iri_has_sequence_identifier, Literal('Journal Test', datatype=XSD.string))

        # Volume 5 of the venue
        finder.add_triple(volume_uri, RDF.type, GraphEntity.iri_journal_volume)
        finder.add_triple(volume_uri, GraphEntity.iri_part_of, venue_uri)
        finder.add_triple(volume_uri, GraphEntity.iri_has_sequence_identifier, Literal('5', datatype=XSD.string))

        # Issue 3 of volume 5
        finder.add_triple(issue_uri, RDF.type, GraphEntity.iri_journal_issue)
        finder.add_triple(issue_uri, GraphEntity.iri_part_of, volume_uri)
        finder.add_triple(issue_uri, GraphEntity.iri_has_sequence_identifier, Literal('3', datatype=XSD.string))

        # Call with prefixed metaID
        result = finder.retrieve_venue_from_local_graph('br/8001')

        # Verify volume was found with correct prefixed ID
        assert '5' in result['volume']
        assert result['volume']['5']['id'] == 'br/8002'

        # Verify issue was found under the volume with correct prefixed ID
        assert '3' in result['volume']['5']['issue']
        assert result['volume']['5']['issue']['3']['id'] == 'br/8003'


@pytest.fixture(scope="class")
def vvi_endpoint():
    """Set up test data for VVI query isolation tests."""
    ENDPOINT = 'http://127.0.0.1:8805?access-token=qlever_test_token'
    reset_server(server=ENDPOINT)

    # Upload test data: two venues with different ISSNs, each with their own volume
    test_triples = [
        # Venue A (br/9001) with ISSN 1111-1111
        '<https://w3id.org/oc/meta/br/9001> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Journal> .',
        '<https://w3id.org/oc/meta/br/9001> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/9001> .',
        '<https://w3id.org/oc/meta/id/9001> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier> .',
        '<https://w3id.org/oc/meta/id/9001> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .',
        '<https://w3id.org/oc/meta/id/9001> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "1111-1111"^^<http://www.w3.org/2001/XMLSchema#string> .',
        # Volume 10 of Venue A
        '<https://w3id.org/oc/meta/br/9002> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalVolume> .',
        '<https://w3id.org/oc/meta/br/9002> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/9001> .',
        '<https://w3id.org/oc/meta/br/9002> <http://purl.org/spar/fabio/hasSequenceIdentifier> "10"^^<http://www.w3.org/2001/XMLSchema#string> .',
        # Venue B (br/9003) with ISSN 2222-2222
        '<https://w3id.org/oc/meta/br/9003> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Journal> .',
        '<https://w3id.org/oc/meta/br/9003> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/9002> .',
        '<https://w3id.org/oc/meta/id/9002> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier> .',
        '<https://w3id.org/oc/meta/id/9002> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .',
        '<https://w3id.org/oc/meta/id/9002> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "2222-2222"^^<http://www.w3.org/2001/XMLSchema#string> .',
        # Volume 20 of Venue B
        '<https://w3id.org/oc/meta/br/9004> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalVolume> .',
        '<https://w3id.org/oc/meta/br/9004> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/9003> .',
        '<https://w3id.org/oc/meta/br/9004> <http://purl.org/spar/fabio/hasSequenceIdentifier> "20"^^<http://www.w3.org/2001/XMLSchema#string> .',
    ]

    with SPARQLClient(ENDPOINT, timeout=60) as client:
        for triple in test_triples:
            query = f"INSERT DATA {{ GRAPH <https://w3id.org/oc/meta/br/> {{ {triple} }} }}"
            client.update(query)

    return ENDPOINT


class TestVVIQueryIsolation:
    """Test that VVI queries only search under the correct venues."""

    def test_vvi_queries_only_search_correct_venues(self, vvi_endpoint):
        """Test that VVI queries only search under venues matching each tuple's identifiers.

        This test verifies the fix for the bug where VVI queries were incorrectly
        searching under ALL venues instead of just the venues matching each VVI tuple.
        With the bug, searching for volume "10" under venue with ISSN 2222-2222 would
        also incorrectly search under venue with ISSN 1111-1111.
        """
        BASE_IRI = 'https://w3id.org/oc/meta/'
        settings = {'virtuoso_full_text_search': False}
        finder = ResourceFinder(vvi_endpoint, BASE_IRI, settings=settings)

        # VVI tuples: each should only search under its corresponding venue
        vvis = {
            ("10", "", None, ("issn:1111-1111",)),  # Volume 10 of Venue A
            ("20", "", None, ("issn:2222-2222",)),  # Volume 20 of Venue B
        }

        finder.get_everything_about_res(metavals=set(), identifiers=set(), vvis=vvis)

        # Verify both volumes were found
        volume_10_uri = URIRef('https://w3id.org/oc/meta/br/9002')
        volume_20_uri = URIRef('https://w3id.org/oc/meta/br/9004')
        venue_a_uri = URIRef('https://w3id.org/oc/meta/br/9001')
        venue_b_uri = URIRef('https://w3id.org/oc/meta/br/9003')

        # Check that volume 10 is in local graph and is part of venue A (not venue B)
        assert str(volume_10_uri) in finder._spo
        volume_10_parents = finder._get_objects(str(volume_10_uri), str(GraphEntity.iri_part_of))
        assert str(venue_a_uri) in volume_10_parents, \
            "Volume 10 should be part of Venue A"

        # Check that volume 20 is in local graph and is part of venue B (not venue A)
        assert str(volume_20_uri) in finder._spo
        volume_20_parents = finder._get_objects(str(volume_20_uri), str(GraphEntity.iri_part_of))
        assert str(venue_b_uri) in volume_20_parents, \
            "Volume 20 should be part of Venue B"


FINDER_BASE_IRI = "https://w3id.org/oc/meta/"
FINDER_SERVER = 'http://127.0.0.1:8805?access-token=qlever_test_token'


class TestFinderConstructFullName:
    def test_only_given_name(self):
        finder = ResourceFinder(FINDER_SERVER, FINDER_BASE_IRI)
        result = finder._construct_full_name("", "", "John")
        assert result == ", John"


class TestFinderRetrieveReFromBrMeta:
    @pytest.fixture
    def finder_with_re_data(self):
        base_iri = "https://w3id.org/oc/meta"
        finder = ResourceFinder(FINDER_SERVER, base_iri + "/")

        br1_uri = URIRef(f"{base_iri}/br/test1")
        re1_uri = URIRef(f"{base_iri}/re/test1")
        finder.add_triple(br1_uri, GraphEntity.iri_embodiment, re1_uri)
        finder.add_triple(re1_uri, GraphEntity.iri_starting_page, Literal("100"))

        br2_uri = URIRef(f"{base_iri}/br/test2")
        re2_uri = URIRef(f"{base_iri}/re/test2")
        finder.add_triple(br2_uri, GraphEntity.iri_embodiment, re2_uri)
        finder.add_triple(re2_uri, GraphEntity.iri_ending_page, Literal("200"))

        return finder

    def test_only_starting_page(self, finder_with_re_data):
        result = finder_with_re_data.retrieve_re_from_br_meta("br/test1")
        assert result is not None
        assert result[0] == "re/test1"
        assert result[1] == "100-100"

    def test_only_ending_page(self, finder_with_re_data):
        result = finder_with_re_data.retrieve_re_from_br_meta("br/test2")
        assert result is not None
        assert result[0] == "re/test2"
        assert result[1] == "200-200"


class TestFinderGetSubgraph:
    def test_get_subgraph_not_found(self):
        finder = ResourceFinder(FINDER_SERVER, FINDER_BASE_IRI)
        result = finder.get_subgraph(f"{FINDER_BASE_IRI}br/nonexistent")
        assert result is None

    def test_get_subgraph_reconstructs_uris(self):
        finder = ResourceFinder(FINDER_SERVER, FINDER_BASE_IRI)
        br = f"{FINDER_BASE_IRI}br/0601"
        id_uri = f"{FINDER_BASE_IRI}id/0601"
        finder._add_triple(br, str(RDF.type), str(GraphEntity.iri_expression))
        finder._add_triple(br, str(GraphEntity.iri_has_identifier), id_uri)
        g = finder.get_subgraph(br)
        assert g is not None
        triples = set(g)
        assert (br, str(RDF.type), RDFTerm("uri", str(GraphEntity.iri_expression))) in triples
        assert (br, str(GraphEntity.iri_has_identifier), RDFTerm("uri", id_uri)) in triples
        assert len(triples) == 2

    def test_get_subgraph_reconstructs_typed_literals(self):
        finder = ResourceFinder(FINDER_SERVER, FINDER_BASE_IRI)
        br = f"{FINDER_BASE_IRI}br/0602"
        title = "A Test Title"
        date = "2025-01-15"
        finder._add_triple(br, str(GraphEntity.iri_title), title, o_datatype=str(XSD.string))
        finder._add_triple(br, str(GraphEntity.iri_has_publication_date), date, o_datatype=str(XSD.date))
        g = finder.get_subgraph(br)
        assert g is not None
        triples = set(g)
        assert (br, str(GraphEntity.iri_title), RDFTerm("literal", title, str(XSD.string))) in triples
        assert (br, str(GraphEntity.iri_has_publication_date), RDFTerm("literal", date, str(XSD.date))) in triples
        assert len(triples) == 2

    def test_get_subgraph_reconstructs_plain_literals(self):
        finder = ResourceFinder(FINDER_SERVER, FINDER_BASE_IRI)
        br = f"{FINDER_BASE_IRI}br/0603"
        value = "some plain value"
        finder._add_triple(br, str(GraphEntity.iri_title), value)
        g = finder.get_subgraph(br)
        assert g is not None
        triples = set(g)
        assert (br, str(GraphEntity.iri_title), RDFTerm("literal", value, str(XSD.string))) in triples

    def test_get_subgraph_accepts_uriref(self):
        finder = ResourceFinder(FINDER_SERVER, FINDER_BASE_IRI)
        br = f"{FINDER_BASE_IRI}br/0604"
        finder._add_triple(br, str(RDF.type), str(GraphEntity.iri_expression))
        assert finder.get_subgraph(URIRef(br)) is not None
        assert finder.get_subgraph(br) is not None


class TestFinderRetrieveVenueFromLocalGraphIssueDirectlyInVenue:
    def test_issue_directly_in_venue(self):
        base_iri = "https://w3id.org/oc/meta"
        finder = ResourceFinder(FINDER_SERVER, base_iri + "/")

        venue_uri = URIRef(f"{base_iri}/br/venue1")
        issue_uri = URIRef(f"{base_iri}/br/issue1")

        finder.add_triple(issue_uri, RDF.type, GraphEntity.iri_journal_issue)
        finder.add_triple(issue_uri, GraphEntity.iri_part_of, venue_uri)
        finder.add_triple(issue_uri, GraphEntity.iri_has_sequence_identifier, Literal("5", datatype=XSD.string))

        result = finder.retrieve_venue_from_local_graph("br/venue1")

        assert "5" in result["issue"]
        assert result["issue"]["5"]["id"] == "br/issue1"


class TestFinderRetrievePublisherDeepNesting:
    @pytest.fixture
    def finder_with_publisher_data(self):
        base_iri = "https://w3id.org/oc/meta"
        finder = ResourceFinder(FINDER_SERVER, base_iri + "/")

        br_uri = URIRef(f"{base_iri}/br/deep1")
        issue_uri = URIRef(f"{base_iri}/br/issue1")
        vol_uri = URIRef(f"{base_iri}/br/vol1")
        ar_uri = URIRef(f"{base_iri}/ar/pub1")
        ra_uri = URIRef(f"{base_iri}/ra/publisher1")

        finder.add_triple(br_uri, GraphEntity.iri_part_of, issue_uri)
        finder.add_triple(issue_uri, GraphEntity.iri_part_of, vol_uri)
        finder.add_triple(vol_uri, GraphEntity.iri_is_document_context_for, ar_uri)
        finder.add_triple(ar_uri, GraphEntity.iri_with_role, GraphEntity.iri_publisher)
        finder.add_triple(ar_uri, GraphEntity.iri_is_held_by, ra_uri)
        finder.add_triple(ra_uri, GraphEntity.iri_name, Literal("Deep Publisher"))

        return finder

    def test_retrieve_publisher_two_levels_deep(self, finder_with_publisher_data):
        result = finder_with_publisher_data.retrieve_publisher_from_br_metaid("br/deep1")
        assert "Deep Publisher" in result


class TestFinderRetrieveRaFromIdMultipleIds:
    @pytest.fixture
    def finder_with_ra_multiple_ids(self):
        base_iri = "https://w3id.org/oc/meta"
        finder = ResourceFinder(FINDER_SERVER, base_iri + "/")

        ra_uri = URIRef(f"{base_iri}/ra/multi1")
        id1_uri = URIRef(f"{base_iri}/id/id1")
        id2_uri = URIRef(f"{base_iri}/id/id2")

        finder.add_triple(ra_uri, GraphEntity.iri_has_identifier, id1_uri)
        finder.add_triple(id1_uri, GraphEntity.iri_uses_identifier_scheme, URIRef(GraphEntity.DATACITE + "orcid"))
        finder.add_triple(id1_uri, GraphEntity.iri_has_literal_value, Literal("0000-0001-1234-5678", datatype=XSD.string))

        finder.add_triple(ra_uri, GraphEntity.iri_has_identifier, id2_uri)
        finder.add_triple(id2_uri, GraphEntity.iri_uses_identifier_scheme, URIRef(GraphEntity.DATACITE + "viaf"))
        finder.add_triple(id2_uri, GraphEntity.iri_has_literal_value, Literal("12345", datatype=XSD.string))

        finder.add_triple(ra_uri, GraphEntity.iri_name, Literal("Multi ID Author"))

        return finder

    def test_retrieve_ra_returns_all_ids(self, finder_with_ra_multiple_ids):
        result = finder_with_ra_multiple_ids.retrieve_ra_from_id("orcid", "0000-0001-1234-5678")
        assert len(result) == 1
        assert result[0][1] == "Multi ID Author"
        id_list = result[0][2]
        id_literals = [id_tuple[1] for id_tuple in id_list]
        assert "orcid:0000-0001-1234-5678" in id_literals
        assert "viaf:12345" in id_literals
