#!python
# Copyright 2022, Arcangelo Massari <arcangelo.massari@unibo.it>, Arianna Moretti <arianna.moretti4@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import unittest

from oc_idmanager import DOIManager

from oc_meta.lib.file_manager import call_api
from oc_meta.plugins.medra.medra_processing import MedraProcessing

doi_manager = DOIManager()
MEDRA_API = doi_manager._api_medra
HEADERS = doi_manager._headers


class MedraProcessingTest(unittest.TestCase):
    def test_extract_from_medra_article(self):
        url = f'{MEDRA_API}10.3233/DS-210053'
        item = call_api(url, HEADERS, 'xml')
        medra_processing = MedraProcessing()
        output = medra_processing.csv_creator(item)
        expected_output = {'id': 'doi:10.3233/ds-210053', 'title': 'Packaging research artefacts with RO-Crate', 
            'author': '; '.join(['Soiland-Reyes, Stian [orcid:0000-0001-9842-9718]', 'Sefton, Peter [orcid:0000-0002-3545-944X]', 'Crosas, Mercè [orcid:0000-0003-1304-1939]', 'Castro, Leyla Jael [orcid:0000-0003-3986-0510]', 'Coppens, Frederik [orcid:0000-0001-6565-5145]', 'Fernández, José M. [orcid:0000-0002-4806-5140]', 'Garijo, Daniel [orcid:0000-0003-0454-7145]', 'Grüning, Björn [orcid:0000-0002-3079-6586]', 'La Rosa, Marco [orcid:0000-0001-5383-6993]', 'Leo, Simone [orcid:0000-0001-8271-5429]', 'Ó Carragáin, Eoghan [orcid:0000-0001-8131-2150]', 'Portier, Marc [orcid:0000-0002-9648-6484]', 'Trisovic, Ana [orcid:0000-0003-1991-0533]', 'RO-Crate Community', 'Groth, Paul [orcid:0000-0003-0183-6910]', 'Goble, Carole [orcid:0000-0003-1219-2137]']), 
            'issue': '2', 'volume': '5', 'venue': 'Data Science [issn:2451-8492 issn:2451-8484]', 'pub_date': '2022-07-20', 'pages': '97-138', 'type': 'journal article', 
            'publisher': 'IOS Press', 'editor': 'Peroni, Silvio [orcid:0000-0003-0530-4305]'}
        self.assertEqual(output, expected_output)

    def test_extract_from_medra_book(self):
        url = f'{MEDRA_API}10.23775/20221026'
        item = call_api(url, HEADERS, 'xml')
        medra_processing = MedraProcessing()
        output = medra_processing.csv_creator(item)
        expected_output = {'id': 'doi:10.23775/20221026', 'title': 'Book of Abstract: 2nd International PEROSH conference on Prolonging Working Life', 
            'author': '; '.join(['PEROSH member institutes']), 'issue': '', 'volume': '', 'venue': '', 'pub_date': '2022-09', 
            'pages': '', 'type': 'book', 'publisher': 'PEROSH', 'editor': ''}
        self.assertEqual(output, expected_output)

    def test_extract_from_medra_series(self):
        url = f'{MEDRA_API}10.17426/58141'
        item = call_api(url, HEADERS, 'xml')
        medra_processing = MedraProcessing()
        output = medra_processing.csv_creator(item)
        expected_output = {
            'id': 'doi:10.17426/58141', 'title': 'L’Aquila oltre i terremoti. Costruzioni e ricostruzioni della città a cura di Simonetta Ciranna e Manuel Vaquero Piñeiro', 
            'author': '', 'issue': '', 'volume': '', 'venue': '', 'pub_date': '2011-07-08', 'pages': '', 
            'type': 'series', 'publisher': 'CROMA - UNIVERSITÀ ROMA TRE', 'editor': ''}
        self.assertEqual(output, expected_output)

    def test_extract_from_medra_book_chapter(self):
        url = f'{MEDRA_API}10.3278/6004498w013'
        item = call_api(url, HEADERS, 'xml')
        medra_processing = MedraProcessing()
        output = medra_processing.csv_creator(item)
        expected_output = {
            'id': 'doi:10.3278/6004498w013', 'title': 'Kapitel 13: Einkommen und Vermögen', 
            'author': 'Becker, Irene', 'issue': '', 'volume': '', 'venue': '', 'pub_date': '2016-10-17', 
            'pages': '', 'type': 'book chapter', 'publisher': 'wbv Media', 'editor': 'Forschungsverbund Sozioökonomische Berichterstattung'}
        self.assertEqual(output, expected_output)