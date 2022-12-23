#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import os
import unittest

from oc_meta.lib.file_manager import get_csv_data
from oc_meta.plugins.csv_generator.csv_generator import generate_csv

BASE = os.path.join('test', 'csv_generator')
RDF = os.path.join(BASE, 'rdf')
OUTPUT = os.path.join(BASE, 'csv')

class TestCSVGenerator(unittest.TestCase):
    def test_generate_csv(self):
        generate_csv(RDF, 10000, 1000, OUTPUT, 3000)
        csv_data = get_csv_data(os.path.join(OUTPUT, '0.csv'))
        expected_csv_data = [
            {'id': 'meta:br/0602 issn:1225-4339', 'title': 'The Korean Journal Of Food And Nutrition', 'author': '', 'issue': '', 'volume': '', 'venue': '', 'page': '', 'pub_date': '', 'type': 'journal', 'publisher': '', 'editor': ''}, 
            {'id': 'meta:br/0601 doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization And Shelf-life Extension Of Seafood Products By Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [meta:ra/0601 orcid:0000-0003-2542-5788]; Mun, Ji-Hye [meta:ra/0602]; Chung, Myong-Soo [meta:ra/0603]', 'issue': '1', 'volume': '25', 'venue': 'The Korean Journal Of Food And Nutrition [issn:1225-4339]', 'page': '69-76', 'pub_date': '2012-03-31', 'type': 'journal article', 'publisher': 'The Korean Society Of Food And Nutrition [meta:ra/0604 crossref:4768]', 'editor': 'Chung, Myong-Soo [meta:ra/0605 orcid:0000-0002-9666-2513]'}]
        self.assertEqual(csv_data, expected_csv_data)