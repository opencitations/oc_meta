#!python
# Copyright 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
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

import os
import unittest

from oc_meta.lib.file_manager import get_csv_data
from oc_meta.plugins.analyser import OCMetaAnalyser, OCMetaCounter

BASE = os.path.join('test', 'analyser')
OUTPUT = os.path.join(BASE, 'output')


class test_Analyser(unittest.TestCase):
    def test_merge_rows_by_id(self):
        ocmeta_counter = OCMetaAnalyser(csv_dump_path=BASE)
        ocmeta_counter.merge_rows_by_id(output_dir=OUTPUT)
        csv_0 = get_csv_data(os.path.join(OUTPUT, '303_2022-09-29T02-39-23.csv'))
        csv_1 = get_csv_data(os.path.join(OUTPUT, '304_2022-09-29T02-39-23.csv'))
        csv_2 = get_csv_data(os.path.join(OUTPUT, '304_2022-09-29T02-39-24.csv'))
        csv_2_old = get_csv_data(os.path.join(BASE, '304_2022-09-29T02-39-24.csv'))
        csv_expected_0 = [
            {'id': 'meta:br/06015', 'title': 'Spatial Distribution of Ion Current Around HVDC Bundle Conductors', 'pub_date': '2012-01', 'page': '380-390', 'type': 'journal article', 'author': 'Zhou, Xiangxian [meta:ra/06016]; Cui, Xiang [meta:ra/06017]; Lu, Tiebing [meta:ra/06018]; Fang, Chao [meta:ra/06019]; Zhen, Yongzan [meta:ra/06020]', 'editor': '', 'publisher': 'Institute of Electrical and Electronics Engineers (IEEE) [crossref:263 meta:ra/0610]', 'volume': '27', 'venue': 'IEEE Transactions on Power Delivery [issn:0885-8977 issn:1937-4208 meta:br/06016]', 'issue': '1'}, 
            {'id': 'meta:br/06038', 'title': 'Space-charge effects in high-density plasmas', 'pub_date': '1982-06', 'page': '454-461', 'type': 'journal article', 'author': 'Morrow, R [meta:ra/06037]', 'editor': '', 'publisher': 'Elsevier BV [crossref:78 meta:ra/0605]', 'volume': '46', 'venue': 'Journal of Computational Physics [issn:0021-9991 meta:br/0604]', 'issue': '3'}]
        csv_expected_1 = [
            {'id': 'meta:br/06044', 'title': 'Spatial Distribution of Ion Current Around HVDC Bundle Conductors', 'pub_date': '2012-01', 'page': '380-390', 'type': 'journal article', 'author': 'Zhou, Xiangxian [meta:ra/06016]; Cui, Xiang [meta:ra/06017]; Lu, Tiebing [meta:ra/06018]; Fang, Chao [meta:ra/06019]; Zhen, Yongzan [meta:ra/06020]', 'editor': '', 'publisher': 'Institute of Electrical and Electronics Engineers (IEEE) [crossref:263 meta:ra/0610]', 'volume': '27', 'venue': 'IEEE Transactions on Power Delivery [issn:0885-8977 issn:1937-4208 meta:br/06016]', 'issue': '1'}, 
            {'id': 'meta:br/06045', 'title': 'Space-charge effects in high-density plasmas', 'pub_date': '1982-06', 'page': '454-461', 'type': 'journal article', 'author': 'Morrow, R [meta:ra/06037]', 'editor': '', 'publisher': 'Elsevier BV [crossref:78 meta:ra/0605]', 'volume': '46', 'venue': 'Journal of Computational Physics [issn:0021-9991 meta:br/0604]', 'issue': '3'}]
        self.assertEqual(csv_0, csv_expected_0)
        self.assertEqual(csv_1, csv_expected_1)
        self.assertEqual(csv_2, csv_2_old)

    def test_count_authors(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        count = ocmeta_counter.count(what='authors')
        self.assertEqual(count, '40')

    def test_count_editor(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        count = ocmeta_counter.count(what='editors')
        self.assertEqual(count, '0')

    def test_count_publisher(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        count = ocmeta_counter.count(what='publishers')
        self.assertEqual(count, '4')

    def test_count_venues(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        count = ocmeta_counter.count(what='venues')
        self.assertEqual(count, '8')

    def test_get_top_publishers_by_venue(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        top: list = ocmeta_counter.get_top(what='publishers', by_what='venue')
        expected_top = [
            ('meta:ra/0610', {'name': 'Institute of Electrical and Electronics Engineers (IEEE)', 'total': 3}), 
            ('meta:ra/0602', {'name': 'Springer Science and Business Media LLC', 'total': 3}), 
            ('meta:ra/0605', {'name': 'Elsevier BV', 'total': 1}), 
            ('meta:ra/06026', {'name': 'IOP Publishing', 'total': 1})]
        top.sort(key=lambda x: str(x[1]['total']) + x[0], reverse=True)
        self.assertEqual(top.sort(key=lambda x: str(x[1]['total']) + x[0], reverse=True), expected_top.sort(key=lambda x: str(x[1]['total']) + x[0], reverse=True))

    def test_get_top_publishers_by_publication(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        top = ocmeta_counter.get_top(what='publishers', by_what='publication')
        expected_top = [
            ('institute of electrical and electronics engineers (ieee)', {'name': 'Institute of Electrical and Electronics Engineers (IEEE)', 'total': 6}), 
            ('elsevier bv', {'name': 'Elsevier BV', 'total': 5}), 
            ('springer science and business media llc', {'name': 'Springer Science and Business Media LLC', 'total': 4}), 
            ('iop publishing', {'name': 'IOP Publishing', 'total': 2})]
        self.assertEqual(top, expected_top)

    def test_get_top_venues_by_publication(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        top = ocmeta_counter.get_top(what='venues', by_what='publication')
        expected_top = [
            ('meta:br/0604', {'name': 'Journal of Computational Physics', 'total': 5}), 
            ('meta:br/06016', {'name': 'IEEE Transactions on Power Delivery', 'total': 3}), 
            ('meta:br/06012', {'name': 'IEEE Transactions on Plasma Science', 'total': 2}), 
            ('meta:br/06022', {'name': 'Journal of Physics D: Applied Physics', 'total': 2}),
            ('meta:br/0602', {'name': 'Insulation of High-Voltage Equipment', 'total': 1}), 
            ('meta:br/0608', {'name': 'IEEE Transactions on Magnetics', 'total': 1}), 
            ('meta:br/06020', {'name': 'Physics and Applications of Pseudosparks', 'total': 1}), 
            ('meta:br/06043', {'name': 'Journal of Electrical Engineering & Technology', 'total': 1})]
        self.assertEqual(top.sort(key=lambda x: str(x[1]['total']) + x[0], reverse=True), expected_top.sort(key=lambda x: str(x[1]['total']) + x[0], reverse=True))

    def test_get_top_years_by_publication(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        top = ocmeta_counter.get_top(what='years', by_what='publication')
        expected_top = [
            ('2012', {'total': 3}), 
            ('1982', {'total': 3}), 
            ('2000', {'total': 2}), 
            ('1973', {'total': 1}),
            ('2003', {'total': 1}), 
            ('1990', {'total': 1}), 
            ('1980', {'total': 1}), 
            ('1991', {'total': 1}), 
            ('1979', {'total': 1}),
            ('2006', {'total': 1})]
        self.assertEqual(top.sort(key=lambda x: str(x[1]['total']) + x[0], reverse=True), expected_top.sort(key=lambda x: str(x[1]['total']) + x[0], reverse=True))