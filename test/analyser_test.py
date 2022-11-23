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
        self.assertEqual(count, 40)

    def test_count_editor(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        count = ocmeta_counter.count(what='editors')
        self.assertEqual(count, 0)

    def test_get_top_publishers_by_venue(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        top = ocmeta_counter.get_top(what='publishers', by_what='venue')
        expected_top = [
            ('meta:ra/0610', {'name': 'Institute of Electrical and Electronics Engineers (IEEE)', 'venue': {'meta:br/06012', 'meta:br/06016', 'meta:br/0608'}, 'total': 3}), 
            ('meta:ra/0602', {'name': 'Springer Science and Business Media LLC', 'venue': {'meta:br/0602', 'meta:br/06020', 'meta:br/06043'}, 'total': 3}), 
            ('meta:ra/0605', {'name': 'Elsevier BV', 'venue': {'meta:br/0604'}, 'total': 1}), 
            ('meta:ra/06026', {'name': 'IOP Publishing', 'venue': {'meta:br/06022'}, 'total': 1})]
        self.assertEqual(top, expected_top)

    def test_get_top_publishers_by_publication(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        top = ocmeta_counter.get_top(what='publishers', by_what='publication')
        expected_top = [
            ('meta:ra/0610', {'name': 'Institute of Electrical and Electronics Engineers (IEEE)', 'publication': {'meta:br/06044', 'meta:br/06046', 'meta:br/0607', 'meta:br/06015', 'meta:br/06025', 'meta:br/06011'}, 'total': 6}), 
            ('meta:ra/0605', {'name': 'Elsevier BV', 'publication': {'meta:br/06047', 'meta:br/0603', 'meta:br/06029', 'meta:br/06045', 'meta:br/06038'}, 'total': 5}), 
            ('meta:ra/0602', {'name': 'Springer Science and Business Media LLC', 'publication': {'meta:br/06019', 'meta:br/06042', 'meta:br/0601', 'meta:br/06037'}, 'total': 4}), 
            ('meta:ra/06026', {'name': 'IOP Publishing', 'publication': {'meta:br/06033', 'meta:br/06021'}, 'total': 2})]
        self.assertEqual(top, expected_top)

    def test_get_top_venues_by_publication(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        top = ocmeta_counter.get_top(what='venues', by_what='publication')
        expected_top = [
            ('meta:br/0604', {'name': 'Journal of Computational Physics', 'publication': {'meta:br/06047', 'meta:br/06038', 'meta:br/06045', 'meta:br/06029', 'meta:br/0603'}, 'total': 5}), 
            ('meta:br/06016', {'name': 'IEEE Transactions on Power Delivery', 'publication': {'meta:br/06046', 'meta:br/06015', 'meta:br/06044'}, 'total': 3}), 
            ('meta:br/06012', {'name': 'IEEE Transactions on Plasma Science', 'publication': {'meta:br/06025', 'meta:br/06011'}, 'total': 2}), 
            ('meta:br/06022', {'name': 'Journal of Physics D: Applied Physics', 'publication': {'meta:br/06021', 'meta:br/06033'}, 'total': 2}),
            ('meta:br/0602', {'name': 'Insulation of High-Voltage Equipment', 'publication': {'meta:br/0601'}, 'total': 1}), 
            ('meta:br/0608', {'name': 'IEEE Transactions on Magnetics', 'publication': {'meta:br/0607'}, 'total': 1}), 
            ('meta:br/06020', {'name': 'Physics and Applications of Pseudosparks', 'publication': {'meta:br/06019'}, 'total': 1}), 
            ('meta:br/06043', {'name': 'Journal of Electrical Engineering & Technology', 'publication': {'meta:br/06042'}, 'total': 1})]
        self.assertEqual(top, expected_top)

    def test_get_top_years_by_publication(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        top = ocmeta_counter.get_top(what='years', by_what='publication')
        expected_top = [
            ('2012', {'publication': {'meta:br/06015', 'meta:br/06046', 'meta:br/06044'}, 'total': 3}), 
            ('1982', {'publication': {'meta:br/06038', 'meta:br/06047', 'meta:br/06045'}, 'total': 3}), 
            ('2000', {'publication': {'meta:br/0607', 'meta:br/0601'}, 'total': 2}), 
            ('1973', {'publication': {'meta:br/0603'}, 'total': 1}),
            ('2003', {'publication': {'meta:br/06011'}, 'total': 1}), 
            ('1990', {'publication': {'meta:br/06019'}, 'total': 1}), 
            ('1980', {'publication': {'meta:br/06021'}, 'total': 1}), 
            ('1991', {'publication': {'meta:br/06025'}, 'total': 1}), 
            ('1979', {'publication': {'meta:br/06029'}, 'total': 1})]
        self.assertEqual(top, expected_top)

    def test_get_top_types_by_publication(self):
        ocmeta_counter = OCMetaCounter(csv_dump_path=OUTPUT)
        top = ocmeta_counter.get_top(what='types', by_what='publication')
        expected_top = [
            ('journal article', {'publication': {'meta:br/06011', 'meta:br/06033', 'meta:br/06045', 'meta:br/06042', 'meta:br/0607', 'meta:br/06044', 'meta:br/06047', 'meta:br/06029', 'meta:br/06021', 'meta:br/06046', 'meta:br/06025', 'meta:br/06015', 'meta:br/0603', 'meta:br/06038'}, 'total': 14}), 
            ('book chapter', {'publication': {'meta:br/0601', 'meta:br/06019'}, 'total': 2}), 
            ('book', {'publication': {'meta:br/06037'}, 'total': 1})]
        self.assertEqual(top, expected_top)