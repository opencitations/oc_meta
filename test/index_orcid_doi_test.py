#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019 Silvio Peroni <essepuntato@gmail.com>
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021 Simone Persiani <iosonopersia@gmail.com>
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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
import shutil
import unittest
from csv import DictReader, DictWriter
from pprint import pprint

from oc_meta.plugins.orcid.index_orcid_doi import Index_orcid_doi

CSV_PATH = os.path.join('test', 'index_orcid_doi', 'output')
SUMMARIES_PATH = os.path.join('test', 'index_orcid_doi', 'orcid')

def load_files_from_dir(dir:str):
    output = list()
    for file in sorted(os.listdir(dir), key=lambda filename: int(filename.split('-')[0].replace('.csv', ''))):
        with open(os.path.join(dir, file), 'r', encoding='utf-8') as f:
            output.extend(list(DictReader(f)))
    return output


class test_Index_orcid_doi(unittest.TestCase):
    def test_explorer(self):
        iOd = Index_orcid_doi(output_path=CSV_PATH, verbose=False)
        iOd.explorer(summaries_path=SUMMARIES_PATH)
        output = sorted(load_files_from_dir(CSV_PATH), key=lambda d: d['id'])
        expected_output = sorted([
            {'id': 'None', 'value': '[0000-0001-5002-1000]'},
            {'id': '10.1016/j.indcrop.2020.112103', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2019/3213521', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.11.028', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.03.004', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1186/s13568-016-0300-2', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.toxicon.2014.04.010', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2014/691742', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1002/rmv.2149', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.01.147', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.02.044', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.ijcard.2016.06.064', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.4103/1319-2442.190782', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1053/j.jrn.2015.04.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.26719/2015.21.5.354', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ckj/sfu046', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s00393-012-1058-9', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1159/000356118', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1111/1756-185x.12007', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s11255-011-0007-x', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.jbspin.2011.06.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ndt/gfq089', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}
        ], key=lambda d: d['id'])
        shutil.rmtree(CSV_PATH)
        self.assertEqual(output, expected_output)

    def test_cache(self):
        os.mkdir(CSV_PATH)
        with open(os.path.join(CSV_PATH, '0.csv'), 'w', encoding='utf-8') as output_file:
            data_to_write = [            
                {'id': '10.1002/rmv.2149', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.transproceed.2019.01.147', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.transproceed.2019.02.044', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.ijcard.2016.06.064', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.4103/1319-2442.190782', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1053/j.jrn.2015.04.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.26719/2015.21.5.354', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1093/ckj/sfu046', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1007/s00393-012-1058-9', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1159/000356118', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1111/1756-185x.12007', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1007/s11255-011-0007-x', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.jbspin.2011.06.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1093/ndt/gfq089', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}
            ]
            dict_writer = DictWriter(output_file, ['id', 'value'])
            dict_writer.writeheader()
            dict_writer.writerows(data_to_write)
        iOd = Index_orcid_doi(output_path=CSV_PATH, verbose=False)
        cache = iOd.cache
        iOd.explorer(summaries_path=SUMMARIES_PATH)
        output = load_files_from_dir(CSV_PATH)
        unordered_output = {key_value['id']:key_value['value'] for key_value in output}
        expected_output = {
            '10.1002/rmv.2149': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1016/j.transproceed.2019.01.147': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1016/j.transproceed.2019.02.044': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1016/j.ijcard.2016.06.064': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.4103/1319-2442.190782': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1053/j.jrn.2015.04.009': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.26719/2015.21.5.354': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1093/ckj/sfu046': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1007/s00393-012-1058-9': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1159/000356118': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1111/1756-185x.12007': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1007/s11255-011-0007-x': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1016/j.jbspin.2011.06.009': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1093/ndt/gfq089': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            'None': '[0000-0001-5002-1000]', 
            '10.1016/j.indcrop.2020.112103': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1155/2019/3213521': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1016/j.bioorg.2018.11.028': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1016/j.bioorg.2018.03.004': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1186/s13568-016-0300-2': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1016/j.toxicon.2014.04.010': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1155/2014/691742': 'Gargouri, Ali [0000-0001-5009-9000]'}
        shutil.rmtree(CSV_PATH)
        self.assertEqual((unordered_output, cache), (expected_output, {'0000-0001-5650-3000'}))

    def test_low_memory(self):
        iOd = Index_orcid_doi(output_path=CSV_PATH, low_memory=True, threshold=10, verbose=False)
        iOd.explorer(summaries_path=SUMMARIES_PATH)
        output = sorted(load_files_from_dir(CSV_PATH), key=lambda d: d['id'])
        expected_output = sorted([
            {'id': 'None', 'value': '[0000-0001-5002-1000]'},
            {'id': '10.1016/j.indcrop.2020.112103', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2019/3213521', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.11.028', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.03.004', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1186/s13568-016-0300-2', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.toxicon.2014.04.010', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2014/691742', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1002/rmv.2149', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.01.147', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.02.044', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.ijcard.2016.06.064', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.4103/1319-2442.190782', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1053/j.jrn.2015.04.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.26719/2015.21.5.354', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ckj/sfu046', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s00393-012-1058-9', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1159/000356118', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1111/1756-185x.12007', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s11255-011-0007-x', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.jbspin.2011.06.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ndt/gfq089', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}
        ], key=lambda d: d['id'])
        shutil.rmtree(CSV_PATH)
        self.assertEqual(output, expected_output)

    def test_cache_low_memory(self):
        os.mkdir(CSV_PATH)
        with open(os.path.join(CSV_PATH, '0.csv'), 'w', encoding='utf-8') as output_file:
            data_to_write = [            
                {'id': '10.1002/rmv.2149', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.transproceed.2019.01.147', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.transproceed.2019.02.044', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.ijcard.2016.06.064', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.4103/1319-2442.190782', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1053/j.jrn.2015.04.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.26719/2015.21.5.354', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1093/ckj/sfu046', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1007/s00393-012-1058-9', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1159/000356118', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1111/1756-185x.12007', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1007/s11255-011-0007-x', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.jbspin.2011.06.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1093/ndt/gfq089', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}
            ]
            dict_writer = DictWriter(output_file, ['id', 'value'])
            dict_writer.writeheader()
            dict_writer.writerows(data_to_write)
        iOd = Index_orcid_doi(output_path=CSV_PATH, low_memory=True, verbose=False)
        cache = iOd.cache
        iOd.explorer(summaries_path=SUMMARIES_PATH)
        output = load_files_from_dir(CSV_PATH)
        unordered_output = {key_value['id']:key_value['value'] for key_value in output}
        expected_output = {
            '10.1002/rmv.2149': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1016/j.transproceed.2019.01.147': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1016/j.transproceed.2019.02.044': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1016/j.ijcard.2016.06.064': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.4103/1319-2442.190782': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1053/j.jrn.2015.04.009': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.26719/2015.21.5.354': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1093/ckj/sfu046': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1007/s00393-012-1058-9': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1159/000356118': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1111/1756-185x.12007': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1007/s11255-011-0007-x': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1016/j.jbspin.2011.06.009': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            '10.1093/ndt/gfq089': 'NasrAllah, Mohamed M [0000-0001-5650-3000]', 
            'None': '[0000-0001-5002-1000]', 
            '10.1016/j.indcrop.2020.112103': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1155/2019/3213521': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1016/j.bioorg.2018.11.028': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1016/j.bioorg.2018.03.004': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1186/s13568-016-0300-2': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1016/j.toxicon.2014.04.010': 'Gargouri, Ali [0000-0001-5009-9000]', 
            '10.1155/2014/691742': 'Gargouri, Ali [0000-0001-5009-9000]'}
        shutil.rmtree(CSV_PATH)
        self.assertEqual((unordered_output, cache), (expected_output, {'0000-0001-5650-3000'}))

if __name__ == '__main__': # pragma: no cover
    unittest.main()
