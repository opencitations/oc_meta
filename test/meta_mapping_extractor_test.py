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


import os
import unittest

from oc_meta.lib.file_manager import get_csv_data
from oc_meta.run.meta_mapping_extractor import extract_metaid_mapping

BASE = os.path.join('test', 'meta_mapping_extractor')

class MetaMappingExtractorTest(unittest.TestCase):
    def test_extract_metaid_mapping(self):
        output_dirpath = os.path.join(BASE, 'output')
        output_filepath = os.path.join(output_dirpath, '1.csv')
        extract_metaid_mapping(BASE, output_dirpath)
        output = get_csv_data(output_filepath)
        os.remove(output_filepath)
        expected_output = [
            {'id': 'meta:br/0601', 'value': 'doi:10.1007/978-3-662-07918-8_3'},
            {'id': 'meta:br/0602', 'value': 'isbn:9783662079188'}, 
            {'id': 'meta:br/0602', 'value': 'isbn:9783642058530'}
            {'id': 'meta:br/0603', 'value': 'doi:10.1016/0021-9991(73)90147-2'},
            {'id': 'meta:br/0604', 'value': 'issn:0021-9991'},
            {'id': 'meta:br/0607', 'value': 'doi:10.1109/20.877674'},
            {'id': 'meta:br/0608', 'value': 'issn:0018-9464'},
            {'id': 'meta:br/06011', 'value': 'doi:10.1109/tps.2003.815469'},
            {'id': 'meta:br/06012', 'value': 'issn:0093-3813'},
            {'id': 'meta:br/06016', 'value': 'issn:0885-8977'}, 
            {'id': 'meta:br/06016', 'value': 'issn:1937-4208'},
            {'id': 'meta:br/06019', 'value': 'doi:10.1007/978-1-4615-3786-1_11'},
            {'id': 'meta:br/06020', 'value': 'isbn:9781461537861'}, 
            {'id': 'meta:br/06020', 'value': 'isbn:9781461366874'},
            {'id': 'meta:br/06021', 'value': 'doi:10.1088/0022-3727/13/1/002'},
            {'id': 'meta:br/06022', 'value': 'issn:1361-6463'}, 
            {'id': 'meta:br/06022', 'value': 'issn:0022-3727'},
            {'id': 'meta:br/06025', 'value': 'doi:10.1109/27.106800'},
            {'id': 'meta:br/06029', 'value': 'doi:10.1016/0021-9991(79)90051-2'},
            {'id': 'meta:br/06033', 'value': 'doi:10.1088/0022-3727/39/14/017'},
            {'id': 'meta:br/06037', 'value': 'isbn:9783663140900'},
            {'id': 'meta:br/06037', 'value': 'isbn:9783528085995'}, 
            {'id': 'meta:br/06037', 'value': 'doi:10.1007/978-3-663-14090-0'}, 
            {'id': 'meta:br/06042', 'value': 'doi:10.1007/s42835-022-01029-y'},
            {'id': 'meta:br/06043', 'value': 'issn:2093-7423'}, 
            {'id': 'meta:br/06043', 'value': 'issn:1975-0102'},
            {'id': 'meta:ra/0602', 'value': 'crossref:297'},
            {'id': 'meta:ra/0605', 'value': 'crossref:78'},
            {'id': 'meta:ra/0610', 'value': 'crossref:263'}, 
            {'id': 'meta:ra/06026', 'value': 'crossref:266'}, 
            {'id': 'meta:ra/06032', 'value': 'orcid:0000-0003-3891-6869'}, 
            {'id': 'meta:ra/06040', 'value': 'orcid:0000-0002-9383-6856'}]
        self.assertEqual(output.sort(key=lambda x: x['id']), expected_output.sort(key=lambda x: x['id']))