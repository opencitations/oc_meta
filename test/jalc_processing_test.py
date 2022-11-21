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
import re

from oc_idmanager import DOIManager

from oc_meta.lib.file_manager import call_api
from oc_meta.lib.master_of_regex import name_and_ids
from oc_meta.plugins.jalc.jalc_processing import JalcProcessing

doi_manager = DOIManager()
JALC_API = doi_manager._api_jalc
HEADERS = doi_manager._headers


class JalcProcessingTest(unittest.TestCase):
    def test_csv_creator(self):
        url = f'{JALC_API}10.15036/arerugi.33.167'
        item = call_api(url, HEADERS)
        datacite_processing = JalcProcessing()
        output = datacite_processing.csv_creator(item)
        expected_output = {'id': 'doi:10.15036/arerugi.33.167', 
        'title': '気管支喘息におけるアセチルコリン吸入試験の標準法の臨床的検討', 
        'author': '牧野, 荘平; 池森, 亨介; 福田, 健; 本島, 新司; 生井, 聖一郎; 戸田, 正夫; 山井, 孝夫; 山田, 吾郎; 湯川, 龍雄', 
        'issue': '3', 'volume': '33', 'venue': {'アレルギー', 'issn:1347-7935', 'issn:0021-4884'}, 'pub_date': '1984', 
        'pages': '167-175', 'type': 'journal article', 'publisher': '一般社団法人 日本アレルギー学会', 'editor': ''}
        venue_name_and_ids = re.search(name_and_ids, output['venue'])
        venue_name = venue_name_and_ids.group(1)
        venue_ids = venue_name_and_ids.group(2)
        output['venue'] = {venue_name}
        output['venue'].update(venue_ids.split())
        self.assertEqual(output, expected_output)