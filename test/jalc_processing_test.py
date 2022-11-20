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
from oc_meta.plugins.jalc.jalc_processing import JalcProcessing

doi_manager = DOIManager()
JALC_API = doi_manager._api_jalc
HEADERS = doi_manager._headers


class JalcProcessingTest(unittest.TestCase):
    def test_csv_creator(self):
        url = f'{JALC_API}10.11514/infopro.2008.0.138.0'
        item = call_api(url, HEADERS)
        datacite_processing = JalcProcessing()
        output = datacite_processing.csv_creator(item)
        expected_output = {'title': '文献データベースの新しい活用方法', 'author': '独立行政法人 科学技術振興機構, &nbsp;', 
            'issue': '0', 'volume': '2008', 'venue': '情報プロフェッショナルシンポジウム予稿集', 'pub_date': '2008', 
            'pages': '138-139', 'type': 'journal article', 'publisher': '一般社団法人 情報科学技術協会', 'editor': ''}
        self.assertEqual(output, expected_output)