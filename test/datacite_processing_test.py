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


from oc_idmanager import DOIManager
from oc_meta.lib.file_manager import call_api
from oc_meta.plugins.datacite.datacite_processing import DataciteProcessing
import unittest


doi_manager = DOIManager()
DATACITE_API = doi_manager._api_datacite
HEADERS = doi_manager._headers


class DataCiteProcessingTest(unittest.TestCase):
    def test_csv_creator(self):
        url = f'{DATACITE_API}10.6084/m9.figshare.1468349'
        item = call_api(url, HEADERS)
        datacite_processing = DataciteProcessing()
        output = datacite_processing.csv_creator(item['data'])
        expected_output = {
            'id': 'doi:10.6084/m9.figshare.1468349',
            'title': 'RASH Framework - ESWC 2015 MoM session', 
            'author': 'Peroni, Silvio [orcid:0000-0003-0530-4305]', 
            'editor': '', 'pub_date': '2015', 
            'venue': '', 'volume': '', 'issue': '', 'page': '', 
            'type': 'other', 'publisher': 'figshare'}
        self.assertEqual(output, expected_output)