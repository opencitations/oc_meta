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
from oc_meta.run.get_diff import get_diff

BASE = os.path.join('test', 'get_diff')


class GetDiffTest(unittest.TestCase):
    def test_get_diff(self):
        output_file = os.path.join(BASE, 'diff.csv')
        get_diff(os.path.join(BASE, 'csv'), os.path.join(BASE, 'new.csv'), output_file)
        output = get_csv_data(output_file)
        expected_output = [{'id': '10.1093/bioinformatics'}]
        os.remove(output_file)
        self.assertEqual(output, expected_output)