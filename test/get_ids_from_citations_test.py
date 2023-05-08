import os
import shutil
import unittest

from oc_meta.lib.file_manager import get_csv_data
from oc_meta.plugins.get_ids_from_citations import get_ids_from_citations

BASE_DIR = os.path.join('test', 'get_ids_from_citations')
OUTPUT_DIR = os.path.join('test', 'get_ids_from_citations', 'output')


class test_GetIdsFromCitations(unittest.TestCase):
    def test_get_ids_from_citations_no_right_files(self):
        with self.assertRaises(RuntimeError):
            get_ids_from_citations(citations_dir=os.path.join(BASE_DIR, 'wrong_input'), output_dir=os.path.join(OUTPUT_DIR, 'wrong_output'), verbose=False)

    def test_get_ids_from_citations_csv(self):
        get_ids_from_citations(citations_dir=os.path.join(BASE_DIR, 'input_csv'), output_dir=os.path.join(OUTPUT_DIR, 'csv'), threshold=3, verbose=True)
        get_ids_from_citations(citations_dir=os.path.join(BASE_DIR, 'input_csv'), output_dir=os.path.join(OUTPUT_DIR, 'csv'), threshold=2, verbose=True)
        output = list()
        output_dir = os.path.join(OUTPUT_DIR, 'csv')
        for filename in os.listdir(output_dir):
            output.extend(get_csv_data(os.path.join(output_dir, filename)))
        expected_output = [{'id': '2140506'}, {'id': '2942070'}, {'id': '1523579'}, {'id': '7097569'}, {'id': '10.1108/jd-12-2013-0166'}, {'id': '10.1023/a:1021919228368'}, {'id': '10.1093/bioinformatics'}]
        self.assertEqual(output, expected_output)

    def test_get_ids_from_citations_zip(self):
        get_ids_from_citations(citations_dir=os.path.join(BASE_DIR, 'input_zip'), output_dir=os.path.join(OUTPUT_DIR, 'zip'), threshold=2, verbose=True)
        output = list()
        output_dir = os.path.join(OUTPUT_DIR, 'zip')
        for filename in os.listdir(output_dir):
            output.extend(get_csv_data(os.path.join(output_dir, filename)))
        expected_output = sorted([{'id': '2140506'}, {'id': '2942070'}, {'id': '1523579'}, {'id': '7097569'}, {'id': '10.1108/jd-12-2013-0166'}, {'id': '10.1023/a:1021919228368'}, {'id': '10.1093/bioinformatics'}], key=lambda x:''.join(x['id']))
        shutil.rmtree(output_dir)
        self.assertEqual(sorted(output, key=lambda x:''.join(x['id'])), expected_output)
 