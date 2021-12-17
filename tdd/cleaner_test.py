import unittest
from meta.scripts.cleaner import *
from pprint import pprint

class test_cleaner(unittest.TestCase):
    def test_clen_hyphen(self):
        broken_strings = ['100­101', '100−101', '100–101', '100–101', '100—101', '100⁃101', '100−101']
        fixed_strings = list()
        for string in broken_strings:
            fixed_string = normalize_hyphens(string)
            fixed_strings.append(fixed_string)
        expected_output = ['100-101', '100-101', '100-101', '100-101', '100-101', '100-101', '100-101']
        self.assertEqual(fixed_strings, expected_output)
    
    def test_clean_title(self):
        inputs = [
            'OpenCitations, an infrastructure organization for open scholarship',
            'OPENCITATIONS, AN INFRASTRUCTURE ORGANIZATION FOR OPEN SCHOLARSHIP',
            'opencitations, an infrastructure organization for open scholarship',
            'OpenCitations,             an infrastructure organization for open scholarship',
            ' ""agile""    "Knowledge"   graph   testing ù ò       à  with   TESTaLOD (!incredible!) έτος  汉字'
        ]
        outputs = list()
        for input in inputs:
            outputs.append(clean_title(input))
        expected_output = [
            'OpenCitations, An Infrastructure Organization For Open Scholarship',
            'Opencitations, An Infrastructure Organization For Open Scholarship',
            'Opencitations, An Infrastructure Organization For Open Scholarship',
            'OpenCitations, An Infrastructure Organization For Open Scholarship',
            '""Agile"" "Knowledge" Graph Testing Ù Ò À With TESTaLOD (!Incredible!) Έτος 汉字'
        ]
        self.assertEqual(outputs, expected_output)

    def testdate_parse_hack_ValueError(self):
        # All these dates must raise a ValueError
        inputs = ['2020-02-30', '2020-27-12', '9999-27-12', '100000', 'godopoli']
        for input in inputs:
            with self.assertRaises(ValueError):
                date_parse_hack(input)

    def testdate_parse_hack(self):
        # All these dates must not raise a ValueError
        inputs = ['2020-02-11', '2020-12-12', '2000', '2000-12']
        outputs = list()
        for input in inputs:
            outputs.append(date_parse_hack(input))
        expected_output = ['2020-02-11', '2020-12-12', '2000', '2000-12']
        self.assertEqual(outputs, expected_output)
    
    def test_clean_date_all_valid(self):
        inputs = ['2020-02-11', '2020-12-12', '2000', '2000-12']
        outputs = list()
        for input in inputs:
            outputs.append(clean_date(input))
        expected_output = ['2020-02-11', '2020-12-12', '2000', '2000-12']
        self.assertEqual(outputs, expected_output)

    def test_clean_date_all_invalid(self):
        inputs = ['02-11', '11', '100000', 'godopoli']
        outputs = list()
        for input in inputs:
            outputs.append(clean_date(input))
        expected_output = ['', '', '', '']
        self.assertEqual(outputs, expected_output)
        
if __name__ == '__main__':
    unittest.main()