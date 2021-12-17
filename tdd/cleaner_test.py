import unittest
from meta.scripts.cleaner import Cleaner
from pprint import pprint

class test_Cleaner(unittest.TestCase):
    def test_clen_hyphen(self):
        broken_strings = ['100­101', '100−101', '100–101', '100–101', '100—101', '100⁃101', '100−101']
        fixed_strings = list()
        for string in broken_strings:
            fixed_string = Cleaner(string).normalize_hyphens()
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
            outputs.append(Cleaner(input).clean_title())
        expected_output = [
            'OpenCitations, An Infrastructure Organization For Open Scholarship',
            'Opencitations, An Infrastructure Organization For Open Scholarship',
            'Opencitations, An Infrastructure Organization For Open Scholarship',
            'OpenCitations, An Infrastructure Organization For Open Scholarship',
            '""Agile"" "Knowledge" Graph Testing Ù Ò À With TESTaLOD (!Incredible!) Έτος 汉字'
        ]
        self.assertEqual(outputs, expected_output)
    
    def test_clean_date_all_valid(self):
        inputs = ['2020-02-11', '2020-12-12', '2000', '2000-12']
        outputs = list()
        for input in inputs:
            outputs.append(Cleaner(input).clean_date())
        expected_output = ['2020-02-11', '2020-12-12', '2000', '2000-12']
        self.assertEqual(outputs, expected_output)

    def test_clean_date_all_invalid(self):
        inputs = ['02-11', '11', '100000', 'godopoli']
        outputs = list()
        for input in inputs:
            outputs.append(Cleaner(input).clean_date())
        expected_output = ['', '', '', '']
        self.assertEqual(outputs, expected_output)
    
    def test_clean_name(self):
        names = ['Peroni, Silvio', 'Peroni, S.', '  Peroni   ,    Silvio  ', 'PERONI, SILVIO', '', 'peroni', 'peroni, Silvio']
        outputs = list()
        for name in names:
            outputs.append(Cleaner(name).clean_name())
        expected_output = ['Peroni, Silvio', 'Peroni, S.', 'Peroni, Silvio', 'Peroni, Silvio', '', 'Peroni', 'Peroni, Silvio']
        self.assertEqual(outputs, expected_output)
        
if __name__ == '__main__':
    unittest.main()