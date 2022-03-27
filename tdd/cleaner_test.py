import unittest
from meta.lib.cleaner import Cleaner
from meta.lib.csvmanager import CSVManager
from pprint import pprint

class test_Cleaner(unittest.TestCase):
    def test_clen_hyphen(self):
        broken_strings = ['100­101', '100−101', '100–101', '100–101', '100—101', '100⁃101', '100−101', '100➖101', '100Ⲻ101', '100﹘101']
        fixed_strings = list()
        for string in broken_strings:
            fixed_string = Cleaner(string).normalize_hyphens()
            fixed_strings.append(fixed_string)
        expected_output = ['100-101', '100-101', '100-101', '100-101', '100-101', '100-101', '100-101', '100-101', '100-101', '100-101']
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
        names = ['Peroni, Silvio', 'Peroni, S.', '  Peroni   ,    Silvio  ', 'PERONI, SILVIO', '', 'peroni', 'peroni, Silvio', 'McSorley, Stephen', 'OECD']
        outputs = list()
        for name in names:
            outputs.append(Cleaner(name).clean_name())
        expected_output = ['Peroni, Silvio', 'Peroni, S.', 'Peroni, Silvio', 'Peroni, Silvio', '', 'Peroni', 'Peroni, Silvio', 'McSorley, Stephen', 'Oecd']
        self.assertEqual(outputs, expected_output)
    
    def test_remove_unwanted_characters(self):
        names = ['Edward ].', 'Bernacki', 'Tom??&OV0165;', 'Gavin         E.', 'Andr[eacute]', 'Albers\u2010Miller', "O'Connor", 'O\u2019Connell', 'Gonz\u0301alez-Santiago', 'Gonz\u00e1lez-Benito', 'Andr&eacute;']
        outputs = list()
        for name in names:
            outputs.append(Cleaner(name).remove_unwanted_characters())
        expected_output = ['Edward', 'Bernacki', 'Tom&OV0165', 'Gavin E.', 'Andreacute', 'Albers-Miller', "O'Connor", 'O’Connell', 'Gonźalez-Santiago', 'González-Benito', 'André']
        self.assertEqual(outputs, expected_output)

    def test_normalize_spaces(self):
        broken_strings = ['100\u0009101', '100\u00A0101', '100\u200B101', '100\u202F101']
        fixed_strings = list()
        for string in broken_strings:
            fixed_string = Cleaner(string).normalize_spaces()
            fixed_strings.append(fixed_string)
        expected_output = ['100 101', '100 101', '100 101', '100 101']
        self.assertEqual(fixed_strings, expected_output)
    
    def test_normalize_id(self):
        identifiers = ['doi:10.1123/ijatt.2015-0070', 'doi:1', 'orcid:0000-0003-0530-4305', 'orcid:0000-0000', 'issn:1479-6708', 'issn:0000-0000', 'isbn:9783319403120', 'isbn:0000-0000']
        output = list()
        csv_manager = CSVManager()
        for id in identifiers:
            output.append(Cleaner(id).normalize_id(valid_dois_cache=csv_manager))
        expected_output = ['doi:10.1123/ijatt.2015-0070', None, 'orcid:0000-0003-0530-4305', None, 'issn:1479-6708', None, 'isbn:9783319403120', None]
        self.assertEqual(output, expected_output)

    def test_normalize_id_with_cache(self):
        identifiers = ['doi:10.1123/ijatt']
        output_data = list()
        csv_manager = CSVManager()
        csv_manager.data = {'10.1123/ijatt.2015-0070': {'v'}}
        for id in identifiers:
            output_data.append(Cleaner(id).normalize_id(valid_dois_cache=csv_manager))
        expected_data = [None]
        expected_cache = {'10.1123/ijatt.2015-0070': {'v'}, '10.1123/ijatt': {'i'}}
        output = (csv_manager.data, output_data)
        expected_output = (expected_cache, expected_data)
        self.assertEqual(output, expected_output)

if __name__ == '__main__':
    unittest.main()