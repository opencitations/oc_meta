import unittest
from pprint import pprint

from oc_meta.lib.cleaner import Cleaner


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
            ' ""agile""    "Knowledge"   graph   testing ù ò       à  with   TESTaLOD (!incredible!) έτος  汉字',
            'Elsevier BV'
        ]
        outputs = list()
        for input in inputs:
            outputs.append(Cleaner(input).clean_title())
        expected_output = [
            'OpenCitations, An Infrastructure Organization For Open Scholarship',
            'Opencitations, An Infrastructure Organization For Open Scholarship',
            'Opencitations, An Infrastructure Organization For Open Scholarship',
            'OpenCitations, An Infrastructure Organization For Open Scholarship',
            '""Agile"" "Knowledge" Graph Testing Ù Ò À With TESTaLOD (!Incredible!) Έτος 汉字',
            'Elsevier BV'
        ]
        self.assertEqual(outputs, expected_output)
    
    def test_clean_date_all_valid(self):
        inputs = ['2020-13-50', '2020-02-50', '2020-02-11', '2020-12-12', '2000', '2000-12', '2000-13']
        outputs = list()
        for input in inputs:
            outputs.append(Cleaner(input).clean_date())
        expected_output = ['2020', '2020-02', '2020-02-11', '2020-12-12', '2000', '2000-12', '2000']
        self.assertEqual(outputs, expected_output)

    def test_clean_date_all_invalid(self):
        inputs = ['100000-13-50', '02-11', '11', '100000', 'godopoli']
        outputs = list()
        for input in inputs:
            outputs.append(Cleaner(input).clean_date())
        expected_output = ['', '', '', '', '']
        self.assertEqual(outputs, expected_output)
    
    def test_clean_name(self):
        names = ['Peroni, Silvio', 'Peroni, S.', '  Peroni   ,    Silvio  ', 'PERONI, SILVIO', '', 'peroni', 'peroni, Silvio', 'McSorley, Stephen', 'OECD', ',']
        outputs = list()
        for name in names:
            outputs.append(Cleaner(name).clean_name())
        expected_output = ['Peroni, Silvio', 'Peroni, S.', 'Peroni, Silvio', 'Peroni, Silvio', '', 'Peroni', 'Peroni, Silvio', 'McSorley, Stephen', 'Oecd', '']
        self.assertEqual(outputs, expected_output)
    
    def test_remove_unwanted_characters(self):
        names = ['Edward ].', 'Bernacki', 'Tom??&OV0165;', 'Gavin         E.', 'Andr[eacute]', 'Albers\u2010Miller', "O'Connor", 'O\u2019Connell', 'Gonz\u0301alez-Santiago', 'Gonz\u00e1lez-Benito', 'Andr&eacute;']
        outputs = list()
        for name in names:
            outputs.append(Cleaner(name).remove_unwanted_characters())
        expected_output = ['Edward', 'Bernacki', 'Tom&OV0165', 'Gavin E.', 'Andreacute', 'Albers-Miller', "O'Connor", 'O’Connell', 'Gonźalez-Santiago', 'González-Benito', 'André']
        self.assertEqual(outputs, expected_output)
    
    def test_clean_ra_list(self):
        names = ['Not Available, Not Available', 'Peroni, Not Available', 'Not Available, Silvio', 'Not Available', 'Peroni, Silvio', ',']
        output = Cleaner.clean_ra_list(names)
        expected_output = ['Peroni, ', 'Peroni, Silvio']
        self.assertEqual(output, expected_output)

    def test_clean_ra_list_duplicates(self):
        names = ['Peroni, Silvio [orcid:0000-0003-0530-4305 viaf:1]', 'Peroni, Not Available', 'Peroni, Silvio [orcid:0000-0003-0530-4305]', 'Massari, Arcangelo']
        output = Cleaner.clean_ra_list(names)
        expected_output = ['Peroni, Silvio [orcid:0000-0003-0530-4305 viaf:1]', 'Peroni, ', 'Massari, Arcangelo']
        self.assertEqual(output, expected_output)

    def test_clean_ra_list_remove_ids(self):
        names = ['Peroni, Silvio [orcid:0000-0003-0530-4305 viaf:1]', 'Peroni, Not Available', 'Perone, Silvio [orcid:0000-0003-0530-4305]', 'Massari, Arcangelo']
        output = Cleaner.clean_ra_list(names)
        expected_output = ['Peroni, Silvio [viaf:1]', 'Peroni, ', 'Perone, Silvio', 'Massari, Arcangelo']
        self.assertEqual(output, expected_output)

    def test_clean_ra_list_only_ids(self):
        names = ['Peroni, Silvio [orcid:0000-0003-0530-4305]', '[orcid:0000-0003-0530-4305 viaf:1]', '[orcid:0000-0003-0530-4306]']
        output = Cleaner.clean_ra_list(names)
        expected_output = ['Peroni, Silvio', '[viaf:1]', '[orcid:0000-0003-0530-4306]']
        self.assertEqual(output, expected_output)

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
        csv_manager = dict()
        for id in identifiers:
            output.append(Cleaner(id).normalize_id(valid_dois_cache=csv_manager))
        expected_output = ['doi:10.1123/ijatt.2015-0070', None, 'orcid:0000-0003-0530-4305', None, 'issn:1479-6708', None, 'isbn:9783319403120', None]
        self.assertEqual(output, expected_output)

    def test_normalize_id_with_cache(self):
        identifiers = ['doi:10.1123/ijatt']
        output_data = list()
        csv_manager = {'10.1123/ijatt.2015-0070': {'v'}}
        for id in identifiers:
            output_data.append(Cleaner(id).normalize_id(valid_dois_cache=csv_manager))
        expected_data = [None]
        self.assertEqual(output_data, expected_data)
    
    def test_clean_volume_and_issue(self):
        invalid_vi_rows = [
            {'pub_date': '','volume': 'Volume 15-Issue 1', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '', 'issue': 'Vol 2 Núm 3', 'type': ''},
            {'pub_date': '','volume': '', 'issue': 'Lang.- Lit. Volume 10 numéro 2', 'type': ''},
            {'pub_date': '','volume': 'Vol. 14 Issue 1', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '', 'issue': 'Vol. 39 N°1', 'type': ''},
            {'pub_date': '','volume': 'Vol. 10, N° 2-3', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '', 'issue': 'Vol. 35 N° spécial 1', 'type': ''},
            {'pub_date': '','volume': 'Vol. XXXIII N° 2', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '', 'issue': 'Volume 14 Issue 5', 'type': ''},
            {'pub_date': '','volume': 'Vol.10, No.3', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '-1', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'Tome II - N°1', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '>6', 'issue': '13,N°2', 'type': ''},
            {'pub_date': '','volume': '9, n° 4', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '${articleBase.volume}', 'issue': 'Tập 55, Số 3', 'type': ''},
            {'pub_date': '','volume': 'Issue 1 Volume 21, 2020', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '', 'issue': "14 (2'2018)", 'type': ''},
            {'pub_date': '','volume': 'Cilt:13 Sayı:3', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '/', 'issue': 'Cilt 21 Sayı 3 Temmuz 2020', 'type': ''},
            {'pub_date': '','volume': '&NA;', 'issue': '&NA;', 'type': ''},
            {'pub_date': '','volume': 'n/a', 'issue': 'n/a', 'type': ''},
            {'pub_date': '','volume': '.', 'issue': '-', 'type': ''},
            {'pub_date': '','volume': '`', 'issue': 'ё', 'type': ''},
            {'pub_date': '','volume': '.38', 'issue': '/4', 'type': ''},
            {'pub_date': '','volume': '74,', 'issue': '501.', 'type': ''},
            {'pub_date': '','volume': '1(3)/', 'issue': '19`', 'type': ''},
            {'pub_date': '','volume': 'No. 4.', 'issue': '3()', 'type': ''},
            {'pub_date': '','volume': '5â\x80\x926', 'issue': '12���13', 'type': ''},
            {'pub_date': '','volume': '38â39', 'issue': '3???4', 'type': ''},
            {'pub_date': '','volume': 'n�183', 'issue': 'N�31-32', 'type': ''},
            {'pub_date': '','volume': 'N?44', 'issue': 'N��49', 'type': ''},
            {'pub_date': '','volume': 'N�1,NF', 'issue': '85 (First Serie', 'type': ''},
            {'pub_date': '','volume': 'issue 2', 'issue': 'Original Series, Volume 1', 'type': ''},
            {'pub_date': '','volume': 'Special Issue 2', 'issue': 'volume 3', 'type': ''},
            {'pub_date': '','volume': '1 special issue', 'issue': 'Vol, 7', 'type': ''},
            {'pub_date': '','volume': 'Special Issue "Urban Morphology”', 'issue': 'vol.7', 'type': ''},
            {'pub_date': '','volume': '', 'issue': 'Tome 1', 'type': ''},
            {'pub_date': '','volume': 'Special_Issue_Number_2', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'Special-Issue-1', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'Special 13', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'Especial 2', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'esp.2', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'spe.2', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '1 S.2', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'Özel Sayı 5', 'issue': '', 'type': 'journal volume'},
            {'pub_date': '','volume': 'ÖS1', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'N° Hors série 10', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'Hors-série 5', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '특별호', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '(13/72) Education', 'issue': '', 'type': ''},
            {'pub_date': '','volume': '(13/72) Language-Literature', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'Sayı: 24', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'Issue 11, Supplement 6', 'issue': '', 'type': ''},
            {'pub_date': '','volume': 'Issue 2. pp. 94-185', 'issue': '', 'type': ''},
            {'pub_date': '', 'volume': '5', 'issue': '6', 'type': ''},
            {'pub_date': '', 'volume': 'Issue 2. pp. 94-185', 'issue': '6', 'type': ''},
            {'pub_date': '', 'volume': '', 'issue': '6', 'type': ''},
            {'pub_date': '', 'volume': '5', 'issue': '', 'type': ''},
            {'pub_date': '', 'volume': 'Not Available', 'issue': 'not available', 'type': ''}
        ]
        for row in invalid_vi_rows:
            Cleaner.clean_volume_and_issue(row)
        expected_output = [
            {'pub_date': '', 'volume': '15', 'issue': '1', 'type': ''}, 
            {'pub_date': '', 'volume': '2', 'issue': '3', 'type': ''}, 
            {'pub_date': '', 'volume': '10', 'issue': '2', 'type': ''}, 
            {'pub_date': '', 'volume': '14', 'issue': '1', 'type': ''}, 
            {'pub_date': '', 'volume': '39', 'issue': '1', 'type': ''}, 
            {'pub_date': '', 'volume': '10', 'issue': '2-3', 'type': ''}, 
            {'pub_date': '', 'volume': '35', 'issue': '1', 'type': ''}, 
            {'pub_date': '', 'volume': 'XXXIII', 'issue': '2', 'type': ''}, 
            {'pub_date': '', 'volume': '14', 'issue': '5', 'type': ''}, 
            {'pub_date': '', 'volume': '10', 'issue': '3', 'type': ''},
            {'pub_date': '', 'volume': '-1', 'issue': '', 'type': ''}, 
            {'pub_date': '', 'volume': 'II', 'issue': '1', 'type': ''}, 
            {'pub_date': '', 'volume': '>6', 'issue': '2', 'type': ''}, 
            {'pub_date': '', 'volume': '9', 'issue': '4', 'type': ''}, 
            {'pub_date': '', 'volume': '55', 'issue': '3', 'type': ''}, 
            {'pub_date': '2020', 'volume': '21', 'issue': '1', 'type': ''}, 
            {'pub_date': '2018', 'volume': '14', 'issue': '2', 'type': ''}, 
            {'pub_date': '', 'volume': '13', 'issue': '3', 'type': ''}, 
            {'pub_date': '2020', 'volume': '21', 'issue': '3', 'type': ''},
            {'pub_date': '', 'volume': '', 'issue': '', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': '', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': '', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': '', 'type': ''},
            {'pub_date': '', 'volume': '.38', 'issue': '4', 'type': ''}, 
            {'pub_date': '', 'volume': '74', 'issue': '501.', 'type': ''}, 
            {'pub_date': '', 'volume': '1(3)', 'issue': '19', 'type': ''}, 
            {'pub_date': '', 'volume': 'No. 4.', 'issue': '3', 'type': ''},
            {'pub_date': '', 'volume': '5-6', 'issue': '12-13', 'type': ''}, 
            {'pub_date': '', 'volume': '38-39', 'issue': '3-4', 'type': ''}, 
            {'pub_date': '', 'volume': '183', 'issue': '31-32', 'type': ''}, 
            {'pub_date': '', 'volume': '44', 'issue': '49', 'type': ''}, 
            {'pub_date': '', 'volume': '1,NF', 'issue': '85 (First Series)', 'type': ''},
            {'pub_date': '', 'volume': 'Original Series, Volume 1', 'issue': 'issue 2', 'type': ''}, 
            {'pub_date': '', 'volume': 'volume 3', 'issue': 'Special Issue 2', 'type': ''}, 
            {'pub_date': '', 'volume': 'Vol, 7', 'issue': '1 special issue', 'type': ''}, 
            {'pub_date': '', 'volume': 'vol.7', 'issue': 'Special Issue "Urban Morphology”', 'type': ''}, 
            {'pub_date': '', 'volume': 'Tome 1', 'issue': '', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'Special_Issue_Number_2', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'Special-Issue-1', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'Special 13', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'Especial 2', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'esp.2', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'spe.2', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': '1 S.2', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'Özel Sayı 5', 'type': 'journal issue'},
            {'pub_date': '', 'volume': '', 'issue': 'ÖS1', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'N° Hors série 10', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'Hors-série 5', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': '특별호', 'type': ''}, 
            {'pub_date': '', 'volume': '(13/72) Education', 'issue': '', 'type': ''}, 
            {'pub_date': '', 'volume': '(13/72) Language-Literature', 'issue': '', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'Sayı: 24', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'Issue 11, Supplement 6', 'type': ''}, 
            {'pub_date': '', 'volume': '', 'issue': 'Issue 2. pp. 94-185', 'type': ''},
            {'pub_date': '', 'volume': '5', 'issue': '6', 'type': ''},
            {'pub_date': '', 'volume': 'Issue 2. pp. 94-185', 'issue': '6', 'type': ''},
            {'pub_date': '', 'volume': '', 'issue': '6', 'type': ''},
            {'pub_date': '', 'volume': '5', 'issue': '', 'type': ''},
            {'pub_date': '', 'volume': '', 'issue': '', 'type': ''}
        ]
        self.assertEqual(invalid_vi_rows, expected_output)
    
    def test_remove_ascii(self):
        clean_strings = []
        broken_strings = ['5â\x80\x926']
        for string in broken_strings:
            clean_strings.append(Cleaner(string).remove_ascii())
        expected_output = ['5 6']
        self.assertEqual(clean_strings, expected_output)


if __name__ == '__main__': # pragma: no cover
    unittest.main()