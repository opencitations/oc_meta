# SPDX-FileCopyrightText: 2022-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from oc_meta.lib.cleaner import (
    clean_agent_name,
    clean_date,
    clean_name,
    clean_ra_list,
    clean_title,
    clean_volume_and_issue,
    normalize_hyphens,
    normalize_id,
    normalize_spaces,
    remove_ascii,
)
from oc_meta.lib.master_of_regex import split_name_and_ids


class TestCleaner:
    def test_clen_hyphen(self):
        broken_strings = ['100­101', '100−101', '100–101', '100–101', '100—101', '100⁃101', '100−101', '100➖101', '100Ⲻ101', '100﹘101']
        fixed_strings = list()
        for string in broken_strings:
            fixed_string = normalize_hyphens(string)
            fixed_strings.append(fixed_string)
        expected_output = ['100-101', '100-101', '100-101', '100-101', '100-101', '100-101', '100-101', '100-101', '100-101', '100-101']
        assert fixed_strings == expected_output
    
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
            outputs.append(clean_title(input))
        expected_output = [
            'OpenCitations, An Infrastructure Organization For Open Scholarship',
            'Opencitations, An Infrastructure Organization For Open Scholarship',
            'Opencitations, An Infrastructure Organization For Open Scholarship',
            'OpenCitations, An Infrastructure Organization For Open Scholarship',
            '""Agile"" "Knowledge" Graph Testing Ù Ò À With TESTaLOD (!Incredible!) Έτος 汉字',
            'Elsevier BV'
        ]
        assert outputs == expected_output
    
    def test_clean_date_all_valid(self):
        inputs = ['2020-13-50', '2020-02-50', '2020-02-11', '2020-12-12', '2000', '2000-12', '2000-13']
        outputs = list()
        for input in inputs:
            outputs.append(clean_date(input))
        expected_output = ['2020', '2020-02', '2020-02-11', '2020-12-12', '2000', '2000-12', '2000']
        assert outputs == expected_output

    def test_clean_date_all_invalid(self):
        inputs = ['100000-13-50', '02-11', '11', '100000', 'godopoli']
        outputs = list()
        for input in inputs:
            outputs.append(clean_date(input))
        expected_output = ['', '', '', '', '']
        assert outputs == expected_output
    
    def test_clean_name(self):
        names = ['Peroni, Silvio', 'Peroni, S.', '  Peroni   ,    Silvio  ', 'PERONI, SILVIO', '', 'peroni', 'peroni, Silvio', 'McSorley, Stephen', 'OECD', ',']
        outputs = list()
        for name in names:
            outputs.append(clean_name(name))
        expected_output = ['Peroni, Silvio', 'Peroni, S.', 'Peroni, Silvio', 'Peroni, Silvio', '', 'Peroni', 'Peroni, Silvio', 'McSorley, Stephen', 'Oecd', '']
        assert outputs == expected_output
    
    def test_clean_agent_name(self):
        names = ['Edward ].', 'Bernacki', 'Tom??&OV0165;', 'Gavin         E.', 'Andr[eacute]', 'Albers\u2010Miller', "O'Connor", 'O\u2019Connell', 'Gonz\u0301alez-Santiago', 'Gonz\u00e1lez-Benito', 'Andr&eacute;']
        outputs = list()
        for name in names:
            outputs.append(clean_agent_name(name))
        expected_output = ['Edward', 'Bernacki', 'Tom&OV0165', 'Gavin E.', 'Andreacute', 'Albers-Miller', "O'Connor", 'O’Connell', 'Gonźalez-Santiago', 'González-Benito', 'André']
        assert outputs == expected_output
    
    def test_clean_ra_list(self):
        names = ['Not Available, Not Available', 'Peroni, Not Available', 'Not Available, Silvio', 'Not Available', 'Peroni, Silvio', ',']
        output = clean_ra_list(names)
        expected_output = ['Peroni, ', 'Peroni, Silvio']
        assert output == expected_output

    def test_clean_ra_list_duplicates(self):
        names = ['Peroni, Silvio [orcid:0000-0003-0530-4305 viaf:1]', 'Peroni, Not Available', 'Peroni, Silvio [orcid:0000-0003-0530-4305]', 'Massari, Arcangelo']
        output = clean_ra_list(names)
        expected_output = ['Peroni, Silvio [orcid:0000-0003-0530-4305 viaf:1]', 'Peroni, ', 'Massari, Arcangelo']
        assert output == expected_output

    def test_clean_ra_list_remove_ids(self):
        names = ['Peroni, Silvio [orcid:0000-0003-0530-4305 viaf:1]', 'Peroni, Not Available', 'Perone, Silvio [orcid:0000-0003-0530-4305]', 'Massari, Arcangelo']
        output = clean_ra_list(names)
        expected_output = ['Peroni, Silvio [viaf:1]', 'Peroni, ', 'Perone, Silvio', 'Massari, Arcangelo']
        assert output == expected_output

    def test_clean_ra_list_only_ids(self):
        names = ['Peroni, Silvio [orcid:0000-0003-0530-4305]', '[orcid:0000-0003-0530-4305 viaf:1]', '[orcid:0000-0003-0530-4306]']
        output = clean_ra_list(names)
        expected_output = ['Peroni, Silvio', '[viaf:1]', '[orcid:0000-0003-0530-4306]']
        assert output == expected_output

    def test_clean_ra_list_strips_stray_brackets_in_bare_name(self):
        names = ['[Labour Party[', '[[foo]]', 'Acme ]Inc[']
        output = clean_ra_list(names)
        expected_output = ['Labour Party', 'foo', 'Acme Inc']
        assert output == expected_output

    def test_normalize_spaces(self):
        broken_strings = ['100\u0009101', '100\u00A0101', '100\u200B101', '100\u202F101']
        fixed_strings = list()
        for string in broken_strings:
            fixed_string = normalize_spaces(string)
            fixed_strings.append(fixed_string)
        expected_output = ['100 101', '100 101', '100 101', '100 101']
        assert fixed_strings == expected_output
    
    def test_normalize_id(self):
        identifiers = ['doi:10.1123/ijatt.2015-0070', 'doi:1', 'orcid:0000-0003-0530-4305', 'orcid:0000-0000', 'issn:1479-6708', 'issn:0000-0000', 'isbn:9783319403120', 'isbn:0000-0000']
        output = list()
        for id in identifiers:
            output.append(normalize_id(id))
        expected_output = ['doi:10.1123/ijatt.2015-0070', None, 'orcid:0000-0003-0530-4305', None, 'issn:1479-6708', None, 'isbn:9783319403120', None]
        assert output == expected_output
    
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
            clean_volume_and_issue(row)
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
        assert invalid_vi_rows == expected_output
    
    def test_remove_ascii(self):
        clean_strings = []
        broken_strings = ['5â\x80\x926']
        for string in broken_strings:
            clean_strings.append(remove_ascii(string))
        expected_output = ['5 6']
        assert clean_strings == expected_output


class TestNameAndIdsRegex:
    """Regex-level regression coverage for ``split_name_and_ids``."""

    def test_stray_bracket_before_ids_not_captured(self):
        # Regression: JaLC row `doi:10.11501/13834445` produced curated publisher
        # `[Labour Party[ [omid:ra/06047190147]`. The greedy id pattern used to
        # absorb the stray '[' giving ids = '[omid:ra/06047190147', which the
        # Creator then appended to base_iri -> invalid RA URI and crash.
        name, ids = split_name_and_ids('[Labour Party[ [omid:ra/06047190147]')
        assert name == '[Labour Party['
        assert ids == 'omid:ra/06047190147'

    def test_bare_name_without_brackets(self):
        assert split_name_and_ids('Peroni, Silvio') == ('Peroni, Silvio', '')

    def test_empty_string(self):
        assert split_name_and_ids('') == ('', '')

    def test_name_with_ids(self):
        assert split_name_and_ids('Peroni, Silvio [orcid:0000-0003-0530-4305]') == (
            'Peroni, Silvio',
            'orcid:0000-0003-0530-4305',
        )

    def test_multi_ra_returns_first_ids_block(self):
        _, ids = split_name_and_ids('A1 [orcid:111]; A2 [orcid:222]')
        assert ids == 'orcid:111'

    def test_ids_only(self):
        assert split_name_and_ids('[orcid:0000-0003-0530-4305 viaf:1]') == (
            '',
            'orcid:0000-0003-0530-4305 viaf:1',
        )