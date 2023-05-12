#!python
# Copyright 2019, Silvio Peroni <essepuntato@gmail.com>
# Copyright 2022, Giuseppe Grieco <giuseppe.grieco3@unibo.it>, Arianna Moretti <arianna.moretti4@unibo.it>, Elia Rizzetto <elia.rizzetto@studio.unibo.it>, Arcangelo Massari <arcangelo.massari@unibo.it>
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


import json
import unittest
from os import makedirs
from os.path import exists, join

from oc_idmanager import *
from oc_idmanager.url import URLManager
from oc_idmanager.jid import JIDManager

class IdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")
        self.valid_doi_1 = "10.1108/jd-12-2013-0166"
        self.valid_doi_2 = "10.1130/2015.2513(00)"
        self.invalid_doi_1 = "10.1108/12-2013-0166"
        self.invalid_doi_2 = "10.1371"

        # class extension for pubmedid
        self.valid_pmid_1 = "2942070"
        self.valid_pmid_2 = "1509982"
        self.invalid_pmid_1 = "0067308798798"
        self.invalid_pmid_2 = "pmid:174777777777"

        test_dir = join("test", "data")
        with open(join(test_dir, "glob.json"), encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_issn_1 = "2376-5992"
        self.valid_issn_2 = "1474-175X"
        self.invalid_issn_1 = "2376-599C"
        self.invalid_issn_2 = "2376-5995"
        self.invalid_issn_3 = "2376-599"

        self.valid_orcid_1 = "0000-0003-0530-4305"
        self.valid_orcid_2 = "0000-0001-5506-523X"
        self.invalid_orcid_1 = "0000-0003-0530-430C"
        self.invalid_orcid_2 = "0000-0001-5506-5232"
        self.invalid_orcid_3 = "0000-0001-5506-523"
        self.invalid_orcid_4 = "1-5506-5232"

        self.valid_isbn_1 = "9780134093413"
        self.valid_isbn_2 = "978-3-16-148410-0"
        self.valid_isbn_3 = "0-19-852663-6"
        self.invalid_isbn_1 = "0-19-850000-6"
        self.invalid_isbn_2 = "978-3-16-148410-99"

        self.valid_wikidata_1 = "Q34433"
        self.valid_wikidata_2 = "Q24698708"
        self.valid_wikidata_3 = "Q15767074"
        self.invalid_wikidata_1 = "Q34433Q345"
        self.invalid_wikidata_3 = "Q12"  # not existing yet

        self.valid_wikipedia_1 = "30456"
        self.valid_wikipedia_2 = "43744177" # category page
        self.invalid_wikipedia_1 = "01267548"
        self.invalid_wikipedia_2 = "Berlin_Wall"

        self.valid_url_1 = "https://datacite.org/"
        self.valid_url_2 = "opencitations.net"
        self.valid_url_3 = "https://www.nih.gov/"
        self.valid_url_4 = "https://it.wikipedia.org/wiki/Muro di Berlino"
        self.invalid_url_1 = "https://www.nih.gov/invalid_url"
        self.invalid_url_2 = "opencitations.net/not a real page .org"  # not existing yet

        self.valid_ror_1 = "https://ror.org/040jc3p57"
        self.valid_ror_2 = "01111rn36"
        self.invalid_ror_1 = "la673822"
        self.invalid_ror_2 = ".org/560jc3p57"

        self.valid_viaf_1 = "5604148947771454950004"
        self.valid_viaf_2 = "234145033"
        self.invalid_viaf_1 = "012517637138"

        self.valid_pmcid_1 = "PMC8384044"
        self.valid_pmcid_2 = "PMC6716460"
        self.invalid_pmcid_1 = "0128564"
        self.invalid_pmcid_2 = "PMC6716"

        self.valid_jid_1 = "otoljpn1970"
        self.valid_jid_2 = "jscej1944b"
        self.valid_jid_3 = "japeoj" #SYS_ERR_009
        self.invalid_jid_1 = "hjmee"
        self.invalid_jid_2 = "jee1973e"
        
    
    def test_pmcid_normalise(self):
        pcm = PMCIDManager()
        self.assertEqual(
            pcm.normalise(self.valid_pmcid_1),
            pcm.normalise(' ' + self.valid_pmcid_1),
        )
        self.assertEqual(
            pcm.normalise(self.valid_pmcid_2),
            pcm.normalise("https://www.ncbi.nlm.nih.gov/pmc/articles/" + self.valid_pmcid_2),
        )

    def test_pmcid_is_valid(self):
        pcm = PMCIDManager()
        self.assertTrue(pcm.is_valid(self.valid_pmcid_1))
        self.assertTrue(pcm.is_valid(self.valid_pmcid_2))
        self.assertFalse(pcm.is_valid(self.invalid_pmcid_1))
        self.assertFalse(pcm.is_valid(self.invalid_pmcid_2))
    def test_viaf_normalise(self):
        vm = ViafManager()
        self.assertEqual(
            vm.normalise(self.valid_viaf_1),
            vm.normalise(' ' + self.valid_viaf_1),
        )

    def test_viaf_is_valid(self):
        vm = ViafManager()
        self.assertTrue(vm.is_valid(self.valid_viaf_1))
        self.assertTrue(vm.is_valid(self.valid_viaf_2))
        self.assertFalse(vm.is_valid(self.invalid_viaf_1))

    def test_ror_normalise(self):
        rm = RORManager()
        self.assertEqual(
            rm.normalise(self.valid_ror_1),
            rm.normalise(self.valid_ror_1.replace("https://", "")),
        )
        self.assertEqual(
            rm.normalise(self.valid_ror_2),
            rm.normalise("https://ror.org/" + self.valid_ror_2),
        )

    def test_ror_is_valid(self):
        rm = RORManager()
        self.assertTrue(rm.is_valid(self.valid_ror_1))
        self.assertTrue(rm.is_valid(self.valid_ror_2))
        self.assertFalse(rm.is_valid(self.invalid_ror_1))
        self.assertFalse(rm.is_valid(self.invalid_ror_2))

    def test_url_normalise(self):
        um = URLManager()
        self.assertEqual(
            um.normalise(self.valid_url_1),
            um.normalise(self.valid_url_1.replace("https://.", "https://www.")),
        )
        self.assertEqual(
            um.normalise(self.valid_url_2),
            um.normalise("https://" + self.valid_url_2),
        )
        self.assertEqual(
            um.normalise(self.valid_url_3),
            um.normalise(
                self.valid_url_3.replace("https://www.", "https://")
            ),
        )

    def test_url_valid(self):
        um_nofile = URLManager()
        self.assertTrue(um_nofile.is_valid(self.valid_url_1))
        self.assertTrue(um_nofile.is_valid(self.valid_url_2))
        self.assertTrue(um_nofile.is_valid(self.valid_url_3))
        self.assertTrue(um_nofile.is_valid(self.valid_url_4))
        self.assertFalse(um_nofile.is_valid(self.invalid_url_1))
        self.assertFalse(um_nofile.is_valid(self.invalid_url_2))

        um_file = URLManager(self.data, use_api_service=False)
        self.assertTrue(um_file.normalise(self.valid_url_1, include_prefix=True) in self.data)
        self.assertTrue(um_file.normalise(self.valid_url_2, include_prefix=True) in self.data)

        clean_data = {}

        um_nofile_noapi = URLManager(clean_data, use_api_service=False)
        self.assertTrue(um_nofile_noapi.is_valid(self.valid_url_1))
        self.assertTrue(um_nofile_noapi.is_valid(self.invalid_url_1))

    def test_doi_normalise(self):
        dm = DOIManager()
        self.assertEqual(
            self.valid_doi_1,
            dm.normalise(self.valid_doi_1.upper().replace("10.", "doi: 10. ")),
        )
        self.assertEqual(
            self.valid_doi_1,
            dm.normalise(self.valid_doi_1.upper().replace("10.", "doi:10.")),
        )
        self.assertEqual(
            self.valid_doi_1,
            dm.normalise(
                self.valid_doi_1.upper().replace("10.", "https://doi.org/10.")
            ),
        )

    def test_doi_is_valid(self):
        dm_nofile = DOIManager()
        self.assertTrue(dm_nofile.is_valid(self.valid_doi_1))
        self.assertTrue(dm_nofile.is_valid(self.valid_doi_2))
        self.assertFalse(dm_nofile.is_valid(self.invalid_doi_1))
        self.assertFalse(dm_nofile.is_valid(self.invalid_doi_2))

        dm_file = DOIManager(self.data, use_api_service=False)
        self.assertTrue(dm_file.normalise(self.valid_doi_1, include_prefix=True) in self.data)
        self.assertTrue(dm_file.normalise(self.invalid_doi_1, include_prefix=True) in self.data)
        self.assertTrue(dm_file.is_valid(self.valid_doi_1))
        self.assertFalse(dm_file.is_valid(self.invalid_doi_1))

        clean_data = {}
        dm_nofile_noapi = DOIManager(clean_data, use_api_service=False)
        self.assertTrue(dm_nofile_noapi.is_valid(self.valid_doi_1))
        self.assertTrue(dm_nofile_noapi.is_valid(self.invalid_doi_1))

    def test_pmid_normalise(self):
        pm = PMIDManager()
        self.assertEqual(
            self.valid_pmid_1, pm.normalise(self.valid_pmid_1.replace("", "pmid:"))
        )
        self.assertEqual(
            self.valid_pmid_1, pm.normalise(self.valid_pmid_1.replace("", " "))
        )
        self.assertEqual(
            self.valid_pmid_1,
            pm.normalise("https://pubmed.ncbi.nlm.nih.gov/" + self.valid_pmid_1),
        )
        self.assertEqual(self.valid_pmid_2, pm.normalise("000" + self.valid_pmid_2))

    def test_pmid_is_valid(self):
        pm_nofile = PMIDManager()
        self.assertTrue(pm_nofile.is_valid(self.valid_pmid_1))
        self.assertTrue(pm_nofile.is_valid(self.valid_pmid_2))
        self.assertFalse(pm_nofile.is_valid(self.invalid_pmid_1))
        self.assertFalse(pm_nofile.is_valid(self.invalid_pmid_2))

        pm_file = PMIDManager(self.data, use_api_service=False)
        self.assertTrue(pm_file.normalise(self.valid_pmid_1, include_prefix=True) in self.data)
        self.assertTrue(pm_file.normalise(self.invalid_pmid_1, include_prefix=True) in self.data)
        self.assertTrue(pm_file.is_valid(self.valid_pmid_1))
        self.assertFalse(pm_file.is_valid(self.invalid_pmid_1))

        clean_data = {}
        pm_nofile_noapi = PMIDManager(clean_data, use_api_service=False)
        self.assertTrue(pm_nofile_noapi.is_valid(self.valid_pmid_1))
        self.assertTrue(pm_nofile_noapi.is_valid(self.invalid_pmid_1))

    def test_issn_normalise(self):
        im = ISSNManager()
        self.assertEqual(
            self.valid_issn_1, im.normalise(self.valid_issn_1.replace("-", "  "))
        )
        self.assertEqual(
            self.valid_issn_2, im.normalise(self.valid_issn_2.replace("-", "  "))
        )
        self.assertEqual(
            self.invalid_issn_3, im.normalise(self.invalid_issn_3.replace("-", "  "))
        )

    def test_issn_is_valid(self):
        im = ISSNManager()
        self.assertTrue(im.is_valid(self.valid_issn_1))
        self.assertTrue(im.is_valid(self.valid_issn_2))
        self.assertFalse(im.is_valid(self.invalid_issn_1))
        self.assertFalse(im.is_valid(self.invalid_issn_2))
        self.assertFalse(im.is_valid(self.invalid_issn_3))

        im_file = ISSNManager(self.data)
        self.assertTrue(im_file.normalise(self.valid_issn_1, include_prefix=True) in self.data)
        self.assertTrue(im_file.normalise(self.valid_issn_2, include_prefix=True) in self.data)
        self.assertTrue(im_file.normalise(self.invalid_issn_2, include_prefix=True) in self.data)
        self.assertTrue(im_file.is_valid((im_file.normalise(self.valid_issn_1, include_prefix=True))))
        self.assertTrue(im_file.is_valid((im_file.normalise(self.valid_issn_2, include_prefix=True))))
        self.assertFalse(im_file.is_valid((im_file.normalise(self.invalid_issn_2, include_prefix=True))))

    def test_orcid_normalise(self):
        om = ORCIDManager()
        self.assertEqual(
            self.valid_orcid_1, om.normalise(self.valid_orcid_1.replace("-", "  "))
        )
        self.assertEqual(
            self.valid_orcid_1, om.normalise("https://orcid.org/" + self.valid_orcid_1)
        )
        self.assertEqual(
            self.valid_orcid_2, om.normalise(self.valid_orcid_2.replace("-", "  "))
        )
        self.assertEqual(
            self.invalid_orcid_3, om.normalise(self.invalid_orcid_3.replace("-", "  "))
        )

    def test_orcid_is_valid(self):
        om = ORCIDManager()
        self.assertTrue(om.is_valid(self.valid_orcid_1))
        self.assertTrue(om.is_valid(self.valid_orcid_2))
        self.assertFalse(om.is_valid(self.invalid_orcid_1))
        self.assertFalse(om.is_valid(self.invalid_orcid_2))
        self.assertFalse(om.is_valid(self.invalid_orcid_3))
        self.assertFalse(om.is_valid(self.invalid_orcid_4))

        om_file = ORCIDManager(self.data, use_api_service=False)
        self.assertTrue(om_file.normalise(self.valid_orcid_1, include_prefix=True) in self.data)
        self.assertTrue(om_file.normalise(self.valid_orcid_2, include_prefix=True) in self.data)
        self.assertTrue(om_file.is_valid(om_file.normalise(self.valid_orcid_1, include_prefix=True)))
        self.assertTrue(om_file.is_valid(om_file.normalise(self.valid_orcid_2, include_prefix=True)))

        clean_data = {}
        om_nofile_noapi = ORCIDManager(clean_data, use_api_service=False)
        self.assertTrue(om_nofile_noapi.is_valid(self.valid_orcid_1))
        self.assertTrue(om_nofile_noapi.is_valid(self.valid_orcid_2))

    def test_isbn_normalise(self):
        im = ISBNManager()
        self.assertEqual(
            self.valid_isbn_1, im.normalise("978-0-13-409341-3")
        )
        self.assertEqual(
            self.valid_isbn_1, im.normalise("ISBN" + self.valid_isbn_1)
        )
        self.assertEqual(
            im.normalise(self.valid_isbn_2), im.normalise(self.valid_isbn_2.replace("-", "  "))
        )

    def test_isbn_is_valid(self):
        im = ISBNManager()
        self.assertTrue(im.is_valid(self.valid_isbn_1))
        self.assertTrue(im.is_valid(self.valid_isbn_2))
        self.assertTrue(im.is_valid(self.valid_isbn_3))
        self.assertFalse(im.is_valid(self.invalid_isbn_2))
        self.assertFalse(im.is_valid(self.invalid_isbn_1))

        im_file = ISBNManager(self.data)
        self.assertTrue(im_file.normalise(self.valid_isbn_1, include_prefix=True) in self.data)
        self.assertTrue(im_file.normalise(self.valid_isbn_2, include_prefix=True) in self.data)
        self.assertTrue(im_file.normalise(self.invalid_isbn_2, include_prefix=True) in self.data)
        self.assertTrue(im_file.is_valid((im_file.normalise(self.valid_isbn_1, include_prefix=True))))
        self.assertTrue(im_file.is_valid((im_file.normalise(self.valid_isbn_2, include_prefix=True))))
        self.assertFalse(im_file.is_valid((im_file.normalise(self.invalid_isbn_2, include_prefix=True))))

    def test_wikipedia_normalise(self):
        wpm = WikipediaManager()
        self.assertTrue(
            self.valid_wikipedia_1,
            wpm.normalise("30456")
        )
        self.assertTrue(
            self.valid_wikipedia_2,
            wpm.normalise(self.valid_wikipedia_2)
        )
        self.assertTrue(
            self.valid_wikipedia_2,
            wpm.normalise("wikipedia" + self.valid_wikipedia_2)
        )

    def test_wikipedia_is_valid(self):
        wpm = WikipediaManager()
        self.assertTrue(wpm.is_valid(self.valid_wikipedia_1))
        self.assertTrue(wpm.is_valid(self.valid_wikipedia_2))
        self.assertFalse(wpm.is_valid(self.invalid_wikipedia_1))
        self.assertFalse(wpm.is_valid(self.invalid_wikipedia_2))

        wpm_file = WikipediaManager(self.data)
        self.assertTrue(wpm_file.normalise(self.valid_wikipedia_1, include_prefix=True) in self.data)
        self.assertTrue(wpm_file.normalise(self.valid_wikipedia_2, include_prefix=True) in self.data)
        self.assertTrue(wpm_file.normalise(self.invalid_wikipedia_1, include_prefix=True) in self.data)
        self.assertTrue(wpm_file.is_valid((wpm_file.normalise(self.valid_wikipedia_1, include_prefix=True))))
        self.assertTrue(wpm_file.is_valid((wpm_file.normalise(self.valid_wikipedia_2, include_prefix=True))))
        self.assertFalse(wpm_file.is_valid((wpm_file.normalise(self.invalid_wikipedia_1, include_prefix=True))))

        clean_data = {}
        wpm_nofile_noapi = WikipediaManager(clean_data, use_api_service=False)
        self.assertTrue(wpm_nofile_noapi.is_valid(self.valid_wikipedia_1))
        self.assertTrue(wpm_nofile_noapi.is_valid(self.valid_wikipedia_2))

    def test_wikidata_normalise(self):
        wdm = WikidataManager()
        self.assertTrue(
            self.valid_wikidata_1,
            wdm.normalise(self.valid_wikidata_1.replace("Q", "https://www.wikidata.org/wiki/Q"))
        )
        self.assertTrue(
            self.valid_wikidata_2,
            wdm.normalise(self.valid_wikidata_2)
        )
        self.assertTrue(
            self.valid_wikidata_2,
            wdm.normalise(self.valid_wikidata_2.replace("Q", "wikidata: Q"))
        )
        self.assertTrue(
            self.valid_wikidata_3,
            wdm.normalise((self.valid_wikidata_3.replace("Q", "Q ")))
        )

    def test_wikidata_is_valid(self):
        wdm = WikidataManager()
        self.assertTrue(wdm.is_valid(self.valid_wikidata_1))
        self.assertTrue(wdm.is_valid(self.valid_wikidata_2))
        self.assertTrue(wdm.is_valid(self.valid_wikidata_3))
        self.assertFalse(wdm.is_valid(self.invalid_wikidata_1))
        self.assertFalse(wdm.is_valid(self.invalid_wikidata_3))

        wdm_file = WikidataManager(self.data)
        self.assertTrue(wdm_file.normalise(self.valid_wikidata_1, include_prefix=True) in self.data)
        self.assertTrue(wdm_file.normalise(self.valid_wikidata_2, include_prefix=True) in self.data)
        self.assertTrue(wdm_file.normalise(self.invalid_wikidata_3, include_prefix=True) in self.data)
        self.assertTrue(wdm_file.is_valid((wdm_file.normalise(self.valid_wikidata_1, include_prefix=True))))
        self.assertTrue(wdm_file.is_valid((wdm_file.normalise(self.valid_wikidata_2, include_prefix=True))))
        self.assertFalse(wdm_file.is_valid((wdm_file.normalise(self.invalid_wikidata_3, include_prefix=True))))

        clean_data = {}
        wdm_nofile_noapi = WikidataManager(clean_data, use_api_service=False)
        self.assertTrue(wdm_nofile_noapi.is_valid(self.valid_wikidata_1))
        self.assertTrue(wdm_nofile_noapi.is_valid(self.valid_wikidata_2))

    def test_jid_normalise(self):
        jm = JIDManager()
        self.assertEqual(
            self.valid_jid_1, jm.normalise(self.valid_jid_1.replace("", " "))
        )
        self.assertEqual(
            self.valid_jid_2, jm.normalise("jid:"+ self.valid_jid_2),
        )

    def test_jid_syntax_ok(self):
        jm=JIDManager()
        self.assertTrue(jm.syntax_ok(self.valid_jid_1))
        self.assertTrue(jm.syntax_ok(self.invalid_jid_1))
        self.assertFalse(jm.syntax_ok('1950'+self.valid_jid_1))


    def test_jid_is_valid(self):
        jm = JIDManager()
        self.assertTrue(jm.is_valid(self.valid_jid_1))
        self.assertTrue(jm.is_valid(self.valid_jid_2))
        self.assertFalse(jm.is_valid(self.invalid_jid_1))
        self.assertFalse(jm.is_valid(self.invalid_jid_2))
        
        jm_file = JIDManager(self.data)
        self.assertTrue(jm_file.normalise(self.valid_jid_1, include_prefix=True) in self.data)
        self.assertTrue(jm_file.normalise(self.valid_jid_2, include_prefix=True) in self.data)
        self.assertTrue(jm_file.normalise(self.valid_jid_3, include_prefix=True) in self.data)
        self.assertTrue(jm_file.is_valid(jm_file.normalise(self.valid_jid_1, include_prefix=True)))
        self.assertTrue(jm_file.is_valid(jm_file.normalise(self.valid_jid_2, include_prefix=True)))
        self.assertTrue(jm_file.is_valid(jm_file.normalise(self.valid_jid_3, include_prefix=True)))
        
        clean_data = {}
        jm_nofile_noapi = JIDManager(clean_data, use_api_service=False)
        self.assertTrue(jm_nofile_noapi.is_valid(self.valid_jid_1))
        self.assertTrue(jm_nofile_noapi.is_valid(self.invalid_jid_1))

    def test_jid_exists(self):
        with self.subTest(msg="get_extra_info = True, allow_extra_api=None"):
            jm=JIDManager()
            output = jm.exists(self.valid_jid_1, get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {"valid":True})
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info = True, allow_extra_api=None"):
            jm=JIDManager()
            output = jm.exists(self.valid_jid_3, get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {"valid":True})
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info = True, allow_extra_api=None"):
            jm=JIDManager()
            output = jm.exists(self.invalid_jid_1, get_extra_info=True, allow_extra_api=None)
            expected_output = (False, {"valid":False})
            self.assertEqual(output, expected_output)
        
    
    def test_exists(self):
        with self.subTest(msg="get_extra_info=True, allow_extra_api=None"):
            doi_manager = DOIManager()
            output = doi_manager.exists('10.1007/s11192-022-04367-w', get_extra_info=True, allow_extra_api=None)
            expected_output = (True, {'id': '10.1007/s11192-022-04367-w', 'valid': True, 'ra': 'unknown'})
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=False, allow_extra_api=None"):
            doi_manager = DOIManager()
            output = doi_manager.exists('10.1007/s11192-022-04367-w', get_extra_info=False, allow_extra_api=None)
            expected_output = True
            self.assertEqual(output, expected_output)
        with self.subTest(msg="get_extra_info=False, allow_extra_api='crossref'"):
            doi_manager = DOIManager()
            output = doi_manager.exists('10.1007/s11192-022-04367-w', get_extra_info=False, allow_extra_api='crossref')
            expected_output = True
            self.assertEqual(output, expected_output)

id = IdentifierManagerTest()

