#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2023 Arianna Moretti <arianna.moretti4@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import os.path
import shutil
import unittest
from os.path import join

from oc_meta.run.pubmed_process import *


class PubMedProcess(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        self.test_dir = join("test", "pubmed_process")
        self.output_dir = join(self.test_dir, "tmp")
        self.support_mat = join(self.test_dir, "support_mat")
        self.doi_orcid = join("test", "pubmed_processing", "iod")

        self.publishers_file = join(self.support_mat, "publishers.json")
        self.journals_file = join(self.support_mat, "journals.json")

        self.publishers_dir_todel = join(self.support_mat, "publishers")
        self.publishers_file_todel = join(self.publishers_dir_todel, "publishers.json")

        self.journals_dir_todel = join(self.support_mat, "journals")
        self.journals_file_todel = join(self.journals_dir_todel, "journals.json")

        self.madeup_data_dir = join(self.support_mat, "made_up_mat")
        self.madeup_publishers = join(self.madeup_data_dir, "publishers.json")
        self.madeup_journals = join(self.madeup_data_dir,"journals.json")
        self.madeup_input = join(self.madeup_data_dir,"input")
        self.madeup_iod = join(self.madeup_data_dir,"iod")

        self.input_dirt_short= join(self.test_dir,"csv_files_short")
        self.input_dirt_iod= join(self.test_dir,"csv_file_iod")
        self.input_dirt_sample= join(self.test_dir,"csv_files_sample")
        self.input_dirt_compr= join(self.test_dir,"CSV_iCiteMD_zipped.zip")

        self.processing_csv_row_base = os.path.join('test', 'pubmed_processing')
        self._id_orcid_data = os.path.join(self.processing_csv_row_base, 'iod')

    def test_preprocess_base(self):
        """Test base functionalities of the POCI processor for producing META csv tables"""
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        preprocess(pubmed_csv_dir=self.input_dirt_sample, publishers_filepath=self.publishers_file, journals_filepath= self.journals_file, csv_dir=self.output_dir, orcid_doi_filepath=self.doi_orcid)

        output = dict()
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                output[file] = list(csv.DictReader(f))
        expected_output= {
            'CSVFile_1.csv':
                [
                    {'id': 'pmid:118',
                     'title': 'Proceedings: Comparison of the effects of selective alpha and beta-receptor agonists on intracellular cyclic AMP levels and glycogen phosphorylase activity in guinea-pig liver.',
                     'author': 'D Osborn; D H Jenkinson',
                     'pub_date': '1975',
                     'venue': 'British journal of pharmacology [issn:0007-1188]',
                     'volume': '',
                     'issue': '',
                     'page': '',
                     'type': 'journal article',
                     'publisher': '',
                     'editor': ''},
                    {'id': 'pmid:120',
                     'title': 'Proceedings: Do anti-psychotic drugs act by dopamine receptor blockade in the nucleus accumbens.',
                     'author': 'T J Crow; J F Deakin; A Longden',
                     'pub_date': '1975',
                     'venue': 'British journal of pharmacology [issn:0007-1188]',
                     'volume': '',
                     'issue': '',
                     'page': '',
                     'type': 'journal article',
                     'publisher': '',
                     'editor': ''},
                    {'id': 'pmid:351 doi:10.2527/jas1975.4151249x',
                     'title': 'Analyses of rumen fluid from "sudden death", lactic acidotic and healthy cattle fed high concentrate ration.',
                     'author': 'J R Wilson; E E Bartley; H D Anthony; B E Brent; D A Sapienza; T E Chapman; A D Dayton; R J Milleret; R A Frey; R M Meyer',
                     'pub_date': '1975',
                     'venue': 'Journal of animal science [issn:0021-8812]',
                     'volume': '',
                     'issue': '',
                     'page': '',
                     'type': 'journal article',
                     'publisher': 'American Society of Animal Science (ASAS)',
                     'editor': ''},
                    {'id': 'pmid:352 doi:10.2527/jas1975.4151314x',
                     'title': 'Mitochondrial traits of muscle from stress-susceptible pigs.',
                     'author': 'D R Campion; J C Olson; D G Topel; L L Christian; D L Kuhlers',
                     'pub_date': '1975',
                     'venue': 'Journal of animal science [issn:0021-8812]',
                     'volume': '',
                     'issue': '',
                     'page': '',
                     'type': 'journal article',
                     'publisher': 'American Society of Animal Science (ASAS)',
                     'editor': ''},
                    {'id': 'pmid:353 doi:10.1152/jappl.1975.39.4.580',
                     'title': 'Local control of pulmonary resistance and lung compliance in the canine lung.',
                     'author': 'R L Coon; C C Rattenborg; J P Kampine',
                     'pub_date': '1975',
                     'venue': 'Journal of applied physiology [issn:0021-8987]',
                     'volume': '',
                     'issue': '',
                     'page': '',
                     'type': 'journal article',
                     'publisher': 'American Physiological Society',
                     'editor': ''}
                ]
        }

        elements_in_output = list()
        for l in output.values():
            for e in l:
                elements_in_output.append(e)

        elements_expected = list()
        for l in expected_output.values():
            for e in l:
                elements_expected.append(e)

        self.assertCountEqual(elements_in_output, elements_expected)
        shutil.rmtree(self.output_dir)

    def test_preprocess_interval_number(self):
        """Test that the processed rows are correctly distributed in output files, with respect to the
        interval number specified in input"""

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        preprocess(pubmed_csv_dir=self.input_dirt_sample, publishers_filepath=self.publishers_file, journals_filepath= self.journals_file, csv_dir=self.output_dir, orcid_doi_filepath=self.doi_orcid, interval=2)

        output = dict()
        n_files = 0
        for file in os.listdir(self.output_dir):
            n_files += 1
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                output[n_files] = len(list(csv.DictReader(f)))

        expected_files = 3
        expected_ents_per_file = 2
        last_file_ents = 1
        n_files_full = len([x for x in output.values() if x == 2])
        n_files_rem= len([x for x in output.values() if x == 1])
        self.assertEqual(expected_files, len(output.keys()))
        self.assertEqual(n_files_full, 2)
        self.assertEqual(n_files_rem, 1)
        self.assertTrue(max(output.values())==expected_ents_per_file)
        self.assertTrue(min(output.values())==last_file_ents)
        shutil.rmtree(self.output_dir)

    def test_preprocess_save_recovered_publishers(self):
        """Test that data is correctly recovered using API and stored in new support file, if no support material was provided in input"""
        if not os.path.exists(self.publishers_dir_todel):
            os.makedirs(self.publishers_dir_todel)
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        preprocess(pubmed_csv_dir=self.input_dirt_sample, publishers_filepath=self.publishers_file_todel, journals_filepath= self.journals_file, csv_dir=self.output_dir, orcid_doi_filepath=self.doi_orcid, interval=1)
        #test that the information processed was successfully saved each <interval> number of rows.
        prefixes_encountered = set()
        self.assertTrue(os.path.exists(self.publishers_file_todel))
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                ent_list = list(csv.DictReader(f))
                for e in ent_list:
                    if e.get("id"):
                        doi = [x for x in e.get("id").split(" ") if x.startswith("doi")]
                        if doi:
                            for d in doi:
                                pref = d.split('/')[0]
                                if pref:
                                    lenpref = len("doi:")
                                    pref = pref[lenpref:]
                                    prefixes_encountered.add(pref)

        with open(self.publishers_file_todel, "r") as dobj:
            pref_pub_dict = json.load(dobj)
        self.assertCountEqual(prefixes_encountered, pref_pub_dict.keys())

        shutil.rmtree(self.output_dir)
        shutil.rmtree(self.publishers_dir_todel)

    def test_preprocess_zip_input(self):
        """Test that the processed on zip compressed input"""

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        preprocess(pubmed_csv_dir=self.input_dirt_compr, publishers_filepath=self.publishers_file, journals_filepath= self.journals_file, csv_dir=self.output_dir, orcid_doi_filepath=self.doi_orcid, interval=200)

        output = dict()
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                output[file] = list(csv.DictReader(f))
        processed_lines = 0
        for k,v in output.items():
            processed_lines += len(v)
        self.assertEqual(processed_lines, 29)

        todel_path = ".".join(self.input_dirt_compr.split(".")[:-1])+"decompr_zip_dir"
        shutil.rmtree(todel_path)
        shutil.rmtree(self.output_dir)


    def test_preprocess_save_recovered_journals(self):
        """Test that data is correctly recovered using API and stored in new support file, if no support material was provided in input"""
        if not os.path.exists(self.journals_dir_todel):
            os.makedirs(self.journals_dir_todel)
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        preprocess(pubmed_csv_dir=self.input_dirt_sample, publishers_filepath=self.publishers_file, journals_filepath= self.journals_file_todel, csv_dir=self.output_dir, orcid_doi_filepath=self.doi_orcid, interval=1)
        #test that the information processed was successfully saved at each <interval> number of rows.
        issns_encountered = set()
        self.assertTrue(os.path.exists(self.journals_file_todel))

        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                ent_list = list(csv.DictReader(f))
                for e in ent_list:
                    if e.get("venue"):
                        if "[" in e.get("venue") and "]" in e.get("venue"):
                            split_venue_ids = e.get("venue").split("[")
                            keep_ids=split_venue_ids[1]
                            keep_ids = keep_ids.split("]")[0]
                            split_issn = keep_ids.split(" ")
                            issn = [x for x in split_issn if x.startswith("issn")]
                            if issn:
                                for i in issn:
                                    issns_encountered.add(i)

        with open(self.journals_file_todel, "r") as dobj:
            jour_issns_dict = json.load(dobj)
        issn_in_map_file = set()
        for k,v in jour_issns_dict.items():
            for e in v["issn"]:
                issn_in_map_file.add(e)

        self.assertCountEqual(issns_encountered, issn_in_map_file)

        shutil.rmtree(self.output_dir)
        shutil.rmtree(self.journals_dir_todel)

    def test_preprocess_support_data(self):
        """Test that the support material is correctly used, if provided. In particular, fake data is used in this test, in order to check that the information provided in support material is preferred to the use of API, when possible"""
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        preprocess(pubmed_csv_dir=self.input_dirt_short, publishers_filepath=self.madeup_publishers, journals_filepath= self.madeup_journals, csv_dir=self.output_dir, orcid_doi_filepath=self.doi_orcid, interval=1)
        #test that the information processed was successfully saved after each <interval> number of rows.
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                ent_list = list(csv.DictReader(f))
                for e in ent_list:
                    if "pmid:1" in e.get("id").split(" ") and "doi:10.1016/0006-2944(75)90147-7" in e.get("id").split(" "):
                        self.assertEqual(e.get("venue"), "Made Up Title 1 [issn:0001-000X]")
                        self.assertEqual(e.get("publisher"), "Made Up Publisher")
                    if "pmid:324" in e.get("id").split(" ") and "doi:10.1016/0019-2791(75)90174-3" in e.get("id").split(" "):
                        self.assertEqual(e.get("venue"), "Made Up Title 2 [issn:0000-0000]")
                        self.assertEqual(e.get("publisher"), "Made Up Publisher")


        shutil.rmtree(self.output_dir)

    def test_preprocess_id_orcid_map(self):
        """Test the id-orcid mapping information is correctly used to associate the RA name to its ORCID id, if provided. """
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        preprocess(pubmed_csv_dir=self.input_dirt_iod, publishers_filepath=self.publishers_file, journals_filepath= self.journals_file, csv_dir=self.output_dir, orcid_doi_filepath=self.doi_orcid, interval=1)
        #test that the information processed was successfully saved each <interval> number of rows.
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                ent_list = list(csv.DictReader(f))
                for e in ent_list:
                    if e.get("id") == "pmid:2 doi:10.1016/0006-291x(75)90482-9":
                        self.assertIn("Sarma, R H [orcid:0000-0000-0000-0000]", e.get("author"))
        shutil.rmtree(self.output_dir)

    def test_preprocess_id_orcid_map_with_homonyms(self):
        """Test the id-orcid mapping information is correctly used to associate the RA name to its ORCID id, if provided. """

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        preprocess(pubmed_csv_dir=self.madeup_input, publishers_filepath=self.publishers_file, journals_filepath= self.journals_file, csv_dir=self.output_dir, orcid_doi_filepath=self.madeup_iod, interval=1)
        # test that the information processed was successfully saved at each <interval> number of rows.
        processed_ents = []
        for file in os.listdir(self.output_dir):
            with open(os.path.join(self.output_dir, file), 'r', encoding='utf-8') as f:
                ent_list = list(csv.DictReader(f))
                for e in ent_list:
                    processed_ents.append(e)
        ent = processed_ents[0]
        expected_aut_cont = "K S Bose; R H Sarma; Sarma, Harold R. [orcid:0000-0000-0000-0005]; Sarma R. Henry Jack; Sarma, Roy Henry [orcid:0000-0000-0000-0000]"
        self.assertEqual(expected_aut_cont, ent.get("author"))

        shutil.rmtree(self.output_dir)

if __name__ == '__main__':
    unittest.main()
