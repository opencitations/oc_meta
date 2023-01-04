import json
import math
import os.path
import shutil
import unittest
from os import makedirs, sep, walk
from os.path import exists

import pandas as pd

from oc_meta.preprocessing.datacite import DatacitePreProcessing
from oc_meta.preprocessing.nih import NIHPreProcessing


class PreprocessingTest(unittest.TestCase):
        def setUp(self):
            self._input_dir_nih = "test/preprocess/data_nih"
            self._output_dir_nih = "test/preprocess/tmp_data_nih"
            self._input_dir_dc = "test/preprocess/data_datacite"
            self._output_dir_dc_lm = "test/preprocess/tmp_data_datacite_lm"
            self._output_dir_dc_nlm = "test/preprocess/tmp_data_datacite_nlm"
            self._interval = 78
            self._relation_type_datacite = ["references", "isreferencedby", "cites", "iscitedby"]

        def test_nih_preprocessing(self):
            self._nih_pp = NIHPreProcessing(self._input_dir_nih, self._output_dir_nih, self._interval)
            if exists(self._output_dir_nih):
                shutil.rmtree(self._output_dir_nih)
            makedirs(self._output_dir_nih)
            self._nih_pp.split_input()
            len_lines = 0
            for file in (self._nih_pp.get_all_files(self._input_dir_nih, self._nih_pp._req_type))[0]:
                len_lines += len(pd.read_csv(file))
            number_of_files_produced = len_lines // self._interval
            if len_lines % self._interval != 0:
                number_of_files_produced += 1
            self.assertTrue(len(self._nih_pp.get_all_files(self._output_dir_nih, self._nih_pp._req_type)[0]) > 0)
            self.assertEqual(len(self._nih_pp.get_all_files(self._output_dir_nih, self._nih_pp._req_type)[0]), number_of_files_produced)


        def test_dc_preprocessing(self):
            self._dc_pp = DatacitePreProcessing(self._input_dir_dc, self._output_dir_dc_lm, self._interval)
            if exists(self._output_dir_dc_lm):
                shutil.rmtree(self._output_dir_dc_lm)
            makedirs(self._output_dir_dc_lm)
            self._dc_pp.split_input()
            len_lines_input = 0
            for file in self._dc_pp.get_all_files(self._input_dir_dc, self._dc_pp._req_type)[0]:
                f = open(file, encoding="utf8")
                lines_with_relids = [json.loads(line) for line in f if json.loads(line).get("attributes").get("relatedIdentifiers")]
                lines_with_citations = []
                if lines_with_relids:
                    lines_with_needed_fields = [line for line in lines_with_relids if [i for i in line["attributes"]["relatedIdentifiers"] if (i.get("relatedIdentifierType") and i.get("relationType") and i.get("relatedIdentifier"))]]
                    if lines_with_needed_fields:
                        lines_with_citations = [line for line in lines_with_needed_fields if [i for i in line["attributes"]["relatedIdentifiers"] if (i["relatedIdentifierType"].lower()=="doi" and i["relationType"].lower() in self._relation_type_datacite)]]
                len_lines_input = len(lines_with_citations)
                f.close()
            len_out_files = len([name for name in os.listdir(self._output_dir_dc_lm) if os.path.isfile(os.path.join(self._output_dir_dc_lm, name))])

            self.assertTrue(len(self._dc_pp.get_all_files(self._output_dir_dc_lm, self._dc_pp._req_type)[0]) > 0)
            self.assertEqual(math.ceil(len_lines_input/self._interval), len_out_files)

        def test_dc_preprocessing_no_low_memory(self):
            self._dc_pp = DatacitePreProcessing(self._input_dir_dc, self._output_dir_dc_nlm, self._interval, low_memo=False)
            if exists(self._output_dir_dc_nlm):
                shutil.rmtree(self._output_dir_dc_nlm)
            makedirs(self._output_dir_dc_nlm)
            self._dc_pp.split_input()
            len_lines_input = 0
            for file in self._dc_pp.get_all_files(self._input_dir_dc, self._dc_pp._req_type)[0]:
                f = open(file, encoding="utf8")
                lines_with_relids = [json.loads(line) for line in f if json.loads(line).get("attributes").get("relatedIdentifiers")]
                lines_with_citations = []
                if lines_with_relids:
                    lines_with_needed_fields = [line for line in lines_with_relids if [i for i in line["attributes"]["relatedIdentifiers"] if (i.get("relatedIdentifierType") and i.get("relationType") and i.get("relatedIdentifier"))]]
                    if lines_with_needed_fields:
                        lines_with_citations = [line for line in lines_with_needed_fields if [i for i in line["attributes"]["relatedIdentifiers"] if (i["relatedIdentifierType"].lower()=="doi" and i["relationType"].lower() in self._relation_type_datacite)]]
                len_lines_input = len(lines_with_citations)
                f.close()

            len_out_files = len([name for name in os.listdir(self._output_dir_dc_nlm) if os.path.isfile(os.path.join(self._output_dir_dc_nlm, name))])

            self.assertTrue(len(self._dc_pp.get_all_files(self._output_dir_dc_nlm, self._dc_pp._req_type)[0]) > 0)
            self.assertEqual(math.ceil(len_lines_input/self._interval), len_out_files)



if __name__ == '__main__':
    unittest.main()