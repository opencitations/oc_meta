import unittest, os, shutil
from meta.orcid.index_orcid_doi import Index_orcid_doi
from csv import DictReader, DictWriter
from pprint import pprint

CSV_PATH = 'meta\\tdd\\index_orcid_doi\\output'
SUMMARIES_PATH = 'meta\\tdd\\index_orcid_doi\\orcid'

def load_files_from_dir(dir:str):
    output = list()
    for dir, _, files in os.walk(dir):
        for file in files:
            output.extend(list(DictReader(open(os.path.join(dir, file), 'r', encoding='utf-8'))))
    return output


class test_Index_orcid_doi(unittest.TestCase):
    def test_explorer(self):
        iOd = Index_orcid_doi(output_path=CSV_PATH, verbose=False)
        iOd.explorer(summaries_path=SUMMARIES_PATH)
        output = load_files_from_dir(CSV_PATH)
        expected_output = [
            {'id': 'None', 'value': '[0000-0001-5002-1000]'},
            {'id': '10.1016/j.indcrop.2020.112103', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2019/3213521', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.11.028', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.03.004', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1186/s13568-016-0300-2', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.toxicon.2014.04.010', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2014/691742', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1002/rmv.2149', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.01.147', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.02.044', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.ijcard.2016.06.064', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.4103/1319-2442.190782', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1053/j.jrn.2015.04.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.26719/2015.21.5.354', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ckj/sfu046', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s00393-012-1058-9', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1159/000356118', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1111/1756-185x.12007', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s11255-011-0007-x', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.jbspin.2011.06.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ndt/gfq089', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}
        ]
        shutil.rmtree(CSV_PATH)
        self.assertEqual(output, expected_output)

    def test_cache(self):
        os.mkdir(CSV_PATH)
        with open(os.path.join(CSV_PATH, '0.csv'), 'w', encoding='utf-8') as output_file:
            data_to_write = [            
                {'id': '10.1002/rmv.2149', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.transproceed.2019.01.147', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.transproceed.2019.02.044', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.ijcard.2016.06.064', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.4103/1319-2442.190782', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1053/j.jrn.2015.04.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.26719/2015.21.5.354', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1093/ckj/sfu046', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1007/s00393-012-1058-9', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1159/000356118', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1111/1756-185x.12007', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1007/s11255-011-0007-x', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.jbspin.2011.06.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1093/ndt/gfq089', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}
            ]
            dict_writer = DictWriter(output_file, ['id', 'value'])
            dict_writer.writeheader()
            dict_writer.writerows(data_to_write)
        iOd = Index_orcid_doi(output_path=CSV_PATH, verbose=False)
        cache = iOd.cache
        iOd.explorer(summaries_path=SUMMARIES_PATH)
        output = load_files_from_dir(CSV_PATH)
        expected_output = [
            {'id': '10.1002/rmv.2149', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.01.147', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.02.044', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.ijcard.2016.06.064', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.4103/1319-2442.190782', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1053/j.jrn.2015.04.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.26719/2015.21.5.354', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ckj/sfu046', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s00393-012-1058-9', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1159/000356118', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1111/1756-185x.12007', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s11255-011-0007-x', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.jbspin.2011.06.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ndt/gfq089', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'},
            {'id': 'None', 'value': '[0000-0001-5002-1000]'},
            {'id': '10.1016/j.indcrop.2020.112103', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2019/3213521', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.11.028', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.03.004', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1186/s13568-016-0300-2', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.toxicon.2014.04.010', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2014/691742', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
        ]
        shutil.rmtree(CSV_PATH)
        self.assertEqual((output, cache), (expected_output, {'0000-0001-5650-3000'}))

    def test_low_memory(self):
        iOd = Index_orcid_doi(output_path=CSV_PATH, low_memory=True, verbose=False)
        iOd.explorer(summaries_path=SUMMARIES_PATH)
        output = load_files_from_dir(CSV_PATH)
        expected_output = [
            {'id': 'None', 'value': '[0000-0001-5002-1000]'},
            {'id': '10.1016/j.indcrop.2020.112103', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2019/3213521', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.11.028', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.03.004', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1186/s13568-016-0300-2', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.toxicon.2014.04.010', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2014/691742', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1002/rmv.2149', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.01.147', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.02.044', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.ijcard.2016.06.064', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.4103/1319-2442.190782', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1053/j.jrn.2015.04.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.26719/2015.21.5.354', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ckj/sfu046', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s00393-012-1058-9', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1159/000356118', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1111/1756-185x.12007', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s11255-011-0007-x', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.jbspin.2011.06.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ndt/gfq089', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}
        ]
        shutil.rmtree(CSV_PATH)
        self.assertEqual(output, expected_output)

    def test_cache_low_memory(self):
        os.mkdir(CSV_PATH)
        with open(os.path.join(CSV_PATH, '0.csv'), 'w', encoding='utf-8') as output_file:
            data_to_write = [            
                {'id': '10.1002/rmv.2149', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.transproceed.2019.01.147', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.transproceed.2019.02.044', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.ijcard.2016.06.064', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.4103/1319-2442.190782', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1053/j.jrn.2015.04.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.26719/2015.21.5.354', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1093/ckj/sfu046', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1007/s00393-012-1058-9', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1159/000356118', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1111/1756-185x.12007', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1007/s11255-011-0007-x', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1016/j.jbspin.2011.06.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
                {'id': '10.1093/ndt/gfq089', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}
            ]
            dict_writer = DictWriter(output_file, ['id', 'value'])
            dict_writer.writeheader()
            dict_writer.writerows(data_to_write)
        iOd = Index_orcid_doi(output_path=CSV_PATH, low_memory=True, verbose=False)
        cache = iOd.cache
        iOd.explorer(summaries_path=SUMMARIES_PATH)
        output = load_files_from_dir(CSV_PATH)
        expected_output = [
            {'id': '10.1002/rmv.2149', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.01.147', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.transproceed.2019.02.044', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.ijcard.2016.06.064', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.4103/1319-2442.190782', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1053/j.jrn.2015.04.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.26719/2015.21.5.354', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ckj/sfu046', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s00393-012-1058-9', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1159/000356118', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1111/1756-185x.12007', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1007/s11255-011-0007-x', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1016/j.jbspin.2011.06.009', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'}, 
            {'id': '10.1093/ndt/gfq089', 'value': 'NasrAllah, Mohamed M [0000-0001-5650-3000]'},
            {'id': 'None', 'value': '[0000-0001-5002-1000]'},
            {'id': '10.1016/j.indcrop.2020.112103', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2019/3213521', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.11.028', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.03.004', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1186/s13568-016-0300-2', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.toxicon.2014.04.010', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2014/691742', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
        ]
        shutil.rmtree(CSV_PATH)
        self.assertEqual((output, cache), (expected_output, {'0000-0001-5650-3000'}))

if __name__ == '__main__':
    unittest.main()
