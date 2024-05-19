import unittest
import os
from oc_meta.run.gen_info_dir import explore_directories
from oc_ocdm.counter_handler.filesystem_counter_handler import FilesystemCounterHandler
from oc_ocdm.support import get_count
import shutil

class TestGenInfoDir(unittest.TestCase):
    
    def setUp(self):
        self.root_dir = os.path.join('test', 'gen_info_dir', 'rdf')
        self.output_dir = os.path.join('test', 'gen_info_dir', 'output')

    def tearDown(self):
        shutil.rmtree(self.output_dir)

    def test_explore_directories(self):
        explore_directories(self.root_dir, self.output_dir)
        creator_info_file_path = os.path.join(self.output_dir, "0670", "creator", "info_file_br.txt")
        curator_info_file_path = os.path.join(self.output_dir, "0670", "curator", "info_file_br.txt")
        self.assertTrue(os.path.exists(creator_info_file_path))
        self.assertTrue(os.path.exists(curator_info_file_path))
        with open(creator_info_file_path, "r") as f:
            lines = f.readlines()
            self.assertEqual(lines[0].strip(), "386000")

        with open(curator_info_file_path, "r") as f:
            lines = f.readlines()
            self.assertEqual(lines[0].strip(), "386000")
        
        prov_counter_handler = FilesystemCounterHandler(info_dir=os.path.join(self.output_dir, '0670', 'creator'), supplier_prefix="0670")
        counter_101 = prov_counter_handler.read_counter(entity_short_name="br", prov_short_name="se", identifier=int(get_count('https://w3id.org/oc/meta/br/0670101')), supplier_prefix="0670")
        counter_3 = prov_counter_handler.read_counter(entity_short_name="br", prov_short_name="se", identifier=int(get_count('https://w3id.org/oc/meta/br/06703')), supplier_prefix="0670")
        self.assertEqual(counter_101, 2)
        self.assertEqual(counter_3, 1)

if __name__ == "__main__":
    unittest.main()