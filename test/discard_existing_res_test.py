import os
import unittest
from shutil import rmtree

from fakeredis import FakeServer, FakeRedis

from oc_meta.lib.file_manager import get_csv_data
from oc_meta.run.discard_existing_res import discard_existing_res


class TestDiscardExistingRes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = FakeServer()
        cls.r = FakeRedis(server=cls.server, decode_responses=True)

    def test_discard_existing_res(self):
        preexisting_ids = {
            'pmid:23130068': 'omid:0601',
            'doi:10.1016/j.freeradbiomed.2008.02.008': 'omid:0602',
            'pmid:18342017': 'omid:0602',
            'pmid:10429655': 'omid:0604'
        }
        self.r.mset(preexisting_ids)
        discard_existing_res(os.path.join('test', 'discard_existing_res', 'input'), os.path.join('test', 'discard_existing_res', 'output'), r=self.r)
        output = get_csv_data(os.path.join('test', 'discard_existing_res', 'output', '1.csv'))
        expected_output = [
            {'id': 'arxiv:1406.6047v1', 'title': 'Efficient Algorithms for de novo Assembly of Alternative Splicing Events from RNA-seq Data', 'author': 'Sacomoto Gustavo', 'pub_date': '2014-01-01', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'arXiv', 'editor': ''}, 
            {'id': 'pmid:30635270', 'title': 'SOD1 activity threshold and TOR signalling modulate VAP(P58S) aggregation via reactive oxygen species-induced proteasomal degradation in a', 'author': 'Kriti Chaplot; Lokesh Pimpale; Balaji Ramalingam; Senthilkumar Deivasigamani; Siddhesh S Kamat; Girish S Ratnaparkhi', 'pub_date': '2018-07-11', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}, 
            {'id': 'pmid:10429655 doi:10.1111/j.1349-7006.1999.tb00794.x', 'title': 'Frameshift mutation of the STK11 gene in a sporadic gastrointestinal cancer with microsatellite instability.', 'author': 'Yusuke Nakamura; Yusuke Nakamura; Hidewaki Nakagawa; Kumiko Koyama; Morito Monden; Masao Kameyama; Shingi Imaoka; Shoji Nakamori', 'pub_date': '1999-06-01', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Wiley [crossref:311]', 'editor': ''}, 
            {'id': 'pmid:28212283 doi:10.3390/nu9020143', 'title': 'The Effects of Tocotrienol and Lovastatin Co-Supplementation on Bone Dynamic Histomorphometry and Bone Morphogenetic Protein-2 Expression in Rats with Estrogen Deficiency', 'author': 'Soelaiman Ima-Nirwana; Norazlina Mohamed; Kok-Yong Chin [orcid:0000-0001-6628-1552]; Saif Abdul-Majeed', 'pub_date': '2017-02-15', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'MDPI AG [crossref:1968]', 'editor': ''}, 
            {'id': 'pmid:22557953 doi:10.3389/fnhum.2012.00092', 'title': 'Influence of Cue Exposure on Inhibitory Control and Brain Activation in Patients with Alcohol Dependence', 'author': 'Ramona Kessel; Barbara Drüke; Maren Boecker; Verena Mainz; Thomas Forkmann; Siegfried Gauggel [orcid:0000-0002-2742-4917]', 'pub_date': '2012-05-01', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Frontiers Research Foundation', 'editor': ''}, 
            {'id': 'doi:10.4103/0974-777x.83533 pmid:21887059', 'title': 'Comparing absolute lymphocyte count to total lymphocyte count, as a CD4 T cell surrogate, to initiate antiretroviral therapy', 'author': 'Srirangaraj Sreenivasan; Venkatesha Dasegowda', 'pub_date': '2011-01-01', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Medknow [crossref:2581]', 'editor': ''},
            {'id': 'pmid:15479473', 'title': 'Current practices in the spatial analysis of cancer: flies in the ointment.', 'author': 'Jacquez Geoffrey M', 'pub_date': '2004-09-28', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}, 
            {'id': 'pmid:27336866', 'title': 'Surgical innovation : The ethical agenda', 'author': 'Broekman Marike L.; Carrière Michelle E.; Bredenoord Annelien L.', 'pub_date': '2016-06-01', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}]
        self.assertEqual(len(output), 8)
        self.assertEqual(output, expected_output)
        rmtree(os.path.join('test', 'discard_existing_res', 'output'))
        self.r.flushdb()


if __name__ == '__main__': # pragma: no cover
    unittest.main()