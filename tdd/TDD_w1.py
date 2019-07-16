import unittest
from w1_test import *
import csv
class TestConverterDemo(unittest.TestCase):

    def setUp(self):
        self.data = {'id': 'doi:10.3233/DS-170012', 'title': 'Automating semantic publishing',
                   'author': 'Peroni, Silvio [orcid:0000-0003-0530-4305]', 'pub_date': '2017',
                   'venue': 'Data Science [issn:2451-8484; issn:2451-8492]', 'volume': '1', 'issue': '1-2',
                   'page': '155-173', 'type': 'journal article', 'publisher': 'IOS Press [crossref:7437]'}
        self.row = Migrator(self.data)

    def test_id_job (self):
        result = self.row.id_job()['doi'][0]
        self.assertEqual(result, 'doi:10.3233/DS-170012')

    def test_title_job(self):
        result = self.row.title_job()
        self.assertEqual(result, 'Automating semantic publishing')

    def test_author_job (self):
        result = self.row.author_job()
        aut_name = result['name']
        orcid = result['orcid'][0]
        self.assertEqual(aut_name, 'Peroni, Silvio')
        self.assertEqual(orcid, 'orcid:0000-0003-0530-4305')

    def test_pub_date_job(self):
        result = self.row.pub_date_job()
        self.assertEqual(result, '2017')

    def test_venue_job (self):
        result = self.row.venue_job()
        venue_name = result['name']
        issn1 = result['issn'][0]
        issn2 = result['issn'][1]
        self.assertEqual(venue_name, 'Data Science')
        self.assertEqual(issn1, 'issn:2451-8484')
        self.assertEqual(issn2, 'issn:2451-8492')

    def test_volume_job(self):
        result = self.row.volume_job()
        self.assertEqual(result, '1')

    def test_issue_job(self):
        result = self.row.issue_job()
        self.assertEqual(result, '1-2')

    def test_page_job(self):
        result = self.row.page_job()
        self.assertEqual(result, '155-173')

    def test_type_job(self):
        result = self.row.type_job()
        self.assertEqual(result, 'journal article')

    def test_publisher_job(self):
        result = self.row.publisher_job()
        pub_name = result['name']
        crsref = result ['crossref'][0]
        self.assertEqual(pub_name, 'IOS Press')
        self.assertEqual(crsref, 'crossref:7437')

if __name__ == '__main__':
    unittest.main()


