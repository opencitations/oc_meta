from pprint import pprint
from ramose import APIManager
import json
import unittest


CONFIG = 'api/oc_meta_v1.hf'
api_manager = APIManager([CONFIG])
api_base = 'http://127.0.0.1:8080/api/v1'

class test_API(unittest.TestCase):
    def test_metadata(self):
        operation_url = 'metadata'
        request = 'doi/10.1001/2012.jama.10456__10.1001/2012.jama.10503'
        call = "%s/%s/%s" % (api_base, operation_url, request)
        op = api_manager.get_op(call)
        status, result, format = op.exec()
        status_expected = 200
        result_expected = [
            {'id': 'doi:10.1001/2012.jama.10456 meta:br/2389', 
            'title': 'Cardiovascular Risk Assessment In The 21St Century', 
            'author': {'Wilson, Peter W. F.', 'Gaziano, J. Michael'}, 
            'date': '2012-08-22',
            'page': '816',
            'issue': '8',
            'volume': '308',
            'venue': 'Jama [issn:0098-7484]', 
            'type': 'journal article', 
            'publisher': 'American Medical Association (ama) [crossref:10]', 
            'editor': ''
            }, 
            {'id': 'doi:10.1001/2012.jama.10503 meta:br/2392', 
            'title': 'Aortic Stiffness, Blood Pressure Progression, And Incident Hypertension', 
            'author': {'Kaess, Bernhard M.', 'Rong, Jian', 'Vasan, Ramachandran S.', 'Mitchell, Gary F.', 'Hamburg, Naomi M. [orcid:0000-0001-5504-5589]', 'Levy, Daniel [orcid:0000-0003-1843-8724]', 'Benjamin, Emelia J. [orcid:0000-0003-4076-2336]', 'Larson, Martin G. [orcid:0000-0002-9631-1254]', 'Vita, Joseph A. [orcid:0000-0001-5607-1797]'}, 
            'date': '2012-09-05',
            'page': '875',
            'issue': '9', 
            'volume': '308', 
            'venue': 'Jama [issn:0098-7484]', 
            'type': 'journal article', 
            'publisher': 'American Medical Association (ama) [crossref:10]', 
            'editor': ''}]
        format_expected = 'application/json'
        output = status, [{k:set(v.split('; ')) if k=='author' else v for k,v in el.items()} for el in json.loads(result)], format
        expected_output = status_expected, result_expected, format_expected
        self.assertEqual(output, expected_output)


