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
            {
                'res': 'https://w3id.org/oc/meta/br/2392', 
                'type': 'journal article', 
                'date': '2012-09-05', 
                'num_': '', 
                'part1_': 'https://w3id.org/oc/meta/br/4396', 
                'title1_': '', 
                'num1_': '9', 
                'type1_': 'http://purl.org/spar/fabio/Expression ;and; http://purl.org/spar/fabio/JournalIssue', 
                'part2_': 'https://w3id.org/oc/meta/br/4394', 
                'title2_': '', 
                'num2_': '308', 
                'type2_': 'http://purl.org/spar/fabio/Expression ;and; http://purl.org/spar/fabio/JournalVolume', 
                'part3_': 'https://w3id.org/oc/meta/br/4393', 
                'title3_': 'Jama', 
                'num3_': '', 'type3_': 
                'http://purl.org/spar/fabio/Expression ;and; http://purl.org/spar/fabio/Journal'
            }, 
            {
                'res': 'https://w3id.org/oc/meta/br/2389', 
                'type': 'journal article', 
                'date': '2012-08-22', 
                'num_': '', 
                'part1_': 'https://w3id.org/oc/meta/br/4397', 
                'title1_': '', 
                'num1_': '8', 
                'type1_': 'http://purl.org/spar/fabio/Expression ;and; http://purl.org/spar/fabio/JournalIssue', 
                'part2_': 'https://w3id.org/oc/meta/br/4394', 
                'title2_': '', 
                'num2_': '308', 
                'type2_': 'http://purl.org/spar/fabio/Expression ;and; http://purl.org/spar/fabio/JournalVolume', 
                'part3_': 'https://w3id.org/oc/meta/br/4393', 
                'title3_': 'Jama', 
                'num3_': '', 
                'type3_': 'http://purl.org/spar/fabio/Expression ;and; http://purl.org/spar/fabio/Journal'
            }
        ]
        format_expected = 'application/json'
        output = status, json.loads(result), format
        expected_output = status_expected, result_expected, format_expected
        self.assertEqual(output, expected_output)


