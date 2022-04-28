from meta.lib.file_manager import get_data
from meta.run.meta_process import MetaProcess, run_meta_process
from test.curator_test import reset_server
import json
import os
import shutil
import sys
import unittest


BASE_DIR = os.path.join('test', 'meta_process')


class test_ProcessTest(unittest.TestCase):
    def test_get_data(self):
        filepath = os.path.join(BASE_DIR, 'long_field.csv')
        data = get_data(filepath)
        field_size = sys.getsizeof(data[0]['author'])
        self.assertEqual(field_size, 137622)

    def test_run_meta_process(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_1')
        meta_process = MetaProcess(config=os.path.join(BASE_DIR, 'meta_config_1.yaml'))
        run_meta_process(meta_process)
        output = list()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, 'csv')):
            for file in filenames:
                output.extend(get_data(os.path.join(dirpath, file)))
        expected_output = [
            {'id': 'doi:10.17117/na.2015.08.1067 meta:br/0601', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal Of Food And Nutrition [issn:1225-4339 meta:br/0603]', 'volume': '26', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623 meta:ra/0601]', 'editor': 'Naimi, Elmehdi [orcid:0000-0002-4126-8519 meta:ra/0602]'}, 
            {'id': 'issn:1524-4539 issn:0009-7322 meta:br/0602', 'title': 'Circulation', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.069 meta:br/0605', 'title': 'Nonthermal Sterilization And Shelf-life Extension Of Seafood Products By Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788 meta:ra/0603]; Mun, Ji-Hye [meta:ra/0604]; Chung, Myong-Soo [meta:ra/0605]', 'pub_date': '2012-03-31', 'venue': 'The Korean Journal Of Food And Nutrition [issn:1225-4339 meta:br/0603]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society Of Food And Nutrition [crossref:4768 meta:ra/0606]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513 meta:ra/0607]'}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.077 meta:br/0606', 'title': 'Properties Of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho [meta:ra/0608]; Shin, Hae-Hun [meta:ra/0609]; Kim, Young-Shik [orcid:0000-0001-5673-6314 meta:ra/06010]; Kook, Moo-Chang [meta:ra/06011]', 'pub_date': '2012-03-31', 'venue': 'The Korean Journal Of Food And Nutrition [issn:1225-4339 meta:br/0603]', 'volume': '', 'issue': '2', 'page': '77-82', 'type': 'journal article', 'publisher': 'The Korean Society Of Food And Nutrition [crossref:4768 meta:ra/0606]', 'editor': ''}]
        output = sorted(sorted(d.items()) for d in output)
        expected_output = sorted(sorted(d.items()) for d in expected_output)
        shutil.rmtree(output_folder)
        self.assertEqual(output, expected_output)

    def test_run_meta_process_two_workers(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_2')
        meta_process = MetaProcess(config=os.path.join(BASE_DIR, 'meta_config_2.yaml'))
        meta_process.workers_number = 2
        run_meta_process(meta_process)
        output = list()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, 'csv')):
            for file in filenames:
                output.extend(get_data(os.path.join(dirpath, file)))
        shutil.rmtree(output_folder)
        expected_output = [
            {'id': 'doi:10.17117/na.2015.08.1067 meta:br/06101', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal Of Food And Nutrition [issn:1225-4339 meta:br/06103]', 'volume': '26', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623 meta:ra/06101]', 'editor': 'Naimi, Elmehdi [orcid:0000-0002-4126-8519 meta:ra/06102]'}, 
            {'id': 'issn:1524-4539 issn:0009-7322 meta:br/06102', 'title': 'Circulation', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.069 meta:br/06201', 'title': 'Nonthermal Sterilization And Shelf-life Extension Of Seafood Products By Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788 meta:ra/06201]; Mun, Ji-Hye [meta:ra/06202]; Chung, Myong-Soo [meta:ra/06203]', 'pub_date': '2012-03-31', 'venue': 'The Korean Journal Of Food And Nutrition [issn:1225-4339 meta:br/06203]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society Of Food And Nutrition [crossref:4768 meta:ra/06204]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513 meta:ra/06205]'}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.077 meta:br/06202', 'title': 'Properties Of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho [meta:ra/06206]; Shin, Hae-Hun [meta:ra/06207]; Kim, Young-Shik [orcid:0000-0001-5673-6314 meta:ra/06208]; Kook, Moo-Chang [meta:ra/06209]', 'pub_date': '2012-03-31', 'venue': 'The Korean Journal Of Food And Nutrition [issn:1225-4339 meta:br/06203]', 'volume': '', 'issue': '2', 'page': '77-82', 'type': 'journal article', 'publisher': 'The Korean Society Of Food And Nutrition [crossref:4768 meta:ra/06204]', 'editor': ''}]
        output = sorted(sorted(d.items()) for d in output)
        expected_output = sorted(sorted(d.items()) for d in expected_output)
        self.assertEqual(output, expected_output)

    def test_provenance(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_3')	
        meta_process = MetaProcess(config=os.path.join(BASE_DIR, 'meta_config_3.yaml'))
        meta_process.input_csv_dir = os.path.join(BASE_DIR, 'input')
        run_meta_process(meta_process)
        meta_process.input_csv_dir = os.path.join(BASE_DIR, 'input_2')
        run_meta_process(meta_process)
        meta_process.input_csv_dir = os.path.join(BASE_DIR, 'input')
        run_meta_process(meta_process)
        output = dict()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, 'rdf')):
            if dirpath.endswith('prov'):
                print(dirpath)
                for filename in filenames:
                    if filename.endswith('.json'):
                        filepath = os.path.join(dirpath, filename)
                        with open(filepath, 'r', encoding='utf-8') as f:
                            provenance = json.load(f)
                            essential_provenance = [{graph:[{p:set(v[0]['@value'].split('INSERT DATA { GRAPH <https://w3id.org/oc/meta/br/> { ')[1].split(' } }')[0].split('\n')) if '@value' in v[0] else v if isinstance(v, list) else v for p,v in se.items() 
                                if p not in {'http://www.w3.org/ns/prov#generatedAtTime', 'http://purl.org/dc/terms/description', '@type', 'http://www.w3.org/ns/prov#hadPrimarySource', 'http://www.w3.org/ns/prov#wasAttributedTo', 'http://www.w3.org/ns/prov#invalidatedAtTime'}} 
                                for se in sorted(ses, key=lambda d: d['@id'])] for graph,ses in entity.items() if graph != '@id'} for entity in sorted(provenance, key=lambda x:x['@id'])]
                            output[dirpath.split(os.sep)[4]] = essential_provenance
        expected_output = {
            'ar': [
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/ar/0601/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/ar/0601'}]}]}, 
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/ar/0602/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/ar/0602'}]}]}, 
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/ar/0603/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/ar/0603'}]}]}], 
            'br': [
                {'@graph': [
                    {'@id': 'https://w3id.org/oc/meta/br/0601/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/br/0601'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/br/0601/prov/se/2', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/br/0601'}], 'http://www.w3.org/ns/prov#wasDerivedFrom': [{'@id': 'https://w3id.org/oc/meta/br/0601/prov/se/1'}], 
                        'https://w3id.org/oc/ontology/hasUpdateQuery': {
                            '<https://w3id.org/oc/meta/br/0601> <http://purl.org/spar/pro/isDocumentContextFor> <https://w3id.org/oc/meta/ar/0601> .', 
                            '<https://w3id.org/oc/meta/br/0601> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0603> .', 
                            '<https://w3id.org/oc/meta/br/0601> <http://purl.org/spar/pro/isDocumentContextFor> <https://w3id.org/oc/meta/ar/0603> .', 
                            '<https://w3id.org/oc/meta/br/0601> <http://purl.org/spar/pro/isDocumentContextFor> <https://w3id.org/oc/meta/ar/0602> .', 
                            '<https://w3id.org/oc/meta/br/0601> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2015-08-22"^^<http://www.w3.org/2001/XMLSchema#date> .', 
                            '<https://w3id.org/oc/meta/br/0601> <http://purl.org/dc/terms/title> "Some Aspects Of The Evolution Of Chernozems Under The Influence Of Natural And Anthropogenic Factors" .'}}]}, 
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/br/0602/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/br/0602'}]}]}, 
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/br/0603/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/br/0603'}]}]}], 
            'id': [
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/id/0601/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/id/0601'}]}]}, 
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/id/0602/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/id/0602'}]}]}, 
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/id/0603/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/id/0603'}]}]}, 
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/id/0604/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/id/0604'}]}]}], 
            'ra': [
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/ra/0601/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/ra/0601'}]}]}, 
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/ra/0602/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/ra/0602'}]}]}], 
            're': [
                {'@graph': [{'@id': 'https://w3id.org/oc/meta/re/0601/prov/se/1', 'http://www.w3.org/ns/prov#specializationOf': [{'@id': 'https://w3id.org/oc/meta/re/0601'}]}]}]}
        shutil.rmtree(output_folder)
        self.assertEqual(output, expected_output)


if __name__ == '__main__':
    unittest.main()