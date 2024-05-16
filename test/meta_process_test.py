import json
import os
import shutil
import subprocess
import sys
import unittest
from datetime import datetime
from zipfile import ZipFile

import rdflib
import yaml
from rdflib import ConjunctiveGraph, Graph, Literal, Namespace, URIRef
from SPARQLWrapper import JSON, POST, SPARQLWrapper

from oc_meta.lib.file_manager import get_csv_data
from oc_meta.run.meta_process import merge_rdf_files, run_meta_process

BASE_DIR = os.path.join('test', 'meta_process')
SERVER = 'http://127.0.0.1:9999/blazegraph/sparql'

def reset_server(server:str=SERVER) -> None:
    ts = SPARQLWrapper(server)
    ts.setQuery('delete{?x ?y ?z} where{?x ?y ?z}')
    ts.setMethod(POST)
    ts.query()

def delete_output_zip(base_dir:str, start_time:datetime) -> None:
    for file in os.listdir(base_dir):
        if file.startswith('meta_output') and file.endswith('.zip'):
            file_creation_time = file.split('meta_output_')[1].replace('.zip', '')
            file_creation_time = datetime.strptime(file_creation_time, '%Y-%m-%dT%H_%M_%S_%f')
            was_created_after_time = True if file_creation_time > start_time else False
            if was_created_after_time:
                os.remove(os.path.join(base_dir, file))

class test_ProcessTest(unittest.TestCase):
    def test_merge_rdf_files(self):
        test_dir = os.path.join(BASE_DIR, "merge_rdf_files_test_dir")
        prov_dir = os.path.join(test_dir, "prov")
        if not os.path.exists(test_dir):
            os.mkdir(test_dir)
        if not os.path.exists(prov_dir):
            os.mkdir(prov_dir)

        ns = Namespace("http://example.org/")
        se_ns = Namespace("http://example.org/se/")
        
        g0 = Graph()
        g0.add((ns["subject0"], ns["predicate"], Literal("object0")))
        g1 = Graph()
        g1.add((ns["subject1"], ns["predicate"], Literal("object1")))
        g2 = Graph()
        g2.add((ns["subject2"], ns["predicate"], Literal("object2")))
        
        se0 = Graph()
        se0.add((se_ns["subject0"], se_ns["predicate"], Literal("object0")))
        se1 = Graph()
        se1.add((se_ns["subject1"], se_ns["predicate"], Literal("object1")))
        se2 = Graph()
        se2.add((se_ns["subject2"], se_ns["predicate"], Literal("object2")))
        
        # Salvataggio dei grafi in file JSON-LD separati
        file_path0 = os.path.join(test_dir, "1.json")
        file_path1 = os.path.join(test_dir, "1_1.json")
        file_path2 = os.path.join(test_dir, "1_2.json")

        se_file_path0 = os.path.join(prov_dir, "se.json")
        se_file_path1 = os.path.join(prov_dir, "se_1.json")
        se_file_path2 = os.path.join(prov_dir, "se_2.json")

        with open(file_path0, 'w') as f:
            f.write(g0.serialize(format='json-ld'))
        with open(file_path1, 'w') as f:
            f.write(g1.serialize(format='json-ld'))
        with open(file_path2, 'w') as f:
            f.write(g2.serialize(format='json-ld'))

        with open(se_file_path0, 'w') as f:
            f.write(se0.serialize(format='json-ld'))
        with open(se_file_path1, 'w') as f:
            f.write(se1.serialize(format='json-ld'))
        with open(se_file_path2, 'w') as f:
            f.write(se2.serialize(format='json-ld'))

        merge_rdf_files(test_dir, zip_output_rdf=False)

        merged_file_path = os.path.join(test_dir, "1.json")
        merged_se_file_path = os.path.join(prov_dir, "se.json")
        self.assertTrue(os.path.exists(merged_file_path))
        self.assertTrue(os.path.exists(merged_se_file_path))

        with open(merged_file_path, 'r') as f:
            merged_data = json.load(f)
            g = Graph().parse(data=json.dumps(merged_data), format='json-ld')
            triples = {triple for triple in g}
            triples_expected = {
                (rdflib.term.URIRef('http://example.org/subject0'), rdflib.term.URIRef('http://example.org/predicate'), rdflib.term.Literal('object0')), 
                (rdflib.term.URIRef('http://example.org/subject2'), rdflib.term.URIRef('http://example.org/predicate'), rdflib.term.Literal('object2')), 
                (rdflib.term.URIRef('http://example.org/subject1'), rdflib.term.URIRef('http://example.org/predicate'), rdflib.term.Literal('object1'))}
            self.assertEqual(triples, triples_expected)

        with open(merged_se_file_path, 'r') as f:
            merged_data = json.load(f)
            g = Graph().parse(data=json.dumps(merged_data), format='json-ld')
            triples = {triple for triple in g}
            triples_expected = {
                (rdflib.term.URIRef('http://example.org/se/subject0'), rdflib.term.URIRef('http://example.org/se/predicate'), rdflib.term.Literal('object0')), 
                (rdflib.term.URIRef('http://example.org/se/subject2'), rdflib.term.URIRef('http://example.org/se/predicate'), rdflib.term.Literal('object2')), 
                (rdflib.term.URIRef('http://example.org/se/subject1'), rdflib.term.URIRef('http://example.org/se/predicate'), rdflib.term.Literal('object1'))}
            self.assertEqual(triples, triples_expected)
        
        shutil.rmtree(test_dir)
    
    def test_get_csv_data(self):
        filepath = os.path.join(BASE_DIR, 'long_field.csv')
        data = get_csv_data(filepath)
        field_size = sys.getsizeof(data[0]['author'])
        self.assertEqual(field_size, 137622)

    def test_run_meta_process(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_1')
        meta_config_path = os.path.join(BASE_DIR, 'meta_config_1.yaml')
        with open(meta_config_path, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        now = datetime.now()
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        output = list()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, 'csv')):
            for file in filenames:
                output.extend(get_csv_data(os.path.join(dirpath, file)))
        expected_output = [
            {'id': 'doi:10.17117/na.2015.08.1067 omid:br/0601', 'title': '', 'author': '', 'pub_date': '', 'venue': 'Scientometrics [issn:0138-9130 issn:1588-2861 omid:br/0603]', 'volume': '26', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623 omid:ra/0601]', 'editor': 'Naimi, Elmehdi [orcid:0000-0002-4126-8519 omid:ra/0602]'}, 
            {'id': 'issn:1524-4539 issn:0009-7322 omid:br/0602', 'title': 'Circulation', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.069 omid:br/0605', 'title': 'Nonthermal Sterilization And Shelf-life Extension Of Seafood Products By Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788 omid:ra/0603]; Mun, Ji-Hye [omid:ra/0604]; Chung, Myong-Soo [omid:ra/0605]', 'pub_date': '2012-03-31', 'venue': 'The Korean Journal Of Food And Nutrition [issn:1225-4339 omid:br/0608]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society Of Food And Nutrition [crossref:4768 omid:ra/0606]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513 omid:ra/0607]'}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.077 omid:br/0606', 'title': 'Properties Of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho [omid:ra/0608]; Shin, Hae-Hun [omid:ra/0609]; Kim, Young-Shik [orcid:0000-0001-5673-6314 omid:ra/06010]; Kook, Moo-Chang [omid:ra/06011]', 'pub_date': '2012-03-31', 'venue': 'The Korean Journal Of Food And Nutrition [issn:1225-4339 omid:br/0608]', 'volume': '', 'issue': '2', 'page': '77-82', 'type': 'journal article', 'publisher': 'The Korean Society Of Food And Nutrition [crossref:4768 omid:ra/0606]', 'editor': ''},
            {'id': 'doi:10.1097/01.rct.0000185385.35389.cd omid:br/0607', 'title': 'Comprehensive Assessment Of Lung CT Attenuation Alteration At Perfusion Defects Of Acute Pulmonary Thromboembolism With Breath-Hold SPECT-CT Fusion Images', 'author': 'Suga, Kazuyoshi [omid:ra/06012]; Kawakami, Yasuhiko [omid:ra/06013]; Iwanaga, Hideyuki [omid:ra/06014]; Hayashi, Noriko [omid:ra/06015]; Seto, Aska [omid:ra/06016]; Matsunaga, Naofumi [omid:ra/06017]', 'pub_date': '2006-01', 'venue': 'Journal Of Computer Assisted Tomography [issn:0363-8715 omid:br/06012]', 'volume': '30', 'issue': '1', 'page': '83-91', 'type': 'journal article', 'publisher': 'Ovid Technologies (Wolters Kluwer Health) [crossref:276 omid:ra/06018]', 'editor': ''}]
        output = sorted(sorted(d.items()) for d in output)
        expected_output = sorted(sorted(d.items()) for d in expected_output)
        self.maxDiff = None
        shutil.rmtree(output_folder)
        delete_output_zip('.', now)
        self.assertEqual(output, expected_output)

    def test_run_meta_process_ids_only(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_5')
        meta_config_path = os.path.join(BASE_DIR, 'meta_config_5.yaml')
        now = datetime.now()
        with open(meta_config_path, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        run_meta_process(settings, meta_config_path=meta_config_path)
        output = list()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, 'csv')):
            for file in filenames:
                output.extend(get_csv_data(os.path.join(dirpath, file)))
        expected_output = [{
            'id': 'doi:10.17117/na.2015.08.1067 omid:br/0601', 'title': 'Some Aspects Of The Evolution Of Chernozems Under The Influence Of Natural And Anthropogenic Factors', 
            'author': '[orcid:0000-0002-4126-8519 omid:ra/0601]; [orcid:0000-0003-0530-4305 omid:ra/0602]', 'pub_date': '2015-08-22', 
            'venue': '[issn:1225-4339 omid:br/0602]', 'volume': '26', 'issue': '', 'page': '50', 'type': 'journal article', 
            'publisher': '[crossref:6623 omid:ra/0603]', 'editor': '[orcid:0000-0002-4126-8519 omid:ra/0601]; [orcid:0000-0002-8420-0696 omid:ra/0604]'}]
        output = sorted(sorted(d.items()) for d in output)
        expected_output = sorted(sorted(d.items()) for d in expected_output)
        self.maxDiff = None
        shutil.rmtree(output_folder)
        delete_output_zip('.', now)
        self.assertEqual(output, expected_output)

    def test_run_meta_process_two_workers(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_2')
        meta_config_path=os.path.join(BASE_DIR, 'meta_config_2.yaml')
        with open(meta_config_path, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        settings['workers_number'] = 2
        now = datetime.now()
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        output = list()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, 'csv')):
            for file in filenames:
                output.extend(get_csv_data(os.path.join(dirpath, file)))
        shutil.rmtree(output_folder)
        delete_output_zip('.', now)
        expected_output = [
            {'id': 'doi:10.17117/na.2015.08.1067 omid:br/06101', 'title': '', 'author': '', 'pub_date': '', 'venue': 'Scientometrics [issn:0138-9130 issn:1588-2861 omid:br/06103]', 'volume': '26', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623 omid:ra/06101]', 'editor': 'Naimi, Elmehdi [orcid:0000-0002-4126-8519 omid:ra/06102]'}, 
            {'id': 'issn:1524-4539 issn:0009-7322 omid:br/06102', 'title': 'Circulation', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.069 omid:br/06201', 'title': 'Nonthermal Sterilization And Shelf-life Extension Of Seafood Products By Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788 omid:ra/06201]; Mun, Ji-Hye [omid:ra/06202]; Chung, Myong-Soo [omid:ra/06203]', 'pub_date': '2012-03-31', 'venue': 'The Korean Journal Of Food And Nutrition [issn:1225-4339 omid:br/06204]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society Of Food And Nutrition [crossref:4768 omid:ra/06204]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513 omid:ra/06205]'}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.077 omid:br/06202', 'title': 'Properties Of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho [omid:ra/06206]; Shin, Hae-Hun [omid:ra/06207]; Kim, Young-Shik [orcid:0000-0001-5673-6314 omid:ra/06208]; Kook, Moo-Chang [omid:ra/06209]', 'pub_date': '2012-03-31', 'venue': 'The Korean Journal Of Food And Nutrition [issn:1225-4339 omid:br/06204]', 'volume': '', 'issue': '2', 'page': '77-82', 'type': 'journal article', 'publisher': 'The Korean Society Of Food And Nutrition [crossref:4768 omid:ra/06204]', 'editor': ''},
            {'id': 'doi:10.1097/01.rct.0000185385.35389.cd omid:br/06203', 'title': 'Comprehensive Assessment Of Lung CT Attenuation Alteration At Perfusion Defects Of Acute Pulmonary Thromboembolism With Breath-Hold SPECT-CT Fusion Images', 'author': 'Suga, Kazuyoshi [omid:ra/062010]; Kawakami, Yasuhiko [omid:ra/062011]; Iwanaga, Hideyuki [omid:ra/062012]; Hayashi, Noriko [omid:ra/062013]; Seto, Aska [omid:ra/062014]; Matsunaga, Naofumi [omid:ra/062015]', 'pub_date': '2006-01', 'venue': 'Journal Of Computer Assisted Tomography [issn:0363-8715 omid:br/06208]', 'volume': '30', 'issue': '1', 'page': '83-91', 'type': 'journal article', 'publisher': 'Ovid Technologies (Wolters Kluwer Health) [crossref:276 omid:ra/062016]', 'editor': ''}]
        output = sorted(sorted(d.items()) for d in output)
        expected_output = sorted(sorted(d.items()) for d in expected_output)
        self.assertEqual(output, expected_output)

    def test_provenance(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_3')	
        now = datetime.now()
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)
        delete_output_zip('.', now)
        meta_config_path=os.path.join(BASE_DIR, 'meta_config_3.yaml')
        with open(meta_config_path, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        settings['input_csv_dir'] = os.path.join(BASE_DIR, 'input')
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        settings['input_csv_dir'] = os.path.join(BASE_DIR, 'input_2')
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        settings['input_csv_dir'] = os.path.join(BASE_DIR, 'input')
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        output = dict()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, 'rdf')):
            if dirpath.endswith('prov'):
                for filename in filenames:
                    if filename.endswith('.json'):
                        filepath = os.path.join(dirpath, filename)
                        with open(filepath, 'r', encoding='utf-8') as f:
                            provenance = json.load(f)
                            essential_provenance = [{graph:[{p:set(v[0]['@value'].split('INSERT DATA { GRAPH <https://w3id.org/oc/meta/br/> { ')[1].split(' } }')[0].split(' .')) if '@value' in v[0] else v if isinstance(v, list) else v for p,v in se.items() 
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
                            '',
                            '<https://w3id.org/oc/meta/br/0601> <http://purl.org/spar/pro/isDocumentContextFor> <https://w3id.org/oc/meta/ar/0601>', 
                            '<https://w3id.org/oc/meta/br/0601> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0603>', 
                            '<https://w3id.org/oc/meta/br/0601> <http://purl.org/spar/pro/isDocumentContextFor> <https://w3id.org/oc/meta/ar/0602>',
                            '<https://w3id.org/oc/meta/br/0601> <http://purl.org/spar/pro/isDocumentContextFor> <https://w3id.org/oc/meta/ar/0603>',
                            '<https://w3id.org/oc/meta/br/0601> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2015-08-22"^^<http://www.w3.org/2001/XMLSchema#date>', 
                            '<https://w3id.org/oc/meta/br/0601> <http://purl.org/dc/terms/title> "Some Aspects Of The Evolution Of Chernozems Under The Influence Of Natural And Anthropogenic Factors"'}}]}, 
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
        delete_output_zip('.', now)
        self.maxDiff = None
        self.assertEqual(output, expected_output)

    def test_run_meta_process_thread_safe(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_4')
        meta_config_path=os.path.join(BASE_DIR, 'meta_config_4.yaml')
        with open(meta_config_path, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        original_input_csv_dir = settings['input_csv_dir']
        settings['input_csv_dir'] = os.path.join(original_input_csv_dir, 'preprocess')
        now = datetime.now()
        settings['workers_number'] = 1
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        proc = subprocess.run([sys.executable, '-m', 'oc_meta.run.meta_process', '-c', meta_config_path], capture_output=True, text=True)
        output = dict()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, 'rdf')):
            if not dirpath.endswith('prov'):
                for filename in filenames:
                    if filename.endswith('.zip'):
                        with ZipFile(os.path.join(dirpath, filename)) as archive:
                            with archive.open(filename.replace('.zip', '.json')) as f:
                                rdf = json.load(f)
                                output.setdefault(dirpath.split(os.sep)[4], list())
                                rdf_sorted = [{k:sorted([{p:o for p,o in p_o.items() if p not in {'@type', 'http://purl.org/spar/pro/isDocumentContextFor'}} for p_o in v], key=lambda d: d['@id']) for k,v in graph.items() if k == '@graph'} for graph in rdf]
                                output[dirpath.split(os.sep)[4]].extend(rdf_sorted)
        shutil.rmtree(output_folder)
        delete_output_zip('.', now)
        expected_output = {
            'ar': [
                {'@graph': [
                    {'@id': 'https://w3id.org/oc/meta/ar/0604', '@type': ['http://purl.org/spar/pro/RoleInTime'], 'http://purl.org/spar/pro/isHeldBy': [{'@id': 'https://w3id.org/oc/meta/ra/0604'}], 'http://purl.org/spar/pro/withRole': [{'@id': 'http://purl.org/spar/pro/publisher'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/ar/0602', '@type': ['http://purl.org/spar/pro/RoleInTime'], 'http://purl.org/spar/pro/isHeldBy': [{'@id': 'https://w3id.org/oc/meta/ra/0602'}], 'http://purl.org/spar/pro/withRole': [{'@id': 'http://purl.org/spar/pro/author'}], 'https://w3id.org/oc/ontology/hasNext': [{'@id': 'https://w3id.org/oc/meta/ar/0603'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/ar/0603', '@type': ['http://purl.org/spar/pro/RoleInTime'], 'http://purl.org/spar/pro/isHeldBy': [{'@id': 'https://w3id.org/oc/meta/ra/0603'}], 'http://purl.org/spar/pro/withRole': [{'@id': 'http://purl.org/spar/pro/author'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/ar/0605', '@type': ['http://purl.org/spar/pro/RoleInTime'], 'http://purl.org/spar/pro/isHeldBy': [{'@id': 'https://w3id.org/oc/meta/ra/0605'}], 'http://purl.org/spar/pro/withRole': [{'@id': 'http://purl.org/spar/pro/editor'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/ar/0601', '@type': ['http://purl.org/spar/pro/RoleInTime'], 'http://purl.org/spar/pro/isHeldBy': [{'@id': 'https://w3id.org/oc/meta/ra/0601'}], 'http://purl.org/spar/pro/withRole': [{'@id': 'http://purl.org/spar/pro/author'}], 'https://w3id.org/oc/ontology/hasNext': [{'@id': 'https://w3id.org/oc/meta/ar/0602'}]}], 
                '@id': 'https://w3id.org/oc/meta/ar/'}], 
            'br': [
                {'@graph': [
                    {'@id': 'https://w3id.org/oc/meta/br/0601', 
                        '@type': ['http://purl.org/spar/fabio/Expression', 'http://purl.org/spar/fabio/JournalArticle'], 
                        'http://prismstandard.org/namespaces/basic/2.0/publicationDate': [{'@type': 'http://www.w3.org/2001/XMLSchema#date', '@value': '2012-03-31'}], 
                        'http://purl.org/dc/terms/title': [{'@value': 'Nonthermal Sterilization And Shelf-life Extension Of Seafood Products By Intense Pulsed Light Treatment'}], 
                        'http://purl.org/spar/datacite/hasIdentifier': [{'@id': 'https://w3id.org/oc/meta/id/0601'}], 
                        'http://purl.org/spar/pro/isDocumentContextFor': [{'@id': 'https://w3id.org/oc/meta/ar/0603'}, {'@id': 'https://w3id.org/oc/meta/ar/0601'}, {'@id': 'https://w3id.org/oc/meta/ar/0604'}, {'@id': 'https://w3id.org/oc/meta/ar/0602'}, {'@id': 'https://w3id.org/oc/meta/ar/0605'}], 
                        'http://purl.org/vocab/frbr/core#embodiment': [{'@id': 'https://w3id.org/oc/meta/re/0601'}], 
                        'http://purl.org/vocab/frbr/core#partOf': [{'@id': 'https://w3id.org/oc/meta/br/0604'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/br/0604', '@type': ['http://purl.org/spar/fabio/JournalIssue', 'http://purl.org/spar/fabio/Expression'], 'http://purl.org/spar/fabio/hasSequenceIdentifier': [{'@value': '1'}], 'http://purl.org/vocab/frbr/core#partOf': [{'@id': 'https://w3id.org/oc/meta/br/0603'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/br/0602', '@type': ['http://purl.org/spar/fabio/Expression', 'http://purl.org/spar/fabio/Journal'], 'http://purl.org/dc/terms/title': [{'@value': 'The Korean Journal Of Food And Nutrition'}], 'http://purl.org/spar/datacite/hasIdentifier': [{'@id': 'https://w3id.org/oc/meta/id/0602'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/br/0603', '@type': ['http://purl.org/spar/fabio/Expression', 'http://purl.org/spar/fabio/JournalVolume'], 'http://purl.org/spar/fabio/hasSequenceIdentifier': [{'@value': '25'}], 'http://purl.org/vocab/frbr/core#partOf': [{'@id': 'https://w3id.org/oc/meta/br/0602'}]}], 
                '@id': 'https://w3id.org/oc/meta/br/'}], 
            'id': [
                {'@graph': [
                    {'@id': 'https://w3id.org/oc/meta/id/0605', '@type': ['http://purl.org/spar/datacite/Identifier'], 'http://purl.org/spar/datacite/usesIdentifierScheme': [{'@id': 'http://purl.org/spar/datacite/orcid'}], 'http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue': [{'@value': '0000-0002-9666-2513'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/id/0601', '@type': ['http://purl.org/spar/datacite/Identifier'], 'http://purl.org/spar/datacite/usesIdentifierScheme': [{'@id': 'http://purl.org/spar/datacite/doi'}], 'http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue': [{'@value': '10.9799/ksfan.2012.25.1.069'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/id/0603', '@type': ['http://purl.org/spar/datacite/Identifier'], 'http://purl.org/spar/datacite/usesIdentifierScheme': [{'@id': 'http://purl.org/spar/datacite/orcid'}], 'http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue': [{'@value': '0000-0003-2542-5788'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/id/0604', '@type': ['http://purl.org/spar/datacite/Identifier'], 'http://purl.org/spar/datacite/usesIdentifierScheme': [{'@id': 'http://purl.org/spar/datacite/crossref'}], 'http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue': [{'@value': '4768'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/id/0602', '@type': ['http://purl.org/spar/datacite/Identifier'], 'http://purl.org/spar/datacite/usesIdentifierScheme': [{'@id': 'http://purl.org/spar/datacite/issn'}], 'http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue': [{'@value': '1225-4339'}]}], 
                '@id': 'https://w3id.org/oc/meta/id/'}], 
            'ra': [
                {'@graph': [
                    {'@id': 'https://w3id.org/oc/meta/ra/0605', '@type': ['http://xmlns.com/foaf/0.1/Agent'], 'http://purl.org/spar/datacite/hasIdentifier': [{'@id': 'https://w3id.org/oc/meta/id/0605'}], 'http://xmlns.com/foaf/0.1/familyName': [{'@value': 'Chung'}], 'http://xmlns.com/foaf/0.1/givenName': [{'@value': 'Myong-Soo'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/ra/0602', '@type': ['http://xmlns.com/foaf/0.1/Agent'], 'http://xmlns.com/foaf/0.1/familyName': [{'@value': 'Mun'}], 'http://xmlns.com/foaf/0.1/givenName': [{'@value': 'Ji-Hye'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/ra/0604', '@type': ['http://xmlns.com/foaf/0.1/Agent'], 'http://purl.org/spar/datacite/hasIdentifier': [{'@id': 'https://w3id.org/oc/meta/id/0604'}], 'http://xmlns.com/foaf/0.1/name': [{'@value': 'The Korean Society Of Food And Nutrition'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/ra/0603', '@type': ['http://xmlns.com/foaf/0.1/Agent'], 'http://xmlns.com/foaf/0.1/familyName': [{'@value': 'Chung'}], 'http://xmlns.com/foaf/0.1/givenName': [{'@value': 'Myong-Soo'}]}, 
                    {'@id': 'https://w3id.org/oc/meta/ra/0601', '@type': ['http://xmlns.com/foaf/0.1/Agent'], 'http://purl.org/spar/datacite/hasIdentifier': [{'@id': 'https://w3id.org/oc/meta/id/0603'}], 'http://xmlns.com/foaf/0.1/familyName': [{'@value': 'Cheigh'}], 'http://xmlns.com/foaf/0.1/givenName': [{'@value': 'Chan-Ick'}]}], 
                '@id': 'https://w3id.org/oc/meta/ra/'}], 
            're': [
                {'@graph': [
                    {'@id': 'https://w3id.org/oc/meta/re/0601', '@type': ['http://purl.org/spar/fabio/Manifestation'], 'http://prismstandard.org/namespaces/basic/2.0/endingPage': [{'@value': '76'}], 'http://prismstandard.org/namespaces/basic/2.0/startingPage': [{'@value': '69'}]}], 
                '@id': 'https://w3id.org/oc/meta/re/'}]}
        expected_output = {folder:[{k:sorted([{p:o for p,o in p_o.items() if p not in {'@type', 'http://purl.org/spar/pro/isDocumentContextFor'}} for p_o in v], key=lambda d: d['@id']) for k,v in graph.items() if k == '@graph'} for graph in data] for folder, data in expected_output.items()}
        self.assertEqual(output, expected_output)
        self.assertFalse('Reader: ERROR' in proc.stdout or 'Storer: ERROR' in proc.stdout)
    
    def test_silencer_on(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_6')
        now = datetime.now()
        meta_config_path=os.path.join(BASE_DIR, 'meta_config_6.yaml')
        with open(meta_config_path, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        settings['input_csv_dir'] = os.path.join(BASE_DIR, 'same_as_input_2_with_other_authors')
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        query_agents = '''
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT (COUNT (?agent) AS ?agent_count)
            WHERE {
                <https://w3id.org/oc/meta/br/0601> pro:isDocumentContextFor ?agent.
            }
        '''
        endpoint = SPARQLWrapper('http://localhost:9999/blazegraph/sparql')
        endpoint.setQuery(query_agents)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        expected_result = {
            'head': {'vars': ['agent_count']}, 
            'results': {
                'bindings': [{
                    'agent_count': {'datatype': 'http://www.w3.org/2001/XMLSchema#integer', 
                    'type': 'literal', 
                    'value': '3'}}]}}
        shutil.rmtree(output_folder)
        delete_output_zip('.', now)
        self.assertEqual(result, expected_result)

    def test_silencer_off(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_7')
        now = datetime.now()
        meta_config_path=os.path.join(BASE_DIR, 'meta_config_7.yaml')
        with open(meta_config_path, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        settings['input_csv_dir'] = os.path.join(BASE_DIR, 'same_as_input_2_with_other_authors')
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        query_agents = '''
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT (COUNT (?agent) AS ?agent_count)
            WHERE {
                <https://w3id.org/oc/meta/br/0601> pro:isDocumentContextFor ?agent.
            }
        '''
        endpoint = SPARQLWrapper('http://localhost:9999/blazegraph/sparql')
        endpoint.setQuery(query_agents)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        expected_result = {
            'head': {'vars': ['agent_count']}, 
            'results': {
                'bindings': [{
                    'agent_count': {'datatype': 'http://www.w3.org/2001/XMLSchema#integer', 
                    'type': 'literal', 
                    'value': '6'}}]}}
        shutil.rmtree(output_folder)
        delete_output_zip('.', now)
        self.assertEqual(result, expected_result)

    def test_omid_in_input_data(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_8')
        now = datetime.now()
        meta_config_path_without_openalex=os.path.join(BASE_DIR, 'meta_config_8.yaml')
        meta_config_path_with_openalex=os.path.join(BASE_DIR, 'meta_config_9.yaml')
        with open(meta_config_path_without_openalex, encoding='utf-8') as file:
            settings_without_openalex = yaml.full_load(file)
        with open(meta_config_path_with_openalex, encoding='utf-8') as file:
            settings_with_openalex = yaml.full_load(file)
        run_meta_process(settings=settings_without_openalex, meta_config_path=meta_config_path_without_openalex)
        run_meta_process(settings=settings_with_openalex, meta_config_path=meta_config_path_with_openalex)
        query_all = '''
            PREFIX fabio: <http://purl.org/spar/fabio/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            CONSTRUCT {?s ?p ?o. ?id ?id_p ?id_o.}
            WHERE {
                ?s a fabio:JournalArticle;
                    ?p ?o.
                ?s datacite:hasIdentifier ?id.
                ?id ?id_p ?id_o.
            }
        '''
        endpoint = SPARQLWrapper('http://localhost:9999/blazegraph/sparql')
        endpoint.setQuery(query_all)
        result = endpoint.queryAndConvert()
        expected_result = Graph()
        expected_result.parse(location=os.path.join(BASE_DIR, 'test_omid_in_input_data.json'), format='json-ld')
        prov_graph = ConjunctiveGraph()
        for dirpath, dirnames, filenames in os.walk(os.path.join(output_folder, 'rdf')):
            if 'br' in dirpath and 'prov' in dirpath:
                for filename in filenames:
                    prov_graph.parse(source=os.path.join(dirpath, filename), format='json-ld')
        
        expected_prov_graph = ConjunctiveGraph()
        expected_prov_graph.parse(os.path.join(BASE_DIR, 'test_omid_in_input_data_prov.json'), format='json-ld')
        prov_graph.remove((None, URIRef('http://www.w3.org/ns/prov#generatedAtTime'), None))
        expected_prov_graph.remove((None, URIRef('http://www.w3.org/ns/prov#generatedAtTime'), None))
        prov_graph.remove((None, URIRef('http://www.w3.org/ns/prov#invalidatedAtTime'), None))
        expected_prov_graph.remove((None, URIRef('http://www.w3.org/ns/prov#invalidatedAtTime'), None))
        shutil.rmtree(output_folder)

        self.assertTrue(normalize_graph(result).isomorphic(normalize_graph(expected_result)))
        self.assertTrue(prov_graph.isomorphic(expected_prov_graph))
        delete_output_zip('.', now)

    def test_publishers_sequence(self):
        reset_server()
        output_folder = os.path.join(BASE_DIR, 'output_9')
        meta_config_path = os.path.join(BASE_DIR, 'meta_config_10.yaml')
        now = datetime.now()
        with open(meta_config_path, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        query_all = '''
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            CONSTRUCT {?br ?p ?o. ?o ?op ?oo. ?oo ?oop ?ooo. ?ooo ?ooop ?oooo.}
            WHERE {
                ?id literal:hasLiteralValue "10.17117/na.2015.08.1067";
                    datacite:usesIdentifierScheme datacite:doi;
                    ^datacite:hasIdentifier ?br.
                ?br ?p ?o.
                ?o ?op ?oo.
                ?oo ?oop ?ooo.
                ?ooo ?ooop ?oooo.
            }
        '''
        endpoint = SPARQLWrapper('http://localhost:9999/blazegraph/sparql')
        endpoint.setQuery(query_all)
        result = endpoint.queryAndConvert()
        expected_result = Graph()
        expected_result.parse(os.path.join(BASE_DIR, 'test_publishers_sequence.json'), format='json-ld')
        self.assertTrue(normalize_graph(result).isomorphic(normalize_graph(expected_result)))
        shutil.rmtree(output_folder)
        delete_output_zip('.', now)

def normalize_graph(graph):
    """
    Normalizza i letterali nel grafo rimuovendo i tipi di dato espliciti.
    """
    normalized_graph = Graph()
    for subject, predicate, obj in graph:
        if isinstance(obj, Literal) and obj.datatype is not None:
            normalized_obj = Literal(obj.toPython())
            normalized_graph.add((subject, predicate, normalized_obj))
        else:
            normalized_graph.add((subject, predicate, obj))
    return normalized_graph

if __name__ == '__main__': # pragma: no cover
    unittest.main()