from csv import DictReader
from oc_meta.lib.file_manager import get_data
from oc_meta.plugins.csv_generator.csv_generator import CSVGenerator
from test.curator_test import reset_server, add_data_ts, SERVER
import os
import shutil
import unittest

BASE = os.path.join('test', 'csv_generator')
CONFIG = os.path.join(BASE, 'csv_generator_config.yaml')
OUTPUT_DIR = os.path.join(BASE, 'csv')
REAL_DATA_RDF = os.path.join(BASE, 'real_data.nt')

class TestCSVGenerator(unittest.TestCase):
    def test_generate_csv(self):
        reset_server(server=SERVER)
        add_data_ts(server=SERVER, data_path=REAL_DATA_RDF)
        csv_generator = CSVGenerator(config=CONFIG)
        csv_generator.generate_csv()
        output = get_data(os.path.join(OUTPUT_DIR, '060', '100', '10.csv'))
        expected_output = [
            {'id': 'meta:br/0601 doi:10.1108/03068299610124298', 'title': 'Ethics And Efficiency In Organizations', 'author': 'Hausken, Kjell [meta:ra/0601]', 'pub_date': '1996-09', 'venue': 'International Journal Of Social Economics [meta:br/06011 issn:0306-8293]', 'volume': '23', 'issue': '9', 'page': '15-40', 'type': 'journal article', 'publisher': 'Emerald [meta:ra/0602 crossref:140]', 'editor': ''}, 
            {'id': 'meta:br/0602 doi:10.1108/10610429810209746', 'title': 'Steel Price Determination In The European Community', 'author': 'Richardson, P.K. [meta:ra/0603]', 'pub_date': '1998-02', 'venue': 'Journal Of Product & Brand Management [meta:br/06014 issn:1061-0421]', 'volume': '7', 'issue': '1', 'page': '62-73', 'type': 'journal article', 'publisher': 'Emerald [meta:ra/0602 crossref:140]', 'editor': ''}, 
            {'id': 'meta:br/0603 doi:10.1108/14684520210424557', 'title': 'Users, End‐Users, And End‐User Searchers Of Online Information: A Historical Overview', 'author': 'Farber, Miriam [meta:ra/0604]; Shoham, Snunith [meta:ra/0605]', 'pub_date': '2002-04', 'venue': 'Online Information Review [meta:br/06017 issn:1468-4527]', 'volume': '26', 'issue': '2', 'page': '92-100', 'type': 'journal article', 'publisher': 'Emerald [meta:ra/0602 crossref:140]', 'editor': ''}, 
            {'id': 'meta:br/0604 doi:10.1080/10357710220147433', 'title': 'The Merger Of The Foreign Affairs And Trade Departments Revisited', 'author': 'Harris, Stuart [meta:ra/0606]', 'pub_date': '2002-07', 'venue': 'Australian Journal Of International Affairs [meta:br/06020 issn:1035-7718 issn:1465-332X]', 'volume': '56', 'issue': '2', 'page': '223-235', 'type': 'journal article', 'publisher': 'Informa Uk Limited [meta:ra/0607 crossref:301]', 'editor': ''}, 
            {'id': 'meta:br/0605 doi:10.1149/1.1481718', 'title': 'Preparation Of Cu-Co Alloy Thin Films On n-Si By Galvanostatic DC Electrodeposition', 'author': 'Pattanaik, Gyana R. [meta:ra/0608]; Pandya, Dinesh K. [meta:ra/0609]; Kashyap, Subhash C. [meta:ra/06010]', 'pub_date': '2002', 'venue': 'Journal Of The Electrochemical Society [meta:br/06023 issn:0013-4651]', 'volume': '149', 'issue': '7', 'page': 'C363-C363', 'type': 'journal article', 'publisher': 'The Electrochemical Society [meta:ra/06011 crossref:77]', 'editor': ''}, 
            {'id': 'meta:br/0606 doi:10.1108/09544789910214836', 'title': 'Within The Canadian Boundaries: A Close Look At Canadian Industries Implementing Continuous Improvement', 'author': 'Jha, Shelly [meta:ra/06012]; Michela, John [meta:ra/06013]; Noori, Hamid [meta:ra/06014]', 'pub_date': '1999-06', 'venue': 'The TQM Magazine [meta:br/06026 issn:0954-478X]', 'volume': '11', 'issue': '3', 'page': '188-197', 'type': 'journal article', 'publisher': 'Emerald [meta:ra/0602 crossref:140]', 'editor': ''}, 
            {'id': 'meta:br/0607 doi:10.1029/2001wr00713', 'title': 'An Approximate Analytical Solution For non-Darcy Flow Toward A Well In Fractured Media', 'author': 'Wu, Yu-Shu [meta:ra/06015]', 'pub_date': '2002-03', 'venue': 'Water Resources Research [meta:br/06029 issn:0043-1397]', 'volume': '38', 'issue': '3', 'page': '5-7', 'type': 'journal article', 'publisher': 'American Geophysical Union (Agu) [meta:ra/06016 crossref:13]', 'editor': ''}, 
            {'id': 'meta:br/0608 doi:10.1046/j.1523-1755.2002.00414.x', 'title': 'Proactive Monitoring Of Pediatric Hemodialysis Vascular Access: Effects Of Ultrasound Dilution On Thrombosis Rates', 'author': 'Goldstein, Stuart L. [meta:ra/06017]; Allsteadt, Amelia [meta:ra/06018]; Smith, Carolyn M. [meta:ra/06019]; Currier, Helen [meta:ra/06020]', 'pub_date': '2002-07', 'venue': 'Kidney International [meta:br/06032 issn:0085-2538]', 'volume': '62', 'issue': '1', 'page': '272-275', 'type': 'journal article', 'publisher': 'Elsevier Bv [meta:ra/06021 crossref:78]', 'editor': ''},
            {'id': 'meta:br/0609 doi:10.1134/1.1493387', 'title': 'Types Of Discrete Symmetries Of Convection In A Plane Fluid Layer', 'author': 'Kistovich, A. V. [meta:ra/06022]; Chashechkin, Yu. D. [meta:ra/06023]', 'pub_date': '2002-06', 'venue': 'Doklady Physics [meta:br/06035 issn:1028-3358 issn:1562-6903]', 'volume': '47', 'issue': '6', 'page': '458-460', 'type': 'journal article', 'publisher': 'Pleiades Publishing Ltd [meta:ra/06024 crossref:137]', 'editor': ''}, 
            {'id': 'meta:br/06010 doi:10.1108/14635779910245115', 'title': 'Tools And Supporting Techniques For Design Quality', 'author': 'Franceschini, Fiorenzo [meta:ra/06025]; Rossetto, Sergio [meta:ra/06026]', 'pub_date': '1999-09', 'venue': 'Benchmarking: An International Journal [meta:br/06038 issn:1463-5771]', 'volume': '6', 'issue': '3', 'page': '212-219', 'type': 'journal article', 'publisher': 'Emerald [meta:ra/0602 crossref:140]', 'editor': ''}
        ]
        shutil.rmtree(OUTPUT_DIR)
        self.assertEqual(output, expected_output)