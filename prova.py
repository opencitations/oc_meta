'''

'''

'''
from migrator import *
from rdflib import URIRef
url = "https://w3id.org/OC/meta/"

setgraph = GraphSet(url, "", "fff/")
u = URIRef("https://w3id.org/OC/meta/ssss")
pub_aut = setgraph.add_ra("agent", source_agent=None, source=None, res=u, wanted_type = True)
ub_aut = setgraph.add_ra("agent", source_agent=None, source=None, res=u, wanted_type = True)


pro = ProvSet(setgraph, default_dir="prov_count/", info_dir="prov_count/", resource_finder=None, dir_split=None, n_file_item=5, supplier_prefix=None,base_iri="https://w3id.org/OC/provmeta/",context_path=None)
pro._add_prov(short_name="prov", prov_type="yftftr", res="fsdsfds", resp_agent="OC")
for g in setgraph.graphs():
    for x,y,z in g:
        print(x,y,z)

'''

from migrator import *
from scripts.storer import *
from scripts.resfinder import ResourceFinder
from scripts.conf import reference_dir, base_iri, context_path, info_dir, triplestore_url, orcid_conf_path, \
    base_dir, temp_dir_for_rdf_loading, context_file_path, dir_split_number, items_per_file, triplestore_url_real, \
    dataset_home, reference_dir_done, reference_dir_error, interface, supplier_dir, default_dir, do_parallel, \
    sharing_dir

testcase_csv = "tdd/testcases/testcase_data/testcase_01_data.csv"
testcase_id_br = "tdd/testcases/testcase_data/indices/01/index_id_br_01.csv"
testcase_id_ra = "tdd/testcases/testcase_data/indices/01/index_id_ra_01.csv"
testcase_ar = "tdd/testcases/testcase_data/indices/01/index_ar_01.csv"
testcase_re = "tdd/testcases/testcase_data/indices/01/index_re_01.csv"
testcase_vi = "tdd/testcases/testcase_data/indices/01/index_vi_01.json"
testcase_ttl = "tdd/testcases/testcase_01.ttl"

with open(testcase_csv, 'r') as csvfile:
    reader = csv.DictReader(csvfile, delimiter="\t")
    data = [dict(x) for x in reader]

migrator = Migrator(data, testcase_id_ra, testcase_id_br, testcase_re, testcase_ar, testcase_vi).setgraph

#st = Storer(nt=True, graph_set=migrator.setgraph)

#st.upload_and_store("", triplestore_url="http://127.0.0.1:9999/blazegraph/sparql", base_iri=None, context_path=None)
'''
prov = ProvSet(migrator, base_iri, context_path, default_dir, "",
               ResourceFinder(base_dir=base_dir, base_iri=base_iri,
                              tmp_dir=temp_dir_for_rdf_loading,
                              context_map=
                              {},
                              dir_split=dir_split_number,
                              n_file_item=items_per_file,
                              default_dir=default_dir),
               dir_split_number, items_per_file, "", wanted_label=False)
prov.generate_provenance()

gg = Graph()
for x in prov.graphs():
    gg += x

gg.serialize("ffff.nt", format="nt11")

'''
res_storer = Storer(migrator,context_map={},dir_split=dir_split_number,n_file_item=items_per_file,default_dir=default_dir, nt=True)
res_storer.upload_and_store(base_dir, triplestore_url, base_iri, context_path, temp_dir_for_rdf_loading)
