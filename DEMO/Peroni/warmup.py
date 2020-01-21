from creator import *
from curator import *
from crossref.crossrefProcessing import *
from scripts.storer import *
from scripts.resfinder import ResourceFinder
from scripts.conf import reference_dir, base_iri, context_path, info_dir, triplestore_url, orcid_conf_path, \
    base_dir, temp_dir_for_rdf_loading, context_file_path, dir_split_number, items_per_file, triplestore_url_real, \
    dataset_home, reference_dir_done, reference_dir_error, interface, supplier_dir, default_dir, do_parallel, \
    sharing_dir


def warmup (raw_json_path, doi_orcid, name, doi_csv):
    server = "http://127.0.0.1:9999/blazegraph/sparql"
    row_csv = crossrefProcessing(raw_json_path, doi_orcid, doi_csv).data
    clean_csv = Curator(row_csv, server, info_dir=info_dir, filename=name, path="csv/indices/" + name + "/")
    print(clean_csv.log)
    crossref_csv = "csv/indices/" + name + "/data_" + name + ".csv"
    crossref_id_br = "csv/indices/" + name + "/index_id_br_" + name + ".csv"
    crossref_id_ra = "csv/indices/" + name + "/index_id_ra_" + name + ".csv"
    crossref_ar = "csv/indices/" + name + "/index_ar_" + name + ".csv"
    crossref_re = "csv/indices/" + name + "/index_re_" + name + ".csv"
    crossref_vi = "csv/indices/" + name + "/index_vi_" + name + ".json"

    with open(crossref_csv, 'r', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        data = [dict(x) for x in reader]

    creator = Creator(data, base_iri, crossref_id_ra, crossref_id_br, crossref_re, crossref_ar, crossref_vi).setgraph

    prov = ProvSet(creator, base_iri, context_path, default_dir, "counter_prov/counter_",
                   ResourceFinder(base_dir=base_dir, base_iri=base_iri,
                                  tmp_dir=temp_dir_for_rdf_loading,
                                  context_map=
                                  {},
                                  dir_split=dir_split_number,
                                  n_file_item=items_per_file,
                                  default_dir=default_dir),
                   dir_split_number, items_per_file, "", wanted_label=False)
    prov.generate_provenance("meta_demo_agent")

    res_storer = Storer(creator,
                        context_map={},
                        dir_split=dir_split_number,
                        n_file_item=items_per_file,
                        default_dir=default_dir,
                        nt=True)

    prov_storer = Storer(prov,
                         context_map={},
                         dir_split=dir_split_number,
                         n_file_item=items_per_file,
                         nt=True)

    res_storer.upload_and_store(
        base_dir, triplestore_url, base_iri, context_path,
        temp_dir_for_rdf_loading)

    prov_storer.store_all(
        base_dir, base_iri, context_path,
        temp_dir_for_rdf_loading)


def reset():
    with open(info_dir + "br.txt", 'w') as br:
        br.write('0')
    with open(info_dir + "id.txt", 'w') as br:
        br.write('0')
    with open(info_dir + "ra.txt", 'w') as br:
        br.write('0')
    with open(info_dir + "ar.txt", 'w') as br:
        br.write('0')
    with open(info_dir + "re.txt", 'w') as br:
        br.write('0')

def reset_server(server):
    ts = sparql.SPARQLServer(server)
    ts.update('delete{?x ?y ?z} where{?x ?y ?z}')

#eset()
reset_server("http://127.0.0.1:9999/blazegraph/sparql")
warmup("json/Peroni.json", "csv/orcid.csv", "Peroni", "csv/doi.csv")
warmup("json/Vitali.json", "csv/orcid.csv", "Vitali", "csv/doi.csv")