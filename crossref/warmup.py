from migrator import *
from converter import *
from crossref.crossrefBeautify import *
def warmup (raw_data_path):
    row_csv = crossrefBeautify(raw_data_path, "inde_orc_pro.csv").data
    name = "crossref_orcid_prova"
    server = "http://127.0.0.1:9999/blazegraph/sparql"
    clean_csv = Converter(row_csv, server, filename=name, path="csv/indices/" + name + "/")
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

    #migrator = Migrator(data, crossref_id_ra, crossref_id_br, crossref_re, crossref_ar, crossref_vi)

    #migrator.final_graph.serialize(name + ".ttl", format="ttl" )

#warmup("json/1.json")
#warmup("json/2019.json")
#warmup("json/Feynman.json")
#warmup("json/Dumontier.json")
warmup("json/haleem_mohamed.json")

