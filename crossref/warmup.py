from migrator import *
from converter import *
import json

def warmup (raw_data_path):
    row_list = list()
    with open(raw_data_path, encoding="utf-8") as json_file:
        raw_data = json.load(json_file)
        for x in raw_data["items"]:
            row = dict()
            idlist = list()
            keys = ["id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"]
            for k in keys:
                row[k] = ""

            for y in x:
                if y.isupper() and "ISSN" not in y:
                    if isinstance(x[y], list):
                        idlist.append(str(y.lower()) + ":" + str(x[y][0]))
                    else:
                        idlist.append(str(y.lower()) + ":" + str(x[y]))
            row["id"] = " ".join(idlist)

            if x["title"]:
                if isinstance(x["title"], list):
                    row["title"] = x["title"][0]
                else:
                    row["title"] = x["title"]

            autlist = list()
            if "author" in x:
                for at in x["author"]:
                    if "family" in at:
                        if "given" in at:
                            aut = at["family"] + ", " + at["given"]
                        else:
                            aut = at["family"] + ", "
                        autlist.append(aut)
                row["author"] = "; ".join(autlist)

            if "issued" in x:
                row["pub_date"] = "-".join([str(y) for y in x["issued"]["date-parts"][0]])
            if "container-title" in x:
                for y in x:
                    if y.isupper() and "ISSN" in y:
                        if isinstance(x[y], list):
                            id = str(y.lower()) + ":" + str(x[y][0])
                        else:
                            id = str(y.lower()) + ":" + str(x[y])

                row["venue"] = x["container-title"][0] + " [" + id + "]"
            if "volume" in x:
                row["volume"] = x["volume"]
            if "issue" in x:
                row["issue"] = x["issue"]
            if "page" in x:
                row["page"] = x["page"]
            if "type" in x:
                row["type"] = x["type"].replace("-", " ")
            if "publisher" in x:
                row["publisher"] = x["publisher"]
            if "editor" in x:
                row["editor"] = ""


            row_list.append(row)

    row_csv = row_list[0:10]
    name = "crossref_01"
    server = "http://127.0.0.1:9999/blazegraph/sparql"
    #csv = Converter(row_csv, server, filename=name, path="csv/indices/" + name + "/")

    crossref_csv = "csv/indices/" + name + "/data_" + name + ".csv"
    crossref_id_br = "csv/indices/" + name + "/index_id_br_" + name + ".csv"
    crossref_id_ra = "csv/indices/" + name + "/index_id_ra_" + name + ".csv"
    crossref_ar = "csv/indices/" + name + "/index_ar_" + name + ".csv"
    crossref_re = "csv/indices/" + name + "/index_re_" + name + ".csv"
    crossref_vi = "csv/indices/" + name + "/index_vi_" + name + ".json"

    with open(crossref_csv, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        data = [dict(x) for x in reader]

    migrator = Migrator(data, crossref_id_ra, crossref_id_br, crossref_re, crossref_ar, crossref_vi)

    migrator.final_graph.serialize(name + ".ttl", format="ttl" )

warmup("1.json")