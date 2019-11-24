import json, html
from bs4 import BeautifulSoup
import csv
from scripts.orcidmanager import ORCIDManager
class crossrefBeautify:

    def __init__(self, raw_data_path, orcid_index):
        self.orcid_index = datacollect(orcid_index)
        self.data = list()
        with open(raw_data_path, encoding="utf-8") as json_file:

            raw_data = json.load(json_file)
            input_data = raw_data["items"]

            for x in input_data:
                row = dict()

                self.doi = None
                #create empty row
                keys = ["id", "title", "author", "pub_date", "venue", "volume", "issue", "page", "type", "publisher",
                        "editor"]
                for k in keys:
                    row[k] = ""

                if "type" in x:
                    row["type"] = x["type"].replace("-", " ")

                #row["id"]
                idlist = list()
                if "DOI" in x:
                    if isinstance(x["DOI"], list):
                        idlist.append(str("doi:" + str(x["DOI"][0])))
                        self.doi = str(x["DOI"][0])
                    else:
                        idlist.append(str("doi:" + str(x["DOI"])))
                        self.doi = str(x["DOI"])

                if "ISBN" in x:
                    if row["type"] in {"book", "monograph", "edited book"}:
                        if isinstance(x["ISBN"], list):
                            idlist.append(str("isbn:" + str(x["ISBN"][0])))
                        else:
                            idlist.append(str("isbn:" + str(x["ISBN"])))

                if "ISSN" in x:
                    if row["type"] in {"journal", "series", "report series", "standard series"}:
                        if isinstance(x["ISSN"], list):
                            idlist.append(str("issn:" + str(x["ISSN"][0])))
                        else:
                            idlist.append(str("issn:" + str(x["ISSN"])))

                row["id"] = " ".join(idlist)

                #row["title"]
                if "title" in x:
                    if x["title"]:
                        if isinstance(x["title"], list):
                            text_title = x["title"][0]
                        else:
                            text_title = x["title"]

                        soup = BeautifulSoup(text_title, "html.parser")
                        row["title"] = soup.get_text()

                #row["author"]
                if "author" in x:
                    dict_orcid = None
                    if self.doi and not all("ORCID" in at for at in x["author"]):
                        dict_orcid = self.orcid_finder()
                    autlist = list()
                    for at in x["author"]:
                        if "family" in at:
                            f_name = at["family"]
                            g_name = at["given"]
                            if "given" in at:
                                aut = f_name + ", " + g_name
                            else:
                                aut = f_name + ", "

                            if "ORCID" in at:
                                if isinstance(at["ORCID"], list):
                                    orcid = ORCIDManager().normalise(str(at["ORCID"][0]))
                                else:
                                    orcid = ORCIDManager().normalise(str(at["ORCID"]))
                                aut = aut + " [" + "orcid:" + orcid + "]"
                            elif dict_orcid:
                                for x in dict_orcid:
                                    orc_n = dict_orcid[x].split(", ")
                                    orc_f = orc_n[0]
                                    orc_g = orc_n[1]
                                    if (f_name.lower() in orc_f.lower() or orc_f.lower() in f_name.lower()):
                                        #and (g_name.lower() in orc_g.lower() or orc_g.lower() in g_name.lower()):
                                        aut = aut + " [" + "orcid:" + str(x) + "]"
                            autlist.append(aut)

                    row["author"] = "; ".join(autlist)

                #row["date"]
                if "issued" in x:
                    row["pub_date"] = "-".join([str(y) for y in x["issued"]["date-parts"][0]])


                #row["venue"]
                if "container-title" in x:
                    if isinstance(x["container-title"], list):
                        ventit = str(x["container-title"][0])
                    else:
                        ventit = str(x["container-title"])
                    ven_soup = BeautifulSoup(ventit, "html.parser")
                    ventit = html.unescape(ven_soup.get_text())

                    if "ISBN" in x:
                        if row["type"] not in {"book", "monograph", "edited book"}:
                            if isinstance(x["ISBN"], list):
                                venid = str("isbn:" + str(x["ISBN"][0]))
                            else:
                                venid = str("isbn:" + str(x["ISBN"]))

                    if "ISSN" in x:
                        if row["type"] not in {"journal", "series", "report series", "standard series"}:
                            if isinstance(x["ISSN"], list):
                                venid = str("issn:" + str(x["ISSN"][0]))
                            else:
                                venid = str("issn:" + str(x["ISSN"]))
                    if venid:
                        row["venue"] = ventit + " [" + venid + "]"
                    else:
                        row["venue"] = ventit

                if "volume" in x:
                    row["volume"] = x["volume"]
                if "issue" in x:
                    row["issue"] = x["issue"]
                if "page" in x:
                    row["page"] = x["page"]

                if "publisher" in x:
                    if "member" in x:
                        row["publisher"] = x["publisher"] + " [" + "crossref:" + x["member"] + "]"
                    else:
                        row["publisher"] = x["publisher"]


                if "editor" in x:
                    editlist = list()
                    for ed in x["editor"]:
                        if "family" in ed:
                            if "given" in ed:
                                edit = ed["family"] + ", " + ed["given"]
                            else:
                                edit = ed["family"] + ", "
                            if "ORCID" in ed:
                                if isinstance(ed["ORCID"], list):
                                    edit = edit + " [" + str("orcid:" + str(ed["ORCID"][0])) + "]"
                                else:
                                    edit = edit + " [" + str("orcid:" + str(ed["ORCID"])) + "]"
                            editlist.append(edit)
                    row["editor"] = "; ".join(editlist)
                self.data.append(row)

    def orcid_finder(self):
        found = dict()
        if self.doi in self.orcid_index:
            orcids = self.orcid_index[self.doi]
            if orcids:
                orcids = orcids.split("; ")
                for orc in orcids:
                    orc = orc.replace("]", "").split(" [")
                    found[orc[1]] = orc[0].lower()
        return found

def datacollect(data_path):
    data = dict()
    with open(data_path, 'r', encoding='utf-8') as csvfile:
        csv.field_size_limit(500000000)
        reader = csv.DictReader(csvfile, delimiter="\t")
        for x in reader:
            data[x["doi"].lower()] = x["orcid"]
        return  data
