import json, html
from bs4 import BeautifulSoup
from scripts.id_manager.orcidmanager import ORCIDManager
from scripts.csvmanager import CSVManager
from scripts.id_manager.issnmanager import ISSNManager
from scripts.id_manager.isbnmanager import ISBNManager
from scripts.id_manager.doimanager import DOIManager


class crossrefBeautify:

    def __init__(self, raw_data_path, orcid_index):
        self.orcid_index = CSVManager(orcid_index)
        self.data = list()
        with open(raw_data_path, encoding="utf-8") as json_file:

            raw_data = json.load(json_file)
            input_data = raw_data["items"]

            for x in input_data:
                row = dict()

                #create empty row
                keys = ["id", "title", "author", "pub_date", "venue", "volume", "issue", "page", "type", "publisher",
                        "editor"]
                for k in keys:
                    row[k] = ""

                if "type" in x:
                    row["type"] = x["type"].replace("-", " ")

                #row["id"]
                idlist = list()
                doi = None
                if "DOI" in x:
                    if isinstance(x["DOI"], list):
                        doi = DOIManager().normalise(str(x["DOI"][0]))
                    else:
                        doi = DOIManager().normalise(str(x["DOI"]))
                    idlist.append(str("doi:" + doi))

                if "ISBN" in x:
                    if row["type"] in {"book", "monograph", "edited book"}:
                        if isinstance(x["ISBN"], list):
                            isbnid = str(x["ISBN"][0])
                        else:
                            isbnid = str(x["ISBN"])
                        if ISBNManager().is_valid(isbnid):
                            isbnid = ISBNManager().normalise(isbnid)
                            idlist.append(str("isbn:" + isbnid))

                if "ISSN" in x:
                    if row["type"] in {"journal", "series", "report series", "standard series"}:
                        if isinstance(x["ISSN"], list):
                            issnid = str(x["ISSN"][0])
                        else:
                            issnid = str(x["ISSN"])
                        if ISSNManager().is_valid(issnid):
                            issnid = ISSNManager().normalise(issnid)
                            idlist.append(str("issn:" + issnid))
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
                    if doi and not all("ORCID" in at for at in x["author"]):
                        dict_orcid = self.orcid_finder(doi)
                    autlist = list()
                    for at in x["author"]:
                        if "family" in at:
                            f_name = at["family"]
                            g_name = at["given"]
                            if "given" in at:
                                aut = f_name + ", " + g_name
                            else:
                                aut = f_name + ", "
                            orcid = None
                            if "ORCID" in at:
                                if isinstance(at["ORCID"], list):
                                    orcid = str(at["ORCID"][0])
                                else:
                                    orcid = str(at["ORCID"])
                                if ORCIDManager().is_valid(orcid):
                                    orcid = ORCIDManager().normalise(orcid)
                                else:
                                    orcid = None
                            elif dict_orcid:
                                for x in dict_orcid:
                                    orc_n = dict_orcid[x].split(", ")
                                    orc_f = orc_n[0]
                                    orc_g = orc_n[1]
                                    if (f_name.lower() in orc_f.lower() or orc_f.lower() in f_name.lower()):
                                        #and (g_name.lower() in orc_g.lower() or orc_g.lower() in g_name.lower()):
                                        orcid = x
                            if orcid:
                                aut = aut + " [" + "orcid:" + str(orcid) + "]"
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
                    venidlist = list()
                    if "ISBN" in x:
                        if row["type"] not in {"book", "monograph", "edited book"}:
                            if isinstance(x["ISBN"], list):
                                venisbnid = str(x["ISBN"][0])
                            else:
                                venisbnid = str(x["ISBN"])
                            if ISBNManager().is_valid(venisbnid):
                                venisbnid = ISBNManager().normalise(venisbnid)
                                venidlist.append(str("isbn:" + venisbnid))

                    if "ISSN" in x:
                        if row["type"] not in {"journal", "series", "report series", "standard series"}:
                            if isinstance(x["ISSN"], list):
                                venissnid = str("issn:" + str(x["ISSN"][0]))
                            else:
                                venissnid = str("issn:" + str(x["ISSN"]))
                            if ISSNManager().is_valid(venissnid):
                                venissnid = ISSNManager().normalise(venissnid)
                                venidlist.append(str("issn:" + venissnid))
                    if venidlist:
                        row["venue"] = ventit + " [" + " ".join(venidlist) + "]"
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
                            edorcid = None
                            if "ORCID" in ed:
                                if isinstance(ed["ORCID"], list):
                                    edorcid = str(ed["ORCID"][0])
                                else:
                                    edorcid = str(ed["ORCID"])
                                if ORCIDManager().is_valid(edorcid):
                                    edorcid = ORCIDManager().normalise(edorcid)
                                else:
                                    edorcid = None
                                if edorcid:
                                    edit = edit + " [orcid:" + str(edorcid) + "]"
                            editlist.append(edit)
                    row["editor"] = "; ".join(editlist)
                self.data.append(row)

    def orcid_finder(self, doi):
        found = dict()
        doi = doi.lower()
        orcids = self.orcid_index.get_value(doi)
        if orcids:
            for orcid in orcids:
                orcid = orcid.split("; ")
                for orc in orcid:
                    orc = orc.replace("]", "").split(" [")
                    found[orc[1]] = orc[0].lower()
        return found
