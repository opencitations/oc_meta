from scripts.graphlib import *
import re, csv, json

class Migrator():
    def __init__(self, data, ra_index_csv, br_index_csv, re_index_csv, ar_index_csv, vi_index_json):
        self.url = "https://w3id.org/oc/meta/"

        self.setgraph = GraphSet(self.url, "", "counter/", wanted_label=False, forced_type=True)

        self.ra_index = self.indexer_id(ra_index_csv)

        self.br_index = self.indexer_id(br_index_csv)

        self.re_index = self.index_re(re_index_csv)

        self.ar_index = self.index_ar(ar_index_csv)

        with open(vi_index_json) as json_file:
            self.vi_index = json.load(json_file)



        for row in data:
            self.row_meta = ""
            ids = row['id']
            title = row['title']
            authors = row['author']
            pub_date = row['pub_date']
            venue = row['venue']
            vol = row['volume']
            issue = row['issue']
            page = row['page']
            self.type = row['type']
            publisher = row['publisher']
            editor = row['editor']


            self.venue_graph = None
            self.vol_graph = None
            self.issue_graph = None

            self.id_job(ids)
            self.title_job(title)
            self.author_job(authors)
            self.pub_date_job(pub_date)
            self.venue_job(venue, vol, issue)
            self.page_job(page)
            self.type_job(self.type)
            if publisher:
                self.publisher_job(publisher)
            if editor:
                self.editor_job(editor)



    @staticmethod
    def index_re (csv_path):
        index = dict()
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter='\t')
            id_index = [dict(x) for x in reader]
            for row in id_index:
                index[row["br"]] = row["re"]
        return index

    @staticmethod
    def index_ar (csv_path):
        index = dict()
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter='\t')
            id_index = [dict(x) for x in reader]
            for row in id_index:
                index[row["meta"]] = dict()
                index[row["meta"]]["author"] = Migrator.ar_worker(row["author"])
                index[row["meta"]]["editor"] = Migrator.ar_worker(row["editor"])
                index[row["meta"]]["publisher"] = Migrator.ar_worker(row["publisher"])
        return index

    @staticmethod
    def ar_worker(str):
        if str:
            ar_dict = dict()
            couples = str.split("; ")
            for c in couples:
                cou = c.split(", ")
                ar_dict[cou[1]] = cou[0]
            return ar_dict
        else:
            return dict()
    @staticmethod
    def indexer_id ( csv_index):
        index = dict()
        index['crossref'] = dict()
        index["doi"] = dict()
        index["issn"] = dict()
        index["isbn"] = dict()
        index["orcid"] = dict()
        index["pmid"] = dict()
        index['pmcid'] = dict()
        index['url'] = dict()
        index['viaf'] = dict()
        index['wikidata'] = dict()


        with open(csv_index, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter='\t')
            id_index = [dict(x) for x in reader]

            for row in id_index:
                if row["id"].startswith("crossref"):
                    id = row["id"].replace('crossref:', '')
                    index['crossref'][id] = row["meta"]

                elif row["id"].startswith("doi"):
                    id = row["id"].replace('doi:', '')
                    index['doi'][id] = row["meta"]

                elif row["id"].startswith("issn"):
                    id = row["id"].replace('issn:', '')
                    index['issn'][id] = row["meta"]

                elif row["id"].startswith("isbn"):
                    id = row["id"].replace('isbn:', '')
                    index['isbn'][id] = row["meta"]

                elif row["id"].startswith("orcid"):
                    id = row["id"].replace('orcid:', '')
                    index['orcid'][id] = row["meta"]

                elif row["id"].startswith("pmid"):
                    id = row["id"].replace('pmid:', '')
                    index['pmid'][id] = row["meta"]

                elif row["id"].startswith("pmcid"):
                    id = row["id"].replace('pmcid:', '')
                    index['pmcid'][id] = row["meta"]

                elif row["id"].startswith("url"):
                    id = row["id"].replace('url:', '')
                    index['url'][id] = row["meta"]

                elif row["id"].startswith("viaf"):
                    id = row["id"].replace('viaf:', '')
                    index['viaf'][id] = row["meta"]

                elif row["id"].startswith("wikidata"):
                    id = row["id"].replace('wikidata:', '')
                    index['wikidata'][id] = row["meta"]

        return index


    def id_job (self, ids):
        idslist = re.split(r'\s+', ids)

        #publication id
        for id in idslist:
            if "meta:" in id:
                id = id.replace("meta:", "")
                self.row_meta = id.replace("br/", "")
                url = URIRef(self.url + id)
                self.br_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=url)

        for id in idslist:
            self.id_creator(self.br_graph, id, ra=False)

    def title_job (self, title):
        if title:
            self.br_graph.create_title(title)


    def author_job (self, authors):
        if authors:
            authorslist = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', authors)

            aut_role_list = list()
            for aut in authorslist:
                aut_id = re.search(r'\[\s*(.*?)\s*\]', aut).group(1)
                aut_id_list = aut_id.split(" ")

                for id in aut_id_list:
                    if "meta:" in id:
                        id = str(id).replace('meta:', "")
                        url = URIRef(self.url + id)
                        aut_meta = id.replace('ra/', "")
                        pub_aut = self.setgraph.add_ra("agent", source_agent=None, source=None, res=url)
                        author_name = re.search(r'(.*?)\s*\[.*?\]', aut).group(1)
                        if "," in author_name:
                            author_name_splitted = re.split(r'\s*,\s*', author_name)
                            firstName = author_name_splitted[1]
                            lastName = author_name_splitted[0]
                            if firstName.strip():
                                pub_aut.create_given_name(firstName)
                            pub_aut.create_family_name(lastName)
                        else:
                            pub_aut.create_name(author_name)

            # lists of authors' IDs
                for id in aut_id_list:
                    self.id_creator(pub_aut, id, ra=True)

                #Author ROLE
                AR = self.ar_index[self.row_meta]["author"][aut_meta]
                ar_id = "ar/" + str(AR)
                url_ar = URIRef(self.url + ar_id)
                pub_aut_role = self.setgraph.add_ar("agent", source_agent=None, source=None, res=url_ar)
                pub_aut_role.create_author(self.br_graph)
                pub_aut.has_role(pub_aut_role)
                aut_role_list.append(pub_aut_role)
                if len(aut_role_list) > 1:
                    pub_aut_role.follows(aut_role_list[aut_role_list.index(pub_aut_role)-1])

    def pub_date_job (self, pub_date):
        if pub_date:
            datelist = list()
            datesplit = pub_date.split("-")
            if datesplit:
                for x in datesplit:
                    datelist.append(int(x))
            else:
                datelist.append(int(pub_date))
            self.br_graph.create_pub_date(datelist)

    def venue_job (self, venue, vol, issue):

        if venue:
            venue_id = re.search(r'\[\s*(.*?)\s*\]', venue).group(1)
            venue_id_list = venue_id.split(" ")

            for id in venue_id_list:
                if "meta:" in id:
                    ven_id = str(id).replace("meta:", "")
                    url = URIRef(self.url + ven_id)
                    venue_title = re.search(r'(.*?)\s*\[.*?\]', venue).group(1)
                    self.venue_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=url)
                    if self.type == "journal article" or self.type == "journal volume" or self.type == "journal issue":
                        self.venue_graph.create_journal()
                    elif self.type == "book chapter" or self.type == "book part":
                        self.venue_graph.create_book()
                    elif self.type == "proceedings article":
                        self.venue_graph.create_proceedings()
                    elif self.type == "report":
                        self.venue_graph.create_report_series()
                    elif self.type == "standard":
                        self.venue_graph.create_standard_series()



                    self.venue_graph.create_title(venue_title)

            for id in venue_id_list:
                self.id_creator(self.venue_graph, id, ra=False)

        if (self.type == "journal article" or self.type == "journal issue") and vol:

            meta_ven = ven_id.replace("br/", "")
            vol_meta = self.vi_index[meta_ven]["volume"][vol]["id"]
            vol_meta = "br/" + vol_meta
            url = URIRef(self.url + vol_meta)
            self.vol_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=url)
            self.vol_graph.create_volume()
            self.vol_graph.create_number(vol)

        if self.type == "journal article" and issue:

            meta_ven = ven_id.replace("br/", "")
            if vol:
                iss_meta = self.vi_index[meta_ven]["volume"][vol]["issue"][issue]["id"]
            else:
                iss_meta = self.vi_index[meta_ven]["issue"][issue]["id"]

            iss_meta = "br/" + iss_meta
            url = URIRef(self.url + iss_meta)
            self.issue_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=url)
            self.issue_graph.create_issue()
            self.issue_graph.create_number(issue)

        if venue and vol and issue:
            self.issue_graph.has_part(self.br_graph)
            self.vol_graph.has_part(self.issue_graph)
            self.venue_graph.has_part(self.vol_graph)

        elif venue and vol and not issue:
            self.vol_graph.has_part(self.br_graph)
            self.venue_graph.has_part(self.vol_graph)

        elif venue and not vol and not issue:
            self.venue_graph.has_part(self.br_graph)

        elif venue and not vol and issue:
            self.issue_graph.has_part(self.br_graph)
            self.venue_graph.has_part(self.issue_graph)


    def page_job (self, page):
        if page:
            res_em = self.re_index[self.row_meta]
            re_id = "re/" + str(res_em)
            url_re = URIRef(self.url + re_id)
            form = self.setgraph.add_re("agent", source_agent=None, source=None, res=url_re)
            form.create_starting_page(page)
            form.create_ending_page(page)
            self.br_graph.has_format(form)


    def type_job (self, type):
        if type == "archival document":
            self.br_graph.create_archival_document()
        elif type == "book":
            self.br_graph.create_book()
        elif type == "book chapter":
            self.br_graph.create_book_chapter()
        elif type == "book part":
            self.br_graph.create_book_part()
        elif type == "book section":
            self.br_graph.create_book_section()
        elif type == "book series":
            self.br_graph.create_book_series()
        elif type == "book set":
            self.br_graph.create_book_set()
        elif type == "data file":
            self.br_graph.create_dataset()
        elif type == "dissertation":
            self.br_graph.create_dissertation()
        elif type == "journal":
            self.br_graph.create_journal()
        elif type == "journal article":
            self.br_graph.create_journal_article()
        elif type == "journal issue":
            self.br_graph.create_issue()
        elif type == "journal volume":
            self.br_graph.create_volume()
        elif type == "proceedings article":
            self.br_graph.create_proceedings_article()
        elif type == "proceedings":
            self.br_graph.create_proceedings()
        elif type == "reference book":
            self.br_graph.create_reference_book()
        elif type == "reference entry":
            self.br_graph.create_reference_entry()
        elif type == "report":
            self.br_graph.create_report()
        elif type == "standard":
            self.br_graph.create_standard()
        elif type == "series":
            self.br_graph.create_series()




    def publisher_job (self, publisher):

        publ_id = re.search(r'\[\s*(.*?)\s*\]', publisher).group(1)
        publ_id_list = publ_id.split(" ")

        for id in publ_id_list:
            if "meta:" in id:
                id = str(id).replace("meta:", "")
                pub_meta = id.replace("ra/","")
                url = URIRef(self.url + id)
                publ_name = re.search(r'(.*?)\s*\[.*?\]', publisher).group(1)
                publ = self.setgraph.add_ra("agent", source_agent=None, source=None, res=url)
                publ.create_name(publ_name)

        for id in publ_id_list:
            self.id_creator(publ, id, ra = True)

        #publisherRole
        AR = self.ar_index[self.row_meta]["publisher"][pub_meta]
        ar_id = "ar/" + str(AR)
        url_ar = URIRef(self.url + ar_id)
        publ_role = self.setgraph.add_ar("agent", source_agent=None, source=None, res=url_ar)
        publ_role.create_publisher(self.br_graph)
        publ.has_role(publ_role)





    def editor_job (self, editor):
        editorslist = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', editor)

        edit_role_list = list()
        for ed in editorslist:
            ed_id = re.search(r'\[\s*(.*?)\s*\]', ed).group(1)
            ed_id_list = ed_id.split(" ")

            for id in ed_id_list:
                if "meta:" in id:
                    id = str(id).replace("meta:", "")
                    ed_meta = id.replace("ra/", "")
                    url = URIRef(self.url + id)
                    pub_ed = self.setgraph.add_ra("agent", source_agent=None, source=None, res=url)
                    editor_name = re.search(r'(.*?)\s*\[.*?\]', ed).group(1)
                    if "," in editor_name:
                        editor_name_splitted = re.split(r'\s*,\s*', editor_name)
                        firstName = editor_name_splitted[1]
                        lastName = editor_name_splitted[0]
                        if firstName.strip():
                            pub_ed.create_given_name(firstName)
                        pub_ed.create_family_name(lastName)
                    else:
                        pub_ed.create_name(editor_name)


        # lists of editor's IDs
            for id in ed_id_list:
                self.id_creator(pub_ed, id, ra=True)

        #editorRole
            AR = self.ar_index[self.row_meta]["editor"][ed_meta]
            ar_id = "ar/" + str(AR)
            url_ar = URIRef(self.url + ar_id)
            pub_ed_role = self.setgraph.add_ar("agent", source_agent=None, source=None, res=url_ar)

            if self.type == "journal article" and self.issue_graph:
                pub_ed_role.create_editor(self.issue_graph)
            elif self.type == "journal article" and not self.issue_graph and self.vol_graph:
                pub_ed_role.create_editor(self.vol_graph)
            elif (self.type == "book chapter" or self.type == "book part") and self.venue_graph:
                pub_ed_role.create_editor(self.venue_graph)
            else:
                pub_ed_role.create_editor(self.br_graph)

            pub_ed.has_role(pub_ed_role)
            edit_role_list.append(pub_ed_role)
            if len(edit_role_list) > 1:
                pub_ed_role.follows(edit_role_list[edit_role_list.index(pub_ed_role)-1])




    def id_creator (self,graph, id, ra):

        new_id = None

        if ra:
            if id.startswith("crossref"):
                id = id.replace('crossref:', '')
                res = self.ra_index['crossref'][id]
                url = URIRef(self.url + "id/" + res)
                new_id = self.setgraph.add_id("agent", source_agent=None, source=None, res=url)
                new_id.create_crossref(id)

            elif id.startswith("orcid"):
                id = id.replace("orcid:", "")
                res = self.ra_index['orcid'][id]
                url = URIRef(self.url + "id/" + res)
                new_id = self.setgraph.add_id("agent", source_agent=None, source=None, res=url)
                new_id.create_orcid(id)

            elif id.startswith("viaf"):
                id = id.replace("viaf:", "")
                res = self.ra_index['viaf'][id]
                url = URIRef(self.url + "id/" + res)
                new_id = self.setgraph.add_id("agent", source_agent=None, source=None, res=url)
                new_id.create_viaf(id)

            elif id.startswith("wikidata"):
                id = id.replace("wikidata:", "")
                res = self.ra_index['wikidata'][id]
                url = URIRef(self.url + "id/" + res)
                new_id = self.setgraph.add_id("agent", source_agent=None, source=None, res=url)
                new_id.create_wikidata(id)

        elif id.startswith("doi"):
            id = id.replace("doi:", "")
            res = self.br_index['doi'][id]
            url = URIRef(self.url + "id/" + res)
            new_id = self.setgraph.add_id("agent", source_agent=None, source=None, res=url)
            new_id.create_doi(id)

        elif id.startswith("issn"):
            id = id.replace("issn:", "")
            res = self.br_index['issn'][id]
            url = URIRef(self.url + "id/" + res)
            new_id = self.setgraph.add_id("agent", source_agent=None, source=None, res=url)
            new_id.create_issn(id)

        elif id.startswith("isbn"):
            id = id.replace("isbn:", "")
            res = self.br_index['isbn'][id]
            url = URIRef(self.url + "id/" + res)
            new_id = self.setgraph.add_id("agent", source_agent=None, source=None, res=url)
            new_id.create_isbn(id)

        elif id.startswith("pmid"):
            id = id.replace("pmid:", "")
            res = self.br_index['pmid'][id]
            url = URIRef(self.url + "id/" + res)
            new_id = self.setgraph.add_id("agent", source_agent=None, source=None, res=url)
            new_id.create_pmid(id)

        elif id.startswith("pmcid"):
            id = id.replace("pmcid:", "")
            res = self.br_index['pmcid'][id]
            url = URIRef(self.url + "id/" + res)
            new_id = self.setgraph.add_id("agent", source_agent=None, source=None, res=url)
            new_id.create_pmcid(id)

        elif id.startswith("url"):
            id = id.replace("url:", "")
            res = self.br_index['url'][id]
            url = URIRef(self.url + "id/" + res)
            new_id = self.setgraph.add_id("agent", source_agent=None, source=None, res=url)
            new_id.create_url(id)

        elif id.startswith("wikidata"):
            id = id.replace("wikidata:", "")
            res = self.br_index['wikidata'][id]
            url = URIRef(self.url + "id/" + res)
            new_id = self.setgraph.add_id("agent", source_agent=None, source=None, res=url)
            new_id.create_wikidata(id)

        if new_id:
            graph.has_id(new_id)