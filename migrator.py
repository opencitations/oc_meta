from graphlib import *
import re

class Migrator():
    def __init__(self, data):
        self.setgraph = GraphSet("https://w3id.org/OC/meta/", "", "counter/")

        for row in data:
            ids = row['id']
            title = row['title']
            authors = row['author']
            pub_date = row['pub_date']
            venue = row['venue']
            vol = row['volume']
            issue = row['issue']
            page = row['page']
            type = row['type']
            publisher = row['publisher']

            self.url = "https://w3id.org/OC/meta/"

            self.id_job(ids)
            self.title_job(title)
            self.author_job(authors)
            self.pub_date_job(pub_date)
            self.venue_job(venue, vol, issue)
            self.page_job(page)
            self.type_job(type)
            self.publisher_job(publisher)

        self.final_graph = Graph()
        for g in self.setgraph.graphs():
            self.final_graph += g



    def id_job (self, ids):
        idslist = re.split(r'\s*;\s*', ids)

        #publication id
        for id in idslist:
            if "meta:" in id:
                id = id.replace("meta:", "")
                url = URIRef(self.url + id)
                self.br_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=url)

        for id in idslist:
            if "doi:" in id:
                id = id.replace("doi:", "")
                pub_doi = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                pub_doi.create_doi(id)
                self.br_graph.has_id(pub_doi)

            if "wikidata" in id:
                id = id.replace("wikidata:", "")
                pub_wikidata = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                pub_wikidata.create_wikidata(id)
                self.br_graph.has_id(pub_wikidata)

    def title_job (self, title):
        self.br_graph.create_title(title)


    def author_job (self, authors):
        authorslist = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', authors)

        for aut in authorslist:
            aut_id = re.search(r'\[\s*(.*?)\s*\]', aut).group(1)
            aut_id_list = re.split(r'\s*;\s*', aut_id)

            for id in aut_id_list:
                if "meta:" in id:
                    id = id.replace("meta:", "")
                    url = URIRef(self.url + id)
                    pub_aut = self.setgraph.add_ra("agent", source_agent=None, source=None, res=url)
                    author_name = re.search(r'(.*?)\s*\[.*?\]', aut).group(1)
                    author_name_splitted = re.split(r'\s*,\s*', author_name)
                    firstName = author_name_splitted[1]
                    lastName = author_name_splitted[0]
                    pub_aut.create_given_name(firstName)
                    pub_aut.create_family_name(lastName)

        # lists of authors' IDs
            for id in aut_id_list:
                if "orcid" in id:
                    id = id.replace("orcid:", "")
                    pub_aut_orcid = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                    pub_aut_orcid.create_orcid(id)
                    pub_aut.has_id(pub_aut_orcid)
                if "viaf" in id:
                    id = id.replace("viaf:", "")
                    pub_aut_viaf = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                    pub_aut_viaf.create_viaf(id)
                    pub_aut.has_id(pub_aut_viaf)
                if "wikidata" in id:
                    id = id.replace("wikidata:", "")
                    pub_aut_wikidata = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                    pub_aut_wikidata.create_wikidata(id)
                    pub_aut.has_id(pub_aut_wikidata)


        # authorRole
            pub_aut_role = self.setgraph.add_ar("agent", source_agent=None, source=None, res=None)
            pub_aut_role.create_author(self.br_graph)
            pub_aut.has_role(pub_aut_role)

    def pub_date_job (self, pub_date):
        datelist = list()
        datelist.append(int(pub_date))
        self.br_graph.create_pub_date(datelist)

    def venue_job (self, venue, vol, issue):

        if venue:
            venue_id = re.search(r'\[\s*(.*?)\s*\]', venue).group(1)
            venue_id_list = re.split(r'\s*;\s*', venue_id)

            for id in venue_id_list:
                if "meta:" in id:
                    id = id.replace("meta:", "")
                    url = URIRef(self.url + id)
                    venue_title = re.search(r'(.*?)\s*\[.*?\]', venue).group(1)
                    venue_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=url)
                    venue_graph.create_title(venue_title)

            for id in venue_id_list:
                if "issn" in id:
                    id = id.replace("issn:", "")
                    venue_issn = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                    venue_issn.create_issn(id)
                    venue_graph.has_id(venue_issn)
                if "doi" in id:
                    id = id.replace("doi:", "")
                    venue_doi = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                    venue_doi.create_doi(id)
                    venue_graph.has_id(venue_doi)
                if "isbn" in id:
                    id = id.replace("isbn:", "")
                    venue_isbn = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                    venue_isbn.create_isbn(id)
                    venue_graph.has_id(venue_isbn)


        if vol:
            vol_id = re.search(r'\[\s*(.*?)\s*\]', vol).group(1)
            vol_id_list = re.split(r'\s*;\s*', vol_id)

            for id in vol_id_list:
                if "meta:" in id:
                    id = id.replace("meta:", "")
                    url = URIRef(self.url + id)
                    vol_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=url)
                    vol_title = re.search(r'(.*?)\s*\[.*?\]', vol).group(1)
                    vol_graph.create_volume()
                    vol_graph.create_number(vol_title)


        if issue:
            issue_id = re.search(r'\[\s*(.*?)\s*\]', issue).group(1)
            issue_id_list = re.split(r'\s*;\s*', issue_id)

            for id in issue_id_list:
                if "meta:" in id:
                    id = id.replace("meta:", "")
                    url = URIRef(self.url + id)
                    issue_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=url)
                    issue_title = re.search(r'(.*?)\s*\[.*?\]', issue).group(1)
                    issue_graph.create_issue()
                    issue_graph.create_number(issue_title)

        if venue and vol and issue:
            issue_graph.has_part(self.br_graph)
            vol_graph.has_part(issue_graph)
            venue_graph.has_part(vol_graph)
        elif venue and not vol and issue:
            issue_graph.has_part(self.br_graph)
            venue_graph.has_part(issue_graph)
        elif venue and vol and not issue:
            vol_graph.has_part(self.br_graph)
            venue_graph.has_part(vol_graph)
        elif not venue and vol and issue:
            issue_graph.has_part(self.br_graph)
            vol_graph.has_part(issue_graph)
        elif venue and not vol and not issue:
            venue_graph.has_part(self.br_graph)
        elif not venue and vol and not issue:
            vol_graph.has_part(self.br_graph)
        elif not venue and not vol and issue:
            issue_graph.has_part(self.br_graph)


    def page_job (self, page):
        if page:
            form = self.setgraph.add_re("agent", source_agent=None, source=None, res=None)
            pages = page.split("-")
            form.create_starting_page(pages[0])
            form.create_ending_page(pages[1])
            self.br_graph.has_format(form)


    def type_job (self, type):
        if type == "journal article":
            self.br_graph.create_journal_article()
        elif type == "book chapter":
            self.br_graph.create_book_chapter()


    def publisher_job (self, publisher):

        publ_id = re.search(r'\[\s*(.*?)\s*\]', publisher).group(1)
        publ_id_list = re.split(r'\s*;\s*', publ_id)

        for id in publ_id_list:
            if "meta:" in id:
                id = id.replace("meta:", "")
                url = URIRef(self.url + id)
                publ_name = re.search(r'(.*?)\s*\[.*?\]', publisher).group(1)
                publ = self.setgraph.add_ra("agent", source_agent=None, source=None, res=url)
                publ.create_name(publ_name)

        for id in publ_id_list:
                if 'crossref' in id:
                    id = id.replace('crossref:','')
                    publ_crossref = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                    publ_crossref.create_crossref(id)
                    publ.has_id(publ_crossref)

        # authorRole
        publ_role = self.setgraph.add_ar("agent", source_agent=None, source=None, res=None)
        publ_role.create_publisher(self.br_graph)
        publ.has_role(publ_role)