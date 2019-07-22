from graphlib import *
import re

class Migrator():
    def __init__(self, row, sgraph):
        self.ids = row['id']
        self.title = row['title']
        self.authors = row['author']
        self.pub_date = row['pub_date']
        self.venue = row['venue']
        self.vol = row['volume']
        self.issue = row['issue']
        self.page = row['page']
        self.type = row['type']
        self.publ = row['publisher']

        #New BR
        self.setgraph = sgraph

        self.br_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=None)
        with open('counter/br.txt', 'r') as file:
            self.name = str(int(file.read()))



    def id_job (self):
        ids = self.ids
        idslist = re.split(r'\s*;\s*', ids)

        #publication id
        for id in idslist:
            if "doi" in id:
                id = id.replace("doi:", "")
                pub_doi = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                pub_doi.create_doi(id)
                self.br_graph.has_id(pub_doi)

                #serialization
                with open('counter/id.txt', 'r') as file:
                    name = str(int(file.read()))
                IDgraph = pub_doi.g
                IDgraph.serialize(destination='rdf/id/' + name +'.json', format='json-ld', auto_compact=True)

            #TODO 'if "wikidata"...'
            #if "wikidata" in id:
            #    id = id.replace("wikidata:", "")
            #    wikidatalist.append(id)


    def title_job (self):
        self.br_graph.create_title(self.title)


    def author_job (self):
        authors = self.authors
        authorslist = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', authors)

        for aut in authorslist:
            pub_aut = self.setgraph.add_ra("agent", source_agent=None, source=None, res=None)
            author_name = re.search(r'(.*?)\s*\[.*?\]', aut).group(1)
            author_name_splitted = re.split(r'\s*,\s*', author_name)
            firstName = author_name_splitted[1]
            lastName = author_name_splitted[0]
            pub_aut.create_given_name(firstName)
            pub_aut.create_family_name(lastName)

            aut_id = re.search(r'\[\s*(.*?)\s*\]', aut).group(1)
            aut_id_list = re.split(r'\s*;\s*', aut_id)


        # lists of authors' IDs
            for id in aut_id_list:
                if "orcid" in id:
                    id = id.replace("orcid:", "")
                    pub_aut_orcid = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                    pub_aut_orcid.create_orcid(id)
                    pub_aut.has_id(pub_aut_orcid)
                    AutOrcid = pub_aut_orcid.g
                    with open('counter/id.txt', 'r') as file:
                        name = str(int(file.read()))
                    AutOrcid.serialize(destination='rdf/id/' + name + '.json', format='json-ld', auto_compact=True)

        # authorRole
            pub_aut_role = self.setgraph.add_ar("agent", source_agent=None, source=None, res=None)
            pub_aut_role.create_author(self.br_graph)
            pub_aut.has_role(pub_aut_role)
            AutRole = pub_aut_role.g
            with open('counter/ar.txt', 'r') as file:
                name = str(int(file.read()))
            AutRole.serialize(destination='rdf/ar/' + name + '.json', format='json-ld', auto_compact=True)

            Aut = pub_aut.g
            with open('counter/ra.txt', 'r') as file:
                name = str(int(file.read()))
            Aut.serialize(destination='rdf/ra/' + name + '.json', format='json-ld', auto_compact=True)


    def pub_date_job (self):
        datelist = list()
        datelist.append(int(self.pub_date))
        self.br_graph.create_pub_date(datelist)

    def venue_job (self):
        venue_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=None)
        with open('counter/br.txt', 'r') as file:
            venue_filename = str(int(file.read()))

        venue_title = re.search(r'(.*?)\s*\[.*?\]', self.venue).group(1)
        venue_graph.create_title(venue_title)

        venue_id = re.search(r'\[\s*(.*?)\s*\]', self.venue).group(1)
        venue_id_list = re.split(r'\s*;\s*', venue_id)

        for id in venue_id_list:
            if "issn" in id:
                id = id.replace("issn:", "")
                venue_issn = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                venue_issn.create_issn(id)
                venue_graph.has_id(venue_issn)
                VenueIssn = venue_issn.g
                with open('counter/id.txt', 'r') as file:
                    name = str(int(file.read()))
                VenueIssn.serialize(destination='rdf/id/' + name + '.json', format='json-ld', auto_compact=True)

        if self.vol:
            vol_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=None)
            with open('counter/br.txt', 'r') as file:
                vol_filename = str(int(file.read()))
            vol_graph.create_volume()
            vol_graph.create_number(self.vol)
            venue_graph.has_part(vol_graph)

            if self.issue:
                issue_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=None)
                with open('counter/br.txt', 'r') as file:
                    issue_filename = str(int(file.read()))
                issue_graph.create_issue()
                issue_graph.create_number(self.issue)
                vol_graph.has_part(issue_graph)
                issue_graph.has_part(self.br_graph)

                VenueGraph = venue_graph.g
                VenueGraph.serialize(destination='rdf/br/' + venue_filename + '.json', format='json-ld', auto_compact=True)
                VolGraph = vol_graph.g
                VolGraph.serialize(destination='rdf/br/' + vol_filename + '.json', format='json-ld', auto_compact=True)
                IssueGraph = issue_graph.g
                IssueGraph.serialize(destination='rdf/br/' + issue_filename + '.json', format='json-ld', auto_compact=True)

            else:
                venue_graph.has_part(vol_graph)
                vol_graph.has_part(self.br_graph)
                VenueGraph = venue_graph.g
                VenueGraph.serialize(destination='rdf/br/' + venue_filename + '.json', format='json-ld', auto_compact=True)
                VolGraph = vol_graph.g
                VolGraph.serialize(destination='rdf/br/' + vol_filename + '.json', format='json-ld', auto_compact=True)

        else:
            venue_graph.has_part(self.br_graph)
            VenueGraph = venue_graph.g
            VenueGraph.serialize(destination='rdf/br/' + venue_filename + '.json', format='json-ld', auto_compact=True)

    #TODO
    def page_job (self):
        page = self.page
        return page

    def type_job (self):
        if self.type == "journal article":
            self.br_graph.create_journal_article()
        elif self.type == "book chapter":
            self.br_graph.create_book_chapter()

    def publisher_job (self):
        publ_name = re.search(r'(.*?)\s*\[.*?\]', self.publ).group(1)
        publ = self.setgraph.add_ra("agent", source_agent=None, source=None, res=None)
        publ.create_name(publ_name)

        publ_id = re.search(r'\[\s*(.*?)\s*\]', self.publ).group(1)
        publ_id_list = re.split(r'\s*;\s*', publ_id)

        #for id in publ_id_list:
        #        if 'crossref' in id:
        #            id = id.replace('crossref:','')
        #            pub_crossref = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)

        # authorRole
        publ_role = self.setgraph.add_ar("agent", source_agent=None, source=None, res=None)
        publ_role.create_publisher(self.br_graph)
        publ.has_role(publ_role)
        PublRole = publ_role.g
        with open('counter/ar.txt', 'r') as file:
            name = str(int(file.read()))
        PublRole.serialize(destination='rdf/ar/' + name + '.json', format='json-ld', auto_compact=True)

        Publ = publ.g
        with open('counter/ra.txt', 'r') as file:
            name = str(int(file.read()))
        Publ.serialize(destination='rdf/ra/' + name + '.json', format='json-ld', auto_compact=True)


