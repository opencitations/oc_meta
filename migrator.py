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
        self.pub = row['publisher']

        #New BR
        self.setgraph = sgraph
        self.br_graph = self.setgraph.add_br("agent", source_agent=None, source=None, res=None)



    def id_job (self):
        ids = self.ids
        idslist = re.split(r'\s*;\s*', ids)

        #publication id
        for id in idslist:
            if "doi" in id:
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
                    pub_aut_orcid = self.setgraph.add_id("agent", source_agent=None, source=None, res=None)
                    pub_aut_orcid.create_orcid(id)
                    pub_aut.has_id(pub_aut_orcid)
                    AutOrcid = pub_aut_orcid.g
                    with open('counter/id.txt', 'r') as file:
                        name = str(int(file.read()))
                    AutOrcid.serialize(destination='rdf/id/' + name + '.json', format='json-ld')

        # authorRole
            pub_aut_role = self.setgraph.add_ar("agent", source_agent=None, source=None, res=None)
            pub_aut_role.create_author(self.br_graph)
            pub_aut.has_role(pub_aut_role)
            AutRole = pub_aut_role.g
            with open('counter/ar.txt', 'r') as file:
                name = str(int(file.read()))
            AutRole.serialize(destination='rdf/ar/' + name + '.json', format='json-ld')

            Aut = pub_aut.g
            with open('counter/ra.txt', 'r') as file:
                name = str(int(file.read()))
            Aut.serialize(destination='rdf/ra/' + name + '.json', format='json-ld')


    def pub_date_job (self):
        datelist = list()
        datelist.append(int(self.pub_date))
        self.br_graph.create_pub_date(datelist)

#TODO
    def venue_job (self):
        venue = self.venue
        venuedict = dict()
        venue_title = re.search(r'(.*?)\s*\[.*?\]', venue).group(1)
        venuedict['name'] = venue_title
        venue_id = re.search(r'\[\s*(.*?)\s*\]', venue).group(1)
        venue_id_list = re.split(r'\s*;\s*', venue_id)
        issn_list = list()
        for x in venue_id_list:
            if "issn" in x:
                issn_list.append(x)
        venuedict['issn'] = issn_list
        return venuedict

    def volume_job (self):
        vol = self.vol
        return vol

    def issue_job (self):
        issue = self.issue
        return issue

    def page_job (self):
        page = self.page
        return page

    def type_job (self):
        type = self.type
        return type

    def publisher_job (self):
        pub = self.pub
        pub_name = re.search(r'(.*?)\s*\[.*?\]', pub).group(1)
        pubdict = dict()
        pubdict['name'] = pub_name
        pub_id = re.search(r'\[\s*(.*?)\s*\]', pub).group(1)
        pub_id_list = re.split(r'\s*;\s*', pub_id)
        crossreflist = list()
        for x in pub_id_list:
            if 'crossref' in x:
                crossreflist.append(x)
        pubdict['crossref'] = crossreflist
        return pubdict
