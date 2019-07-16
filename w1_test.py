from graphlib import *
from rdflib import Graph
import re, csv


      #  with open("example.csv", 'r', encoding='utf-8') as csvfile:
      #      reader = csv.DictReader(csvfile)
      #      self.data = [dict(x) for x in reader]
      #      for row in self.data:


class Migrator():
    def __init__(self, row):
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

    def id_job (self):
        ids = self.ids
        idslist = re.split(r'\s*;\s*', ids)
        iddict = dict()
        doi_list = list()
        wikidatalist = list()
        for id in idslist:
            if "doi" in id:
                doi_list.append(id)
            if "wikidata" in id:
                wikidatalist.append(id)
        iddict['doi'] = doi_list
        iddict['wikidata'] = wikidatalist
        return iddict


    def title_job (self):
        title = self.title
        return title

    def author_job (self):
        authors = self.authors
        authorslist = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', authors)
        authordict = dict()
        for aut in authorslist:
            author_name = re.search(r'(.*?)\s*\[.*?\]', aut).group(1)
            authordict['name'] = author_name
            aut_id = re.search(r'\[\s*(.*?)\s*\]', aut).group(1)
            aut_id_list = re.split(r'\s*;\s*', aut_id)
            orcidlist = list()
            for x in aut_id_list:
                if "orcid" in x:
                    orcidlist.append(x)
        authordict['orcid'] = orcidlist
        return authordict

    def pub_date_job (self):
        pub_date = self.pub_date
        return pub_date

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

    #Do we need all in 1? I suppose --- It's incomplete (need to return something ^.^" it also need test)
    def all_in (self):
        self.id_job()
        self.title_job()
        self.author_job()
        self.pub_date_job()
        self.venue_job()
        self.volume_job()
        self.issue_job()
        self.page_job()
        self.type_job()
        self.publisher_job()





