#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019 Silvio Peroni <essepuntato@gmail.com>
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021 Simone Persiani <iosonopersia@gmail.com>
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


from meta.lib.master_of_regex import comma_and_spaces, name_and_ids, one_or_more_spaces, semicolon_in_people_field
from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic_entity import BibliographicEntity
from oc_ocdm.support import create_date
from rdflib import URIRef
import re


class Creator(object):
    def __init__(self, data, base_iri, info_dir, supplier_prefix, resp_agent, ra_index, br_index, re_index_csv, ar_index_csv, vi_index):
        self.url = base_iri
        self.setgraph = GraphSet(self.url, info_dir, supplier_prefix, wanted_label=False)
        self.resp_agent = resp_agent

        self.ra_id_schemas = {'crossref', 'orcid', 'viaf', 'wikidata'}
        self.br_id_schemas = {'doi', 'issn', 'isbn', 'pmid', 'pmcid', 'url', 'wikidata', 'wikipedia'}
        self.schemas = self.ra_id_schemas.union(self.br_id_schemas)

        self.ra_index = self.indexer_id(ra_index)
        self.br_index = self.indexer_id(br_index)
        self.re_index = self.index_re(re_index_csv)
        self.ar_index = self.index_ar(ar_index_csv)
        self.vi_index = vi_index
        self.data = data

    def creator(self, source=None):
        self.src = source
        for row in self.data:
            self.row_meta = ''
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
            self.id_action(ids)
            self.title_action(title)
            self.author_action(authors)
            self.pub_date_action(pub_date)
            self.vvi_action(venue, vol, issue)
            self.page_action(page)
            self.type_action(self.type)
            if publisher:
                self.publisher_action(publisher)
            if editor:
                self.editor_action(editor)
        return self.setgraph

    @staticmethod
    def index_re(id_index):
        index = dict()
        for row in id_index:
            index[row['br']] = row['re']
        return index

    @staticmethod
    def index_ar(id_index):
        index = dict()
        for row in id_index:
            index[row['meta']] = dict()
            index[row['meta']]['author'] = Creator.__ar_worker(row['author'])
            index[row['meta']]['editor'] = Creator.__ar_worker(row['editor'])
            index[row['meta']]['publisher'] = Creator.__ar_worker(row['publisher'])
        return index

    @staticmethod
    def __ar_worker(s:str) -> dict:
        if s:
            ar_dict = dict()
            couples = s.split('; ')
            for c in couples:
                cou = c.split(', ')
                ar_dict[cou[1]] = cou[0]
            return ar_dict
        else:
            return dict()

    def indexer_id(self, csv_index):
        index = dict()
        for schema in self.schemas:
            index[schema] = dict()
        for row in csv_index:
            for schema in self.schemas:
                if row['id'].startswith(schema):
                    identifier = row['id'].replace(f'{schema}:', '')
                    index[schema][identifier] = row['meta']
        return index

    def id_action(self, ids):
        idslist = re.split(one_or_more_spaces, ids)
        # publication id
        for identifier in idslist:
            if 'meta:' in identifier:
                identifier = identifier.replace('meta:', '')
                self.row_meta = identifier.replace('br/', '')
                url = URIRef(self.url + identifier)
                self.br_graph = self.setgraph.add_br(self.resp_agent, source=self.src, res=url)
        for identifier in idslist:
            self.id_creator(self.br_graph, identifier, ra=False)

    def title_action(self, title):
        if title:
            self.br_graph.has_title(title)

    def author_action(self, authors):
        if authors:
            authorslist = re.split(semicolon_in_people_field, authors)
            aut_role_list = list()
            for aut in authorslist:
                aut_and_ids = re.search(name_and_ids, aut)
                aut_id = aut_and_ids.group(2)
                aut_id_list = aut_id.split(' ')
                for identifier in aut_id_list:
                    if 'meta:' in identifier:
                        identifier = str(identifier).replace('meta:', '')
                        url = URIRef(self.url + identifier)
                        aut_meta = identifier.replace('ra/', '')
                        pub_aut = self.setgraph.add_ra(self.resp_agent, source=self.src, res=url)
                        author_name = aut_and_ids.group(1)
                        if ',' in author_name:
                            author_name_splitted = re.split(comma_and_spaces, author_name)
                            first_name = author_name_splitted[1]
                            last_name = author_name_splitted[0]
                            if first_name.strip():
                                pub_aut.has_given_name(first_name)
                            pub_aut.has_family_name(last_name)
                        else:
                            pub_aut.has_name(author_name)
                # lists of authors' IDs
                for identifier in aut_id_list:
                    self.id_creator(pub_aut, identifier, ra=True)
                # Author ROLE
                AR = self.ar_index[self.row_meta]['author'][aut_meta]
                ar_id = 'ar/' + str(AR)
                url_ar = URIRef(self.url + ar_id)
                pub_aut_role = self.setgraph.add_ar(self.resp_agent, source=self.src, res=url_ar)
                pub_aut_role.create_author()
                self.br_graph.has_contributor(pub_aut_role)
                pub_aut_role.is_held_by(pub_aut)
                aut_role_list.append(pub_aut_role)
                if len(aut_role_list) > 1:
                    aut_role_list[aut_role_list.index(pub_aut_role)-1].has_next(pub_aut_role)

    def pub_date_action(self, pub_date):
        if pub_date:
            datelist = list()
            datesplit = pub_date.split('-')
            if datesplit:
                for x in datesplit:
                    datelist.append(int(x))
            else:
                datelist.append(int(pub_date))
            str_date = create_date(datelist)
            self.br_graph.has_pub_date(str_date)

    def vvi_action(self, venue, vol, issue):
        if venue:
            venue_and_ids = re.search(name_and_ids, venue)
            venue_ids = venue_and_ids.group(2)
            venue_ids_list = venue_ids.split()
            for identifier in venue_ids_list:
                if 'meta:' in identifier:
                    ven_id = str(identifier).replace('meta:', '')
                    url = URIRef(self.url + ven_id)
                    venue_title = venue_and_ids.group(1)
                    self.venue_graph = self.setgraph.add_br(self.resp_agent, source=self.src, res=url)
                    venue_type = self.get_venue_type(self.type, venue_ids_list)
                    if venue_type:
                        venue_type = venue_type.replace(' ', '_')
                        getattr(self.venue_graph, f'create_{venue_type}')()
                    self.venue_graph.has_title(venue_title)
            for identifier in venue_ids_list:
                self.id_creator(self.venue_graph, identifier, ra=False)
        if self.type == 'journal article' or self.type == 'journal issue' or self.type == 'journal volume':
            meta_ven = ven_id.replace('br/', '')
            if vol:
                vol_meta = self.vi_index[meta_ven]['volume'][vol]['id']
                vol_meta = 'br/' + vol_meta
                vol_url = URIRef(self.url + vol_meta)
                self.vol_graph = self.setgraph.add_br(self.resp_agent, source=self.src, res=vol_url)
                self.vol_graph.create_volume()
                self.vol_graph.has_number(vol)
            if issue:
                if vol:
                    issue_meta = self.vi_index[meta_ven]['volume'][vol]['issue'][issue]['id']
                else:
                    issue_meta = self.vi_index[meta_ven]['issue'][issue]['id']
                issue_meta = 'br/' + issue_meta
                issue_url = URIRef(self.url + issue_meta)
                self.issue_graph = self.setgraph.add_br(self.resp_agent, source=self.src, res=issue_url)
                self.issue_graph.create_issue()
                self.issue_graph.has_number(issue)
        if venue and vol and issue:
            self.br_graph.is_part_of(self.issue_graph)
            self.issue_graph.is_part_of(self.vol_graph)
            self.vol_graph.is_part_of(self.venue_graph)
        elif venue and vol and not issue:
            self.br_graph.is_part_of(self.vol_graph)
            self.vol_graph.is_part_of(self.venue_graph)
        elif venue and not vol and not issue:
            self.br_graph.is_part_of(self.venue_graph)
        elif venue and not vol and issue:
            self.br_graph.is_part_of(self.issue_graph)
            self.issue_graph.is_part_of(self.venue_graph)

    @classmethod
    def get_venue_type(cls, br_type:str, venue_ids:str) -> str:
        if br_type in {'journal article', 'journal volume', 'journal issue'}:
            venue_type = 'journal'
        elif br_type in {'book chapter', 'book part', 'book section', 'book track'}:
            venue_type = 'book'
        elif br_type in {'book', 'edited book', 'monograph', 'reference book'}:
            venue_type = 'book series'
        elif br_type == 'proceedings article':
            venue_type = 'proceedings'
        elif br_type in {'proceedings', 'report', 'standard'}:
            venue_type = 'series'
        elif br_type == 'reference entry':
            schemas = {venue_id.split(':')[0] for venue_id in venue_ids}
            if 'isbn' in schemas and 'issn' not in schemas:
                venue_type = 'reference book'
            elif 'issn' in schemas and 'isbn' not in schemas:
                venue_type = 'journal'
            elif 'issn' in schemas and 'isbn' in schemas:
                # It is undecidable
                venue_type = ''
        elif br_type == 'report series':
            venue_type = 'report series'
        elif not br_type:
            venue_type = ''
        return venue_type

    def page_action(self, page):
        if page:
            res_em = self.re_index[self.row_meta]
            re_id = 're/' + str(res_em)
            url_re = URIRef(self.url + re_id)
            form = self.setgraph.add_re(self.resp_agent, source=self.src, res=url_re)
            form.has_starting_page(page)
            form.has_ending_page(page)
            self.br_graph.has_format(form)

    def type_action(self, entity_type):
        if entity_type == 'archival document':
            self.br_graph.create_archival_document()
        elif entity_type == 'book':
            self.br_graph.create_book()
        elif entity_type == 'book chapter':
            self.br_graph.create_book_chapter()
        elif entity_type == 'book part':
            self.br_graph.create_book_part()
        elif entity_type == 'book section':
            self.br_graph.create_book_section()
        elif entity_type == 'book series':
            self.br_graph.create_book_series()
        elif entity_type == 'book set':
            self.br_graph.create_book_set()
        elif entity_type in {'data file', 'dataset'}:
            self.br_graph.create_dataset()
        elif entity_type == 'dissertation':
            self.br_graph.create_dissertation()
        elif entity_type == 'journal':
            self.br_graph.create_journal()
        elif entity_type == 'journal article':
            self.br_graph.create_journal_article()
        elif entity_type == 'journal issue':
            self.br_graph.create_issue()
        elif entity_type == 'journal volume':
            self.br_graph.create_volume()
        elif entity_type == 'peer review':
            self.br_graph.create_peer_review()
        elif entity_type == 'proceedings':
            self.br_graph.create_proceedings()
        elif entity_type == 'proceedings article':
            self.br_graph.create_proceedings_article()
        elif entity_type == 'proceedings series':
            self.br_graph.create_proceedings_series()
        elif entity_type == 'reference book':
            self.br_graph.create_reference_book()
        elif entity_type == 'reference entry':
            self.br_graph.create_reference_entry()
        elif entity_type == 'report':
            self.br_graph.create_report()
        elif entity_type == 'standard':
            self.br_graph.create_standard()
        elif entity_type == 'series':
            self.br_graph.create_series()
        elif entity_type == 'web content':
            self.br_graph.create_web_content()

    def publisher_action(self, publisher):
        publ_and_ids = re.search(name_and_ids, publisher)
        publ_id = publ_and_ids.group(2)
        publ_id_list = publ_id.split(' ')
        for identifier in publ_id_list:
            if 'meta:' in identifier:
                identifier = str(identifier).replace('meta:', '')
                pub_meta = identifier.replace('ra/', '')
                url = URIRef(self.url + identifier)
                publ_name = publ_and_ids.group(1)
                publ = self.setgraph.add_ra(self.resp_agent, source=self.src, res=url)
                publ.has_name(publ_name)
        for identifier in publ_id_list:
            self.id_creator(publ, identifier, ra=True)
        # publisherRole
        AR = self.ar_index[self.row_meta]['publisher'][pub_meta]
        ar_id = 'ar/' + str(AR)
        url_ar = URIRef(self.url + ar_id)
        publ_role = self.setgraph.add_ar(self.resp_agent, source=self.src, res=url_ar)
        publ_role.create_publisher()
        self.br_graph.has_contributor(publ_role)
        publ_role.is_held_by(publ)

    def editor_action(self, editor):
        editorslist = re.split(semicolon_in_people_field, editor)
        edit_role_list = list()
        for ed in editorslist:
            ed_and_ids = re.search(name_and_ids, ed)
            ed_id = ed_and_ids.group(2)
            ed_id_list = ed_id.split(' ')
            for identifier in ed_id_list:
                if 'meta:' in identifier:
                    identifier = str(identifier).replace('meta:', '')
                    ed_meta = identifier.replace('ra/', '')
                    url = URIRef(self.url + identifier)
                    pub_ed = self.setgraph.add_ra(self.resp_agent, source=self.src, res=url)
                    editor_name = ed_and_ids.group(1)
                    if ',' in editor_name:
                        editor_name_splitted = re.split(comma_and_spaces, editor_name)
                        firstName = editor_name_splitted[1]
                        lastName = editor_name_splitted[0]
                        if firstName.strip():
                            pub_ed.has_given_name(firstName)
                        pub_ed.has_family_name(lastName)
                    else:
                        pub_ed.has_name(editor_name)
            # lists of editor's IDs
            for identifier in ed_id_list:
                self.id_creator(pub_ed, identifier, ra=True)
            # editorRole
            AR = self.ar_index[self.row_meta]['editor'][ed_meta]
            ar_id = 'ar/' + str(AR)
            url_ar = URIRef(self.url + ar_id)
            pub_ed_role = self.setgraph.add_ar(self.resp_agent, source=self.src, res=url_ar)
            if self.type == 'proceedings article' and self.venue_graph:
                pub_ed_role.create_editor()
                self.venue_graph.has_contributor(pub_ed_role)
            elif (self.type == 'book chapter' or self.type == 'book part') and self.venue_graph:
                pub_ed_role.create_editor()
                self.venue_graph.has_contributor(pub_ed_role)
            else:
                pub_ed_role.create_editor()
                self.br_graph.has_contributor(pub_ed_role)
            pub_ed_role.is_held_by(pub_ed)
            edit_role_list.append(pub_ed_role)
            if len(edit_role_list) > 1:
                edit_role_list[edit_role_list.index(pub_ed_role)-1].has_next(pub_ed_role)

    def id_creator(self, graph:BibliographicEntity, identifier:str, ra:bool) -> None:
        new_id = None
        if ra:
            for ra_id_schema in self.ra_id_schemas:
                if identifier.startswith(ra_id_schema):
                    identifier = identifier.replace(f'{ra_id_schema}:', '')
                    res = self.ra_index[ra_id_schema][identifier]
                    url = URIRef(self.url + 'id/' + res)
                    new_id = self.setgraph.add_id(self.resp_agent, source=self.src, res=url)
                    getattr(new_id, f'create_{ra_id_schema}')(identifier)
        else:
            for br_id_schema in self.br_id_schemas:
                if identifier.startswith(br_id_schema):
                    identifier = identifier.replace(f'{br_id_schema}:', '')
                    res = self.br_index[br_id_schema][identifier]
                    url = URIRef(self.url + 'id/' + res)
                    new_id = self.setgraph.add_id(self.resp_agent, source=self.src, res=url)
                    getattr(new_id, f'create_{br_id_schema}')(identifier)
        if new_id:
            graph.has_identifier(new_id)
