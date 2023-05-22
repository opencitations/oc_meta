#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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

from __future__ import annotations

import re

from oc_ocdm.graph import GraphSet
from rdflib import Graph, URIRef

from oc_meta.core.creator import Creator
from oc_meta.lib.finder import ResourceFinder
from oc_meta.lib.master_of_regex import (comma_and_spaces, name_and_ids,
                                         semicolon_in_people_field)


class RespAgentsCreator(Creator):
    def __init__(self, data:list, endpoint:str, base_iri:str, info_dir:str, supplier_prefix:str, resp_agent:str, ra_index:dict, preexisting_entities: set, everything_everywhere_allatonce: Graph, meta_config_path:str|None=None):
        self.url = base_iri
        self.setgraph = GraphSet(self.url, info_dir, supplier_prefix, wanted_label=False)
        self.finder = ResourceFinder(ts_url = endpoint, base_iri = base_iri, local_g=everything_everywhere_allatonce, meta_config_path=meta_config_path)
        self.resp_agent = resp_agent
        self.ra_id_schemas = {'crossref', 'orcid', 'viaf', 'wikidata'}
        self.br_id_schemas = {'doi', 'issn', 'isbn', 'pmid', 'pmcid', 'url', 'wikidata', 'wikipedia'}
        self.schemas = self.ra_id_schemas.union(self.br_id_schemas)
        self.ra_index = self.indexer_id(ra_index)
        self.preexisting_entities = preexisting_entities
        self.preexisting_graphs = dict()
        self.data = data

    def creator(self, source=None):
        self.src = source
        for row in self.data:
            authors = row['author']
            publisher = row['publisher']
            editor = row['editor']
            self.author_action(authors)
            if publisher:
                self.publisher_action(publisher)
            if editor:
                self.editor_action(editor)
        return self.setgraph

    def author_action(self, authors):
        if authors:
            authorslist = re.split(semicolon_in_people_field, authors)
            for aut in authorslist:
                aut_and_ids = re.search(name_and_ids, aut)
                aut_id = aut_and_ids.group(2)
                aut_id_list = aut_id.split()
                for identifier in aut_id_list:
                    if 'omid:' in identifier:
                        identifier = str(identifier).replace('omid:', '')
                        preexisting_entity = True if identifier in self.preexisting_entities else False
                        url = URIRef(self.url + identifier)
                        preexisting_graph = self.finder.get_subgraph(url, self.preexisting_graphs) if preexisting_entity else None
                        pub_aut = self.setgraph.add_ra(self.resp_agent, source=self.src, res=url, preexisting_graph=preexisting_graph)
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

    def publisher_action(self, publisher):
        publ_and_ids = re.search(name_and_ids, publisher)
        publ_id = publ_and_ids.group(2)
        publ_id_list = publ_id.split()
        for identifier in publ_id_list:
            if 'omid:' in identifier:
                identifier = str(identifier).replace('omid:', '')
                preexisting_entity = True if identifier in self.preexisting_entities else False
                url = URIRef(self.url + identifier)
                publ_name = publ_and_ids.group(1)
                preexisting_graph = self.finder.get_subgraph(url, self.preexisting_graphs) if preexisting_entity else None
                publ = self.setgraph.add_ra(self.resp_agent, source=self.src, res=url, preexisting_graph=preexisting_graph)
                publ.has_name(publ_name)
        for identifier in publ_id_list:
            self.id_creator(publ, identifier, ra=True)

    def editor_action(self, editor):
        editorslist = re.split(semicolon_in_people_field, editor)
        for ed in editorslist:
            ed_and_ids = re.search(name_and_ids, ed)
            ed_id = ed_and_ids.group(2)
            ed_id_list = ed_id.split(' ')
            for identifier in ed_id_list:
                if 'omid:' in identifier:
                    identifier = str(identifier).replace('omid:', '')
                    preexisting_entity = True if identifier in self.preexisting_entities else False
                    url = URIRef(self.url + identifier)
                    preexisting_graph = self.finder.get_subgraph(url, self.preexisting_graphs) if preexisting_entity else None
                    pub_ed = self.setgraph.add_ra(self.resp_agent, source=self.src, res=url, preexisting_graph=preexisting_graph)
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