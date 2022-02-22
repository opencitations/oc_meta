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


from meta.lib.master_of_regex import comma_and_spaces, name_and_ids, semicolon_in_people_field
from meta.scripts.creator import Creator
from oc_ocdm.graph import GraphSet
from rdflib import URIRef
import re


class RespAgentsCreator(Creator):
    def __init__(self, data, base_iri, info_dir, supplier_prefix, resp_agent, ra_index):
        self.url = base_iri
        self.setgraph = GraphSet(self.url, info_dir, supplier_prefix, wanted_label=False)
        self.resp_agent = resp_agent

        self.ra_id_schemas = {'crossref', 'orcid', 'viaf', 'wikidata'}
        self.br_id_schemas = {'doi', 'issn', 'isbn', 'pmid', 'pmcid', 'url', 'wikidata', 'wikipedia'}
        self.schemas = self.ra_id_schemas.union(self.br_id_schemas)

        self.ra_index = self.indexer_id(ra_index)
        self.data = data

    def creator(self, source=None):
        self.src = source
        for row in self.data:
            authors = row['author']
            self.author_action(authors)
        return self.setgraph

    def author_action(self, authors):
        if authors:
            authorslist = re.split(semicolon_in_people_field, authors)
            for aut in authorslist:
                aut_and_ids = re.search(name_and_ids, aut)
                aut_id = aut_and_ids.group(2)
                aut_id_list = aut_id.split()
                for identifier in aut_id_list:
                    if 'meta:' in identifier:
                        identifier = str(identifier).replace('meta:', '')
                        url = URIRef(self.url + identifier)
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