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

from __future__ import annotations

import re
from typing import List

from oc_meta.core.curator import get_edited_br_metaid
from oc_meta.lib.finder import ResourceFinder
from oc_meta.lib.master_of_regex import (
    comma_and_spaces,
    name_and_ids,
    one_or_more_spaces,
    semicolon_in_people_field,
)
from rdflib import Graph, URIRef

from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic import BibliographicResource
from oc_ocdm.graph.entities.bibliographic_entity import BibliographicEntity
from oc_ocdm.support import create_date


class Creator(object):
    def __init__(
        self,
        data: list,
        endpoint: str,
        base_iri: str,
        counter_handler: RedisCounterHandler,
        supplier_prefix: str,
        resp_agent: str,
        ra_index: dict,
        br_index: dict,
        re_index_csv: dict,
        ar_index_csv: dict,
        vi_index: dict,
        preexisting_entities: set,
        everything_everywhere_allatonce: Graph,
        settings: dict = None,
        meta_config_path: str = None,
    ):
        self.url = base_iri
        self.setgraph = GraphSet(
            self.url,
            supplier_prefix=supplier_prefix,
            wanted_label=False,
            custom_counter_handler=counter_handler,
        )
        self.resp_agent = resp_agent
        self.finder = ResourceFinder(
            ts_url=endpoint,
            base_iri=base_iri,
            local_g=everything_everywhere_allatonce,
            settings=settings,
            meta_config_path=meta_config_path,
        )

        self.ra_id_schemas = {"crossref", "orcid", "viaf", "wikidata", "ror"}
        self.br_id_schemas = {
            "arxiv",
            "doi",
            "issn",
            "isbn",
            "jid",
            "openalex",
            "pmid",
            "pmcid",
            "url",
            "wikidata",
            "wikipedia",
        }
        self.schemas = self.ra_id_schemas.union(self.br_id_schemas)

        self.ra_index = self.indexer_id(ra_index)
        self.br_index = self.indexer_id(br_index)
        self.re_index = self.index_re(re_index_csv)
        self.ar_index = self.index_ar(ar_index_csv)
        self.vi_index = vi_index
        self.preexisting_entities = preexisting_entities
        self.preexisting_graphs = dict()
        self.data = data
        self.counter_handler = counter_handler

    def creator(self, source=None):
        self.src = source
        for row in self.data:
            self.row_meta = ""
            self.venue_meta = ""
            ids = row["id"]
            title = row["title"]
            authors = row["author"]
            pub_date = row["pub_date"]
            venue = row["venue"]
            vol = row["volume"]
            issue = row["issue"]
            page = row["page"]
            self.type = row["type"]
            publisher = row["publisher"]
            editor = row["editor"]
            self.venue_graph = None
            self.vol_graph = None
            self.issue_graph = None
            self.id_action(ids)
            self.vvi_action(venue, vol, issue)
            self.title_action(title)
            self.author_action(authors)
            self.pub_date_action(pub_date)
            self.page_action(page)
            self.type_action(self.type)
            self.publisher_action(publisher)
            self.editor_action(editor, row)
        return self.setgraph

    @staticmethod
    def index_re(id_index):
        index = dict()
        for row in id_index:
            index[row["br"]] = row["re"]
        return index

    @staticmethod
    def index_ar(id_index):
        index = dict()
        for row in id_index:
            index[row["meta"]] = dict()
            index[row["meta"]]["author"] = Creator.__ar_worker(row["author"])
            index[row["meta"]]["editor"] = Creator.__ar_worker(row["editor"])
            index[row["meta"]]["publisher"] = Creator.__ar_worker(row["publisher"])
        return index

    @staticmethod
    def __ar_worker(s: str) -> dict:
        if s:
            ar_dict = dict()
            couples = s.split("; ")
            for c in couples:
                cou = c.split(", ")
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
                if row["id"].startswith(schema):
                    identifier = row["id"].replace(f"{schema}:", "")
                    index[schema][identifier] = row["meta"]
        return index

    def id_action(self, ids):
        idslist = re.split(one_or_more_spaces, ids)
        # publication id
        for identifier in idslist:
            if "omid:" in identifier:
                identifier = identifier.replace("omid:", "")
                preexisting_entity = (
                    True if identifier in self.preexisting_entities else False
                )
                self.row_meta = identifier.replace("br/", "")
                url = URIRef(self.url + identifier)
                preexisting_graph = (
                    self.finder.get_subgraph(url, self.preexisting_graphs)
                    if preexisting_entity
                    else None
                )
                self.br_graph = self.setgraph.add_br(
                    self.resp_agent,
                    source=self.src,
                    res=url,
                    preexisting_graph=preexisting_graph,
                )
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
                aut_id_list = aut_id.split(" ")
                for identifier in aut_id_list:
                    if "omid:" in identifier:
                        identifier = str(identifier).replace("omid:", "")
                        preexisting_entity = (
                            True if identifier in self.preexisting_entities else False
                        )
                        url = URIRef(self.url + identifier)
                        aut_meta = identifier.replace("ra/", "")
                        preexisting_graph = (
                            self.finder.get_subgraph(url, self.preexisting_graphs)
                            if preexisting_entity
                            else None
                        )
                        pub_aut = self.setgraph.add_ra(
                            self.resp_agent,
                            source=self.src,
                            res=url,
                            preexisting_graph=preexisting_graph,
                        )
                        author_name = aut_and_ids.group(1)
                        if "," in author_name:
                            author_name_splitted = re.split(
                                comma_and_spaces, author_name
                            )
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
                AR = self.ar_index[self.row_meta]["author"][aut_meta]
                ar_id = "ar/" + str(AR)
                preexisting_entity = (
                    True if ar_id in self.preexisting_entities else False
                )
                url_ar = URIRef(self.url + ar_id)
                preexisting_graph = (
                    self.finder.get_subgraph(url_ar, self.preexisting_graphs)
                    if preexisting_entity
                    else None
                )
                pub_aut_role = self.setgraph.add_ar(
                    self.resp_agent,
                    source=self.src,
                    res=url_ar,
                    preexisting_graph=preexisting_graph,
                )
                pub_aut_role.create_author()
                self.br_graph.has_contributor(pub_aut_role)
                pub_aut_role.is_held_by(pub_aut)
                aut_role_list.append(pub_aut_role)
                if len(aut_role_list) > 1:
                    aut_role_list[aut_role_list.index(pub_aut_role) - 1].has_next(
                        pub_aut_role
                    )

    def pub_date_action(self, pub_date):
        if pub_date:
            datelist = list()
            datesplit = pub_date.split("-")
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
                if "omid:" in identifier:
                    ven_id = str(identifier).replace("omid:", "")
                    self.venue_meta = ven_id.replace("br/", "")
                    preexisting_entity = (
                        True if ven_id in self.preexisting_entities else False
                    )
                    url = URIRef(self.url + ven_id)
                    venue_title = venue_and_ids.group(1)
                    preexisting_graph = (
                        self.finder.get_subgraph(url, self.preexisting_graphs)
                        if preexisting_entity
                        else None
                    )
                    self.venue_graph = self.setgraph.add_br(
                        self.resp_agent,
                        source=self.src,
                        res=url,
                        preexisting_graph=preexisting_graph,
                    )
                    try:
                        venue_type = self.get_venue_type(self.type, venue_ids_list)
                    except UnboundLocalError:
                        error_message = f"[INFO:Creator] I found the venue {venue} for the resource of type {self.type}, but I don't know how to handle it"
                        raise UnboundLocalError(error_message)
                    if venue_type:
                        venue_type = venue_type.replace(" ", "_")
                        getattr(self.venue_graph, f"create_{venue_type}")()
                    self.venue_graph.has_title(venue_title)
            for identifier in venue_ids_list:
                self.id_creator(self.venue_graph, identifier, ra=False)
            if self.type in {"journal article", "journal volume", "journal issue"}:
                if vol:
                    vol_meta = self.vi_index[self.venue_meta]["volume"][vol]["id"]
                    vol_meta = "br/" + vol_meta
                    preexisting_entity = (
                        True if vol_meta in self.preexisting_entities else False
                    )
                    vol_url = URIRef(self.url + vol_meta)
                    preexisting_graph = (
                        self.finder.get_subgraph(vol_url, self.preexisting_graphs)
                        if preexisting_entity
                        else None
                    )
                    self.vol_graph = self.setgraph.add_br(
                        self.resp_agent,
                        source=self.src,
                        res=vol_url,
                        preexisting_graph=preexisting_graph,
                    )
                    self.vol_graph.create_volume()
                    self.vol_graph.has_number(vol)
                if issue:
                    if vol:
                        issue_meta = self.vi_index[self.venue_meta]["volume"][vol][
                            "issue"
                        ][issue]["id"]
                    else:
                        issue_meta = self.vi_index[self.venue_meta]["issue"][issue][
                            "id"
                        ]
                    issue_meta = "br/" + issue_meta
                    preexisting_entity = (
                        True if issue_meta in self.preexisting_entities else False
                    )
                    issue_url = URIRef(self.url + issue_meta)
                    preexisting_graph = (
                        self.finder.get_subgraph(issue_url, self.preexisting_graphs)
                        if preexisting_entity
                        else None
                    )
                    self.issue_graph = self.setgraph.add_br(
                        self.resp_agent,
                        source=self.src,
                        res=issue_url,
                        preexisting_graph=preexisting_graph,
                    )
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
    def get_venue_type(cls, br_type: str, venue_ids: list) -> str:
        schemas = {venue_id.split(":", maxsplit=1)[0] for venue_id in venue_ids}
        venue_type = ""
        if br_type in {"journal article", "journal volume", "journal issue"}:
            venue_type = "journal"
        elif br_type in {"book chapter", "book part", "book section", "book track"}:
            venue_type = "book"
        elif br_type in {"book", "edited book", "monograph", "reference book"}:
            venue_type = "book series"
        elif br_type == "proceedings article":
            venue_type = "proceedings"
        elif br_type in {"proceedings", "report", "standard", "series"}:
            venue_type = "series"
        elif br_type == "reference entry":
            venue_type = "reference book"
        elif br_type == "report series":
            venue_type = "report series"
        elif not br_type or br_type in {"dataset", "data file"}:
            venue_type = ""
        # Check the type based on the identifier scheme
        if any(
            identifier for identifier in venue_ids if not identifier.startswith("omid:")
        ):
            if venue_type in {"journal", "book series", "series", "report series"}:
                if "isbn" in schemas or "issn" not in schemas:
                    # It is undecidable
                    venue_type = ""
            elif venue_type in {"book", "proceedings"}:
                if "issn" in schemas or "isbn" not in schemas:
                    venue_type = ""
            elif venue_type == "reference book":
                if "isbn" in schemas and "issn" not in schemas:
                    venue_type = "reference book"
                elif "issn" in schemas and "isbn" not in schemas:
                    venue_type = "journal"
                elif "issn" in schemas and "isbn" in schemas:
                    venue_type = ""
        return venue_type

    def page_action(self, page):
        if page:
            res_em = self.re_index[self.row_meta]
            re_id = "re/" + str(res_em)
            preexisting_entity = True if re_id in self.preexisting_entities else False
            url_re = URIRef(self.url + re_id)
            preexisting_graph = (
                self.finder.get_subgraph(url_re, self.preexisting_graphs)
                if preexisting_entity
                else None
            )
            form = self.setgraph.add_re(
                self.resp_agent,
                source=self.src,
                res=url_re,
                preexisting_graph=preexisting_graph,
            )
            form.has_starting_page(page)
            form.has_ending_page(page)
            self.br_graph.has_format(form)

    def type_action(self, entity_type):
        if entity_type == "abstract":
            self.br_graph.create_abstract()
        if entity_type == "archival document":
            self.br_graph.create_archival_document()
        elif entity_type == "audio document":
            self.br_graph.create_audio_document()
        elif entity_type == "book":
            self.br_graph.create_book()
        elif entity_type == "book chapter":
            self.br_graph.create_book_chapter()
        elif entity_type == "book part":
            self.br_graph.create_book_part()
        elif entity_type == "book section":
            self.br_graph.create_book_section()
        elif entity_type == "book series":
            self.br_graph.create_book_series()
        elif entity_type == "book set":
            self.br_graph.create_book_set()
        elif entity_type == "computer program":
            self.br_graph.create_computer_program()
        elif entity_type in {"data file", "dataset"}:
            self.br_graph.create_dataset()
        elif entity_type == "data management plan":
            self.br_graph.create_data_management_plan()
        elif entity_type == "dissertation":
            self.br_graph.create_dissertation()
        elif entity_type == "editorial":
            self.br_graph.create_editorial()
        # elif entity_type == 'edited book':
        #     self.br_graph.create_edited_book()
        elif entity_type == "journal":
            self.br_graph.create_journal()
        elif entity_type == "journal article":
            self.br_graph.create_journal_article()
        elif entity_type == "journal editorial":
            self.br_graph.create_journal_editorial()
        elif entity_type == "journal issue":
            self.br_graph.create_issue()
        elif entity_type == "journal volume":
            self.br_graph.create_volume()
        elif entity_type == "newspaper":
            self.br_graph.create_newspaper()
        elif entity_type == "newspaper article":
            self.br_graph.create_newspaper_article()
        elif entity_type == "newspaper issue":
            self.br_graph.create_newspaper_issue()
        # elif entity_type == 'monograph':
        #     self.br_graph.create_monograph()
        elif entity_type == "peer review":
            self.br_graph.create_peer_review()
        elif entity_type == "preprint":
            self.br_graph.create_preprint()
        elif entity_type == "presentation":
            self.br_graph.create_presentation()
        elif entity_type == "proceedings":
            self.br_graph.create_proceedings()
        elif entity_type == "proceedings article":
            self.br_graph.create_proceedings_article()
        # elif entity_type == 'proceedings series':
        #     self.br_graph.create_proceedings_series()
        elif entity_type == "reference book":
            self.br_graph.create_reference_book()
        elif entity_type == "reference entry":
            self.br_graph.create_reference_entry()
        elif entity_type == "report":
            self.br_graph.create_report()
        elif entity_type == "report series":
            self.br_graph.create_report_series()
        elif entity_type == "retraction notice":
            self.br_graph.create_retraction_notice()
        elif entity_type == "standard":
            self.br_graph.create_standard()
        elif entity_type == "series":
            self.br_graph.create_series()
        # elif entity_type == 'standard series':
        #     self.br_graph.create_standard_series()()
        elif entity_type == "web content":
            self.br_graph.create_web_content()

    def publisher_action(self, publisher):
        if publisher:
            publishers_list = re.split(semicolon_in_people_field, publisher)
            pub_role_list = list()
            for pub in publishers_list:
                publ_and_ids = re.search(name_and_ids, pub)
                publ_id = publ_and_ids.group(2)
                publ_id_list = publ_id.split()
                for identifier in publ_id_list:
                    if "omid:" in identifier:
                        identifier = str(identifier).replace("omid:", "")
                        preexisting_entity = (
                            True if identifier in self.preexisting_entities else False
                        )
                        pub_meta = identifier.replace("ra/", "")
                        url = URIRef(self.url + identifier)
                        publ_name = publ_and_ids.group(1)
                        preexisting_graph = (
                            self.finder.get_subgraph(url, self.preexisting_graphs)
                            if preexisting_entity
                            else None
                        )
                        publ = self.setgraph.add_ra(
                            self.resp_agent,
                            source=self.src,
                            res=url,
                            preexisting_graph=preexisting_graph,
                        )
                        publ.has_name(publ_name)
                for identifier in publ_id_list:
                    self.id_creator(publ, identifier, ra=True)
                # publisherRole
                AR = self.ar_index[self.row_meta]["publisher"][pub_meta]
                ar_id = "ar/" + str(AR)
                preexisting_entity = (
                    True if ar_id in self.preexisting_entities else False
                )
                url_ar = URIRef(self.url + ar_id)
                preexisting_graph = self.finder.get_subgraph(
                    url_ar, self.preexisting_graphs
                )
                publ_role = self.setgraph.add_ar(
                    self.resp_agent,
                    source=self.src,
                    res=url_ar,
                    preexisting_graph=preexisting_graph,
                )
                publ_role.create_publisher()
                self.br_graph.has_contributor(publ_role)
                publ_role.is_held_by(publ)
                pub_role_list.append(publ_role)
                if len(pub_role_list) > 1:
                    pub_role_list[pub_role_list.index(publ_role) - 1].has_next(
                        publ_role
                    )

    def editor_action(self, editor, row):
        if editor:
            editorslist = re.split(semicolon_in_people_field, editor)
            edit_role_list = list()
            for ed in editorslist:
                ed_and_ids = re.search(name_and_ids, ed)
                ed_id = ed_and_ids.group(2)
                ed_id_list = ed_id.split(" ")
                for identifier in ed_id_list:
                    if "omid:" in identifier:
                        identifier = str(identifier).replace("omid:", "")
                        preexisting_entity = (
                            True if identifier in self.preexisting_entities else False
                        )
                        ed_meta = identifier.replace("ra/", "")
                        url = URIRef(self.url + identifier)
                        preexisting_graph = (
                            self.finder.get_subgraph(url, self.preexisting_graphs)
                            if preexisting_entity
                            else None
                        )
                        pub_ed = self.setgraph.add_ra(
                            self.resp_agent,
                            source=self.src,
                            res=url,
                            preexisting_graph=preexisting_graph,
                        )
                        editor_name = ed_and_ids.group(1)
                        if "," in editor_name:
                            editor_name_splitted = re.split(
                                comma_and_spaces, editor_name
                            )
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
                br_key = get_edited_br_metaid(row, self.row_meta, self.venue_meta)
                AR = self.ar_index[br_key]["editor"][ed_meta]
                ar_id = "ar/" + str(AR)
                preexisting_entity = (
                    True if ar_id in self.preexisting_entities else False
                )
                url_ar = URIRef(self.url + ar_id)
                preexisting_graph = (
                    self.finder.get_subgraph(url_ar, self.preexisting_graphs)
                    if preexisting_entity
                    else None
                )
                pub_ed_role = self.setgraph.add_ar(
                    self.resp_agent,
                    source=self.src,
                    res=url_ar,
                    preexisting_graph=preexisting_graph,
                )
                pub_ed_role.create_editor()
                br_graphs: List[BibliographicResource] = [
                    self.br_graph,
                    self.issue_graph,
                    self.vol_graph,
                    self.venue_graph,
                ]
                for graph in br_graphs:
                    if br_key == self.__res_metaid(graph):
                        graph.has_contributor(pub_ed_role)
                pub_ed_role.is_held_by(pub_ed)
                edit_role_list.append(pub_ed_role)
            for i, edit_role in enumerate(edit_role_list):
                if i > 0:
                    edit_role_list[i - 1].has_next(edit_role)

    def __res_metaid(self, graph: BibliographicResource):
        if graph:
            return graph.res.replace(f"{self.url}br/", "")

    def id_creator(self, graph: BibliographicEntity, identifier: str, ra: bool) -> None:
        new_id = None
        # Skip temporary identifiers - they should not be saved in the final dataset
        if identifier.startswith("temp:"):
            return

        if ra:
            for ra_id_schema in self.ra_id_schemas:
                if identifier.startswith(ra_id_schema):
                    identifier = identifier.replace(f"{ra_id_schema}:", "")
                    res = self.ra_index[ra_id_schema][identifier]
                    preexisting_entity = (
                        True if f"id/{res}" in self.preexisting_entities else False
                    )
                    url = URIRef(self.url + "id/" + res)
                    preexisting_graph = (
                        self.finder.get_subgraph(url, self.preexisting_graphs)
                        if preexisting_entity
                        else None
                    )
                    new_id = self.setgraph.add_id(
                        self.resp_agent,
                        source=self.src,
                        res=url,
                        preexisting_graph=preexisting_graph,
                    )
                    getattr(new_id, f"create_{ra_id_schema}")(identifier)
        else:
            for br_id_schema in self.br_id_schemas:
                if identifier.startswith(br_id_schema):
                    identifier = identifier.replace(f"{br_id_schema}:", "")
                    res = self.br_index[br_id_schema][identifier]
                    preexisting_entity = (
                        True if f"id/{res}" in self.preexisting_entities else False
                    )
                    url = URIRef(self.url + "id/" + res)
                    preexisting_graph = (
                        self.finder.get_subgraph(url, self.preexisting_graphs)
                        if preexisting_entity
                        else None
                    )
                    new_id = self.setgraph.add_id(
                        self.resp_agent,
                        source=self.src,
                        res=url,
                        preexisting_graph=preexisting_graph,
                    )
                    getattr(new_id, f"create_{br_id_schema}")(identifier)
        if new_id:
            graph.has_identifier(new_id)
