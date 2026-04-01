# SPDX-FileCopyrightText: 2019 Silvio Peroni <silvio.peroni@unibo.it>
# SPDX-FileCopyrightText: 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# SPDX-FileCopyrightText: 2021 Simone Persiani <iosonopersia@gmail.com>
# SPDX-FileCopyrightText: 2021-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

from typing import TYPE_CHECKING, List

from oc_meta.core.curator import get_edited_br_metaid
from oc_meta.lib.finder import ResourceFinder
from oc_meta.lib.master_of_regex import (
    RE_COMMA_AND_SPACES,
    RE_NAME_AND_IDS,
    RE_ONE_OR_MORE_SPACES,
    RE_SEMICOLON_IN_PEOPLE_FIELD,
)
from rdflib import URIRef

from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic import BibliographicResource
from oc_ocdm.graph.entities.bibliographic_entity import BibliographicEntity
from oc_ocdm.support import create_date

if TYPE_CHECKING:
    from rich.progress import Progress


class Creator(object):
    def __init__(
        self,
        data: list,
        finder: ResourceFinder,
        base_iri: str,
        counter_handler: RedisCounterHandler,
        supplier_prefix: str,
        resp_agent: str,
        ra_index: list,
        br_index: list,
        re_index_csv: list,
        ar_index_csv: list,
        vi_index: dict,
        silencer: list | None = None,
        progress: Progress | None = None,
    ):
        self.url = base_iri
        self.progress = progress
        self.setgraph = GraphSet(
            self.url,
            supplier_prefix=supplier_prefix,
            wanted_label=False,
            custom_counter_handler=counter_handler,
        )
        self.resp_agent = resp_agent
        self.finder = finder

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
        self.data = data
        self.counter_handler = counter_handler
        self.silencer = silencer or []

    def _has_existing_roles(self, br_uri: URIRef) -> dict:
        """
        Check if BR has existing author/editor/publisher roles in local_g.

        Args:
            br_uri: URI of the bibliographic resource

        Returns:
            Dictionary with keys 'author', 'editor', 'publisher' indicating presence of existing roles
        """
        has_roles = {
            "author": False,
            "editor": False,
            "publisher": False
        }

        pro_isDocumentContextFor = URIRef("http://purl.org/spar/pro/isDocumentContextFor")
        pro_withRole = URIRef("http://purl.org/spar/pro/withRole")
        pro_author = URIRef("http://purl.org/spar/pro/author")
        pro_editor = URIRef("http://purl.org/spar/pro/editor")
        pro_publisher = URIRef("http://purl.org/spar/pro/publisher")

        for _, _, ar_uri in self.finder.local_g.triples((br_uri, pro_isDocumentContextFor, None)):
            for _, _, role in self.finder.local_g.triples((ar_uri, pro_withRole, None)):
                if role == pro_author:
                    has_roles["author"] = True
                elif role == pro_editor:
                    has_roles["editor"] = True
                elif role == pro_publisher:
                    has_roles["publisher"] = True

        return has_roles

    def creator(self, source=None):
        self.src = source
        task_id = None
        if self.progress:
            task_id = self.progress.add_task(
                "  [dim]Creating RDF entities[/dim]", total=len(self.data)
            )
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

            br_uri = URIRef(f"{self.url}{self.row_meta}")
            br_is_preexisting = br_uri in self.finder.prebuilt_subgraphs

            skip_author = False
            skip_publisher = False
            skip_editor = False

            if br_is_preexisting:
                existing_roles = self._has_existing_roles(br_uri)

                skip_author = "author" in self.silencer and existing_roles["author"]
                skip_publisher = "publisher" in self.silencer and existing_roles["publisher"]
                skip_editor = "editor" in self.silencer and existing_roles["editor"]

            if not skip_author:
                self.author_action(authors)

            self.pub_date_action(pub_date)
            self.page_action(page)
            self.type_action(self.type)

            if not skip_publisher:
                self.publisher_action(publisher)

            if not skip_editor:
                self.editor_action(editor, row)

            if self.progress and task_id is not None:
                self.progress.advance(task_id)

        if self.progress and task_id is not None:
            self.progress.remove_task(task_id)
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
        index = {schema: {} for schema in self.schemas}
        for row in csv_index:
            if row_id := row["id"]:
                schema, value = row_id.split(":", 1)
                if schema in index:
                    index[schema][value] = row["meta"]
        return index

    def id_action(self, ids):
        idslist = RE_ONE_OR_MORE_SPACES.split(ids)
        # publication id
        for identifier in idslist:
            if "omid:" in identifier:
                identifier = identifier.replace("omid:", "")
                url = URIRef(self.url + identifier)
                preexisting_entity = url in self.finder.prebuilt_subgraphs
                self.row_meta = identifier
                preexisting_graph = (
                    self.finder.get_subgraph(url)
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
            authorslist = RE_SEMICOLON_IN_PEOPLE_FIELD.split(authors)
            aut_role_list = list()
            for aut in authorslist:
                aut_and_ids = RE_NAME_AND_IDS.search(aut)
                assert aut_and_ids is not None
                aut_id = aut_and_ids.group(2)
                aut_id_list = aut_id.split(" ")
                author_ra = None
                aut_meta = ""
                for identifier in aut_id_list:
                    if "omid:" in identifier:
                        identifier = str(identifier).replace("omid:", "")
                        url = URIRef(self.url + identifier)
                        preexisting_entity = url in self.finder.prebuilt_subgraphs
                        aut_meta = identifier
                        preexisting_graph = (
                            self.finder.get_subgraph(url)
                            if preexisting_entity
                            else None
                        )
                        author_ra = self.setgraph.add_ra(
                            self.resp_agent,
                            source=self.src,
                            res=url,
                            preexisting_graph=preexisting_graph,
                        )
                        author_name = aut_and_ids.group(1)
                        if "," in author_name:
                            author_name_splitted = RE_COMMA_AND_SPACES.split(
                                author_name
                            )
                            first_name = author_name_splitted[1]
                            last_name = author_name_splitted[0]
                            if first_name.strip():
                                author_ra.has_given_name(first_name)
                            author_ra.has_family_name(last_name)
                        else:
                            author_ra.has_name(author_name)
                assert author_ra is not None
                for identifier in aut_id_list:
                    self.id_creator(author_ra, identifier, ra=True)
                ar_meta = self.ar_index[self.row_meta]["author"][aut_meta]
                ar_url = URIRef(self.url + ar_meta)
                preexisting_entity = ar_url in self.finder.prebuilt_subgraphs
                preexisting_graph = (
                    self.finder.get_subgraph(ar_url)
                    if preexisting_entity
                    else None
                )
                author_ra_role = self.setgraph.add_ar(
                    self.resp_agent,
                    source=self.src,
                    res=ar_url,
                    preexisting_graph=preexisting_graph,
                )
                author_ra_role.create_author()
                self.br_graph.has_contributor(author_ra_role)
                author_ra_role.is_held_by(author_ra)
                aut_role_list.append(author_ra_role)
                if len(aut_role_list) > 1:
                    aut_role_list[-2].has_next(author_ra_role)

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
            if str_date:
                self.br_graph.has_pub_date(str_date)

    def vvi_action(self, venue, vol, issue):
        if venue:
            venue_and_ids = RE_NAME_AND_IDS.search(venue)
            assert venue_and_ids is not None
            venue_ids = venue_and_ids.group(2)
            venue_ids_list = venue_ids.split()
            for identifier in venue_ids_list:
                if "omid:" in identifier:
                    ven_id = str(identifier).replace("omid:", "")
                    self.venue_meta = ven_id
                    url = URIRef(self.url + ven_id)
                    preexisting_entity = url in self.finder.prebuilt_subgraphs
                    venue_title = venue_and_ids.group(1)
                    preexisting_graph = (
                        self.finder.get_subgraph(url)
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
            assert self.venue_graph is not None
            for identifier in venue_ids_list:
                self.id_creator(self.venue_graph, identifier, ra=False)
            if self.type in {"journal article", "journal volume", "journal issue"}:
                if vol:
                    vol_meta = self.vi_index[self.venue_meta]["volume"][vol]["id"]
                    vol_url = URIRef(self.url + vol_meta)
                    preexisting_entity = vol_url in self.finder.prebuilt_subgraphs
                    preexisting_graph = (
                        self.finder.get_subgraph(vol_url)
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
                    issue_url = URIRef(self.url + issue_meta)
                    preexisting_entity = issue_url in self.finder.prebuilt_subgraphs
                    preexisting_graph = (
                        self.finder.get_subgraph(issue_url)
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
            assert self.issue_graph is not None
            assert self.vol_graph is not None
            assert self.venue_graph is not None
            self.br_graph.is_part_of(self.issue_graph)
            self.issue_graph.is_part_of(self.vol_graph)
            self.vol_graph.is_part_of(self.venue_graph)
        elif venue and vol and not issue:
            assert self.vol_graph is not None
            assert self.venue_graph is not None
            self.br_graph.is_part_of(self.vol_graph)
            self.vol_graph.is_part_of(self.venue_graph)
        elif venue and not vol and not issue:
            assert self.venue_graph is not None
            self.br_graph.is_part_of(self.venue_graph)
        elif venue and not vol and issue:
            assert self.issue_graph is not None
            assert self.venue_graph is not None
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
            re_meta = self.re_index[self.row_meta]
            re_url = URIRef(self.url + re_meta)
            preexisting_entity = re_url in self.finder.prebuilt_subgraphs
            preexisting_graph = (
                self.finder.get_subgraph(re_url)
                if preexisting_entity
                else None
            )
            form = self.setgraph.add_re(
                self.resp_agent,
                source=self.src,
                res=re_url,
                preexisting_graph=preexisting_graph,
            )
            form.has_starting_page(page)
            form.has_ending_page(page)
            self.br_graph.has_format(form)

    _TYPE_TO_METHOD = {
        "abstract": "create_abstract",
        "archival document": "create_archival_document",
        "audio document": "create_audio_document",
        "book": "create_book",
        "book chapter": "create_book_chapter",
        "book part": "create_book_part",
        "book section": "create_book_section",
        "book series": "create_book_series",
        "book set": "create_book_set",
        "computer program": "create_computer_program",
        "data file": "create_dataset",
        "dataset": "create_dataset",
        "data management plan": "create_data_management_plan",
        "dissertation": "create_dissertation",
        "editorial": "create_editorial",
        "journal": "create_journal",
        "journal article": "create_journal_article",
        "journal editorial": "create_journal_editorial",
        "journal issue": "create_issue",
        "journal volume": "create_volume",
        "newspaper": "create_newspaper",
        "newspaper article": "create_newspaper_article",
        "newspaper issue": "create_newspaper_issue",
        "peer review": "create_peer_review",
        "preprint": "create_preprint",
        "presentation": "create_presentation",
        "proceedings": "create_proceedings",
        "proceedings article": "create_proceedings_article",
        "reference book": "create_reference_book",
        "reference entry": "create_reference_entry",
        "report": "create_report",
        "report series": "create_report_series",
        "retraction notice": "create_retraction_notice",
        "standard": "create_standard",
        "series": "create_series",
        "web content": "create_web_content",
    }

    def type_action(self, entity_type):
        method_name = self._TYPE_TO_METHOD.get(entity_type)
        if method_name:
            getattr(self.br_graph, method_name)()

    def publisher_action(self, publisher):
        if publisher:
            publishers_list = RE_SEMICOLON_IN_PEOPLE_FIELD.split(publisher)
            pub_role_list = list()
            for pub in publishers_list:
                publ_and_ids = RE_NAME_AND_IDS.search(pub)
                assert publ_and_ids is not None
                publ_id = publ_and_ids.group(2)
                publ_id_list = publ_id.split()
                publisher_ra = None
                pub_meta = ""
                for identifier in publ_id_list:
                    if "omid:" in identifier:
                        identifier = str(identifier).replace("omid:", "")
                        pub_meta = identifier
                        url = URIRef(self.url + identifier)
                        preexisting_entity = url in self.finder.prebuilt_subgraphs
                        publ_name = publ_and_ids.group(1)
                        preexisting_graph = (
                            self.finder.get_subgraph(url)
                            if preexisting_entity
                            else None
                        )
                        publisher_ra = self.setgraph.add_ra(
                            self.resp_agent,
                            source=self.src,
                            res=url,
                            preexisting_graph=preexisting_graph,
                        )
                        publisher_ra.has_name(publ_name)
                assert publisher_ra is not None
                for identifier in publ_id_list:
                    self.id_creator(publisher_ra, identifier, ra=True)
                ar_meta = self.ar_index[self.row_meta]["publisher"][pub_meta]
                ar_url = URIRef(self.url + ar_meta)
                preexisting_entity = ar_url in self.finder.prebuilt_subgraphs
                preexisting_graph = (
                    self.finder.get_subgraph(ar_url)
                    if preexisting_entity
                    else None
                )
                publ_role = self.setgraph.add_ar(
                    self.resp_agent,
                    source=self.src,
                    res=ar_url,
                    preexisting_graph=preexisting_graph,
                )
                publ_role.create_publisher()
                self.br_graph.has_contributor(publ_role)
                publ_role.is_held_by(publisher_ra)
                pub_role_list.append(publ_role)
                if len(pub_role_list) > 1:
                    pub_role_list[-2].has_next(publ_role)

    def editor_action(self, editor, row):
        if editor:
            editorslist = RE_SEMICOLON_IN_PEOPLE_FIELD.split(editor)
            edit_role_list = list()
            for ed in editorslist:
                ed_and_ids = RE_NAME_AND_IDS.search(ed)
                assert ed_and_ids is not None
                ed_id = ed_and_ids.group(2)
                ed_id_list = ed_id.split(" ")
                editor_ra = None
                ed_meta = ""
                for identifier in ed_id_list:
                    if "omid:" in identifier:
                        identifier = str(identifier).replace("omid:", "")
                        ed_meta = identifier
                        url = URIRef(self.url + identifier)
                        preexisting_entity = url in self.finder.prebuilt_subgraphs
                        preexisting_graph = (
                            self.finder.get_subgraph(url)
                            if preexisting_entity
                            else None
                        )
                        editor_ra = self.setgraph.add_ra(
                            self.resp_agent,
                            source=self.src,
                            res=url,
                            preexisting_graph=preexisting_graph,
                        )
                        editor_name = ed_and_ids.group(1)
                        if "," in editor_name:
                            editor_name_splitted = RE_COMMA_AND_SPACES.split(
                                editor_name
                            )
                            firstName = editor_name_splitted[1]
                            lastName = editor_name_splitted[0]
                            if firstName.strip():
                                editor_ra.has_given_name(firstName)
                            editor_ra.has_family_name(lastName)
                        else:
                            editor_ra.has_name(editor_name)
                assert editor_ra is not None
                for identifier in ed_id_list:
                    self.id_creator(editor_ra, identifier, ra=True)
                br_key = get_edited_br_metaid(row, self.row_meta, self.venue_meta)
                ar_meta = self.ar_index[br_key]["editor"][ed_meta]
                ar_url = URIRef(self.url + ar_meta)
                preexisting_entity = ar_url in self.finder.prebuilt_subgraphs
                preexisting_graph = (
                    self.finder.get_subgraph(ar_url)
                    if preexisting_entity
                    else None
                )
                editor_ra_role = self.setgraph.add_ar(
                    self.resp_agent,
                    source=self.src,
                    res=ar_url,
                    preexisting_graph=preexisting_graph,
                )
                editor_ra_role.create_editor()
                br_graphs: List[BibliographicResource] = [
                    g for g in [self.br_graph, self.issue_graph, self.vol_graph, self.venue_graph]
                    if g is not None
                ]
                for graph in br_graphs:
                    if br_key == self.__res_metaid(graph):
                        graph.has_contributor(editor_ra_role)
                editor_ra_role.is_held_by(editor_ra)
                edit_role_list.append(editor_ra_role)
                if len(edit_role_list) > 1:
                    edit_role_list[-2].has_next(editor_ra_role)

    def __res_metaid(self, graph: BibliographicResource):
        if graph:
            return graph.res.replace(self.url, "")

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
                    url = URIRef(self.url + res)
                    preexisting_entity = url in self.finder.prebuilt_subgraphs
                    preexisting_graph = (
                        self.finder.get_subgraph(url)
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
                    url = URIRef(self.url + res)
                    preexisting_entity = url in self.finder.prebuilt_subgraphs
                    preexisting_graph = (
                        self.finder.get_subgraph(url)
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
