# SPDX-FileCopyrightText: 2019 Silvio Peroni <silvio.peroni@unibo.it>
# SPDX-FileCopyrightText: 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# SPDX-FileCopyrightText: 2021 Simone Persiani <iosonopersia@gmail.com>
# SPDX-FileCopyrightText: 2021-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from contextlib import nullcontext
from typing import TYPE_CHECKING, Dict, List, Tuple

from oc_meta.constants import CONTAINER_EDITOR_TYPES, VALID_ENTITY_TYPES
from oc_meta.lib.cleaner import (
    clean_date,
    clean_name,
    clean_ra_list,
    clean_title,
    clean_volume_and_issue,
    normalize_hyphens,
    normalize_id,
)
from oc_meta.lib.file_manager import *
from oc_meta.lib.finder import *
from oc_meta.lib.master_of_regex import (
    RE_COLON_AND_SPACES,
    RE_MULTIPLE_SPACES,
    RE_NAME_AND_IDS,
    RE_ONE_OR_MORE_SPACES,
    RE_SEMICOLON_IN_PEOPLE_FIELD
)
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler

if TYPE_CHECKING:
    from rich.progress import Progress


def _extract_ids_from_chunk(args: tuple) -> Tuple[set, set, set]:
    rows, valid_dois_cache = args
    all_metavals = set()
    all_identifiers = set()
    all_vvis = set()

    for row in rows:
        metavals = set()
        identifiers = set()
        vvis = set()
        venue_ids = set()
        venue_metaid = None

        if row["id"]:
            id_list = RE_ONE_OR_MORE_SPACES.split(RE_COLON_AND_SPACES.sub(":", row["id"]))
            idslist, metaval = Curator.clean_id_list(id_list, br=True, valid_dois_cache=valid_dois_cache)
            if metaval:
                metavals.add(f"omid:br/{metaval}")
            if idslist:
                identifiers.update(idslist)

        fields_with_an_id = [
            (field, match.group(2).split())
            for field in ["author", "editor", "publisher", "venue", "volume", "issue"]
            if (match := RE_NAME_AND_IDS.search(row[field]))
        ]
        for field, field_ids in fields_with_an_id:
            br = field in ["venue", "volume", "issue"]
            field_idslist, field_metaval = Curator.clean_id_list(field_ids, br=br, valid_dois_cache=valid_dois_cache)
            if field_metaval:
                field_metaval = f"omid:br/{field_metaval}" if br else f"omid:ra/{field_metaval}"
            else:
                field_metaval = ""
            if field_metaval:
                metavals.add(field_metaval)
            if field == "venue":
                venue_metaid = field_metaval
                if field_idslist:
                    venue_ids.update(field_idslist)
            else:
                if field_idslist:
                    identifiers.update(field_idslist)

        if (venue_metaid or venue_ids) and (row["volume"] or row["issue"]):
            vvi = (row["volume"], row["issue"], venue_metaid, tuple(sorted(venue_ids)))
            vvis.add(vvi)

        all_metavals.update(metavals)
        all_identifiers.update(identifiers)
        all_vvis.update(vvis)

    return all_metavals, all_identifiers, all_vvis


class Curator:

    def __init__(
        self,
        data: List[dict],
        ts: str,
        prov_config: str,
        counter_handler: RedisCounterHandler,
        base_iri: str = "https://w3id.org/oc/meta",
        prefix: str = "060",
        valid_dois_cache: dict = dict(),
        settings: dict | None = None,
        silencer: list = [],
        meta_config_path: str | None = None,
        timer=None,
        progress: Progress | None = None,
        min_rows_parallel: int = 1000,
    ):
        self.timer = timer
        self.progress = progress
        self.settings = settings or {}
        self.workers = self.settings.get("workers", 1)
        self.everything_everywhere_allatonce = Graph()
        self.finder = ResourceFinder(
            ts,
            base_iri,
            self.everything_everywhere_allatonce,
            settings=self.settings,
            meta_config_path=meta_config_path,
            workers=self.workers,
        )
        self.base_iri = base_iri
        self.prov_config = prov_config
        # Preliminary pass to clear volume and issue if id is present but venue is missing
        for row in data:
            if row["id"] and (row["volume"] or row["issue"]):
                if not row["venue"]:
                    row["volume"] = ""
                    row["issue"] = ""
                if not row["type"]:
                    row["type"] = "journal article"
        self.data = [
            {field: value.strip() for field, value in row.items()}
            for row in data
            if is_a_valid_row(row)
        ]
        self.prefix = prefix
        # Redis counter handler
        self.counter_handler = counter_handler
        self.brdict = {}
        self.radict = {}
        self.ardict = {}
        self.vvi = {}  # Venue, Volume, Issue
        self.idra = {}  # key id; value metaid of id related to ra
        self.idbr = {}  # key id; value metaid of id related to br
        self.rameta = dict()
        self.brmeta = dict()
        self.armeta = dict()
        self.remeta = dict()
        self.wnb_cnt = 0  # wannabe counter
        self.rowcnt = 0
        self.valid_dois_cache = valid_dois_cache
        self.preexisting_entities = set()
        self.silencer = silencer
        self.min_rows_parallel = min_rows_parallel

    def _timed(self, name: str):
        if self.timer:
            return self.timer.timer(name)
        return nullcontext()

    def collect_identifiers(self, valid_dois_cache):
        return self._collect_identifiers_with_progress(valid_dois_cache, task_id=None)

    def _collect_identifiers_with_progress(self, valid_dois_cache, task_id=None):
        all_metavals = set()
        all_idslist = set()
        all_vvis = set()

        total_rows = len(self.data)
        if total_rows == 0:
            return all_metavals, all_idslist, all_vvis

        if total_rows > self.min_rows_parallel and self.workers > 1:
            chunks = []
            for i in range(0, total_rows, self.min_rows_parallel):
                chunks.append((self.data[i:i + self.min_rows_parallel], valid_dois_cache))

            with ProcessPoolExecutor(
                max_workers=self.workers,
                mp_context=multiprocessing.get_context('forkserver')
            ) as executor:
                for chunk_metavals, chunk_ids, chunk_vvis in executor.map(_extract_ids_from_chunk, chunks):
                    all_metavals.update(chunk_metavals)
                    all_idslist.update(chunk_ids)
                    all_vvis.update(chunk_vvis)
                    if self.progress and task_id is not None:
                        self.progress.advance(task_id, min(self.min_rows_parallel, total_rows))
        else:
            for row in self.data:
                metavals, idslist, vvis = self.extract_identifiers_and_metavals(
                    row, valid_dois_cache=valid_dois_cache
                )
                all_metavals.update(metavals)
                all_idslist.update(idslist)
                all_vvis.update(vvis)
                if self.progress and task_id is not None:
                    self.progress.advance(task_id)

        return all_metavals, all_idslist, all_vvis

    def extract_identifiers_and_metavals(
        self, row, valid_dois_cache
    ) -> Tuple[set, set, set]:
        metavals = set()
        identifiers = set()
        vvis = set()
        venue_ids = set()
        venue_metaid = None

        if row["id"]:
            idslist, metaval = self.clean_id_list(
                self.split_identifiers(row["id"]),
                br=True,
                valid_dois_cache=valid_dois_cache,
            )
            id_metaval = f"omid:br/{metaval}" if metaval else ""
            if id_metaval:
                metavals.add(id_metaval)
            if idslist:
                identifiers.update(idslist)
        
        fields_with_an_id = [
            (field, match.group(2).split())
            for field in ["author", "editor", "publisher", "venue", "volume", "issue"]
            if (match := RE_NAME_AND_IDS.search(row[field]))
        ]
        for field, field_ids in fields_with_an_id:
            br = field in ["venue", "volume", "issue"]
            field_idslist, field_metaval = self.clean_id_list(
                field_ids, br=br, valid_dois_cache=valid_dois_cache
            )
            if field_metaval:
                field_metaval = (
                    f"omid:br/{field_metaval}" if br else f"omid:ra/{field_metaval}"
                )
            else:
                field_metaval = ""
            if field_metaval:
                metavals.add(field_metaval)
            if field == "venue":
                venue_metaid = field_metaval
                if field_idslist:
                    venue_ids.update(field_idslist)
            else:
                if field_idslist:
                    identifiers.update(field_idslist)
        
        if (venue_metaid or venue_ids) and (row["volume"] or row["issue"]):
            vvi = (row["volume"], row["issue"], venue_metaid, tuple(sorted(venue_ids)))
            vvis.add(vvi)
        
        return metavals, identifiers, vvis

    def split_identifiers(self, field_value):
        return RE_ONE_OR_MORE_SPACES.split(RE_COLON_AND_SPACES.sub(":", field_value))

    def curator(self, filename: str | None = None, path_csv: str | None = None):
        total_rows = len(self.data)

        # Phase 1: Collect identifiers and SPARQL prefetch
        with self._timed("curation__collect_identifiers"):
            task_collect = None
            if self.progress:
                task_collect = self.progress.add_task(
                    "  [dim]Collecting identifiers[/dim]", total=total_rows
                )
            metavals, identifiers, vvis = self._collect_identifiers_with_progress(
                valid_dois_cache=self.valid_dois_cache,
                task_id=task_collect,
            )
            if self.progress and task_collect is not None:
                self.progress.remove_task(task_collect)
            self.finder.get_everything_about_res(
                metavals=metavals, identifiers=identifiers, vvis=vvis,
                progress=self.progress
            )

        # Phase 2: Clean ID (loop over all rows)
        with self._timed("curation__clean_id"):
            task_clean_id = None
            if self.progress:
                task_clean_id = self.progress.add_task(
                    "  [dim]Cleaning IDs[/dim]", total=total_rows
                )
            for row in self.data:
                self.clean_id(row)
                self.rowcnt += 1
                if self.progress and task_clean_id is not None:
                    self.progress.advance(task_clean_id)
            if self.progress and task_clean_id is not None:
                self.progress.remove_task(task_clean_id)

        # Phase 3: Merge duplicate entities
        with self._timed("curation__merge_duplicates"):
            task_merge = None
            if self.progress:
                task_merge = self.progress.add_task(
                    "  [dim]Merging duplicates[/dim]", total=total_rows
                )
            self.merge_duplicate_entities(task_id=task_merge)
            if self.progress and task_merge is not None:
                self.progress.remove_task(task_merge)
            self.clean_metadata_without_id()

        # Phase 4: Clean VVI (venue/volume/issue)
        with self._timed("curation__clean_vvi"):
            self.rowcnt = 0
            task_vvi = None
            if self.progress:
                task_vvi = self.progress.add_task(
                    "  [dim]Cleaning VVI[/dim]", total=total_rows
                )
            for row in self.data:
                self.clean_vvi(row)
                self.rowcnt += 1
                if self.progress and task_vvi is not None:
                    self.progress.advance(task_vvi)
            if self.progress and task_vvi is not None:
                self.progress.remove_task(task_vvi)

        # Phase 5: Clean RA (author + publisher + editor aggregated)
        with self._timed("curation__clean_ra"):
            self.rowcnt = 0
            task_ra = None
            if self.progress:
                task_ra = self.progress.add_task(
                    "  [dim]Cleaning RA[/dim]", total=total_rows
                )
            for row in self.data:
                self.clean_ra(row, "author")
                self.clean_ra(row, "publisher")
                self.clean_ra(row, "editor")
                self.rowcnt += 1
                if self.progress and task_ra is not None:
                    self.progress.advance(task_ra)
            if self.progress and task_ra is not None:
                self.progress.remove_task(task_ra)

        # Phase 6: Finalize (preexisting + meta_maker + enrich + indexer)
        with self._timed("curation__finalize"):
            task_finalize = None
            if self.progress:
                task_finalize = self.progress.add_task(
                    "  [dim]Finalizing[/dim]", total=total_rows
                )
            self.get_preexisting_entities()
            self.meta_maker()
            self.enrich(task_id=task_finalize)
            if self.progress and task_finalize is not None:
                self.progress.remove_task(task_finalize)
            # Remove duplicates
            self.data = list({v["id"]: v for v in self.data}.values())
            self.filename = filename
            self.indexer(path_csv=path_csv)

    # ID
    def clean_id(self, row: Dict[str, str]) -> None:
        """
        The 'clean id()' function is executed for each CSV row.
        In this process, any duplicates are detected by the IDs in the 'id' column.
        For each line, a wannabeID or, if the bibliographic resource was found in the triplestore,
        a MetaID is assigned.
        Finally, this method enrich and clean the fields related to the
        title, venue, volume, issue, page, publication date and type.

        :params row: a dictionary representing a CSV row
        :type row: Dict[str, str]
        :returns: None -- This method modifies the input CSV row without returning it.
        """
        if row["title"]:
            name = clean_title(
                row["title"], bool(self.settings.get("normalize_titles", False))
            )
        else:
            name = ""
        metaval_ids_list = []
        idslist: list = []
        metaval = ""
        if row["id"]:
            idslist = RE_ONE_OR_MORE_SPACES.split(RE_COLON_AND_SPACES.sub(":", row["id"]))
            idslist, metaval = self.clean_id_list(
                idslist, br=True, valid_dois_cache=self.valid_dois_cache
            )
            id_metaval = f"omid:br/{metaval}" if metaval else ""
            metaval_ids_list.append((id_metaval, idslist))
        fields_with_an_id = [
            (field, match.group(2).split())
            for field in ["author", "editor", "publisher", "venue", "volume", "issue"]
            if (match := RE_NAME_AND_IDS.search(row[field]))
        ]
        for field, field_ids in fields_with_an_id:
            br = field in ["venue", "volume", "issue"]
            field_idslist, field_metaval = self.clean_id_list(
                field_ids, br=br, valid_dois_cache=self.valid_dois_cache
            )
            if field_metaval:
                field_metaval = (
                    f"omid:br/{field_metaval}" if br else f"omid:ra/{field_metaval}"
                )
            else:
                field_metaval = ""
            metaval_ids_list.append((field_metaval, field_idslist))
        if row["id"]:
            metaval = self.id_worker(
                "id",
                name,
                idslist,
                metaval,
                ra_ent=False,
                br_ent=True,
                vvi_ent=False,
                publ_entity=False,
            )
        else:
            metaval = self.new_entity(self.brdict, name)
        row["title"] = self.brdict[metaval]["title"]
        row["id"] = metaval

    def clean_metadata_without_id(self):
        for row in self.data:
            # page
            if row["page"]:
                row["page"] = normalize_hyphens(row["page"])
            # date
            if pub_date := row["pub_date"]:
                row["pub_date"] = clean_date(normalize_hyphens(pub_date))
            # type
            if row["type"]:
                entity_type = RE_MULTIPLE_SPACES.sub(" ", row["type"].lower()).strip()
                if entity_type == "edited book" or entity_type == "monograph":
                    entity_type = "book"
                elif (
                    entity_type == "report series"
                    or entity_type == "standard series"
                    or entity_type == "proceedings series"
                ):
                    entity_type = "series"
                elif entity_type == "posted content":
                    entity_type = "web content"
                if entity_type in VALID_ENTITY_TYPES:
                    row["type"] = entity_type
                else:
                    row["type"] = ""

    # VVI
    def clean_vvi(self, row: Dict[str, str]) -> None:
        """
        This method performs the deduplication process for venues, volumes and issues.
        The acquired information is stored in the 'vvi' dictionary, that has the following format: ::

            {
                VENUE_IDENTIFIER: {
                    'issue': {SEQUENCE_IDENTIFIER: {'id': META_ID}},
                    'volume': {
                        SEQUENCE_IDENTIFIER: {
                            'id': META_ID,
                            'issue' {SEQUENCE_IDENTIFIER: {'id': META_ID}}
                        }
                    }
                }
            }

            {
                '4416': {
                    'issue': {},
                    'volume': {
                        '166': {'id': '4388', 'issue': {'4': {'id': '4389'}}},
                        '172': {'id': '4434',
                            'issue': {
                                '22': {'id': '4435'},
                                '20': {'id': '4436'},
                                '21': {'id': '4437'},
                                '19': {'id': '4438'}
                            }
                        }
                    }
                }
            }

        :params row: a dictionary representing a CSV row
        :type row: Dict[str, str]
        :returns: None -- This method modifies the input CSV row without returning it.
        """
        if row["type"] not in {
            "journal article",
            "journal volume",
            "journal issue",
        } and (row["volume"] or row["issue"]):
            row["volume"] = ""
            row["issue"] = ""
        clean_volume_and_issue(row=row)
        vol_meta = None
        br_type = row["type"]
        volume = row["volume"]
        issue = row["issue"]
        br_id = row["id"]
        venue = row["venue"]
        # Venue
        if venue:
            # The data must be invalidated, because the resource is journal but a volume or an issue have also been specified
            if br_type == "journal" and (volume or issue):
                row["venue"] = ""
                row["volume"] = ""
                row["issue"] = ""
            venue_id = RE_NAME_AND_IDS.search(venue)
            if venue_id:
                name = clean_title(
                    venue_id.group(1), bool(self.settings.get("normalize_titles", False))
                )
                venue_id = venue_id.group(2)
                idslist = RE_ONE_OR_MORE_SPACES.split(RE_COLON_AND_SPACES.sub(":", venue_id))
                idslist, metaval = self.clean_id_list(
                    idslist, br=True, valid_dois_cache=self.valid_dois_cache
                )

                metaval = self.id_worker(
                    "venue",
                    name,
                    idslist,
                    metaval,
                    ra_ent=False,
                    br_ent=True,
                    vvi_ent=True,
                    publ_entity=False,
                )
                if metaval not in self.vvi:
                    ts_vvi = None
                    if "wannabe" not in metaval:
                        ts_vvi = self.finder.retrieve_venue_from_local_graph(metaval)
                    if "wannabe" in metaval or not ts_vvi:
                        self.vvi[metaval] = dict()
                        self.vvi[metaval]["volume"] = dict()
                        self.vvi[metaval]["issue"] = dict()
                    elif ts_vvi:
                        self.vvi[metaval] = ts_vvi
            else:
                name = clean_title(venue, bool(self.settings.get("normalize_titles", False)))
                metaval = self.new_entity(self.brdict, name)
                self.vvi[metaval] = dict()
                self.vvi[metaval]["volume"] = dict()
                self.vvi[metaval]["issue"] = dict()
            row["venue"] = metaval

            # Volume
            if volume and (br_type == "journal issue" or br_type == "journal article"):
                if volume in self.vvi[metaval]["volume"]:
                    vol_meta = self.vvi[metaval]["volume"][volume]["id"]
                else:
                    vol_meta = self.new_entity(self.brdict, "")
                    self.vvi[metaval]["volume"][volume] = dict()
                    self.vvi[metaval]["volume"][volume]["id"] = vol_meta
                    self.vvi[metaval]["volume"][volume]["issue"] = dict()
            elif volume and br_type == "journal volume":
                # The data must be invalidated, because the resource is a journal volume but an issue has also been specified
                if issue:
                    row["volume"] = ""
                    row["issue"] = ""
                else:
                    vol_meta = br_id
                    self.volume_issue(
                        vol_meta, self.vvi[metaval]["volume"], volume, row
                    )

            # Issue
            if issue and br_type == "journal article":
                row["issue"] = issue
                if vol_meta:
                    if issue not in self.vvi[metaval]["volume"][volume]["issue"]:
                        issue_meta = self.new_entity(self.brdict, "")
                        self.vvi[metaval]["volume"][volume]["issue"][issue] = dict()
                        self.vvi[metaval]["volume"][volume]["issue"][issue][
                            "id"
                        ] = issue_meta
                else:
                    if issue not in self.vvi[metaval]["issue"]:
                        issue_meta = self.new_entity(self.brdict, "")
                        self.vvi[metaval]["issue"][issue] = dict()
                        self.vvi[metaval]["issue"][issue]["id"] = issue_meta
            elif issue and br_type == "journal issue":
                issue_meta = br_id
                if vol_meta:
                    self.volume_issue(
                        issue_meta,
                        self.vvi[metaval]["volume"][volume]["issue"],
                        issue,
                        row,
                    )
                else:
                    self.volume_issue(
                        issue_meta, self.vvi[metaval]["issue"], issue, row
                    )

        else:
            row["venue"] = ""
            row["volume"] = ""
            row["issue"] = ""

    # RA
    def clean_ra(self, row, col_name):
        """
        This method performs the deduplication process for responsible agents (authors, publishers and editors).

        :params row: a dictionary representing a CSV row
        :type row: Dict[str, str]
        :params col_name: the CSV column name. It can be 'author', 'publisher', or 'editor'
        :type col_name: str
        :returns: None -- This method modifies self.ardict, self.radict, and self.idra, and returns None.
        """

        def get_br_metaval_to_check(row, col_name):
            if col_name == "editor":
                return get_edited_br_metaid(row, row["id"], row["venue"])
            else:
                return row["id"]

        def get_br_metaval(br_metaval_to_check):
            if br_metaval_to_check in self.brdict or br_metaval_to_check in self.vvi:
                return br_metaval_to_check
            return next(
                id for id in self.brdict
                if br_metaval_to_check in self.brdict[id]["others"]
            )

        def initialize_ardict_entry(br_metaval):
            if br_metaval not in self.ardict:
                self.ardict[br_metaval] = {"author": [], "editor": [], "publisher": []}

        def initialize_sequence(br_metaval, col_name):
            sequence = []
            if "wannabe" in br_metaval:
                sequence = []
            else:
                sequence_found = self.finder.retrieve_ra_sequence_from_br_meta(
                    br_metaval, col_name
                )
                if sequence_found:
                    sequence = []
                    for agent in sequence_found:
                        for ar_metaid in agent:
                            ra_metaid = agent[ar_metaid][2]
                            sequence.append(tuple((ar_metaid, ra_metaid)))
                            if ra_metaid not in self.radict:
                                self.radict[ra_metaid] = dict()
                                self.radict[ra_metaid]["ids"] = set()
                                self.radict[ra_metaid]["others"] = set()
                                self.radict[ra_metaid]["title"] = agent[ar_metaid][0]
                            for identifier in agent[ar_metaid][1]:
                                # other ids after meta
                                id_metaid = identifier[0]
                                literal = identifier[1]
                                if id_metaid not in self.idra:
                                    self.idra[literal] = id_metaid
                                if literal not in self.radict[ra_metaid]["ids"]:
                                    self.radict[ra_metaid]["ids"].add(literal)
                    self.ardict[br_metaval][col_name].extend(sequence)
                else:
                    sequence = []
            return sequence

        def parse_ra_list(row):
            ra_list = RE_SEMICOLON_IN_PEOPLE_FIELD.split(row[col_name])
            ra_list = clean_ra_list(ra_list)
            return ra_list

        def process_individual_ra(ra, sequence):
            new_elem_seq = True
            ra_id = None
            ra_id_match = RE_NAME_AND_IDS.search(ra)
            raw_name = ra_id_match.group(1) if ra_id_match else ra
            name = clean_name(raw_name)
            if ra_id_match:
                ra_id = ra_id_match.group(2)
            if not ra_id and sequence:
                for _, ra_metaid in sequence:
                    if self.radict[ra_metaid]["title"] == name:
                        ra_id = "omid:ra/" + str(ra_metaid)
                        new_elem_seq = False
                        break
            return ra_id, name, new_elem_seq

        if not row[col_name]:
            return

        br_metaval_to_check = get_br_metaval_to_check(row, col_name)
        br_metaval = get_br_metaval(br_metaval_to_check)
        initialize_ardict_entry(br_metaval)

        sequence = self.ardict[br_metaval].get(col_name, [])
        if not sequence:
            sequence = initialize_sequence(br_metaval, col_name)
        if col_name in self.silencer and sequence:
            return

        ra_list = parse_ra_list(row)
        new_sequence = list()
        change_order = False

        for pos, ra in enumerate(ra_list):
            ra_id, name, new_elem_seq = process_individual_ra(ra, sequence)
            if ra_id:
                ra_id_list = RE_ONE_OR_MORE_SPACES.split(RE_COLON_AND_SPACES.sub(":", ra_id))
                if sequence:
                    ar_ra = None
                    for ps, el in enumerate(sequence):
                        ra_metaid = el[1]
                        for literal in ra_id_list:
                            if literal in self.radict[ra_metaid]["ids"]:
                                if ps != pos:
                                    change_order = True
                                new_elem_seq = False
                                if "wannabe" not in ra_metaid:
                                    ar_ra = ra_metaid
                                    for pos, literal_value in enumerate(ra_id_list):
                                        if "omid" in literal_value:
                                            ra_id_list[pos] = ""
                                        break
                                    ra_id_list = list(filter(None, ra_id_list))
                                    ra_id_list.append("omid:ra/" + ar_ra)
                    if not ar_ra:
                        # new element
                        for ar_metaid, ra_metaid in sequence:
                            if self.radict[ra_metaid]["title"] == name:
                                new_elem_seq = False
                                if "wannabe" not in ra_metaid:
                                    ar_ra = ra_metaid
                                    for pos, i in enumerate(ra_id_list):
                                        if "omid" in i:
                                            ra_id_list[pos] = ""
                                        break
                                    ra_id_list = list(filter(None, ra_id_list))
                                    ra_id_list.append("omid:ra/" + ar_ra)
                if col_name == "publisher":
                    ra_id_list, metaval = self.clean_id_list(
                        ra_id_list, br=False, valid_dois_cache=self.valid_dois_cache
                    )
                    metaval = self.id_worker(
                        "publisher",
                        name,
                        ra_id_list,
                        metaval,
                        ra_ent=True,
                        br_ent=False,
                        vvi_ent=False,
                        publ_entity=True,
                    )
                else:
                    ra_id_list, metaval = self.clean_id_list(
                        ra_id_list, br=False, valid_dois_cache=self.valid_dois_cache
                    )
                    metaval = self.id_worker(
                        col_name,
                        name,
                        ra_id_list,
                        metaval,
                        ra_ent=True,
                        br_ent=False,
                        vvi_ent=False,
                        publ_entity=False,
                    )
                if col_name != "publisher" and metaval in self.radict:
                    full_name: str = self.radict[metaval]["title"]
                    if "," in name and "," in full_name:
                        first_name = name.split(",")[1].strip()
                        if (
                            not full_name.split(",")[1].strip() and first_name
                        ):  # first name found!
                            given_name = full_name.split(",")[0]
                            self.radict[metaval]["title"] = (
                                given_name + ", " + first_name
                            )
            else:
                metaval = self.new_entity(self.radict, name)
            if new_elem_seq:
                role = self.prefix + str(self._add_number("ar"))
                new_sequence.append(tuple((role, metaval)))
        sequence.extend(new_sequence)
        self.ardict[br_metaval][col_name] = sequence

    @staticmethod
    def clean_id_list(
        id_list: List[str], br: bool, valid_dois_cache: dict = dict()
    ) -> Tuple[list, str]:
        """
        Clean IDs in the input list and check if there is a MetaID.

        :params: id_list: a list of IDs
        :type: id_list: List[str]
        :params: br: True if the IDs in id_list refer to bibliographic resources, False otherwise
        :type: br: bool
        :returns: Tuple[list, str]: -- it returns a two-elements tuple, where the first element is the list of cleaned IDs, while the second is a MetaID if any was found.
        """
        pattern = "br/" if br else "ra/"
        metaid = ""
        id_list = list(filter(None, id_list))
        clean_set = set()
        clean_list = []

        for elem in id_list:
            if elem in clean_set:
                continue
            clean_set.add(elem)
            elem = normalize_hyphens(elem)
            identifier = elem.split(":", 1)
            schema = identifier[0].lower()
            value = identifier[1]

            if schema == "omid":
                metaid = value.replace(pattern, "")
            else:
                normalized_id = normalize_id(elem, valid_dois_cache=valid_dois_cache)
                if normalized_id:
                    clean_list.append(normalized_id)

        meta_count = sum(1 for i in id_list if i.lower().startswith("omid"))
        if meta_count > 1:
            clean_list = [i for i in clean_list if not i.lower().startswith("omid")]

        return clean_list, metaid

    def conflict(
        self, idslist: List[str], name: str, id_dict: dict, col_name: str
    ) -> str:
        if col_name in ("id", "venue"):
            entity_dict = self.brdict
        else:
            entity_dict = self.radict
        metaval = self.new_entity(entity_dict, name)
        for identifier in idslist:
            entity_dict[metaval]["ids"].add(identifier)
            if identifier not in id_dict:
                schema_value = identifier.split(":", maxsplit=1)
                found_metaid = self.finder.retrieve_metaid_from_id(
                    schema_value[0], schema_value[1]
                )
                if found_metaid:
                    id_dict[identifier] = found_metaid
                else:
                    self.__update_id_count(id_dict, identifier)
        return metaval

    def finder_sparql(self, list_to_find, br=True, ra=False, vvi=False, publ=False):
        match_elem = list()
        id_set = set()
        res = None
        for elem in list_to_find:
            if len(match_elem) < 2:
                identifier = elem.split(":", maxsplit=1)
                value = identifier[1]
                schema = identifier[0]
                if br:
                    res = self.finder.retrieve_br_from_id(schema, value)
                elif ra:
                    res = self.finder.retrieve_ra_from_id(schema, value)
                if res:
                    for f in res:
                        if f[0] not in id_set:
                            match_elem.append(f)
                            id_set.add(f[0])
        return match_elem

    def ra_update(self, row: dict, br_key: str, col_name: str) -> None:
        if row[col_name]:
            sequence = self.armeta[br_key][col_name]
            ras_list = list()
            for _, ra_id in sequence:
                ra_name = self.rameta[ra_id]["title"]
                ra_ids = self.rameta[ra_id]["ids"]
                ra = self.build_name_ids_string(ra_name, ra_ids)
                ras_list.append(ra)
            row[col_name] = "; ".join(ras_list)

    @staticmethod
    def build_name_ids_string(name: str, ids: set) -> str:
        if name and ids:
            return f"{name} [{' '.join(ids)}]"
        elif name:
            return name
        elif ids:
            return f"[{' '.join(ids)}]"
        return ""

    @staticmethod
    def __local_match(list_to_match, dict_to_match: dict):
        match_elem = dict()
        match_elem["existing"] = list()
        match_elem["wannabe"] = list()
        for elem in list_to_match:
            for k, va in dict_to_match.items():
                if elem in va["ids"]:
                    if "wannabe" in k:
                        if k not in match_elem["wannabe"]:
                            match_elem["wannabe"].append(k)
                    else:
                        if k not in match_elem["existing"]:
                            match_elem["existing"].append(k)
        return match_elem

    def __meta_ar(self, target_br_metaid: str, source_br_key: str, role_type: str) -> None:
        """
        Transfer agent role assignments from working dictionary to finalized dictionary.

        Resolves any remaining placeholder ("wannabe") agent identifiers to their
        final MetaIDs by looking up which finalized agent absorbed them.

        Args:
            target_br_metaid: The final, deduplicated bibliographic resource MetaID
            source_br_key: The intermediate key in ardict (may contain "wannabe")
            role_type: Type of role ("author", "editor", or "publisher")
        """
        for ar_metaid, agent_id in self.ardict[source_br_key][role_type]:
            resolved_ra_metaid = agent_id
            if "wannabe" in agent_id:
                resolved_ra_metaid = self._ra_wannabe_to_meta[agent_id]
            self.armeta[target_br_metaid][role_type].append((ar_metaid, resolved_ra_metaid))

    def __tree_traverse(self, tree: dict, key: str, values: List[Tuple]) -> None:
        for k, v in tree.items():
            if k == key:
                values.append(v)
            elif isinstance(v, dict):
                found = self.__tree_traverse(v, key, values)
                if found is not None:
                    values.append(found)

    def get_preexisting_entities(self) -> None:
        for entity_type in {"br", "ra"}:
            for entity_metaid, data in getattr(self, f"{entity_type}dict").items():
                if not entity_metaid.startswith("wannabe"):
                    self.preexisting_entities.add(f"{entity_type}/{entity_metaid}")
                    for entity_id_literal in data["ids"]:
                        preexisting_entity_id_metaid = getattr(
                            self, f"id{entity_type}"
                        )[entity_id_literal]
                        self.preexisting_entities.add(
                            f"id/{preexisting_entity_id_metaid}"
                        )
        for _, roles in self.ardict.items():
            for _, ar_ras in roles.items():
                for ar_ra in ar_ras:
                    if not ar_ra[1].startswith("wannabe"):
                        self.preexisting_entities.add(f"ar/{ar_ra[0]}")
        for venue_metaid, vi in self.vvi.items():
            if not venue_metaid.startswith("wannabe"):
                wannabe_preexisting_vis = list()
                self.__tree_traverse(vi, "id", wannabe_preexisting_vis)
                self.preexisting_entities.update(
                    {
                        f"br/{vi_metaid}"
                        for vi_metaid in wannabe_preexisting_vis
                        if not vi_metaid.startswith("wannabe")
                    }
                )
        for _, re_metaid in self.remeta.items():
            self.preexisting_entities.add(f"re/{re_metaid[0]}")

    def meta_maker(self):
        """
        For each dictionary ('brdict', 'ardict', 'radict', 'vvi') the corresponding MetaID dictionary is created
        ('brmeta', 'armeta', 'rameta', and 'vvi').
        """
        self._br_wannabe_to_meta: dict[str, str] = {}
        self._ra_wannabe_to_meta: dict[str, str] = {}

        for identifier in self.brdict:
            if "wannabe" in identifier:
                other = identifier
                count = self._add_number("br")
                target_meta = self.prefix + str(count)
                self.brmeta[target_meta] = self.brdict[identifier]
                self.brmeta[target_meta]["others"].add(other)
                self.brmeta[target_meta]["ids"].add("omid:br/" + target_meta)
                self._br_wannabe_to_meta[other] = target_meta
            else:
                target_meta = identifier
                self.brmeta[identifier] = self.brdict[identifier]
                self.brmeta[identifier]["ids"].add("omid:br/" + identifier)
            # Index wannabes in "others" from previous merges
            for existing_other in self.brdict[identifier]["others"]:
                if "wannabe" in existing_other:
                    self._br_wannabe_to_meta[existing_other] = target_meta
        for identifier in self.radict:
            if "wannabe" in identifier:
                other = identifier
                count = self._add_number("ra")
                target_meta = self.prefix + str(count)
                self.rameta[target_meta] = self.radict[identifier]
                self.rameta[target_meta]["others"].add(other)
                self.rameta[target_meta]["ids"].add("omid:ra/" + target_meta)
                self._ra_wannabe_to_meta[other] = target_meta
            else:
                target_meta = identifier
                self.rameta[identifier] = self.radict[identifier]
                self.rameta[identifier]["ids"].add("omid:ra/" + identifier)
            # Index wannabes in "others" from previous merges
            for existing_other in self.radict[identifier]["others"]:
                if "wannabe" in existing_other:
                    self._ra_wannabe_to_meta[existing_other] = target_meta
        for ar_id in self.ardict:
            br_key = ar_id
            if "wannabe" in ar_id:
                br_key = self._br_wannabe_to_meta[ar_id]
            self.armeta[br_key] = dict()
            self.armeta[br_key]["author"] = list()
            self.armeta[br_key]["editor"] = list()
            self.armeta[br_key]["publisher"] = list()
            self.__meta_ar(br_key, ar_id, "author")
            self.__meta_ar(br_key, ar_id, "editor")
            self.__meta_ar(br_key, ar_id, "publisher")
        self.VolIss = dict()
        if self.vvi:
            for venue_meta in self.vvi:
                venue_issue = self.vvi[venue_meta]["issue"]
                if venue_issue:
                    for issue in venue_issue:
                        issue_id = venue_issue[issue]["id"]
                        if "wannabe" in issue_id:
                            self.vvi[venue_meta]["issue"][issue]["id"] = str(
                                self._br_wannabe_to_meta[issue_id]
                            )

                venue_volume = self.vvi[venue_meta]["volume"]
                if venue_volume:
                    for volume in venue_volume:
                        volume_id = venue_volume[volume]["id"]
                        if "wannabe" in volume_id:
                            self.vvi[venue_meta]["volume"][volume]["id"] = str(
                                self._br_wannabe_to_meta[volume_id]
                            )
                        if venue_volume[volume]["issue"]:
                            volume_issue = venue_volume[volume]["issue"]
                            for issue in volume_issue:
                                volume_issue_id = volume_issue[issue]["id"]
                                if "wannabe" in volume_issue_id:
                                    self.vvi[venue_meta]["volume"][volume][
                                        "issue"
                                    ][issue]["id"] = str(
                                        self._br_wannabe_to_meta[volume_issue_id]
                                    )
                if "wannabe" in venue_meta:
                    br_meta = self._br_wannabe_to_meta[venue_meta]
                    self.__merge_VolIss_with_vvi(br_meta, venue_meta)
                else:
                    self.__merge_VolIss_with_vvi(venue_meta, venue_meta)

    def enrich(self, task_id=None):
        """
        This method replaces the wannabeID placeholders with the
        actual data and MetaIDs as a result of the deduplication process.
        """
        for row in self.data:
            metaid = row["id"]
            if "wannabe" in row["id"]:
                metaid = self._br_wannabe_to_meta[row["id"]]
            if row["page"] and (metaid not in self.remeta):
                re_meta = self.finder.retrieve_re_from_br_meta(metaid)
                if re_meta:
                    self.remeta[metaid] = re_meta
                    row["page"] = re_meta[1]
                else:
                    count = self.prefix + str(self._add_number("re"))
                    page = row["page"]
                    self.remeta[metaid] = (count, page)
                    row["page"] = page
            elif metaid in self.remeta:
                row["page"] = self.remeta[metaid][1]
            row["id"] = " ".join(self.brmeta[metaid]["ids"])
            row["title"] = self.brmeta[metaid]["title"]
            venue_metaid = None
            if row["venue"]:
                venue = row["venue"]
                if "wannabe" in venue:
                    venue_metaid = self._br_wannabe_to_meta[venue]
                else:
                    venue_metaid = venue
                row["venue"] = self.build_name_ids_string(
                    self.brmeta[venue_metaid]["title"], self.brmeta[venue_metaid]["ids"]
                )
            br_key_for_editor = get_edited_br_metaid(row, metaid, venue_metaid)
            self.ra_update(row, metaid, "author")
            self.ra_update(row, metaid, "publisher")
            self.ra_update(row, br_key_for_editor, "editor")
            if self.progress and task_id is not None:
                self.progress.advance(task_id)

    @staticmethod
    def name_check(ts_name, name):
        if "," in ts_name:
            names = ts_name.split(",")
            if names[0] and not names[1].strip():
                # there isn't a given name in ts
                if "," in name:
                    gname = name.split(", ")[1]
                    if gname.strip():
                        ts_name = names[0] + ", " + gname
        return ts_name

    def _read_number(self, entity_type: str) -> int:
        return self.counter_handler.read_counter(
            entity_type, supplier_prefix=self.prefix
        )

    def _add_number(self, entity_type: str) -> int:
        return self.counter_handler.increment_counter(
            entity_type, supplier_prefix=self.prefix
        )

    def __update_id_and_entity_dict(
        self,
        existing_ids: list,
        id_dict: dict,
        entity_dict: dict,
        metaval: str,
    ) -> None:
        for identifier in existing_ids:
            if identifier[1] not in id_dict:
                id_dict[identifier[1]] = identifier[0]
            if identifier[1] not in entity_dict[metaval]["ids"]:
                entity_dict[metaval]["ids"].add(identifier[1])

    def indexer(self, path_csv: str | None = None) -> None:
        """
        Transform internal dicts (idra, idbr, armeta, remeta) to list-of-dicts format
        for Creator consumption. Optionally saves the enriched CSV file.

        :params path_csv: Directory path for the enriched CSV output (optional)
        :type path_csv: str
        """
        # ID
        self.index_id_ra = list()
        self.index_id_br = list()
        for entity_type in {"ra", "br"}:
            cur_index = getattr(self, f"id{entity_type}")
            if cur_index:
                for literal in cur_index:
                    row = dict()
                    row["id"] = str(literal)
                    row["meta"] = str(cur_index[literal])
                    getattr(self, f"index_id_{entity_type}").append(row)
            else:
                row = dict()
                row["id"] = ""
                row["meta"] = ""
                getattr(self, f"index_id_{entity_type}").append(row)
        # AR
        self.ar_index = list()
        if self.armeta:
            for metaid in self.armeta:
                index = dict()
                index["meta"] = metaid
                for role in self.armeta[metaid]:
                    list_ar = list()
                    for ar, ra in self.armeta[metaid][role]:
                        list_ar.append(str(ar) + ", " + str(ra))
                    index[role] = "; ".join(list_ar)
                self.ar_index.append(index)
        else:
            row = dict()
            row["meta"] = ""
            row["author"] = ""
            row["editor"] = ""
            row["publisher"] = ""
            self.ar_index.append(row)
        # RE
        self.re_index = list()
        if self.remeta:
            for x in self.remeta:
                r = dict()
                r["br"] = x
                r["re"] = str(self.remeta[x][0])
                self.re_index.append(r)
        else:
            row = dict()
            row["br"] = ""
            row["re"] = ""
            self.re_index.append(row)
        # Save enriched CSV if path provided
        if self.filename and path_csv and self.data:
            name = self.filename + ".csv"
            data_file = os.path.join(path_csv, name)
            write_csv(data_file, self.data)

    def __merge_VolIss_with_vvi(
        self, VolIss_venue_meta: str, vvi_venue_meta: str
    ) -> None:
        if VolIss_venue_meta in self.VolIss:
            for vvi_v in self.vvi[vvi_venue_meta]["volume"]:
                if vvi_v in self.VolIss[VolIss_venue_meta]["volume"]:
                    self.VolIss[VolIss_venue_meta]["volume"][vvi_v]["issue"].update(
                        self.vvi[vvi_venue_meta]["volume"][vvi_v]["issue"]
                    )
                else:
                    self.VolIss[VolIss_venue_meta]["volume"][vvi_v] = self.vvi[
                        vvi_venue_meta
                    ]["volume"][vvi_v]
            self.VolIss[VolIss_venue_meta]["issue"].update(
                self.vvi[vvi_venue_meta]["issue"]
            )
        else:
            self.VolIss[VolIss_venue_meta] = self.vvi[vvi_venue_meta]

    def __update_id_count(self, id_dict, identifier):

        # Prima di creare un nuovo ID, verifichiamo se esiste già nel triplestore
        schema, value = identifier.split(":", maxsplit=1)
        existing_metaid = self.finder.retrieve_metaid_from_id(schema, value)

        if existing_metaid:
            id_dict[identifier] = existing_metaid
        else:
            count = self._add_number("id")
            id_dict[identifier] = self.prefix + str(count)

    @staticmethod
    def merge(
        dict_to_match: dict,
        metaval: str,
        old_meta: str,
        temporary_name: str,
    ) -> None:
        target = dict_to_match[metaval]
        source = dict_to_match[old_meta]
        target["ids"].update(source["ids"])
        target["others"].update(source["others"])
        target["others"].add(old_meta)
        if not target["title"]:
            target["title"] = source["title"] or temporary_name
        del dict_to_match[old_meta]

    def merge_entities_in_csv(
        self,
        idslist: list,
        metaval: str,
        name: str,
        entity_dict: dict,
        id_dict: dict,
    ) -> None:
        found_others = self.__local_match(idslist, entity_dict)
        if found_others["wannabe"]:
            for old_meta in found_others["wannabe"]:
                self.merge(entity_dict, metaval, old_meta, name)
        entry = entity_dict[metaval]
        entry_ids = entry["ids"]
        for identifier in idslist:
            if identifier not in entry_ids:
                entry_ids.add(identifier)
            if identifier not in id_dict:
                self.__update_id_count(id_dict, identifier)
        if not entry["title"] and name:
            entry["title"] = name

    def __update_title(self, entity_dict: dict, metaval: str, name: str) -> None:
        if not entity_dict[metaval]["title"] and name:
            entity_dict[metaval]["title"] = name

    def id_worker(
        self,
        col_name,
        name,
        idslist: List[str],
        metaval: str,
        ra_ent=False,
        br_ent=False,
        vvi_ent=False,
        publ_entity=False,
    ):
        if not ra_ent:
            id_dict = self.idbr
            entity_dict = self.brdict
        else:
            id_dict = self.idra
            entity_dict = self.radict
        # there's meta
        if metaval:
            # MetaID exists among data?
            # meta already in entity_dict (no care about conflicts, we have a meta specified)
            if metaval in entity_dict:
                self.merge_entities_in_csv(idslist, metaval, name, entity_dict, id_dict)
            else:
                found_meta_ts: tuple[str, list[tuple[str, str]], bool] = ("", [], False)
                if ra_ent:
                    found_meta_ts = self.finder.retrieve_ra_from_meta(metaval)
                elif br_ent:
                    found_meta_ts = self.finder.retrieve_br_from_meta(metaval)
                # meta in triplestore
                # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
                if found_meta_ts[2]:
                    title = self.name_check(found_meta_ts[0], name) if col_name in ("author", "editor") else found_meta_ts[0]
                    entity_dict[metaval] = {"ids": set(), "others": set(), "title": title}
                    existing_ids = found_meta_ts[1]
                    self.__update_id_and_entity_dict(
                        existing_ids, id_dict, entity_dict, metaval
                    )
                    self.merge_entities_in_csv(
                        idslist, metaval, name, entity_dict, id_dict
                    )
                # Look for MetaId in the provenance
                else:
                    entity_type = "br" if br_ent or vvi_ent else "ra"
                    metaid_uri = f"{self.base_iri}/{entity_type}/{str(metaval)}"
                    # The entity MetaId after merge if it was merged, empty string otherwise
                    metaval = self.finder.retrieve_metaid_from_merged_entity(
                        metaid_uri=metaid_uri, prov_config=self.prov_config
                    ) or ""
        # there's no meta or there was one but it didn't exist
        # Are there other IDs?
        if idslist and not metaval:
            local_match = self.__local_match(idslist, entity_dict)
            # IDs already exist among data?
            # check in entity_dict
            if local_match["existing"]:
                # ids refer to multiple existing entities
                if len(local_match["existing"]) > 1:
                    # !
                    return self.conflict(idslist, name, id_dict, col_name)
                # ids refer to ONE existing entity
                elif len(local_match["existing"]) == 1:
                    metaval = str(local_match["existing"][0])
                    entry_ids = entity_dict[metaval]["ids"]
                    suspect_ids = [i for i in idslist if i not in entry_ids]
                    if suspect_ids:
                        sparql_match = self.finder_sparql(
                            suspect_ids,
                            br=br_ent,
                            ra=ra_ent,
                            vvi=vvi_ent,
                            publ=publ_entity,
                        )
                        if len(sparql_match) > 1:
                            # !
                            return self.conflict(idslist, name, id_dict, col_name)
            # ids refers to 1 or more wannabe entities
            elif local_match["wannabe"]:
                metaval = str(local_match["wannabe"].pop(0))
                # 5 Merge data from entityA (CSV) with data from EntityX (CSV)
                for old_meta in local_match["wannabe"]:
                    self.merge(entity_dict, metaval, old_meta, name)
                entry_ids = entity_dict[metaval]["ids"]
                suspect_ids = [i for i in idslist if i not in entry_ids]
                if suspect_ids:
                    sparql_match = self.finder_sparql(
                        suspect_ids, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity
                    )
                    if sparql_match:
                        # if 'wannabe' not in metaval or len(sparql_match) > 1:
                        #     # Two entities previously disconnected on the triplestore now become connected
                        #     # !
                        #     return self.conflict(idslist, name, id_dict, col_name)
                        # else:
                        # Collect all existing IDs from all matches
                        existing_ids = []
                        for match in sparql_match:
                            existing_ids.extend(match[2])

                        # new_idslist = [x[1] for x in existing_ids]
                        # new_sparql_match = self.finder_sparql(new_idslist, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
                        # if len(new_sparql_match) > 1:
                        #     # Two entities previously disconnected on the triplestore now become connected
                        #     # !
                        #     return self.conflict(idslist, name, id_dict, col_name)
                        # else:
                        # 4 Merge data from EntityA (CSV) with data from EntityX (CSV) (it has already happened in # 5), update both with data from EntityA (RDF)
                        old_metaval = metaval
                        metaval = sparql_match[0][0]
                        entity_dict[metaval] = {
                            "ids": set(),
                            "others": set(),
                            "title": sparql_match[0][1] or ""
                        }
                        self.__update_id_and_entity_dict(
                            existing_ids, id_dict, entity_dict, metaval
                        )
                        self.merge(
                            entity_dict, metaval, old_metaval, sparql_match[0][1]
                        )
            else:
                sparql_match = self.finder_sparql(
                    idslist, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity
                )
                # if len(sparql_match) > 1:
                #     # !
                #     return self.conflict(idslist, name, id_dict, col_name)
                # elif len(sparql_match) == 1:
                if sparql_match:
                    # Collect all existing IDs from all matches
                    existing_ids = []
                    for match in sparql_match:
                        existing_ids.extend(match[2])

                    # new_idslist = [x[1] for x in existing_ids]
                    # new_sparql_match = self.finder_sparql(new_idslist, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
                    # if len(new_sparql_match) > 1:
                    #     # Two entities previously disconnected on the triplestore now become connected
                    #     # !
                    #     return self.conflict(idslist, name, id_dict, col_name)
                    # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
                    # 3 CONFLICT beteen MetaIDs. MetaID specified in EntityA inside CSV has precedence.
                    # elif len(new_sparql_match) == 1:
                    metaval = sparql_match[0][0]
                    title = self.name_check(sparql_match[0][1], name) if col_name in ("author", "editor") else sparql_match[0][1]
                    entity_dict[metaval] = {"ids": set(), "others": set(), "title": title or name}
                    self.__update_id_and_entity_dict(
                        existing_ids, id_dict, entity_dict, metaval
                    )
                else:
                    # 1 EntityA is a new one
                    metaval = self.new_entity(entity_dict, name)
            entry = entity_dict[metaval]
            entry_ids = entry["ids"]
            for identifier in idslist:
                if identifier not in id_dict:
                    self.__update_id_count(id_dict, identifier)
                if identifier not in entry_ids:
                    entry_ids.add(identifier)
            if not entry["title"] and name:
                entry["title"] = name
        # 1 EntityA is a new one
        if not idslist and not metaval:
            metaval = self.new_entity(entity_dict, name)
        return metaval

    def new_entity(self, entity_dict, name):
        metaval = f"wannabe_{self.wnb_cnt}"
        self.wnb_cnt += 1
        entity_dict[metaval] = {"ids": set(), "others": set(), "title": name}
        return metaval

    def volume_issue(
        self,
        meta: str,
        path: dict,
        value: str,
        row: Dict[str, str],
    ) -> None:
        if "wannabe" not in meta:
            if value in path:
                if "wannabe" in path[value]["id"]:
                    old_meta = path[value]["id"]
                    self.merge(self.brdict, meta, old_meta, row["title"])
                    path[value]["id"] = meta
            else:
                path[value] = {"id": meta}
                if "issue" not in path:
                    path[value]["issue"] = {}
        else:
            if value in path:
                if "wannabe" in path[value]["id"]:
                    old_meta = path[value]["id"]
                    if meta != old_meta:
                        self.merge(self.brdict, meta, old_meta, row["title"])
                        path[value]["id"] = meta
                else:
                    old_meta = path[value]["id"]
                    if "wannabe" not in old_meta and old_meta not in self.brdict:
                        br4dict = self.finder.retrieve_br_from_meta(old_meta)
                        entry = {"ids": set(), "others": set(), "title": br4dict[0] if br4dict else None}
                        self.brdict[old_meta] = entry
                        if br4dict:
                            entry_ids = entry["ids"]
                            for x in br4dict[1]:
                                identifier = x[1]
                                entry_ids.add(identifier)
                                if identifier not in self.idbr:
                                    self.idbr[identifier] = x[0]
                    self.merge(self.brdict, old_meta, meta, row["title"])
            else:
                path[value] = {"id": meta}
                if "issue" not in path:  # it's a Volume
                    path[value]["issue"] = {}

    def merge_duplicate_entities(self, task_id=None) -> None:
        """
        The 'merge_duplicate_entities()' function merge duplicate entities.
        Moreover, it modifies the CSV cells, giving precedence to the first found information
        or data in the triplestore in the case of already existing entities.

        :returns: None -- This method updates the CSV rows and returns None.
        """
        # Build index mapping row IDs to row indices for O(1) lookup
        id_to_indices: dict[str, list[int]] = {}
        for idx, row in enumerate(self.data):
            row_id = row["id"]
            if row_id not in id_to_indices:
                id_to_indices[row_id] = []
            id_to_indices[row_id].append(idx)

        self.rowcnt = 0
        for row in self.data:
            row_id = row["id"]
            if "wannabe" not in row_id:
                self.equalizer(row, row_id)
                # Collect indices of related rows: same ID or in "others"
                related_indices: set[int] = set()
                if row_id in id_to_indices:
                    related_indices.update(id_to_indices[row_id])
                for other_id in self.brdict[row_id]["others"]:
                    if other_id in id_to_indices:
                        related_indices.update(id_to_indices[other_id])
                # Exclude current row: no need to update a row with its own values
                related_indices.discard(self.rowcnt)
                # Update related rows
                for other_idx in related_indices:
                    other_row = self.data[other_idx]
                    for field in row:
                        if row[field] and row[field] != other_row[field]:
                            other_row[field] = row[field]
            if self.progress and task_id is not None:
                self.progress.advance(task_id)
            self.rowcnt += 1

    def extract_name_and_ids(self, venue_str: str) -> Tuple[str, List[str]]:
        """
        Extracts the name and IDs from the venue string.

        :params venue_str: the venue string
        :type venue_str: str
        :returns: Tuple[str, List[str]] -- the name and list of IDs extracted from the venue string
        """
        match = RE_NAME_AND_IDS.search(venue_str)
        if match:
            name = match.group(1).strip()
            ids = match.group(2).strip().split()
        else:
            name = venue_str.strip()
            ids = []
        return name, ids

    def equalizer(self, row: Dict[str, str], metaval: str) -> None:
        """
        Given a CSV row and its MetaID, this function equates the information present in the CSV with that present on the triplestore.

        :params row: a dictionary representing a CSV row
        :type row: Dict[str, str]
        :params metaval: the MetaID identifying the bibliographic resource contained in the input CSV row
        :type metaval: str
        :returns: None -- This method modifies the input CSV row without returning it.
        """
        known_data = self.finder.retrieve_br_info_from_meta(metaval)
        try:
            known_data["author"] = self.__get_resp_agents(metaval, "author")
        except ValueError:
            print(row)
            raise (ValueError)
        known_data["editor"] = self.__get_resp_agents(metaval, "editor")
        known_data["publisher"] = self.finder.retrieve_publisher_from_br_metaid(metaval)
        for datum in ["pub_date", "type", "volume", "issue"]:
            if known_data[datum]:
                row[datum] = known_data[datum]
        for datum in ["author", "editor", "publisher"]:
            if known_data[datum] and not row[datum]:
                row[datum] = known_data[datum]
        if known_data["venue"]:
            current_venue = row["venue"]
            known_venue = known_data["venue"]

            if current_venue:
                # Extract the IDs from the current venue
                current_venue_name, current_venue_ids = self.extract_name_and_ids(
                    current_venue
                )
                known_venue_name, known_venue_ids = self.extract_name_and_ids(
                    known_venue
                )

                current_venue_ids_set = set(current_venue_ids)
                known_venue_ids_set = set(known_venue_ids)

                common_ids = current_venue_ids_set.intersection(known_venue_ids_set)

                if common_ids:
                    # Merge the IDs and use the title from the known venue
                    merged_ids = current_venue_ids_set.union(known_venue_ids_set)
                    row["venue"] = (
                        f"{known_venue_name} [{' '.join(sorted(merged_ids))}]"
                    )
                else:
                    # Use the known venue information entirely
                    row["venue"] = known_venue
            else:
                row["venue"] = known_venue
        if known_data["page"]:
            row["page"] = known_data["page"][1]
            self.remeta[metaval] = known_data["page"]

    def __get_resp_agents(self, metaid: str, column: str) -> str:
        resp_agents = self.finder.retrieve_ra_sequence_from_br_meta(metaid, column)
        output = ""
        if resp_agents:
            full_resp_agents = list()
            for item in resp_agents:
                for _, resp_agent in item.items():
                    author_name = resp_agent[0]
                    ids = [f"omid:ra/{resp_agent[2]}"]
                    ids.extend([id[1] for id in resp_agent[1]])
                    author_ids = "[" + " ".join(ids) + "]"
                    full_resp_agent = author_name + " " + author_ids
                    full_resp_agents.append(full_resp_agent)
            output = "; ".join(full_resp_agents)
        return output


def is_a_valid_row(row: Dict[str, str]) -> bool:
    """
    This method discards invalid rows in the input CSV file.

    :params row: a dictionary representing a CSV row
    :type row: Dict[str, str]
    :returns: bool -- This method returns True if the row is valid, False if it is invalid.
    """
    br_type = " ".join((row["type"].lower()).split())
    br_title = row["title"]
    br_volume = row["volume"]
    br_issue = row["issue"]
    br_venue = row["venue"]
    if row["id"]:
        if (br_volume or br_issue) and (not br_type or not br_venue):
            return False
        return True
    if all(not row[value] for value in row):
        return False
    br_author = row["author"]
    br_editor = row["editor"]
    br_pub_date = row["pub_date"]
    if not br_type or br_type in {
        "book",
        "data file",
        "dataset",
        "dissertation",
        "edited book",
        "journal article",
        "monograph",
        "other",
        "peer review",
        "posted content",
        "web content",
        "proceedings article",
        "report",
        "reference book",
    }:
        is_a_valid_row = (
            True if br_title and br_pub_date and (br_author or br_editor) else False
        )
    elif br_type in {
        "book chapter",
        "book part",
        "book section",
        "book track",
        "component",
        "reference entry",
    }:
        is_a_valid_row = True if br_title and br_venue else False
    elif br_type in {
        "book series",
        "book set",
        "journal",
        "proceedings",
        "proceedings series",
        "report series",
        "standard",
        "standard series",
    }:
        is_a_valid_row = True if br_title else False
    elif br_type == "journal volume":
        is_a_valid_row = True if br_venue and (br_volume or br_title) else False
    elif br_type == "journal issue":
        is_a_valid_row = True if br_venue and (br_issue or br_title) else False
    else:
        is_a_valid_row = False
    return is_a_valid_row


def get_edited_br_metaid(row: dict, metaid: str, venue_metaid: str | None) -> str:
    if row["author"] and row["venue"] and row["type"] in CONTAINER_EDITOR_TYPES and venue_metaid:
        return venue_metaid
    return metaid
