# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import csv
import os
import re
import time
import unicodedata
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, cast

import orjson
import redis
import requests
import yaml
from oc_ocdm.graph import GraphSet
from rich_argparse import RichHelpFormatter

from oc_ds_converter.crossref.crossref_processing import CrossrefProcessing
from oc_ds_converter.datacite.datacite_processing import DataciteProcessing
from oc_ds_converter.pubmed.pubmed_processing import PubmedProcessing
from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import (
    InMemoryStorageManager,
)
from oc_ds_converter.ra_processor import RaProcessor

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.console import create_progress
from oc_meta.lib.file_manager import find_rdf_file
from oc_meta.run.meta.generate_csv import URI_TYPE_DICT, load_json_from_file

HAS_IDENTIFIER = "http://purl.org/spar/datacite/hasIdentifier"
USES_ID_SCHEME = "http://purl.org/spar/datacite/usesIdentifierScheme"
HAS_LITERAL_VALUE = (
    "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"
)
IS_DOC_CONTEXT_FOR = "http://purl.org/spar/pro/isDocumentContextFor"
WITH_ROLE = "http://purl.org/spar/pro/withRole"
IS_HELD_BY = "http://purl.org/spar/pro/isHeldBy"
HAS_NEXT = "https://w3id.org/oc/ontology/hasNext"
FAMILY_NAME = "http://xmlns.com/foaf/0.1/familyName"
GIVEN_NAME = "http://xmlns.com/foaf/0.1/givenName"
FOAF_NAME = "http://xmlns.com/foaf/0.1/name"
DC_TITLE = "http://purl.org/dc/terms/title"
FABIO_EXPRESSION = "http://purl.org/spar/fabio/Expression"

ROLE_MAP = {
    "http://purl.org/spar/pro/author": "author",
    "http://purl.org/spar/pro/editor": "editor",
    "http://purl.org/spar/pro/publisher": "publisher",
}

CSV_COLUMNS = [
    "id",
    "title",
    "author",
    "pub_date",
    "venue",
    "volume",
    "issue",
    "page",
    "type",
    "publisher",
    "editor",
]

CROSSREF_BASE = "https://api.crossref.org/works/"
DATACITE_BASE = "https://api.datacite.org/dois/"
PUBMED_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

SESSION = requests.Session()


class RedisOrcidIndex:
    def __init__(self, host: str, port: int, db: int) -> None:
        self._r = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    @staticmethod
    def _key(doi: str) -> str:
        return doi if doi.startswith("doi:") else f"doi:{doi}"

    def get_value(self, doi: str) -> Optional[set[str]]:
        members = cast("set[str]", self._r.smembers(self._key(doi)))
        return members or None

    def get_values_batch(self, dois: List[str]) -> Dict[str, set[str]]:
        if not dois:
            return {}
        pipe = self._r.pipeline()
        for doi in dois:
            pipe.smembers(self._key(doi))
        results = cast("list[set[str]]", pipe.execute())
        return {doi: members for doi, members in zip(dois, results) if members}


_SHARED_STORAGE: Optional[InMemoryStorageManager] = None
_ORCID_INDEX: Optional[RedisOrcidIndex] = None
_PROCESSORS: Dict[str, RaProcessor] = {}


def _shared_storage() -> InMemoryStorageManager:
    global _SHARED_STORAGE
    if _SHARED_STORAGE is None:
        _SHARED_STORAGE = InMemoryStorageManager()
    return _SHARED_STORAGE


def setup_orcid_index(host: str, port: int, db: int) -> None:
    global _ORCID_INDEX
    _ORCID_INDEX = RedisOrcidIndex(host, port, db)


def get_processor(source: str) -> RaProcessor:
    if source not in _PROCESSORS:
        index = cast("str", _ORCID_INDEX)
        if source == "crossref":
            _PROCESSORS[source] = CrossrefProcessing(
                orcid_index=index,
                storage_manager=_shared_storage(),
                testing=False,
                use_orcid_api=False,
                use_redis_orcid_index=False,
                use_redis_publishers=False,
            )
        elif source == "datacite":
            _PROCESSORS[source] = DataciteProcessing(
                orcid_index=index,
                storage_manager=_shared_storage(),
                testing=False,
                use_orcid_api=False,
                use_ror_api=False,
                use_viaf_api=False,
                use_wikidata_api=False,
            )
        elif source == "pubmed":
            _PROCESSORS[source] = PubmedProcessing(orcid_index=index)
    return _PROCESSORS[source]


def get_supplier_prefix(uri: str) -> str | None:
    match = re.match(r"^(.+)/([a-z][a-z])/(0[1-9]+0)?([1-9][0-9]*)$", uri)
    if match is None:
        return None
    return match.group(3)


def extract_omid_number(uri: str) -> int:
    return int(uri.split("/")[-1])


def normalize_name(name: str) -> str:
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def normalize_orcid(orcid: str) -> str:
    if not orcid:
        return ""
    return (
        orcid.replace("https://orcid.org/", "")
        .replace("http://orcid.org/", "")
        .strip()
        .upper()
    )


def find_entity_in_file(
    uri: str, rdf_dir: str, dir_split: int, items_per_file: int
) -> Optional[dict]:
    filepath = find_rdf_file(uri, rdf_dir, dir_split, items_per_file, zip_output=True)
    if not os.path.exists(filepath):
        return None
    data = load_json_from_file(filepath)
    for graph in data:
        for entity in graph["@graph"]:
            if entity["@id"] == uri:
                return entity
    return None


def load_br_identifiers(
    br_uri: str, rdf_dir: str, dir_split: int, items_per_file: int
) -> Dict[str, str]:
    br_entity = find_entity_in_file(br_uri, rdf_dir, dir_split, items_per_file)
    if not br_entity or HAS_IDENTIFIER not in br_entity:
        return {}
    result = {}
    for id_ref in br_entity[HAS_IDENTIFIER]:
        id_entity = find_entity_in_file(
            id_ref["@id"], rdf_dir, dir_split, items_per_file
        )
        if not id_entity:
            continue
        if USES_ID_SCHEME not in id_entity or HAS_LITERAL_VALUE not in id_entity:
            continue
        scheme = id_entity[USES_ID_SCHEME][0]["@id"].split("/datacite/")[1]
        value = id_entity[HAS_LITERAL_VALUE][0]["@value"]
        result[scheme] = value
    return result


def load_br_core(
    br_uri: str, rdf_dir: str, dir_split: int, items_per_file: int
) -> Dict[str, str]:
    br_entity = find_entity_in_file(br_uri, rdf_dir, dir_split, items_per_file)
    if not br_entity:
        return {"type": "", "title": ""}
    title = ""
    if DC_TITLE in br_entity:
        title = br_entity[DC_TITLE][0]["@value"]
    br_type = ""
    if "@type" in br_entity:
        for t in br_entity["@type"]:
            if t == FABIO_EXPRESSION:
                continue
            mapped = URI_TYPE_DICT.get(t, "")
            if mapped:
                br_type = mapped
                break
    return {"type": br_type, "title": title}


def load_ra_info(
    ra_uri: str, rdf_dir: str, dir_split: int, items_per_file: int
) -> dict:
    ra_entity = find_entity_in_file(ra_uri, rdf_dir, dir_split, items_per_file)
    if not ra_entity:
        return {"family_name": None, "given_name": None, "name": None, "orcid": None}
    family = None
    given = None
    name = None
    orcid = None
    if FAMILY_NAME in ra_entity:
        family = ra_entity[FAMILY_NAME][0]["@value"]
    if GIVEN_NAME in ra_entity:
        given = ra_entity[GIVEN_NAME][0]["@value"]
    if FOAF_NAME in ra_entity:
        name = ra_entity[FOAF_NAME][0]["@value"]
    if HAS_IDENTIFIER in ra_entity:
        for id_ref in ra_entity[HAS_IDENTIFIER]:
            id_entity = find_entity_in_file(
                id_ref["@id"], rdf_dir, dir_split, items_per_file
            )
            if not id_entity:
                continue
            if USES_ID_SCHEME not in id_entity or HAS_LITERAL_VALUE not in id_entity:
                continue
            scheme = id_entity[USES_ID_SCHEME][0]["@id"].split("/datacite/")[1]
            if scheme == "orcid":
                orcid = id_entity[HAS_LITERAL_VALUE][0]["@value"]
                break
    return {"family_name": family, "given_name": given, "name": name, "orcid": orcid}


def load_all_ars_for_br_role(
    br_uri: str, role_type: str, rdf_dir: str, dir_split: int, items_per_file: int
) -> List[dict]:
    br_entity = find_entity_in_file(br_uri, rdf_dir, dir_split, items_per_file)
    if not br_entity or IS_DOC_CONTEXT_FOR not in br_entity:
        return []
    result = []
    for ar_ref in br_entity[IS_DOC_CONTEXT_FOR]:
        ar_uri = ar_ref["@id"]
        ar_entity = find_entity_in_file(ar_uri, rdf_dir, dir_split, items_per_file)
        if not ar_entity or WITH_ROLE not in ar_entity:
            continue
        role_uri = ar_entity[WITH_ROLE][0]["@id"]
        ar_role = ROLE_MAP.get(role_uri, "unknown")
        if ar_role != role_type:
            continue
        ra_uri = None
        if IS_HELD_BY in ar_entity:
            ra_uri = ar_entity[IS_HELD_BY][0]["@id"]
        has_next = []
        if HAS_NEXT in ar_entity:
            has_next = [item["@id"] for item in ar_entity[HAS_NEXT]]
        ra_info = {"family_name": None, "given_name": None, "name": None, "orcid": None}
        if ra_uri:
            ra_info = load_ra_info(ra_uri, rdf_dir, dir_split, items_per_file)
        ra_name = ""
        if ra_info["family_name"] or ra_info["given_name"]:
            parts = [ra_info["family_name"] or "", ra_info["given_name"] or ""]
            ra_name = f"{parts[0]}, {parts[1]}"
        elif ra_info["name"]:
            ra_name = ra_info["name"]
        result.append(
            {
                "ar": ar_uri,
                "ra": ra_uri,
                "ra_name": ra_name,
                "ra_family": ra_info["family_name"],
                "ra_given": ra_info["given_name"],
                "ra_orcid": ra_info["orcid"],
                "has_next": has_next,
            }
        )
    return result


def _strip_orcid_url(orcid: str) -> str:
    if not orcid:
        return orcid
    return orcid.replace("https://orcid.org/", "").replace("http://orcid.org/", "")


def fetch_crossref(doi: str) -> Optional[dict]:
    resp = SESSION.get(CROSSREF_BASE + doi, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    msg = resp.json()["message"]
    authors = []
    for i, a in enumerate(msg.get("author", [])):
        authors.append(
            {
                "family": a.get("family", ""),
                "given": a.get("given", ""),
                "name": a.get("name", ""),
                "orcid": _strip_orcid_url(a.get("ORCID")),
                "position": i,
            }
        )
    editors = []
    for i, e in enumerate(msg.get("editor", [])):
        editors.append(
            {
                "family": e.get("family", ""),
                "given": e.get("given", ""),
                "name": e.get("name", ""),
                "orcid": _strip_orcid_url(e.get("ORCID")),
                "position": i,
            }
        )
    return {
        "author": authors,
        "editor": editors,
        "publisher": msg.get("publisher", ""),
        "publisher_crossref_id": msg.get("member"),
        "source": "crossref",
    }


def fetch_datacite(doi: str) -> Optional[dict]:
    resp = SESSION.get(DATACITE_BASE + doi, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    attrs = resp.json()["data"]["attributes"]
    authors = []
    for i, c in enumerate(attrs.get("creators", [])):
        orcid = None
        for ni in c.get("nameIdentifiers", []):
            if ni.get("nameIdentifierScheme", "").upper() == "ORCID":
                orcid = _strip_orcid_url(ni["nameIdentifier"])
                break
        authors.append(
            {
                "family": c.get("familyName", ""),
                "given": c.get("givenName", ""),
                "name": c.get("name", ""),
                "orcid": orcid,
                "position": i,
            }
        )
    editors = []
    editor_idx = 0
    for c in attrs.get("contributors", []):
        if c.get("contributorType") != "Editor":
            continue
        orcid = None
        for ni in c.get("nameIdentifiers", []):
            if ni.get("nameIdentifierScheme", "").upper() == "ORCID":
                orcid = _strip_orcid_url(ni["nameIdentifier"])
                break
        editors.append(
            {
                "family": c.get("familyName", ""),
                "given": c.get("givenName", ""),
                "name": c.get("name", ""),
                "orcid": orcid,
                "position": editor_idx,
            }
        )
        editor_idx += 1
    publisher = attrs.get("publisher", "")
    if isinstance(publisher, dict):
        publisher = publisher.get("name", "")
    return {
        "author": authors,
        "editor": editors,
        "publisher": publisher,
        "publisher_crossref_id": None,
        "source": "datacite",
    }


def fetch_pubmed(pmid: str) -> Optional[dict]:
    params = {"db": "pubmed", "id": pmid, "rettype": "xml", "retmode": "xml"}
    resp = SESSION.get(PUBMED_BASE, params=params, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    article = root.find(".//Article")
    if article is None:
        return None
    authors = []
    author_list = article.find("AuthorList")
    if author_list is not None:
        for i, author_elem in enumerate(author_list.findall("Author")):
            orcid = None
            for ident in author_elem.findall("Identifier"):
                if ident.get("Source") == "ORCID" and ident.text is not None:
                    orcid = _strip_orcid_url(ident.text)
                    break
            authors.append(
                {
                    "family": author_elem.findtext("LastName", ""),
                    "given": author_elem.findtext("ForeName", ""),
                    "name": author_elem.findtext("CollectiveName", ""),
                    "orcid": orcid,
                    "position": i,
                }
            )
    return {
        "author": authors,
        "editor": [],
        "publisher": "",
        "publisher_crossref_id": None,
        "source": "pubmed",
    }


def fetch_api_data(identifiers: Dict[str, str]) -> Tuple[Optional[dict], str]:
    if "doi" in identifiers:
        doi = identifiers["doi"]
        try:
            result = fetch_crossref(doi)
            if result:
                return result, f"doi:{doi}"
        except requests.RequestException:
            pass
        try:
            result = fetch_datacite(doi)
            if result:
                return result, f"doi:{doi}"
        except requests.RequestException:
            pass
    if "pmid" in identifiers:
        pmid = identifiers["pmid"]
        try:
            time.sleep(0.34)
            result = fetch_pubmed(pmid)
            if result:
                return result, f"pmid:{pmid}"
        except requests.RequestException:
            pass
    return None, ""


def match_ars_to_api(ar_infos: List[dict], api_entries: List[dict]) -> List[str]:
    if not api_entries:
        return []
    orcid_to_pos = {}
    for entry in api_entries:
        if entry["orcid"]:
            orcid_to_pos[normalize_orcid(entry["orcid"])] = entry["position"]
    name_to_positions = defaultdict(list)
    for entry in api_entries:
        if entry["family"]:
            name_to_positions[normalize_name(entry["family"])].append(entry["position"])
    ar_to_position: Dict[str, int] = {}
    used_positions: set = set()
    sorted_ars = sorted(ar_infos, key=lambda a: extract_omid_number(a["ar"]))
    for ar in sorted_ars:
        if ar["ra_orcid"]:
            norm = normalize_orcid(ar["ra_orcid"])
            if norm in orcid_to_pos:
                pos = orcid_to_pos[norm]
                if pos not in used_positions:
                    ar_to_position[ar["ar"]] = pos
                    used_positions.add(pos)
    for ar in sorted_ars:
        if ar["ar"] in ar_to_position:
            continue
        if ar["ra_family"]:
            norm = normalize_name(ar["ra_family"])
            if norm in name_to_positions:
                for pos in name_to_positions[norm]:
                    if pos not in used_positions:
                        ar_to_position[ar["ar"]] = pos
                        used_positions.add(pos)
                        break
    ordered = sorted(ar_to_position.items(), key=lambda x: x[1])
    return [uri for uri, _ in ordered]


def match_publisher_ars(ar_infos: List[dict], api_publisher: str) -> List[str]:
    if not api_publisher:
        return []
    norm_api = normalize_name(api_publisher)
    for ar in sorted(ar_infos, key=lambda a: extract_omid_number(a["ar"])):
        if ar["ra_name"] and normalize_name(ar["ra_name"]) == norm_api:
            return [ar["ar"]]
    return []


def _agents_list(api_data: dict) -> List[dict]:
    agents: List[dict] = []
    for role in ("author", "editor"):
        for entry in api_data[role]:
            agents.append(
                {
                    "family": entry["family"],
                    "given": entry["given"],
                    "name": entry["name"],
                    "orcid": entry["orcid"],
                    "role": role,
                }
            )
    return agents


def build_csv_row(
    identifier: str, role_type: str, api_data: dict, br_core: Dict[str, str]
) -> dict:
    row = {"id": identifier, "type": br_core["type"], "title": br_core["title"]}
    if role_type in ("author", "editor"):
        bare_id = identifier.split(":", 1)[1] if ":" in identifier else identifier
        processor = get_processor(api_data["source"])
        if api_data["source"] == "crossref":
            processor.prefetch_doi_orcid_index([bare_id])  # type: ignore[attr-defined]
        authors, editors = processor.get_agents_strings_list(
            bare_id, _agents_list(api_data)
        )
        row[role_type] = "; ".join(authors if role_type == "author" else editors)
    elif role_type == "publisher":
        publisher_name = api_data["publisher"]
        crossref_id = api_data["publisher_crossref_id"]
        if crossref_id:
            row["publisher"] = f"{publisher_name} [crossref:{crossref_id}]"
        else:
            row["publisher"] = publisher_name
    return row


def generate_csv(corrections: List[dict], output_path: str) -> None:
    ready = [c for c in corrections if c["status"] == "ready"]
    grouped: Dict[Tuple[str, str], dict] = {}
    for c in ready:
        key = (c["br"], c["identifier"])
        if key not in grouped:
            grouped[key] = {col: "" for col in CSV_COLUMNS}
            grouped[key]["id"] = c["identifier"]
        for col in CSV_COLUMNS:
            value = c["csv_row"].get(col, "")
            if value:
                grouped[key][col] = value

    output_dir = os.path.dirname(os.path.abspath(output_path))
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in grouped.values():
            writer.writerow(row)

    print(f"CSV for Meta saved to {output_path} ({len(grouped)} rows)")


def _ar_summary(ar: dict) -> dict:
    return {
        "ar": ar["ar"],
        "ra": ar["ra"],
        "ra_name": ar["ra_name"],
        "has_next": ar["has_next"],
    }


def dry_run(
    config_path: str,
    anomaly_path: str,
    output_path: str,
    csv_output: Optional[str],
    orcid_redis_host: str,
    orcid_redis_port: int,
    orcid_redis_db: int,
) -> None:
    setup_orcid_index(orcid_redis_host, orcid_redis_port, orcid_redis_db)
    with open(config_path, encoding="utf-8") as f:
        settings = yaml.safe_load(f)
    rdf_dir = os.path.join(settings["output_rdf_dir"], "rdf")
    dir_split = settings["dir_split_number"]
    items_per_file = settings["items_per_file"]

    with open(anomaly_path, "rb") as f:
        report = orjson.loads(f.read())

    groups: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for anomaly in report["anomalies"]:
        key = (anomaly["br"], anomaly["role_type"])
        groups[key].append(anomaly)

    corrections = []
    summary = {
        "total_brs": len(groups),
        "api_resolved": 0,
        "no_identifiers": 0,
        "api_error": 0,
        "manual_review_needed": 0,
    }

    with create_progress() as progress:
        task = progress.add_task("Analyzing anomalies", total=len(groups))
        for (br_uri, role_type), anomalies in groups.items():
            anomaly_types = list({a["anomaly_type"] for a in anomalies})
            ar_infos = load_all_ars_for_br_role(
                br_uri, role_type, rdf_dir, dir_split, items_per_file
            )
            ar_summaries = [_ar_summary(ar) for ar in ar_infos]
            all_ar_uris = [ar["ar"] for ar in ar_infos]

            if role_type == "unknown":
                summary["manual_review_needed"] += 1
                corrections.append(
                    {
                        "br": br_uri,
                        "role_type": role_type,
                        "anomalies": anomaly_types,
                        "source": None,
                        "identifier": None,
                        "current_ars": ar_summaries,
                        "csv_row": {},
                        "delete_ars": [],
                        "operations": [],
                        "status": "manual_review",
                    }
                )
                progress.update(task, advance=1)
                continue

            identifiers = load_br_identifiers(
                br_uri, rdf_dir, dir_split, items_per_file
            )

            if not identifiers:
                summary["no_identifiers"] += 1
                corrections.append(
                    {
                        "br": br_uri,
                        "role_type": role_type,
                        "anomalies": anomaly_types,
                        "source": None,
                        "identifier": None,
                        "current_ars": ar_summaries,
                        "csv_row": {},
                        "delete_ars": [],
                        "operations": [],
                        "status": "no_identifiers",
                    }
                )
                progress.update(task, advance=1)
                continue

            api_data, identifier_str = fetch_api_data(identifiers)

            if not api_data:
                summary["api_error"] += 1
                id_str = next((f"{k}:{v}" for k, v in identifiers.items()), None)
                corrections.append(
                    {
                        "br": br_uri,
                        "role_type": role_type,
                        "anomalies": anomaly_types,
                        "source": None,
                        "identifier": id_str,
                        "current_ars": ar_summaries,
                        "csv_row": {},
                        "delete_ars": [],
                        "operations": [],
                        "status": "api_error",
                    }
                )
                progress.update(task, advance=1)
                continue

            extra_fields = {}
            if role_type in ("author", "editor"):
                api_entries = api_data[role_type]
                extra_fields[f"api_{role_type}s"] = api_entries
                api_matched = match_ars_to_api(ar_infos, api_entries)
                extra_fields["api_matched_ars"] = api_matched
                has_api_data = len(api_entries) > 0
            else:
                api_publisher = api_data["publisher"]
                extra_fields["api_publisher"] = api_publisher
                api_matched = match_publisher_ars(ar_infos, api_publisher)
                extra_fields["api_matched_ars"] = api_matched
                has_api_data = bool(api_publisher)

            if has_api_data:
                status = "ready"
                summary["api_resolved"] += 1
            else:
                status = "manual_review"
                summary["manual_review_needed"] += 1

            operations = []
            if status == "ready":
                for ar_uri in all_ar_uris:
                    operations.append({"action": "remove_next", "ar": ar_uri})
                for ar_uri in all_ar_uris:
                    operations.append(
                        {"action": "delete_ar", "ar": ar_uri, "br": br_uri}
                    )

            if status == "ready":
                br_core = load_br_core(br_uri, rdf_dir, dir_split, items_per_file)
                csv_row = build_csv_row(identifier_str, role_type, api_data, br_core)
            else:
                csv_row = {}

            correction = {
                "br": br_uri,
                "role_type": role_type,
                "anomalies": anomaly_types,
                "source": api_data["source"],
                "identifier": identifier_str,
                **extra_fields,
                "current_ars": ar_summaries,
                "csv_row": csv_row,
                "delete_ars": all_ar_uris if status == "ready" else [],
                "operations": operations,
                "status": status,
            }
            corrections.append(correction)
            progress.update(task, advance=1)

    plan = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "config": os.path.abspath(config_path),
        "summary": summary,
        "corrections": corrections,
    }

    output_dir = os.path.dirname(os.path.abspath(output_path))
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(orjson.dumps(plan, option=orjson.OPT_INDENT_2))

    print(f"Correction plan saved to {output_path}")
    print(f"  Total groups: {summary['total_brs']}")
    print(f"  API resolved: {summary['api_resolved']}")
    print(f"  No identifiers: {summary['no_identifiers']}")
    print(f"  API errors: {summary['api_error']}")
    print(f"  Manual review: {summary['manual_review_needed']}")

    if csv_output:
        generate_csv(corrections, csv_output)


def apply_corrections_for_br(
    editor: MetaEditor, br_uri: str, corrections: List[dict]
) -> None:
    supplier_prefix = get_supplier_prefix(br_uri)
    assert supplier_prefix is not None
    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=supplier_prefix,
        custom_counter_handler=editor.counter_handler,
        wanted_label=False,
    )
    entities_to_import: list[str] = [br_uri]
    for correction in corrections:
        for ar_info in correction["current_ars"]:
            entities_to_import.append(ar_info["ar"])
    editor.reader.import_entities_from_triplestore(
        g_set=g_set,
        ts_url=editor.endpoint,
        entities=list(dict.fromkeys(entities_to_import)),
        resp_agent=editor.resp_agent,
        enable_validation=False,
        batch_size=10,
    )
    br_entity = g_set.get_entity(br_uri)
    assert br_entity is not None
    delete_ars = [ar for c in corrections for ar in c["delete_ars"]]
    for ar_uri in dict.fromkeys(delete_ars):
        ar_entity = g_set.get_entity(ar_uri)
        assert ar_entity is not None
        ar_entity.remove_next()  # type: ignore[attr-defined]
        br_entity.remove_contributor(ar_entity)  # type: ignore[attr-defined]
        ar_entity.mark_as_to_be_deleted()
    editor.save(g_set, supplier_prefix)


def execute(config_path: str, plan_path: str, resp_agent: str) -> None:
    with open(plan_path, "rb") as f:
        plan = orjson.loads(f.read())

    editor = MetaEditor(config_path, resp_agent)
    ready_corrections = [c for c in plan["corrections"] if c["status"] == "ready"]

    corrections_by_br: Dict[str, List[dict]] = defaultdict(list)
    for correction in ready_corrections:
        corrections_by_br[correction["br"]].append(correction)

    print(
        f"Executing {len(ready_corrections)} corrections"
        f" across {len(corrections_by_br)} bibliographic resources..."
    )

    succeeded = 0
    failed = 0
    with create_progress() as progress:
        task = progress.add_task("Applying corrections", total=len(corrections_by_br))
        for br_uri, corrections in corrections_by_br.items():
            try:
                apply_corrections_for_br(editor, br_uri, corrections)
                succeeded += 1
            except Exception as e:
                print(f"  Error fixing {br_uri}: {e}")
                failed += 1
            progress.update(task, advance=1)

    print(f"Execution complete: {succeeded} succeeded, {failed} failed")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix hasNext chain anomalies in RDF data",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Meta config YAML file path"
    )
    parser.add_argument(
        "-a", "--anomalies", help="Anomaly report JSON file path (for dry run)"
    )
    parser.add_argument(
        "-o", "--output", help="Output correction plan JSON path (for dry run)"
    )
    parser.add_argument(
        "--csv-output", help="Output CSV path for Meta input (for dry run)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate correction plan without applying",
    )
    parser.add_argument(
        "--execute", metavar="PLAN", help="Execute corrections from a reviewed plan"
    )
    parser.add_argument(
        "-r", "--resp-agent", help="Responsible agent URI (for execute mode)"
    )
    parser.add_argument(
        "--mailto",
        required=True,
        help="Email for the Crossref / DataCite polite pool User-Agent",
    )
    parser.add_argument(
        "--orcid-redis-host",
        default="localhost",
        help="Host of the Redis holding the DOI->ORCID index (default: localhost)",
    )
    parser.add_argument(
        "--orcid-redis-port",
        type=int,
        default=6991,
        help="Port of the DOI->ORCID index Redis (default: 6991)",
    )
    parser.add_argument(
        "--orcid-redis-db",
        type=int,
        default=14,
        help="Redis db number of the DOI->ORCID index (default: 14)",
    )
    args = parser.parse_args()

    SESSION.headers.update({"User-Agent": f"oc_meta_fixer/1.0 (mailto:{args.mailto})"})

    if args.dry_run:
        if not args.anomalies or not args.output:
            parser.error("--dry-run requires -a/--anomalies and -o/--output")
        dry_run(
            args.config,
            args.anomalies,
            args.output,
            args.csv_output,
            args.orcid_redis_host,
            args.orcid_redis_port,
            args.orcid_redis_db,
        )
    elif args.execute:
        if not args.resp_agent:
            parser.error("--execute requires -r/--resp-agent")
        execute(args.config, args.execute, args.resp_agent)
    else:
        parser.error("Specify either --dry-run or --execute")


if __name__ == "__main__":
    main()
