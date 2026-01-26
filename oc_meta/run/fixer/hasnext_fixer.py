from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
import unicodedata
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
import yaml
from oc_ocdm.graph import GraphSet
from rdflib import URIRef
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from oc_meta.plugins.csv_generator_lite.csv_generator_lite import (
    find_file,
    load_json_from_file,
)
from oc_meta.plugins.editor import MetaEditor

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

ROLE_MAP = {
    "http://purl.org/spar/pro/author": "author",
    "http://purl.org/spar/pro/editor": "editor",
    "http://purl.org/spar/pro/publisher": "publisher",
}

CSV_COLUMNS = [
    "id", "title", "author", "pub_date", "venue",
    "volume", "issue", "page", "type", "publisher", "editor",
]

CROSSREF_BASE = "https://api.crossref.org/works/"
DATACITE_BASE = "https://api.datacite.org/dois/"
PUBMED_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

SESSION = requests.Session()
SESSION.headers.update(
    {"User-Agent": "oc_meta_fixer/1.0 (mailto:arcangelo.massari@unibo.it)"}
)


def get_supplier_prefix(uri: str) -> str:
    match = re.match(r"^(.+)/([a-z][a-z])/(0[1-9]+0)?([1-9][0-9]*)$", uri)
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
    filepath = find_file(rdf_dir, dir_split, items_per_file, uri)
    if not filepath:
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
        result.append({
            "ar": ar_uri,
            "ra": ra_uri,
            "ra_name": ra_name,
            "ra_family": ra_info["family_name"],
            "ra_given": ra_info["given_name"],
            "ra_orcid": ra_info["orcid"],
            "has_next": has_next,
        })
    return result


def _strip_orcid_url(orcid: str) -> str:
    if not orcid:
        return orcid
    return (
        orcid.replace("https://orcid.org/", "")
        .replace("http://orcid.org/", "")
    )


def fetch_crossref(doi: str) -> Optional[dict]:
    resp = SESSION.get(CROSSREF_BASE + doi, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    msg = resp.json()["message"]
    authors = []
    for i, a in enumerate(msg.get("author", [])):
        authors.append({
            "family": a.get("family", ""),
            "given": a.get("given", ""),
            "orcid": _strip_orcid_url(a.get("ORCID")),
            "position": i,
        })
    editors = []
    for i, e in enumerate(msg.get("editor", [])):
        editors.append({
            "family": e.get("family", ""),
            "given": e.get("given", ""),
            "orcid": _strip_orcid_url(e.get("ORCID")),
            "position": i,
        })
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
        authors.append({
            "family": c.get("familyName", ""),
            "given": c.get("givenName", ""),
            "orcid": orcid,
            "position": i,
        })
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
        editors.append({
            "family": c.get("familyName", ""),
            "given": c.get("givenName", ""),
            "orcid": orcid,
            "position": editor_idx,
        })
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
                if ident.get("Source") == "ORCID":
                    orcid = _strip_orcid_url(ident.text)
                    break
            authors.append({
                "family": author_elem.findtext("LastName", ""),
                "given": author_elem.findtext("ForeName", ""),
                "orcid": orcid,
                "position": i,
            })
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


def match_ars_to_api(
    ar_infos: List[dict], api_entries: List[dict]
) -> List[str]:
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


def match_publisher_ars(
    ar_infos: List[dict], api_publisher: str
) -> List[str]:
    if not api_publisher:
        return []
    norm_api = normalize_name(api_publisher)
    for ar in sorted(ar_infos, key=lambda a: extract_omid_number(a["ar"])):
        if ar["ra_name"] and normalize_name(ar["ra_name"]) == norm_api:
            return [ar["ar"]]
    return []


def format_person_for_csv(entry: dict) -> str:
    family = entry["family"]
    given = entry["given"]
    if family and given:
        name_str = f"{family}, {given}"
    elif family:
        name_str = family
    elif given:
        name_str = given
    else:
        name_str = ""
    if entry["orcid"]:
        name_str += f" [orcid:{entry['orcid']}]"
    return name_str


def build_csv_row(identifier: str, role_type: str, api_data: dict) -> dict:
    row = {"id": identifier}
    if role_type == "author":
        row["author"] = "; ".join(
            format_person_for_csv(e) for e in api_data["author"]
        )
    elif role_type == "editor":
        row["editor"] = "; ".join(
            format_person_for_csv(e) for e in api_data["editor"]
        )
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
    config_path: str, anomaly_path: str, output_path: str, csv_output: Optional[str]
) -> None:
    with open(config_path, encoding="utf-8") as f:
        settings = yaml.safe_load(f)
    rdf_dir = os.path.join(settings["output_rdf_dir"], "rdf")
    dir_split = settings["dir_split_number"]
    items_per_file = settings["items_per_file"]

    with open(anomaly_path, encoding="utf-8") as f:
        report = json.load(f)

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

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
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
                corrections.append({
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
                })
                progress.update(task, advance=1)
                continue

            identifiers = load_br_identifiers(
                br_uri, rdf_dir, dir_split, items_per_file
            )

            if not identifiers:
                summary["no_identifiers"] += 1
                corrections.append({
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
                })
                progress.update(task, advance=1)
                continue

            api_data, identifier_str = fetch_api_data(identifiers)

            if not api_data:
                summary["api_error"] += 1
                id_str = next(
                    (f"{k}:{v}" for k, v in identifiers.items()), None
                )
                corrections.append({
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
                })
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

            csv_row = (
                build_csv_row(identifier_str, role_type, api_data)
                if status == "ready"
                else {}
            )

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
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, ensure_ascii=False)

    print(f"Correction plan saved to {output_path}")
    print(f"  Total groups: {summary['total_brs']}")
    print(f"  API resolved: {summary['api_resolved']}")
    print(f"  No identifiers: {summary['no_identifiers']}")
    print(f"  API errors: {summary['api_error']}")
    print(f"  Manual review: {summary['manual_review_needed']}")

    if csv_output:
        generate_csv(corrections, csv_output)


def apply_correction(editor: MetaEditor, correction: dict) -> None:
    br_uri = correction["br"]
    supplier_prefix = get_supplier_prefix(br_uri)
    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=supplier_prefix,
        custom_counter_handler=editor.counter_handler,
    )
    entities_to_import = [URIRef(br_uri)]
    for ar_info in correction["current_ars"]:
        entities_to_import.append(URIRef(ar_info["ar"]))
    editor.reader.import_entities_from_triplestore(
        g_set=g_set,
        ts_url=editor.endpoint,
        entities=entities_to_import,
        resp_agent=editor.resp_agent,
        enable_validation=False,
        batch_size=10,
    )
    br_entity = g_set.get_entity(URIRef(br_uri))
    for ar_uri in correction["delete_ars"]:
        ar_entity = g_set.get_entity(URIRef(ar_uri))
        ar_entity.remove_next()
        br_entity.remove_contributor(ar_entity)
        ar_entity.mark_as_to_be_deleted()
    editor.save(g_set, supplier_prefix)


def execute(config_path: str, plan_path: str, resp_agent: str) -> None:
    with open(plan_path, encoding="utf-8") as f:
        plan = json.load(f)

    editor = MetaEditor(config_path, resp_agent)
    ready_corrections = [c for c in plan["corrections"] if c["status"] == "ready"]

    print(f"Executing {len(ready_corrections)} corrections...")

    succeeded = 0
    failed = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task(
            "Applying corrections", total=len(ready_corrections)
        )
        for correction in ready_corrections:
            try:
                apply_correction(editor, correction)
                succeeded += 1
            except Exception as e:
                print(
                    f"  Error fixing {correction['br']}"
                    f" ({correction['role_type']}): {e}"
                )
                failed += 1
            progress.update(task, advance=1)

    print(f"Execution complete: {succeeded} succeeded, {failed} failed")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix hasNext chain anomalies in RDF data"
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
    args = parser.parse_args()

    if args.dry_run:
        if not args.anomalies or not args.output:
            parser.error("--dry-run requires -a/--anomalies and -o/--output")
        dry_run(args.config, args.anomalies, args.output, args.csv_output)
    elif args.execute:
        if not args.resp_agent:
            parser.error("--execute requires -r/--resp-agent")
        execute(args.config, args.execute, args.resp_agent)
    else:
        parser.error("Specify either --dry-run or --execute")


if __name__ == "__main__":
    main()
