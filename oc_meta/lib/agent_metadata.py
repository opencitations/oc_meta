# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TypeAlias, TypedDict, cast
from urllib.parse import quote

import requests
from oc_ds_converter.oc_idmanager import ORCIDManager

JsonValue: TypeAlias = (
    None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
)
JsonObject: TypeAlias = dict[str, JsonValue]

CROSSREF_BASE = "https://api.crossref.org/works/"
DATACITE_BASE = "https://api.datacite.org/dois/"
OPENALEX_WORKS = "https://api.openalex.org/works"
ORCID_BASE = "https://pub.orcid.org/v3.0/"

_ORCID_MANAGER = ORCIDManager(use_api_service=False)


class AgentMetadata(TypedDict):
    family: str
    given: str
    name: str
    orcid: str | None
    position: int
    role: str


class WorkMetadata(TypedDict):
    identifier: str
    source: str
    author: list[AgentMetadata]
    editor: list[AgentMetadata]
    publisher: str


class OrcidProfile(TypedDict):
    orcid: str
    given: str
    family: str
    name: str


def normalize_orcid(orcid: str) -> str:
    normalized = orcid.strip()
    for prefix in ("https://orcid.org/", "http://orcid.org/", "orcid:"):
        if normalized.casefold().startswith(prefix):
            normalized = normalized[len(prefix) :]
            break
    normalized = normalized.upper()
    compact = normalized.replace("-", "")
    if (
        len(compact) == 16
        and compact[:15].isdigit()
        and (compact[-1].isdigit() or compact[-1] == "X")
    ):
        return "-".join(compact[index : index + 4] for index in range(0, 16, 4))
    return normalized


def is_valid_orcid(orcid: str) -> bool:
    normalized = normalize_orcid(orcid)
    return _ORCID_MANAGER.syntax_ok(normalized) and _ORCID_MANAGER.check_digit(
        normalized
    )


def _orcid_or_none(value: str) -> str | None:
    normalized = normalize_orcid(value)
    return normalized if is_valid_orcid(normalized) else None


def _object(value: JsonValue) -> JsonObject:
    return cast(JsonObject, value) if isinstance(value, dict) else {}


def _objects(value: JsonValue) -> list[JsonObject]:
    if not isinstance(value, list):
        return []
    return [cast(JsonObject, item) for item in value if isinstance(item, dict)]


def _string(value: JsonValue) -> str:
    return value if isinstance(value, str) else ""


def _orcid_from_name_identifiers(value: JsonValue) -> str | None:
    for identifier in _objects(value):
        scheme = _string(identifier.get("nameIdentifierScheme")).upper()
        if scheme == "ORCID":
            orcid = _orcid_or_none(_string(identifier.get("nameIdentifier")))
            if orcid is not None:
                return orcid
    return None


def parse_crossref_work(data: JsonObject, identifier: str = "") -> WorkMetadata:
    message = _object(data.get("message"))
    authors = []
    for position, author in enumerate(_objects(message.get("author"))):
        authors.append(
            AgentMetadata(
                family=_string(author.get("family")),
                given=_string(author.get("given")),
                name=_string(author.get("name")),
                orcid=_orcid_or_none(_string(author.get("ORCID"))),
                position=position,
                role="author",
            )
        )
    editors = []
    for position, editor in enumerate(_objects(message.get("editor"))):
        editors.append(
            AgentMetadata(
                family=_string(editor.get("family")),
                given=_string(editor.get("given")),
                name=_string(editor.get("name")),
                orcid=_orcid_or_none(_string(editor.get("ORCID"))),
                position=position,
                role="editor",
            )
        )
    return WorkMetadata(
        identifier=identifier or _string(message.get("DOI")),
        source="crossref",
        author=authors,
        editor=editors,
        publisher=_string(message.get("publisher")),
    )


def parse_datacite_work(data: JsonObject, identifier: str = "") -> WorkMetadata:
    attributes = _object(_object(data.get("data")).get("attributes"))
    authors = []
    for position, creator in enumerate(_objects(attributes.get("creators"))):
        authors.append(
            AgentMetadata(
                family=_string(creator.get("familyName")),
                given=_string(creator.get("givenName")),
                name=_string(creator.get("name")),
                orcid=_orcid_from_name_identifiers(creator.get("nameIdentifiers")),
                position=position,
                role="author",
            )
        )
    editors = []
    for contributor in _objects(attributes.get("contributors")):
        if _string(contributor.get("contributorType")) != "Editor":
            continue
        editors.append(
            AgentMetadata(
                family=_string(contributor.get("familyName")),
                given=_string(contributor.get("givenName")),
                name=_string(contributor.get("name")),
                orcid=_orcid_from_name_identifiers(contributor.get("nameIdentifiers")),
                position=len(editors),
                role="editor",
            )
        )
    publisher_value = attributes.get("publisher")
    publisher = (
        _string(_object(publisher_value).get("name"))
        if isinstance(publisher_value, dict)
        else _string(publisher_value)
    )
    return WorkMetadata(
        identifier=identifier or _string(_object(data.get("data")).get("id")),
        source="datacite",
        author=authors,
        editor=editors,
        publisher=publisher,
    )


def parse_openalex_work(data: JsonObject, identifier: str = "") -> WorkMetadata:
    authors = []
    for position, authorship in enumerate(_objects(data.get("authorships"))):
        author = _object(authorship.get("author"))
        display_name = _string(author.get("display_name"))
        raw_name = _string(authorship.get("raw_author_name")) or display_name
        authors.append(
            AgentMetadata(
                family="",
                given="",
                name=raw_name,
                orcid=_orcid_or_none(_string(author.get("orcid"))),
                position=position,
                role="author",
            )
        )
    return WorkMetadata(
        identifier=identifier or _string(data.get("id")),
        source="openalex",
        author=authors,
        editor=[],
        publisher="",
    )


def parse_orcid_profile(data: JsonObject, orcid: str) -> OrcidProfile:
    person_value = data.get("person")
    person = _object(person_value) if isinstance(person_value, dict) else data
    name = _object(person.get("name"))
    given = _string(_object(name.get("given-names")).get("value"))
    family = _string(_object(name.get("family-name")).get("value"))
    return OrcidProfile(
        orcid=normalize_orcid(orcid),
        given=given,
        family=family,
        name=" ".join(part for part in (given, family) if part),
    )


class ApiCache:
    def __init__(self, path: str) -> None:
        self.connection = sqlite3.connect(path)
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS api_response (
                source TEXT NOT NULL,
                cache_key TEXT NOT NULL,
                status INTEGER NOT NULL,
                body TEXT,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (source, cache_key)
            )
            """
        )
        self.connection.commit()

    def get(self, source: str, cache_key: str) -> tuple[int, JsonObject | None] | None:
        row = self.connection.execute(
            "SELECT status, body FROM api_response WHERE source = ? AND cache_key = ?",
            (source, cache_key),
        ).fetchone()
        if row is None:
            return None
        status = cast(int, row[0])
        body = cast(str | None, row[1])
        return status, cast(JsonObject, json.loads(body)) if body else None

    def set(
        self, source: str, cache_key: str, status: int, body: JsonObject | None
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO api_response (source, cache_key, status, body, fetched_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source, cache_key) DO UPDATE SET
                status = excluded.status,
                body = excluded.body,
                fetched_at = excluded.fetched_at
            """,
            (
                source,
                cache_key,
                status,
                json.dumps(body, ensure_ascii=False) if body is not None else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


@dataclass(slots=True)
class AgentMetadataClient:
    mailto: str
    cache: ApiCache
    refresh_cache: bool = False
    openalex_api_key: str = ""
    timeout: int = 30
    max_attempts: int = 4
    session: requests.Session = field(init=False)

    def __post_init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": f"oc_meta-agent-audit/1.0 (mailto:{self.mailto})",
            }
        )

    def _request(
        self,
        source: str,
        cache_key: str,
        url: str,
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> JsonObject | None:
        if not self.refresh_cache:
            cached = self.cache.get(source, cache_key)
            if cached is not None:
                return cached[1] if cached[0] == 200 else None
        for attempt in range(self.max_attempts):
            try:
                response = self.session.get(
                    url, params=params, headers=headers, timeout=self.timeout
                )
            except requests.RequestException:
                if attempt + 1 == self.max_attempts:
                    raise
                time.sleep(2**attempt)
                continue
            if response.status_code == 200:
                body = cast(JsonObject, response.json())
                self.cache.set(source, cache_key, response.status_code, body)
                return body
            if response.status_code == 404:
                self.cache.set(source, cache_key, response.status_code, None)
                return None
            if response.status_code == 429 or response.status_code >= 500:
                if attempt + 1 == self.max_attempts:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                time.sleep(float(retry_after) if retry_after else 2**attempt)
                continue
            response.raise_for_status()
        return None

    def crossref(self, doi: str) -> WorkMetadata | None:
        data = self._request(
            "crossref",
            doi.lower(),
            CROSSREF_BASE + quote(doi, safe=""),
            {"mailto": self.mailto},
        )
        return parse_crossref_work(data, doi) if data is not None else None

    def datacite(self, doi: str) -> WorkMetadata | None:
        data = self._request(
            "datacite", doi.lower(), DATACITE_BASE + quote(doi, safe="")
        )
        return parse_datacite_work(data, doi) if data is not None else None

    def openalex_work(
        self, doi: str = "", openalex_id: str = ""
    ) -> WorkMetadata | None:
        if openalex_id:
            normalized = openalex_id.rsplit("/", 1)[-1].upper()
            params = (
                {"api_key": self.openalex_api_key} if self.openalex_api_key else None
            )
            data = self._request(
                "openalex", normalized, f"{OPENALEX_WORKS}/{normalized}", params
            )
            return parse_openalex_work(data, normalized) if data is not None else None
        if not doi:
            return None
        params = {"filter": f"doi:{doi}", "per-page": 1}
        if self.openalex_api_key:
            params["api_key"] = self.openalex_api_key
        data = self._request(
            "openalex",
            f"doi:{doi.lower()}",
            OPENALEX_WORKS,
            params,
        )
        if data is None:
            return None
        results = _objects(data.get("results"))
        return parse_openalex_work(results[0], doi) if results else None

    def orcid(self, orcid: str) -> OrcidProfile | None:
        normalized = normalize_orcid(orcid)
        data = self._request(
            "orcid",
            normalized,
            ORCID_BASE + quote(normalized, safe="") + "/person",
            headers={"Accept": "application/vnd.orcid+json"},
        )
        return parse_orcid_profile(data, normalized) if data is not None else None

    def work_sources(self, doi: str, openalex_id: str = "") -> list[WorkMetadata]:
        works = []
        primary = self.crossref(doi) if doi else None
        if primary is None and doi:
            primary = self.datacite(doi)
        if primary is not None:
            works.append(primary)
        openalex = self.openalex_work(doi, openalex_id)
        if openalex is not None:
            works.append(openalex)
        return works

    def close(self) -> None:
        self.session.close()
