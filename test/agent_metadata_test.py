# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from oc_meta.lib.agent_metadata import (
    AgentMetadata,
    AgentMetadataClient,
    ApiCache,
    JsonObject,
    WorkMetadata,
    agents_for_role,
    is_valid_orcid,
    normalize_orcid,
    parse_crossref_work,
    parse_datacite_work,
    parse_openalex_work,
    parse_orcid_profile,
)


def test_orcid_normalization_and_checksum() -> None:
    assert normalize_orcid("HTTPS://ORCID.ORG/0000-0002-1694-233x") == (
        "0000-0002-1694-233X"
    )
    assert normalize_orcid("000000021694233X") == "0000-0002-1694-233X"
    assert is_valid_orcid("0000-0002-1694-233X") is True
    assert is_valid_orcid("0000-0002-1694-2330") is False
    assert is_valid_orcid("not-an-orcid") is False
    assert is_valid_orcid("0000 0002 1694 233X") is False
    assert is_valid_orcid("0000-0002-1694-233Xjunk") is False


def test_parse_crossref_work() -> None:
    result = parse_crossref_work(
        {
            "message": {
                "DOI": "10.1000/example",
                "member": "https://id.crossref.org/member/123",
                "publisher": "Example Press",
                "author": [
                    {
                        "family": "García",
                        "given": "Ana",
                        "ORCID": "https://orcid.org/0000-0002-1825-0097",
                    }
                ],
                "editor": [{"family": "Doe", "given": "Jane"}],
            }
        }
    )
    assert result == {
        "identifier": "10.1000/example",
        "source": "crossref",
        "author": [
            {
                "family": "García",
                "given": "Ana",
                "name": "",
                "orcid": "0000-0002-1825-0097",
                "identifiers": ({"scheme": "orcid", "value": "0000-0002-1825-0097"},),
                "position": 0,
                "role": "author",
            }
        ],
        "editor": [
            {
                "family": "Doe",
                "given": "Jane",
                "name": "",
                "orcid": None,
                "identifiers": (),
                "position": 0,
                "role": "editor",
            }
        ],
        "publisher": "Example Press",
        "publisher_identifiers": ({"scheme": "crossref", "value": "123"},),
    }


def test_agents_for_role() -> None:
    author: AgentMetadata = {
        "family": "Rossi",
        "given": "Ada",
        "name": "",
        "orcid": "0000-0002-8420-0696",
        "identifiers": ({"scheme": "orcid", "value": "0000-0002-8420-0696"},),
        "position": 0,
        "role": "author",
    }
    editor: AgentMetadata = {
        "family": "Bianchi",
        "given": "Luca",
        "name": "",
        "orcid": None,
        "identifiers": (),
        "position": 0,
        "role": "editor",
    }
    work: WorkMetadata = {
        "identifier": "10.1000/example",
        "source": "crossref",
        "author": [author],
        "editor": [editor],
        "publisher": "Example Press",
        "publisher_identifiers": ({"scheme": "crossref", "value": "123"},),
    }

    assert agents_for_role(work, "author") == [author]
    assert agents_for_role(work, "editor") == [editor]
    assert agents_for_role(work, "publisher") == [
        {
            "family": "",
            "given": "",
            "name": "Example Press",
            "orcid": None,
            "identifiers": ({"scheme": "crossref", "value": "123"},),
            "position": 0,
            "role": "publisher",
        }
    ]
    assert agents_for_role(work, "translator") == []


def test_parse_datacite_work() -> None:
    result = parse_datacite_work(
        {
            "data": {
                "id": "10.1000/datacite",
                "attributes": {
                    "publisher": {
                        "name": "Data Press",
                        "publisherIdentifier": "https://ror.org/03yrm5c26",
                        "publisherIdentifierScheme": "ROR",
                    },
                    "creators": [
                        {
                            "familyName": "Rossi",
                            "givenName": "Ada",
                            "name": "Rossi, Ada",
                            "nameIdentifiers": [
                                {
                                    "nameIdentifierScheme": "ORCID",
                                    "nameIdentifier": "0000-0002-8420-0696",
                                },
                                {
                                    "nameIdentifierScheme": "ROR",
                                    "nameIdentifier": "https://ror.org/03yrm5c26",
                                },
                            ],
                        }
                    ],
                    "contributors": [
                        {
                            "contributorType": "Editor",
                            "familyName": "Bianchi",
                            "givenName": "Luca",
                            "name": "Bianchi, Luca",
                            "nameIdentifiers": [],
                        },
                        {
                            "contributorType": "ProjectLeader",
                            "name": "Ignored",
                        },
                    ],
                },
            }
        }
    )
    assert result == {
        "identifier": "10.1000/datacite",
        "source": "datacite",
        "author": [
            {
                "family": "Rossi",
                "given": "Ada",
                "name": "Rossi, Ada",
                "orcid": "0000-0002-8420-0696",
                "identifiers": (
                    {"scheme": "orcid", "value": "0000-0002-8420-0696"},
                    {"scheme": "ror", "value": "03yrm5c26"},
                ),
                "position": 0,
                "role": "author",
            }
        ],
        "editor": [
            {
                "family": "Bianchi",
                "given": "Luca",
                "name": "Bianchi, Luca",
                "orcid": None,
                "identifiers": (),
                "position": 0,
                "role": "editor",
            }
        ],
        "publisher": "Data Press",
        "publisher_identifiers": ({"scheme": "ror", "value": "03yrm5c26"},),
    }


def test_parse_openalex_and_orcid() -> None:
    work = parse_openalex_work(
        {
            "id": "https://openalex.org/W1",
            "authorships": [
                {
                    "raw_author_name": "Ada Rossi",
                    "author": {
                        "display_name": "A. Rossi",
                        "orcid": "https://orcid.org/0000-0002-8420-0696",
                    },
                }
            ],
        }
    )
    profile = parse_orcid_profile(
        {
            "name": {
                "given-names": {"value": "Ada"},
                "family-name": {"value": "Rossi"},
            }
        },
        "0000-0002-8420-0696",
    )
    assert work == {
        "identifier": "https://openalex.org/W1",
        "source": "openalex",
        "author": [
            {
                "family": "",
                "given": "",
                "name": "Ada Rossi",
                "orcid": "0000-0002-8420-0696",
                "identifiers": ({"scheme": "orcid", "value": "0000-0002-8420-0696"},),
                "position": 0,
                "role": "author",
            }
        ],
        "editor": [],
        "publisher": "",
        "publisher_identifiers": (),
    }
    assert profile == {
        "orcid": "0000-0002-8420-0696",
        "given": "Ada",
        "family": "Rossi",
        "name": "Ada Rossi",
    }


def test_api_cache_round_trip(tmp_path) -> None:
    cache = ApiCache(str(tmp_path / "api.sqlite"))
    body: JsonObject = {"message": {"DOI": "10.1000/example"}}
    cache.set("crossref", "10.1000/example", 200, body)
    cache.set("orcid", "missing", 404, None)

    assert cache.get("crossref", "10.1000/example") == (200, body)
    assert cache.get("orcid", "missing") == (404, None)
    assert cache.get("datacite", "unknown") is None
    cache.close()


def test_all_work_sources_queries_each_provider_in_priority_order(
    tmp_path, monkeypatch
) -> None:
    cache = ApiCache(str(tmp_path / "api.sqlite"))
    client = AgentMetadataClient("test@example.org", cache)
    calls: list[tuple[str, ...]] = []
    crossref = parse_crossref_work({"message": {}}, "crossref")
    datacite = parse_datacite_work({"data": {"attributes": {}}}, "datacite")
    openalex = parse_openalex_work({}, "openalex")

    def fetch_crossref(self: AgentMetadataClient, doi: str):
        assert self is client
        calls.append(("crossref", doi))
        return crossref

    def fetch_datacite(self: AgentMetadataClient, doi: str):
        assert self is client
        calls.append(("datacite", doi))
        return datacite

    def fetch_openalex(self: AgentMetadataClient, doi: str, openalex_id: str):
        assert self is client
        calls.append(("openalex", doi, openalex_id))
        return openalex

    monkeypatch.setattr(AgentMetadataClient, "crossref", fetch_crossref)
    monkeypatch.setattr(AgentMetadataClient, "datacite", fetch_datacite)
    monkeypatch.setattr(AgentMetadataClient, "openalex_work", fetch_openalex)

    result = client.all_work_sources("10.1000/example", "W1")

    assert result == [crossref, datacite, openalex]
    assert calls == [
        ("crossref", "10.1000/example"),
        ("datacite", "10.1000/example"),
        ("openalex", "10.1000/example", "W1"),
    ]
    client.close()
    cache.close()
