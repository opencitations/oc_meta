# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from oc_meta.lib.agent_metadata import (
    ApiCache,
    JsonObject,
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
                "position": 0,
                "role": "editor",
            }
        ],
        "publisher": "Example Press",
    }


def test_parse_datacite_work() -> None:
    result = parse_datacite_work(
        {
            "data": {
                "id": "10.1000/datacite",
                "attributes": {
                    "publisher": {"name": "Data Press"},
                    "creators": [
                        {
                            "familyName": "Rossi",
                            "givenName": "Ada",
                            "name": "Rossi, Ada",
                            "nameIdentifiers": [
                                {
                                    "nameIdentifierScheme": "ORCID",
                                    "nameIdentifier": "0000-0002-8420-0696",
                                }
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
                "position": 0,
                "role": "editor",
            }
        ],
        "publisher": "Data Press",
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
                "position": 0,
                "role": "author",
            }
        ],
        "editor": [],
        "publisher": "",
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
