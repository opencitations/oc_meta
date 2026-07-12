# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from oc_meta.lib.agent_metadata import JsonObject
from oc_meta.run.patches import has_next


class FakeResponse:
    def __init__(self, payload: JsonObject) -> None:
        self.status_code = 200
        self.payload = payload

    def json(self) -> JsonObject:
        return self.payload

    def raise_for_status(self) -> None:
        return None


def test_fetch_crossref_uses_shared_agent_parser(monkeypatch) -> None:
    payload: JsonObject = {
        "message": {
            "member": "123",
            "publisher": "Example Press",
            "author": [
                {
                    "family": "Rossi",
                    "given": "Ada",
                    "ORCID": "https://orcid.org/0000-0002-8420-0696",
                }
            ],
        }
    }

    def get(url: str, timeout: int) -> FakeResponse:
        assert url == f"{has_next.CROSSREF_BASE}10.1000/example"
        assert timeout == 30
        return FakeResponse(payload)

    monkeypatch.setattr(has_next.SESSION, "get", get)
    assert has_next.fetch_crossref("10.1000/example") == {
        "author": [
            {
                "family": "Rossi",
                "given": "Ada",
                "name": "",
                "orcid": "0000-0002-8420-0696",
                "position": 0,
            }
        ],
        "editor": [],
        "publisher": "Example Press",
        "publisher_crossref_id": "123",
        "source": "crossref",
    }


def test_fetch_datacite_uses_shared_agent_parser(monkeypatch) -> None:
    payload: JsonObject = {
        "data": {
            "id": "10.1000/example",
            "attributes": {
                "publisher": "Example Repository",
                "creators": [{"name": "Rossi, Ada"}],
                "contributors": [],
            },
        }
    }

    def get(url: str, timeout: int) -> FakeResponse:
        assert url == f"{has_next.DATACITE_BASE}10.1000/example"
        assert timeout == 30
        return FakeResponse(payload)

    monkeypatch.setattr(has_next.SESSION, "get", get)
    assert has_next.fetch_datacite("10.1000/example") == {
        "author": [
            {
                "family": "",
                "given": "",
                "name": "Rossi, Ada",
                "orcid": None,
                "position": 0,
            }
        ],
        "editor": [],
        "publisher": "Example Repository",
        "publisher_crossref_id": None,
        "source": "datacite",
    }
