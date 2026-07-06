# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.graph_entity import GraphEntity

from oc_meta.lib.merge_roles import discard_merged_br_author_editor_roles

RESP = "https://orcid.org/0000-0002-8420-0696"
BASE = "https://w3id.org/oc/meta/"


def _add_br(g_set, number):
    br = g_set.add_br(resp_agent=RESP, res=f"{BASE}br/{number}")
    br.create_journal_article()
    return br


def _add_role(g_set, number, br, role):
    agent = g_set.add_ra(resp_agent=RESP, res=f"{BASE}ra/{number}")
    agent_role = g_set.add_ar(resp_agent=RESP, res=f"{BASE}ar/{number}")
    getattr(agent_role, f"create_{role}")()
    agent_role.is_held_by(agent)
    br.has_contributor(agent_role)
    return agent_role


def _live_roles(br, role_iri):
    return sorted(
        str(contributor.res)
        for contributor in br.get_contributors()
        if not contributor.to_be_deleted and contributor.get_role_type() == role_iri
    )


def test_discard_merged_br_author_editor_roles_preserves_publishers():
    g_set = GraphSet(BASE)
    surviving_br = _add_br(g_set, 601)
    _add_role(g_set, 6011, surviving_br, "author")
    _add_role(g_set, 6012, surviving_br, "editor")

    merged_br = _add_br(g_set, 602)
    author_role = _add_role(g_set, 6021, merged_br, "author")
    editor_role = _add_role(g_set, 6022, merged_br, "editor")
    publisher_role = _add_role(g_set, 6023, merged_br, "publisher")

    discard_merged_br_author_editor_roles(g_set, f"{BASE}br/601", [f"{BASE}br/602"])

    contributor_uris = {
        str(contributor.res) for contributor in merged_br.get_contributors()
    }
    assert contributor_uris == {f"{BASE}ar/6023"}
    assert author_role.to_be_deleted is True
    assert editor_role.to_be_deleted is True
    assert publisher_role.to_be_deleted is False


def test_survivor_without_authors_adopts_merged_chain():
    g_set = GraphSet(BASE)
    _add_br(g_set, 701)
    merged_br = _add_br(g_set, 702)
    adopted_author = _add_role(g_set, 7021, merged_br, "author")

    discard_merged_br_author_editor_roles(g_set, f"{BASE}br/701", [f"{BASE}br/702"])

    assert adopted_author.to_be_deleted is False
    assert _live_roles(merged_br, GraphEntity.iri_author) == [f"{BASE}ar/7021"]


def test_survivor_without_authors_keeps_only_richest_donor():
    g_set = GraphSet(BASE)
    _add_br(g_set, 801)
    poor_donor = _add_br(g_set, 802)
    poor_author = _add_role(g_set, 8021, poor_donor, "author")
    rich_donor = _add_br(g_set, 803)
    rich_author_a = _add_role(g_set, 8031, rich_donor, "author")
    rich_author_b = _add_role(g_set, 8032, rich_donor, "author")

    discard_merged_br_author_editor_roles(
        g_set, f"{BASE}br/801", [f"{BASE}br/802", f"{BASE}br/803"]
    )

    assert poor_author.to_be_deleted is True
    assert rich_author_a.to_be_deleted is False
    assert rich_author_b.to_be_deleted is False
    assert _live_roles(rich_donor, GraphEntity.iri_author) == [
        f"{BASE}ar/8031",
        f"{BASE}ar/8032",
    ]


def test_survivor_with_authors_but_no_editors_adopts_only_editors():
    g_set = GraphSet(BASE)
    surviving_br = _add_br(g_set, 901)
    _add_role(g_set, 9011, surviving_br, "author")
    merged_br = _add_br(g_set, 902)
    discarded_author = _add_role(g_set, 9021, merged_br, "author")
    adopted_editor = _add_role(g_set, 9022, merged_br, "editor")

    discard_merged_br_author_editor_roles(g_set, f"{BASE}br/901", [f"{BASE}br/902"])

    assert discarded_author.to_be_deleted is True
    assert adopted_editor.to_be_deleted is False
    assert _live_roles(merged_br, GraphEntity.iri_editor) == [f"{BASE}ar/9022"]
