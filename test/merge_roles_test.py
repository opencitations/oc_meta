# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from oc_ocdm.graph import GraphSet

from oc_meta.lib.merge_roles import discard_merged_br_author_editor_roles


def test_discard_merged_br_author_editor_roles_preserves_publishers():
    g_set = GraphSet("https://w3id.org/oc/meta/")
    merged_br = g_set.add_br(
        resp_agent="https://orcid.org/0000-0002-8420-0696",
        res="https://w3id.org/oc/meta/br/0601",
    )

    author = g_set.add_ra(
        resp_agent="https://orcid.org/0000-0002-8420-0696",
        res="https://w3id.org/oc/meta/ra/0601",
    )
    author_role = g_set.add_ar(
        resp_agent="https://orcid.org/0000-0002-8420-0696",
        res="https://w3id.org/oc/meta/ar/0601",
    )
    author_role.create_author()
    author_role.is_held_by(author)
    merged_br.has_contributor(author_role)

    editor = g_set.add_ra(
        resp_agent="https://orcid.org/0000-0002-8420-0696",
        res="https://w3id.org/oc/meta/ra/0602",
    )
    editor_role = g_set.add_ar(
        resp_agent="https://orcid.org/0000-0002-8420-0696",
        res="https://w3id.org/oc/meta/ar/0602",
    )
    editor_role.create_editor()
    editor_role.is_held_by(editor)
    merged_br.has_contributor(editor_role)

    publisher = g_set.add_ra(
        resp_agent="https://orcid.org/0000-0002-8420-0696",
        res="https://w3id.org/oc/meta/ra/0603",
    )
    publisher_role = g_set.add_ar(
        resp_agent="https://orcid.org/0000-0002-8420-0696",
        res="https://w3id.org/oc/meta/ar/0603",
    )
    publisher_role.create_publisher()
    publisher_role.is_held_by(publisher)
    merged_br.has_contributor(publisher_role)

    discard_merged_br_author_editor_roles(g_set, ["https://w3id.org/oc/meta/br/0601"])

    contributor_uris = {
        str(contributor.res) for contributor in merged_br.get_contributors()
    }
    assert contributor_uris == {"https://w3id.org/oc/meta/ar/0603"}
    assert author_role.to_be_deleted is True
    assert editor_role.to_be_deleted is True
    assert publisher_role.to_be_deleted is False
