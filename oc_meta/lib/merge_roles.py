# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic.bibliographic_resource import (
    BibliographicResource,
)
from oc_ocdm.graph.graph_entity import GraphEntity


DISCARDED_MERGED_BR_ROLE_TYPES = {GraphEntity.iri_author, GraphEntity.iri_editor}


def discard_merged_br_author_editor_roles(
    g_set: GraphSet, merged_br_uris: list[str]
) -> None:
    for merged_br_uri in merged_br_uris:
        merged_br = g_set.get_entity(merged_br_uri)
        if not isinstance(merged_br, BibliographicResource):
            raise ValueError(
                f"Merged entity is not a bibliographic resource: {merged_br_uri}"
            )

        for contributor in list(merged_br.get_contributors()):
            if contributor.get_role_type() in DISCARDED_MERGED_BR_ROLE_TYPES:
                merged_br.remove_contributor(contributor)
                contributor.mark_as_to_be_deleted()
