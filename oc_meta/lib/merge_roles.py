# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic.bibliographic_resource import (
    BibliographicResource,
)
from oc_ocdm.graph.graph_entity import GraphEntity


DISCARDED_MERGED_BR_ROLE_TYPES = (GraphEntity.iri_author, GraphEntity.iri_editor)


def discard_merged_br_author_editor_roles(
    g_set: GraphSet, surviving_br_uri: str, merged_br_uris: list[str]
) -> None:
    surviving_br = _as_bibliographic_resource(g_set, surviving_br_uri)
    merged_brs = [_as_bibliographic_resource(g_set, uri) for uri in merged_br_uris]
    for role_type in DISCARDED_MERGED_BR_ROLE_TYPES:
        _keep_single_role_chain(surviving_br, merged_brs, role_type)


def _keep_single_role_chain(
    surviving_br: BibliographicResource,
    merged_brs: list[BibliographicResource],
    role_type: str,
) -> None:
    # The survivor's chain wins; a merged chain is only kept when the survivor
    # has none, so the survivor never gets two parallel chains nor loses its
    # only one.
    donor = None
    if not _has_role(surviving_br, role_type):
        donor = _richest_donor(merged_brs, role_type)
    for merged_br in merged_brs:
        if merged_br is donor:
            continue
        for contributor in list(merged_br.get_contributors()):
            if contributor.get_role_type() == role_type:
                merged_br.remove_contributor(contributor)
                contributor.mark_as_to_be_deleted()


def _richest_donor(
    merged_brs: list[BibliographicResource], role_type: str
) -> BibliographicResource | None:
    donors = [br for br in merged_brs if _role_count(br, role_type) > 0]
    if not donors:
        return None
    return min(donors, key=lambda br: (-_role_count(br, role_type), str(br.res)))


def _has_role(br: BibliographicResource, role_type: str) -> bool:
    return any(
        contributor.get_role_type() == role_type
        for contributor in br.get_contributors()
    )


def _role_count(br: BibliographicResource, role_type: str) -> int:
    return sum(
        1
        for contributor in br.get_contributors()
        if contributor.get_role_type() == role_type
    )


def _as_bibliographic_resource(g_set: GraphSet, uri: str) -> BibliographicResource:
    entity = g_set.get_entity(uri)
    if not isinstance(entity, BibliographicResource):
        raise ValueError(f"Entity is not a bibliographic resource: {uri}")
    return entity
