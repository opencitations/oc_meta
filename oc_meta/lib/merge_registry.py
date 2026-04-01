# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations


class EntityStore:
    """
    Unified entity storage with merge tracking and bidirectional identifier index.

    Combines entity data storage, merge tracking (Union-Find), and identifier
    lookups in both directions (entity→ids and id→entities).
    """

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}
        self._merged: dict[str, set[str]] = {}
        self._wannabe_to_meta: dict[str, str] = {}
        self._entity_ids: dict[str, set[str]] = {}
        self._entity_titles: dict[str, str] = {}
        self._id_to_entities: dict[str, set[str]] = {}
        self._id_to_metaid: dict[str, str] = {}

    def find(self, entity_id: str) -> str:
        """
        Find the canonical (root) ID for an entity with path compression.

        If the entity has a MetaID assigned, returns that MetaID.
        Otherwise returns the root of its Union-Find tree.
        """
        if entity_id in self._wannabe_to_meta:
            return self._wannabe_to_meta[entity_id]

        if entity_id not in self._parent:
            return entity_id

        root = entity_id
        while self._parent[root] != root:
            root = self._parent[root]

        current = entity_id
        while current != root:
            next_parent = self._parent[current]
            self._parent[current] = root
            current = next_parent

        if root in self._wannabe_to_meta:
            return self._wannabe_to_meta[root]

        return root

    def merge(self, target: str, source: str) -> None:
        """
        Register that source entity was merged into target entity.

        After this call, find(source) will return find(target).
        """
        target_root = self.find(target)
        source_root = self.find(source)

        if target_root == source_root:
            return

        if target_root not in self._parent:
            self._parent[target_root] = target_root
            self._merged[target_root] = set()
        if source_root not in self._parent:
            self._parent[source_root] = source_root
            self._merged[source_root] = set()

        self._parent[source_root] = target_root

        self._merged[target_root].add(source_root)
        self._merged[target_root].update(self._merged[source_root])
        del self._merged[source_root]

    def get_merged(self, canonical: str) -> set[str]:
        """
        Get all entity IDs that were merged into the canonical entity.

        Returns an empty set if no merges occurred.
        """
        root = self.find(canonical)
        if "wannabe" in root and root in self._wannabe_to_meta:
            root = self._wannabe_to_meta[root]
        return self._merged.get(root, set()).copy()

    def assign_meta(self, wannabe: str, meta: str) -> None:
        """
        Assign a final MetaID to a wannabe entity.

        After this call, find(wannabe) and find() of any entity merged
        into wannabe will return meta.
        """
        root = wannabe
        if wannabe in self._parent:
            root = wannabe
            while self._parent[root] != root:
                root = self._parent[root]

        self._wannabe_to_meta[root] = meta
        self._wannabe_to_meta[wannabe] = meta

        if root in self._merged:
            self._merged[meta] = self._merged.pop(root)
            self._merged[meta].add(root)
        else:
            self._merged[meta] = {wannabe} if wannabe != meta else set()

        for merged_id in list(self._merged.get(meta, [])):
            self._wannabe_to_meta[merged_id] = meta

    def add_entity(self, entity_key: str, title: str = "") -> None:
        """Create new entity with empty ids set and optional title."""
        self._entity_ids[entity_key] = set()
        self._entity_titles[entity_key] = title

    def add_id(self, entity_key: str, identifier: str) -> None:
        """Add identifier to entity, updating both forward and reverse indexes."""
        if entity_key not in self._entity_ids:
            self._entity_ids[entity_key] = set()
        self._entity_ids[entity_key].add(identifier)
        if identifier not in self._id_to_entities:
            self._id_to_entities[identifier] = set()
        self._id_to_entities[identifier].add(entity_key)

    def get_ids(self, entity_key: str) -> set[str]:
        """Get all identifiers for entity."""
        return self._entity_ids.get(entity_key, set()).copy()

    def get_title(self, entity_key: str) -> str:
        """Get title for entity."""
        return self._entity_titles.get(entity_key, "")

    def set_title(self, entity_key: str, title: str) -> None:
        """Set title for entity."""
        self._entity_titles[entity_key] = title

    def has_entity(self, entity_key: str) -> bool:
        """Check if entity exists in store."""
        return entity_key in self._entity_ids

    def remove_entity(self, entity_key: str) -> None:
        """Remove entity from store, cleaning both forward and reverse indexes."""
        identifiers = self._entity_ids.pop(entity_key, set())
        self._entity_titles.pop(entity_key, None)
        for identifier in identifiers:
            if identifier in self._id_to_entities:
                self._id_to_entities[identifier].discard(entity_key)
                if not self._id_to_entities[identifier]:
                    del self._id_to_entities[identifier]

    def merge_entities(self, target: str, source: str) -> None:
        """Merge source entity into target, combining ids and keeping target's title."""
        self.merge(target, source)
        source_ids = self._entity_ids.pop(source, set())
        source_title = self._entity_titles.pop(source, "")
        if target not in self._entity_ids:
            self._entity_ids[target] = set()
        self._entity_ids[target].update(source_ids)
        for identifier in source_ids:
            if identifier in self._id_to_entities:
                self._id_to_entities[identifier].discard(source)
                self._id_to_entities[identifier].add(target)
        if not self._entity_titles.get(target) and source_title:
            self._entity_titles[target] = source_title

    def entities(self) -> dict[str, dict[str, set[str] | str]]:
        """
        Get all entities as dict for backward compatibility.
        Returns: {entity_key: {"ids": set, "title": str}}
        """
        return {
            key: {"ids": self._entity_ids[key].copy(), "title": self._entity_titles.get(key, "")}
            for key in self._entity_ids
        }

    def find_entities(self, id_literal: str) -> set[str]:
        """
        Find all entities that have an identifier.

        Returns an empty set if the identifier is not registered.
        """
        return self._id_to_entities.get(id_literal, set()).copy()

    def find_entity(self, id_literal: str) -> str | None:
        """
        Find the first entity that has an identifier.

        Returns None if the identifier is not registered.
        """
        entities = self._id_to_entities.get(id_literal, set())
        return next(iter(entities), None) if entities else None

    def update_id_entity(self, old_entity: str, new_entity: str) -> None:
        """
        Update all identifiers from old_entity to point to new_entity.

        Used when merging entities externally.
        """
        for id_literal, entities in self._id_to_entities.items():
            if old_entity in entities:
                entities.discard(old_entity)
                entities.add(new_entity)

    def set_id_metaid(self, id_literal: str, metaid: str) -> None:
        """Store the MetaID for an identifier literal."""
        self._id_to_metaid[id_literal] = metaid

    def get_id_metaid(self, id_literal: str) -> str | None:
        """Get the MetaID for an identifier literal."""
        return self._id_to_metaid.get(id_literal)

    def get_id_metaids(self) -> dict[str, str]:
        """Get all identifier → MetaID mappings."""
        return self._id_to_metaid.copy()

    def __iter__(self):
        """Iterate over entity keys."""
        return iter(self._entity_ids)

    def __len__(self) -> int:
        """Return number of entities."""
        return len(self._entity_ids)

    def __contains__(self, entity_key: str) -> bool:
        """Check if entity exists."""
        return entity_key in self._entity_ids
