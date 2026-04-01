# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import pytest

from oc_meta.lib.merge_registry import EntityStore


class TestEntityStore:
    def test_find_unregistered_returns_self(self):
        store = EntityStore()
        result = store.find("wannabe_0")
        assert result == "wannabe_0"

    def test_find_after_meta_assignment(self):
        store = EntityStore()
        store.assign_meta("wannabe_0", "0601")
        result = store.find("wannabe_0")
        assert result == "0601"

    def test_merge_two_wannabes(self):
        store = EntityStore()
        store.merge("wannabe_0", "wannabe_1")
        assert store.find("wannabe_1") == "wannabe_0"
        assert store.find("wannabe_0") == "wannabe_0"

    def test_merge_chain(self):
        store = EntityStore()
        store.merge("wannabe_0", "wannabe_1")
        store.merge("wannabe_0", "wannabe_2")
        assert store.find("wannabe_1") == "wannabe_0"
        assert store.find("wannabe_2") == "wannabe_0"

    def test_merge_then_assign_meta(self):
        store = EntityStore()
        store.merge("wannabe_0", "wannabe_1")
        store.assign_meta("wannabe_0", "0601")
        assert store.find("wannabe_0") == "0601"
        assert store.find("wannabe_1") == "0601"

    def test_merge_into_existing_meta(self):
        store = EntityStore()
        store.merge("0601", "wannabe_0")
        assert store.find("wannabe_0") == "0601"

    def test_get_merged_empty(self):
        store = EntityStore()
        result = store.get_merged("wannabe_0")
        assert result == set()

    def test_get_merged_after_merge(self):
        store = EntityStore()
        store.merge("wannabe_0", "wannabe_1")
        result = store.get_merged("wannabe_0")
        assert result == {"wannabe_1"}

    def test_get_merged_chain(self):
        store = EntityStore()
        store.merge("wannabe_0", "wannabe_1")
        store.merge("wannabe_0", "wannabe_2")
        result = store.get_merged("wannabe_0")
        assert result == {"wannabe_1", "wannabe_2"}

    def test_get_merged_after_meta_assignment(self):
        store = EntityStore()
        store.merge("wannabe_0", "wannabe_1")
        store.assign_meta("wannabe_0", "0601")
        result = store.get_merged("0601")
        assert "wannabe_1" in result
        assert "wannabe_0" in result

    def test_merge_transitive(self):
        store = EntityStore()
        store.merge("wannabe_0", "wannabe_1")
        store.merge("wannabe_1", "wannabe_2")
        assert store.find("wannabe_2") == "wannabe_0"

    def test_merge_with_existing_meta_id(self):
        store = EntityStore()
        store.merge("0601", "wannabe_0")
        store.merge("0601", "wannabe_1")
        assert store.find("wannabe_0") == "0601"
        assert store.find("wannabe_1") == "0601"
        merged = store.get_merged("0601")
        assert "wannabe_0" in merged
        assert "wannabe_1" in merged

    def test_assign_meta_propagates_to_merged(self):
        store = EntityStore()
        store.merge("wannabe_0", "wannabe_1")
        store.merge("wannabe_0", "wannabe_2")
        store.assign_meta("wannabe_0", "0601")
        assert store.find("wannabe_1") == "0601"
        assert store.find("wannabe_2") == "0601"

    def test_merge_idempotent(self):
        store = EntityStore()
        store.merge("wannabe_0", "wannabe_1")
        store.merge("wannabe_0", "wannabe_1")
        assert store.find("wannabe_1") == "wannabe_0"
        assert store.get_merged("wannabe_0") == {"wannabe_1"}

    def test_add_entity(self):
        store = EntityStore()
        store.add_entity("wannabe_0", "Test Title")
        assert store.has_entity("wannabe_0")
        assert store.get_title("wannabe_0") == "Test Title"
        assert store.get_ids("wannabe_0") == set()

    def test_add_id_to_entity(self):
        store = EntityStore()
        store.add_entity("wannabe_0", "Test Title")
        store.add_id("wannabe_0", "doi:10.1234/test")
        store.add_id("wannabe_0", "pmid:12345")
        assert store.get_ids("wannabe_0") == {"doi:10.1234/test", "pmid:12345"}

    def test_add_id_creates_entity_if_missing(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/test")
        assert store.has_entity("wannabe_0")
        assert store.get_ids("wannabe_0") == {"doi:10.1234/test"}

    def test_set_title(self):
        store = EntityStore()
        store.add_entity("wannabe_0", "Original Title")
        store.set_title("wannabe_0", "Updated Title")
        assert store.get_title("wannabe_0") == "Updated Title"

    def test_merge_entities_combines_ids(self):
        store = EntityStore()
        store.add_entity("wannabe_0", "Title A")
        store.add_id("wannabe_0", "doi:10.1234/a")
        store.add_entity("wannabe_1", "Title B")
        store.add_id("wannabe_1", "doi:10.1234/b")
        store.merge_entities("wannabe_0", "wannabe_1")
        assert store.get_ids("wannabe_0") == {"doi:10.1234/a", "doi:10.1234/b"}
        assert not store.has_entity("wannabe_1")
        assert store.find("wannabe_1") == "wannabe_0"

    def test_merge_entities_keeps_target_title(self):
        store = EntityStore()
        store.add_entity("wannabe_0", "Title A")
        store.add_entity("wannabe_1", "Title B")
        store.merge_entities("wannabe_0", "wannabe_1")
        assert store.get_title("wannabe_0") == "Title A"

    def test_merge_entities_uses_source_title_if_target_empty(self):
        store = EntityStore()
        store.add_entity("wannabe_0", "")
        store.add_entity("wannabe_1", "Title B")
        store.merge_entities("wannabe_0", "wannabe_1")
        assert store.get_title("wannabe_0") == "Title B"

    def test_remove_entity(self):
        store = EntityStore()
        store.add_entity("wannabe_0", "Test Title")
        store.add_id("wannabe_0", "doi:10.1234/test")
        store.remove_entity("wannabe_0")
        assert not store.has_entity("wannabe_0")
        assert store.get_ids("wannabe_0") == set()
        assert store.get_title("wannabe_0") == ""

    def test_entities_returns_all(self):
        store = EntityStore()
        store.add_entity("wannabe_0", "Title A")
        store.add_id("wannabe_0", "doi:10.1234/a")
        store.add_entity("wannabe_1", "Title B")
        store.add_id("wannabe_1", "doi:10.1234/b")
        entities = store.entities()
        assert entities == {
            "wannabe_0": {"ids": {"doi:10.1234/a"}, "title": "Title A"},
            "wannabe_1": {"ids": {"doi:10.1234/b"}, "title": "Title B"},
        }

    def test_iter_entities(self):
        store = EntityStore()
        store.add_entity("wannabe_0", "Title A")
        store.add_entity("wannabe_1", "Title B")
        keys = list(store)
        assert set(keys) == {"wannabe_0", "wannabe_1"}

    def test_len_entities(self):
        store = EntityStore()
        store.add_entity("wannabe_0", "Title A")
        store.add_entity("wannabe_1", "Title B")
        assert len(store) == 2

    def test_contains_entity(self):
        store = EntityStore()
        store.add_entity("wannabe_0", "Title A")
        assert "wannabe_0" in store
        assert "wannabe_1" not in store


class TestEntityStoreIdIndex:
    """Test bidirectional identifier index functionality in EntityStore."""

    def test_find_entity_not_found(self):
        store = EntityStore()
        result = store.find_entity("doi:10.1234/test")
        assert result is None

    def test_find_entities_not_found(self):
        store = EntityStore()
        result = store.find_entities("doi:10.1234/test")
        assert result == set()

    def test_add_id_and_find(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/test")
        result = store.find_entity("doi:10.1234/test")
        assert result == "wannabe_0"

    def test_add_id_and_find_entities(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/test")
        result = store.find_entities("doi:10.1234/test")
        assert result == {"wannabe_0"}

    def test_add_multiple_ids_same_entity(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/test")
        store.add_id("wannabe_0", "pmid:12345")
        assert store.find_entities("doi:10.1234/test") == {"wannabe_0"}
        assert store.find_entities("pmid:12345") == {"wannabe_0"}

    def test_multiple_entities_same_id(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/test")
        store.add_id("wannabe_1", "doi:10.1234/test")
        result = store.find_entities("doi:10.1234/test")
        assert result == {"wannabe_0", "wannabe_1"}

    def test_update_id_entity(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/test")
        store.add_id("wannabe_0", "pmid:12345")
        store.update_id_entity("wannabe_0", "0601")
        assert store.find_entities("doi:10.1234/test") == {"0601"}
        assert store.find_entities("pmid:12345") == {"0601"}

    def test_update_id_entity_preserves_others(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/test")
        store.add_id("wannabe_1", "doi:10.1234/test")
        store.update_id_entity("wannabe_0", "0601")
        result = store.find_entities("doi:10.1234/test")
        assert result == {"0601", "wannabe_1"}

    def test_get_ids_multiple(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/test")
        store.add_id("wannabe_0", "pmid:12345")
        store.add_id("wannabe_1", "doi:10.5678/other")
        result = store.get_ids("wannabe_0")
        assert result == {"doi:10.1234/test", "pmid:12345"}

    def test_get_ids_empty(self):
        store = EntityStore()
        result = store.get_ids("wannabe_0")
        assert result == set()

    def test_remove_entity_clears_reverse_index(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/test")
        store.add_id("wannabe_0", "pmid:12345")
        store.remove_entity("wannabe_0")
        assert store.find_entities("doi:10.1234/test") == set()
        assert store.find_entities("pmid:12345") == set()

    def test_remove_entity_preserves_others_in_reverse_index(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/test")
        store.add_id("wannabe_1", "doi:10.1234/test")
        store.remove_entity("wannabe_0")
        result = store.find_entities("doi:10.1234/test")
        assert result == {"wannabe_1"}

    def test_set_id_metaid(self):
        store = EntityStore()
        store.set_id_metaid("doi:10.1234/test", "br/0601")
        assert store.get_id_metaid("doi:10.1234/test") == "br/0601"

    def test_set_id_metaid_ra(self):
        store = EntityStore()
        store.set_id_metaid("orcid:0000-0001-2345-6789", "ra/0601")
        assert store.get_id_metaid("orcid:0000-0001-2345-6789") == "ra/0601"

    def test_get_id_metaid_not_found(self):
        store = EntityStore()
        assert store.get_id_metaid("doi:10.1234/test") is None

    def test_get_id_metaids(self):
        store = EntityStore()
        store.set_id_metaid("doi:10.1234/test", "br/0601")
        store.set_id_metaid("pmid:12345", "br/0602")
        store.set_id_metaid("orcid:0000-0001-2345-6789", "ra/0603")
        result = store.get_id_metaids()
        assert result == {
            "doi:10.1234/test": "br/0601",
            "pmid:12345": "br/0602",
            "orcid:0000-0001-2345-6789": "ra/0603",
        }

    def test_get_id_metaids_empty(self):
        store = EntityStore()
        assert store.get_id_metaids() == {}

    def test_get_id_metaids_returns_copy(self):
        store = EntityStore()
        store.set_id_metaid("doi:10.1234/test", "br/0601")
        result = store.get_id_metaids()
        result["doi:10.5678/other"] = "br/0602"
        assert store.get_id_metaid("doi:10.5678/other") is None

    def test_merge_entities_updates_reverse_index(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/a")
        store.add_id("wannabe_1", "doi:10.1234/b")
        store.merge_entities("wannabe_0", "wannabe_1")
        assert store.find_entities("doi:10.1234/a") == {"wannabe_0"}
        assert store.find_entities("doi:10.1234/b") == {"wannabe_0"}

    def test_bidirectional_sync(self):
        store = EntityStore()
        store.add_id("wannabe_0", "doi:10.1234/test")
        store.add_id("wannabe_0", "pmid:12345")
        assert store.get_ids("wannabe_0") == {"doi:10.1234/test", "pmid:12345"}
        assert store.find_entities("doi:10.1234/test") == {"wannabe_0"}
        assert store.find_entities("pmid:12345") == {"wannabe_0"}
