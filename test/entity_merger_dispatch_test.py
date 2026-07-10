# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from unittest.mock import MagicMock, call, patch

from oc_meta.run.merge.entities import EntityMerger


def _run_process_rows(rows, closure):
    merger = EntityMerger("meta.yaml", "https://example.org/agent")
    meta_editor = MagicMock()
    meta_editor.base_iri = "https://w3id.org/oc/meta/"
    meta_editor.counter_handler = object()
    meta_editor.endpoint = "http://endpoint"
    meta_editor.resp_agent = "https://example.org/agent"
    meta_editor.entity_cache.is_cached.return_value = False
    graph_set = MagicMock()
    storage = MagicMock()

    with (
        patch("oc_meta.run.merge.entities.MetaEditor", return_value=meta_editor),
        patch("oc_meta.run.merge.entities.GraphSet", return_value=graph_set),
        patch(
            "oc_meta.run.merge.entities.compute_identifier_merge_closure",
            return_value=closure,
        ) as compute_identifier_merge_closure,
        patch(
            "oc_meta.run.merge.entities.compute_related_closure",
            return_value=closure,
        ) as compute_related_closure,
        patch.object(merger, "create_storage", return_value=storage) as create_storage,
        patch.object(merger, "merge_clusters_and_save") as merge_clusters_and_save,
    ):
        merger.process_rows(rows)

    return (
        meta_editor,
        graph_set,
        storage,
        compute_identifier_merge_closure,
        compute_related_closure,
        create_storage,
        merge_clusters_and_save,
    )


def test_process_rows_uses_identifier_closure_and_large_import_batches():
    rows = [
        {
            "surviving_entity": "https://w3id.org/oc/meta/id/0601",
            "merged_entities": ["https://w3id.org/oc/meta/id/0602"],
        }
    ]
    closure = {
        "https://w3id.org/oc/meta/id/0601",
        "https://w3id.org/oc/meta/id/0602",
        "https://w3id.org/oc/meta/ra/0601",
    }

    (
        meta_editor,
        graph_set,
        storage,
        compute_identifier_merge_closure,
        compute_related_closure,
        create_storage,
        merge_clusters_and_save,
    ) = _run_process_rows(rows, closure)

    compute_identifier_merge_closure.assert_called_once_with(
        "http://endpoint",
        ["https://w3id.org/oc/meta/id/0601"],
        ["https://w3id.org/oc/meta/id/0602"],
        1000,
    )
    assert compute_related_closure.call_count == 0
    import_kwargs = meta_editor.reader.import_entities_from_triplestore.call_args.kwargs
    assert set(import_kwargs["entities"]) == closure
    assert {
        key: value for key, value in import_kwargs.items() if key != "entities"
    } == {
        "g_set": graph_set,
        "ts_url": "http://endpoint",
        "resp_agent": "https://example.org/agent",
        "enable_validation": False,
        "batch_size": 1000,
    }
    assert meta_editor.entity_cache.add.call_args_list == [
        call(entity) for entity in import_kwargs["entities"]
    ]
    create_storage.assert_called_once_with(meta_editor, graph_set)
    merge_clusters_and_save.assert_called_once_with(
        graph_set,
        storage,
        {"https://w3id.org/oc/meta/id/0601": ["https://w3id.org/oc/meta/id/0602"]},
    )


def test_process_rows_keeps_generic_closure_for_mixed_entity_types():
    rows = [
        {
            "surviving_entity": "https://w3id.org/oc/meta/id/0601",
            "merged_entities": ["https://w3id.org/oc/meta/ra/0602"],
        }
    ]
    closure = {
        "https://w3id.org/oc/meta/id/0601",
        "https://w3id.org/oc/meta/ra/0602",
    }

    (
        meta_editor,
        graph_set,
        storage,
        compute_identifier_merge_closure,
        compute_related_closure,
        create_storage,
        merge_clusters_and_save,
    ) = _run_process_rows(rows, closure)

    assert compute_identifier_merge_closure.call_count == 0
    compute_related_closure.assert_called_once_with(
        "http://endpoint",
        {
            "https://w3id.org/oc/meta/id/0601",
            "https://w3id.org/oc/meta/ra/0602",
        },
        10,
    )
    import_kwargs = meta_editor.reader.import_entities_from_triplestore.call_args.kwargs
    assert set(import_kwargs["entities"]) == closure
    assert {
        key: value for key, value in import_kwargs.items() if key != "entities"
    } == {
        "g_set": graph_set,
        "ts_url": "http://endpoint",
        "resp_agent": "https://example.org/agent",
        "enable_validation": False,
        "batch_size": 10,
    }
    create_storage.assert_called_once_with(meta_editor, graph_set)
    merge_clusters_and_save.assert_called_once_with(
        graph_set,
        storage,
        {"https://w3id.org/oc/meta/id/0601": ["https://w3id.org/oc/meta/ra/0602"]},
    )
