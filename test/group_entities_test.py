import csv
import os
import shutil
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from oc_meta.run.merge.group_entities import (
    UnionFind,
    get_all_related_entities,
    group_entities,
    optimize_groups,
    save_grouped_entities,
)

BASE = os.path.join("test", "group_entities")
OUTPUT = os.path.join(BASE, "output")


class TestUnionFind(unittest.TestCase):
    """Test UnionFind data structure for correctness and edge cases"""

    def setUp(self):
        self.uf = UnionFind()

    def test_find_single_element(self):
        """Test find on a single element"""
        result = self.uf.find("entity1")
        self.assertEqual(result, "entity1")

    def test_union_two_elements(self):
        """Test union of two elements"""
        self.uf.union("entity1", "entity2")
        root1 = self.uf.find("entity1")
        root2 = self.uf.find("entity2")
        self.assertEqual(root1, root2)

    def test_union_multiple_elements(self):
        """Test union of multiple elements forms single group"""
        self.uf.union("entity1", "entity2")
        self.uf.union("entity2", "entity3")
        self.uf.union("entity3", "entity4")

        root = self.uf.find("entity1")
        self.assertEqual(self.uf.find("entity2"), root)
        self.assertEqual(self.uf.find("entity3"), root)
        self.assertEqual(self.uf.find("entity4"), root)

    def test_separate_groups(self):
        """Test that separate unions create separate groups"""
        self.uf.union("entity1", "entity2")
        self.uf.union("entity3", "entity4")

        root1 = self.uf.find("entity1")
        root3 = self.uf.find("entity3")
        self.assertNotEqual(root1, root3)

    def test_path_compression(self):
        """Test that path compression works to flatten structure"""
        self.uf.union("entity1", "entity2")
        self.uf.union("entity2", "entity3")
        self.uf.union("entity3", "entity4")

        self.uf.find("entity4")

        self.assertIn("entity4", self.uf.parent)

    def test_find_long_chain(self):
        """Test find on a long chain of unions"""
        for i in range(100):
            self.uf.union(f"entity{i}", f"entity{i+1}")

        root = self.uf.find("entity0")
        for i in range(101):
            self.assertEqual(self.uf.find(f"entity{i}"), root)

    def test_circular_reference_bug(self):
        """Test that circular references raise ValueError"""
        self.uf.parent["entity1"] = "entity2"
        self.uf.parent["entity2"] = "entity3"
        self.uf.parent["entity3"] = "entity1"

        with self.assertRaises(ValueError) as context:
            self.uf.find("entity1")

        self.assertIn("Cycle detected", str(context.exception))

    def test_self_loop(self):
        """Test handling of self-loop (should not happen)"""
        self.uf.parent["entity1"] = "entity1"
        result = self.uf.find("entity1")
        self.assertEqual(result, "entity1")


class TestQuerySPARQL(unittest.TestCase):
    """Test SPARQL query functions"""

    @patch('oc_meta.run.merge.group_entities.SPARQLWrapper')
    def test_query_sparql_batch(self, mock_sparql):
        """Test batch querying for related entities"""
        mock_instance = MagicMock()
        mock_sparql.return_value = mock_instance
        mock_instance.query.return_value.convert.return_value = {
            'results': {
                'bindings': [
                    {'entity': {'value': 'https://example.org/related1', 'type': 'uri'}},
                    {'entity': {'value': 'https://example.org/related2', 'type': 'uri'}}
                ]
            }
        }

        from oc_meta.run.merge.group_entities import query_sparql_batch
        result = query_sparql_batch("http://endpoint",
                                    ["https://example.org/test1", "https://example.org/test2"])
        self.assertEqual(len(result), 2)
        self.assertIn('https://example.org/related1', result)
        self.assertIn('https://example.org/related2', result)

    @patch('oc_meta.run.merge.group_entities.SPARQLWrapper')
    def test_query_sparql_batch_large_input(self, mock_sparql):
        """Test batch processing with large input (multiple batches)"""
        mock_instance = MagicMock()
        mock_sparql.return_value = mock_instance
        mock_instance.query.return_value.convert.return_value = {
            'results': {'bindings': []}
        }

        from oc_meta.run.merge.group_entities import query_sparql_batch
        uris = [f"https://example.org/entity{i}" for i in range(25)]
        query_sparql_batch("http://endpoint", uris, batch_size=10)

        self.assertEqual(mock_instance.query.call_count, 3)

    @patch('oc_meta.run.merge.group_entities.SPARQLWrapper')
    def test_query_sparql_batch_empty_results(self, mock_sparql):
        """Test handling of empty results"""
        mock_instance = MagicMock()
        mock_sparql.return_value = mock_instance
        mock_instance.query.return_value.convert.return_value = {
            'results': {'bindings': []}
        }

        from oc_meta.run.merge.group_entities import query_sparql_batch
        result = query_sparql_batch("http://endpoint", ["https://example.org/test"])
        self.assertEqual(len(result), 0)

    @patch('oc_meta.run.merge.group_entities.SPARQLWrapper')
    def test_query_sparql_batch_retry_on_failure(self, mock_sparql):
        """Test that retry mechanism works on failures"""
        mock_instance = MagicMock()
        mock_sparql.return_value = mock_instance
        mock_instance.query.side_effect = [
            Exception("Connection error"),
            MagicMock(convert=MagicMock(return_value={'results': {'bindings': []}}))
        ]

        from oc_meta.run.merge.group_entities import query_sparql_batch
        result = query_sparql_batch("http://endpoint", ["https://example.org/test"])
        self.assertEqual(len(result), 0)
        self.assertEqual(mock_instance.query.call_count, 2)

    @patch('oc_meta.run.merge.group_entities.SPARQLWrapper')
    def test_query_sparql_batch_filters_literals(self, mock_sparql):
        """Test that literal values are filtered out (only URIs)"""
        mock_instance = MagicMock()
        mock_sparql.return_value = mock_instance
        mock_instance.query.return_value.convert.return_value = {
            'results': {
                'bindings': [
                    {'entity': {'value': 'https://example.org/uri1', 'type': 'uri'}},
                    {'entity': {'value': 'Some Literal', 'type': 'literal'}},
                    {'entity': {'value': 'https://example.org/uri2', 'type': 'uri'}}
                ]
            }
        }

        from oc_meta.run.merge.group_entities import query_sparql_batch
        result = query_sparql_batch("http://endpoint", ["https://example.org/test"])
        self.assertEqual(len(result), 2)
        self.assertIn('https://example.org/uri1', result)
        self.assertIn('https://example.org/uri2', result)
        self.assertNotIn('Some Literal', result)


class TestGetAllRelatedEntities(unittest.TestCase):
    """Test get_all_related_entities function"""

    @patch('oc_meta.run.merge.group_entities.query_sparql_batch')
    def test_get_all_related_entities_performance_fixed(self, mock_query_batch):
        """Test that batch querying is used (performance fix)"""
        mock_query_batch.return_value = set()

        uris = [f"https://example.org/entity{i}" for i in range(10)]
        get_all_related_entities("http://endpoint", uris)

        self.assertEqual(mock_query_batch.call_count, 1)

    @patch('oc_meta.run.merge.group_entities.query_sparql_batch')
    def test_get_all_related_entities_performance_large_batch(self, mock_query_batch):
        """Test performance with 100 URIs (should be ~10 queries with batch_size=10)"""
        mock_query_batch.return_value = set()

        uris = [f"https://example.org/entity{i}" for i in range(100)]
        get_all_related_entities("http://endpoint", uris, batch_size=10)

        self.assertEqual(mock_query_batch.call_count, 1)

    @patch('oc_meta.run.merge.group_entities.query_sparql_batch')
    def test_get_all_related_entities_includes_input_uris(self, mock_query_batch):
        """Test that input URIs are included in results"""
        mock_query_batch.return_value = set()

        uris = ["https://example.org/entity1", "https://example.org/entity2"]
        result = get_all_related_entities("http://endpoint", uris)

        self.assertIn("https://example.org/entity1", result)
        self.assertIn("https://example.org/entity2", result)

    @patch('oc_meta.run.merge.group_entities.query_sparql_batch')
    def test_get_all_related_entities_combines_results(self, mock_query_batch):
        """Test that batch results are combined with input URIs"""
        mock_query_batch.return_value = {
            "https://example.org/related1",
            "https://example.org/related2"
        }

        result = get_all_related_entities("http://endpoint", ["https://example.org/entity1"])

        self.assertIn("https://example.org/entity1", result)
        self.assertIn("https://example.org/related1", result)
        self.assertIn("https://example.org/related2", result)
        self.assertEqual(len(result), 3)


class TestOptimizeGroups(unittest.TestCase):
    """Test optimize_groups function"""

    def test_optimize_groups_combines_single_groups(self):
        """Test that single-row groups are combined"""
        grouped_data = {
            "group1": pd.DataFrame([{"surviving_entity": "e1", "merged_entities": "e2"}]),
            "group2": pd.DataFrame([{"surviving_entity": "e3", "merged_entities": "e4"}]),
            "group3": pd.DataFrame([{"surviving_entity": "e5", "merged_entities": "e6"}]),
        }

        result = optimize_groups(grouped_data, target_size=2)

        combined_count = sum(1 for df in result.values() if len(df) >= 2)
        self.assertGreater(combined_count, 0)

    def test_optimize_groups_preserves_multi_groups(self):
        """Test that multi-row groups are preserved and singles are combined"""
        grouped_data = {
            "group1": pd.DataFrame([
                {"surviving_entity": "e1", "merged_entities": "e2"},
                {"surviving_entity": "e3", "merged_entities": "e4"}
            ]),
            "group2": pd.DataFrame([{"surviving_entity": "e5", "merged_entities": "e6"}]),
            "group3": pd.DataFrame([{"surviving_entity": "e7", "merged_entities": "e8"}]),
        }

        result = optimize_groups(grouped_data, target_size=2)

        has_two_row_group = any(len(df) == 2 for df in result.values())
        self.assertTrue(has_two_row_group)

        total_rows = sum(len(df) for df in result.values())
        self.assertEqual(total_rows, 4)

    def test_optimize_groups_handles_empty_input(self):
        """Test handling of empty input"""
        result = optimize_groups({}, target_size=10)
        self.assertEqual(len(result), 0)

    def test_optimize_groups_data_loss_bug(self):
        """Test for data loss bug when remaining group < target_size"""
        grouped_data = {}
        for i in range(35):
            grouped_data[f"group{i}"] = pd.DataFrame([{
                "surviving_entity": f"e{i}",
                "merged_entities": f"e{i+100}"
            }])

        result = optimize_groups(grouped_data, target_size=50)

        total_rows_input = sum(len(df) for df in grouped_data.values())
        total_rows_output = sum(len(df) for df in result.values())

        self.assertEqual(total_rows_input, total_rows_output,
                        "Data loss detected: not all rows preserved after optimization")

    def test_optimize_groups_no_multi_groups_edge_case(self):
        """Test edge case where there are no multi-row groups"""
        grouped_data = {}
        for i in range(25):
            grouped_data[f"group{i}"] = pd.DataFrame([{
                "surviving_entity": f"e{i}",
                "merged_entities": f"e{i+100}"
            }])

        result = optimize_groups(grouped_data, target_size=10)

        total_rows_output = sum(len(df) for df in result.values())
        self.assertEqual(total_rows_output, 25)

    def test_optimize_groups_all_multi_groups(self):
        """Test when all groups are already multi-row"""
        grouped_data = {
            "group1": pd.DataFrame([
                {"surviving_entity": "e1", "merged_entities": "e2"},
                {"surviving_entity": "e3", "merged_entities": "e4"}
            ]),
            "group2": pd.DataFrame([
                {"surviving_entity": "e5", "merged_entities": "e6"},
                {"surviving_entity": "e7", "merged_entities": "e8"}
            ]),
        }

        result = optimize_groups(grouped_data, target_size=10)

        self.assertEqual(len(result), 2)
        total_rows = sum(len(df) for df in result.values())
        self.assertEqual(total_rows, 4)


class TestGroupEntities(unittest.TestCase):
    """Test group_entities function"""

    @patch('oc_meta.run.merge.group_entities.get_all_related_entities')
    def test_group_entities_creates_groups(self, mock_get_related):
        """Test that group_entities creates correct groups"""
        mock_get_related.return_value = set()

        df = pd.DataFrame([
            {"surviving_entity": "https://example.org/e1", "merged_entities": "https://example.org/e2"},
            {"surviving_entity": "https://example.org/e3", "merged_entities": "https://example.org/e4"},
        ])

        result = group_entities(df, "http://endpoint")

        self.assertGreater(len(result), 0)

    @patch('oc_meta.run.merge.group_entities.get_all_related_entities')
    def test_group_entities_handles_multiple_merged_entities(self, mock_get_related):
        """Test handling of multiple merged entities (semicolon-separated)"""
        mock_get_related.return_value = set()

        df = pd.DataFrame([
            {"surviving_entity": "https://example.org/e1",
             "merged_entities": "https://example.org/e2; https://example.org/e3; https://example.org/e4"},
        ])

        result = group_entities(df, "http://endpoint")

        self.assertGreater(len(result), 0)

    @patch('oc_meta.run.merge.group_entities.get_all_related_entities')
    def test_group_entities_single_iteration(self, mock_get_related):
        """Test that single iteration is used (performance fix)"""
        mock_get_related.return_value = set()

        df = pd.DataFrame([
            {"surviving_entity": f"https://example.org/e{i}",
             "merged_entities": f"https://example.org/e{i+100}"}
            for i in range(10)
        ])

        result = group_entities(df, "http://endpoint")

        self.assertEqual(mock_get_related.call_count, 10)
        self.assertGreater(len(result), 0)

    @patch('oc_meta.run.merge.group_entities.get_all_related_entities')
    def test_group_entities_no_double_iteration(self, mock_get_related):
        """Test that DataFrame is iterated only once (not twice)"""
        mock_get_related.return_value = set()

        df_mock = MagicMock(spec=pd.DataFrame)
        df_mock.iterrows.return_value = iter([
            (0, pd.Series({"surviving_entity": "https://example.org/e1",
                          "merged_entities": "https://example.org/e2"})),
            (1, pd.Series({"surviving_entity": "https://example.org/e3",
                          "merged_entities": "https://example.org/e4"})),
        ])
        df_mock.shape = (2,)

        group_entities(df_mock, "http://endpoint")

        self.assertEqual(df_mock.iterrows.call_count, 1,
                        "DataFrame.iterrows() should be called only once")


class TestSaveGroupedEntities(unittest.TestCase):
    """Test save_grouped_entities function"""

    def setUp(self):
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)
        os.makedirs(OUTPUT, exist_ok=True)

    def tearDown(self):
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)

    def test_save_grouped_entities_creates_files(self):
        """Test that files are created correctly"""
        grouped_data = {
            "https://example.org/e1": pd.DataFrame([
                {"surviving_entity": "e1", "merged_entities": "e2"}
            ]),
            "https://example.org/e2": pd.DataFrame([
                {"surviving_entity": "e3", "merged_entities": "e4"}
            ]),
        }

        save_grouped_entities(grouped_data, OUTPUT)

        files = os.listdir(OUTPUT)
        self.assertEqual(len(files), 2)
        self.assertTrue(all(f.endswith('.csv') for f in files))

    def test_save_grouped_entities_preserves_data(self):
        """Test that saved data matches input data"""
        grouped_data = {
            "https://example.org/e1": pd.DataFrame([
                {"surviving_entity": "e1", "merged_entities": "e2"},
                {"surviving_entity": "e3", "merged_entities": "e4"}
            ])
        }

        save_grouped_entities(grouped_data, OUTPUT)

        saved_file = os.path.join(OUTPUT, "e1.csv")
        self.assertTrue(os.path.exists(saved_file))

        loaded_df = pd.read_csv(saved_file)
        self.assertEqual(len(loaded_df), 2)
        self.assertIn("surviving_entity", loaded_df.columns)
        self.assertIn("merged_entities", loaded_df.columns)

    def test_save_grouped_entities_handles_special_characters(self):
        """Test handling of special characters in URIs"""
        grouped_data = {
            "https://example.org/e1?param=value": pd.DataFrame([
                {"surviving_entity": "e1", "merged_entities": "e2"}
            ])
        }

        save_grouped_entities(grouped_data, OUTPUT)

        files = os.listdir(OUTPUT)
        self.assertEqual(len(files), 1)

    def test_save_grouped_entities_creates_output_dir(self):
        """Test that output directory is created if it doesn't exist"""
        new_output = os.path.join(OUTPUT, "subdir", "nested")

        grouped_data = {
            "https://example.org/e1": pd.DataFrame([
                {"surviving_entity": "e1", "merged_entities": "e2"}
            ])
        }

        save_grouped_entities(grouped_data, new_output)

        self.assertTrue(os.path.exists(new_output))
        files = os.listdir(new_output)
        self.assertEqual(len(files), 1)


class TestIntegration(unittest.TestCase):
    """Integration tests for complete workflow"""

    def setUp(self):
        if os.path.exists(BASE):
            shutil.rmtree(BASE)
        os.makedirs(BASE, exist_ok=True)
        os.makedirs(OUTPUT, exist_ok=True)

    def tearDown(self):
        if os.path.exists(BASE):
            shutil.rmtree(BASE)

    def test_missing_csv_columns_validation_bug(self):
        """Test that missing required columns causes proper error (validation bug)"""
        from oc_meta.run.merge.group_entities import load_csv

        csv_path = os.path.join(BASE, "invalid.csv")
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["wrong_column"])
            writer.writeheader()
            writer.writerow({"wrong_column": "value"})

        with self.assertRaises(ValueError) as context:
            load_csv(csv_path)

        self.assertIn("missing required columns", str(context.exception))

    @patch('oc_meta.run.merge.group_entities.get_all_related_entities')
    def test_complete_workflow(self, mock_get_related):
        """Test complete workflow from CSV to grouped output"""
        mock_get_related.return_value = set()

        csv_path = os.path.join(BASE, "input.csv")
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["surviving_entity", "merged_entities"])
            writer.writeheader()
            for i in range(5):
                writer.writerow({
                    "surviving_entity": f"https://example.org/e{i}",
                    "merged_entities": f"https://example.org/e{i+100}"
                })

        df = pd.read_csv(csv_path)
        grouped = group_entities(df, "http://endpoint")
        optimized = optimize_groups(grouped, target_size=2)
        save_grouped_entities(optimized, OUTPUT)

        output_files = os.listdir(OUTPUT)
        self.assertGreater(len(output_files), 0)

        total_rows = 0
        for file in output_files:
            file_path = os.path.join(OUTPUT, file)
            df_saved = pd.read_csv(file_path)
            total_rows += len(df_saved)

        self.assertEqual(total_rows, 5)


if __name__ == "__main__":
    unittest.main()
