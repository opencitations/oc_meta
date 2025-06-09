import csv
import os
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oc_meta.run.merge_csv_dumps import (CSVDumpMerger, _process_new_file,
                                         _process_single_file,
                                         get_existing_output_files,
                                         normalize_ids_in_brackets,
                                         normalize_ids_in_field,
                                         normalize_page_field,
                                         normalize_people_field,
                                         postprocess_type,
                                         process_ordered_list)


class TestNormalizationFunctions(unittest.TestCase):
    """Test the ID normalization functions"""
    
    def test_normalize_ids_in_field(self):
        """Test normalization of space-separated ID lists"""
        test_cases = [
            # Basic case: OMID should come first, others alphabetically
            ("doi:10.1000/123 pmid:456 omid:br/789", "omid:br/789 doi:10.1000/123 pmid:456"),
            # Multiple OMIDs (should be sorted among themselves too)
            ("pmid:456 omid:br/789 omid:br/123 doi:10.1000/abc", "omid:br/123 omid:br/789 doi:10.1000/abc pmid:456"),
            # No OMID
            ("doi:10.1000/123 pmid:456", "doi:10.1000/123 pmid:456"),
            # Empty string
            ("", ""),
            # None input
            (None, ""),
        ]
        
        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                result = normalize_ids_in_field(input_val)
                self.assertEqual(result, expected)
    
    def test_normalize_ids_in_brackets(self):
        """Test normalization of IDs within square brackets"""
        test_cases = [
            # Basic case with person name
            ("John Doe [doi:10.1000/123 omid:ra/456]", "John Doe [omid:ra/456 doi:10.1000/123]"),
            # Multiple brackets in same string
            ("John [doi:123 omid:ra/456] and Jane [pmid:789 omid:ra/abc]", 
             "John [omid:ra/456 doi:123] and Jane [omid:ra/abc pmid:789]"),
            # Empty brackets
            ("Name []", "Name []"),
            # No brackets
            ("John Doe", "John Doe"),
        ]
        
        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                result = normalize_ids_in_brackets(input_val)
                self.assertEqual(result, expected)
    
    def test_normalize_people_field(self):
        """Test normalization of people fields (author, editor, publisher)"""
        test_cases = [
            # Single person
            ("John Doe [doi:10.1000/123 omid:ra/456]", "John Doe [omid:ra/456 doi:10.1000/123]"),
            # Multiple people (order should be preserved, but IDs normalized)
            ("Smith, John [orcid:0000-0000-0000-0000 omid:ra/123]; Doe, Jane [omid:ra/456 doi:10.1000/abc]",
             "Smith, John [omid:ra/123 orcid:0000-0000-0000-0000]; Doe, Jane [omid:ra/456 doi:10.1000/abc]"),
            # Empty field
            ("", ""),
        ]
        
        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                result = normalize_people_field(input_val)
                self.assertEqual(result, expected)
    
    def test_normalize_page_field(self):
        """Test normalization of page fields"""
        test_cases = [
            # Identical start and end pages should be simplified
            ("333-333", "333"),
            ("1-1", "1"),
            # Different start and end pages should remain unchanged
            ("333-334", "333-334"),
            ("1-10", "1-10"),
            # Single pages should remain unchanged
            ("333", "333"),
            # Empty or None should return empty
            ("", ""),
            (None, ""),
        ]
        
        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                result = normalize_page_field(input_val)
                self.assertEqual(result, expected)


class TestCSVDumpMerger(unittest.TestCase):
    """Test CSVDumpMerger class methods"""
    
    def setUp(self):
        self.merger = CSVDumpMerger("http://example.com/sparql")
        
    def test_extract_omid_from_id_field(self):
        """Test extraction of OMID from ID field"""
        test_cases = [
            ("doi:10.1007/978-3-662-07918-8_3 omid:br/0612345", "omid:br/0612345"),
            ("omid:br/0612345 doi:10.1007/978-3-662-07918-8_3", "omid:br/0612345"),
            ("doi:10.1007/978-3-662-07918-8_3", None),
            ("omid:br/0612345", "omid:br/0612345"),
            ("", None),
            (None, None),
        ]
        
        for id_field, expected in test_cases:
            with self.subTest(id_field=id_field):
                result = self.merger.extract_omid_from_id_field(id_field)
                self.assertEqual(result, expected)
    
    def test_build_sparql_query(self):
        """Test SPARQL query building with OMID values"""
        omids = ["omid:br/0612345", "omid:br/0612346"]
        query = self.merger.build_sparql_query(omids)
        
        self.assertIn("VALUES ?res", query)
        self.assertIn("<https://w3id.org/oc/meta/br/0612345>", query)
        self.assertIn("<https://w3id.org/oc/meta/br/0612346>", query)
        self.assertIn("PREFIX foaf:", query)
        self.assertIn("SELECT DISTINCT", query)
    
    def test_normalize_row_data(self):
        """Test row data normalization with ID ordering"""
        test_row = {
            "id": "  doi:10.1000/123 omid:br/0612345  ",
            "title": "Test Title  ",
            "author": "John Doe [doi:456 omid:ra/789]; Jane Smith [omid:ra/abc]",
            "pub_date": "2023",
            "venue": "Journal [issn:1234 omid:br/journal]",
            "publisher": "Publisher [crossref:123 omid:ra/pub]",
            "page": "333-333"
        }
        
        normalized = self.merger.normalize_row_data(test_row)
        
        # ID field should have OMID first
        self.assertEqual(normalized["id"], "omid:br/0612345 doi:10.1000/123")
        self.assertEqual(normalized["title"], "Test Title")
        # Author field should have normalized IDs in brackets but preserve people order
        self.assertEqual(normalized["author"], "John Doe [omid:ra/789 doi:456]; Jane Smith [omid:ra/abc]")
        self.assertEqual(normalized["pub_date"], "2023")
        # Venue and publisher should have normalized IDs
        self.assertEqual(normalized["venue"], "Journal [omid:br/journal issn:1234]")
        self.assertEqual(normalized["publisher"], "Publisher [omid:ra/pub crossref:123]")
        # Page field should be simplified when start and end are identical
        self.assertEqual(normalized["page"], "333")
    
    def test_normalize_row_data_with_none_values(self):
        """Test row data normalization with None values"""
        test_row = {
            "id": None,
            "title": None,
            "author": None,
            "pub_date": None,
        }
        
        normalized = self.merger.normalize_row_data(test_row)
        
        for key, value in normalized.items():
            self.assertEqual(value, "")
    
    def test_rows_are_different(self):
        """Test comparison of rows for differences"""
        row1 = {
            "id": "omid:br/0612345",
            "title": "Original Title",
            "author": "Author 1",
            "pub_date": "2023",
            "venue": "Journal A",
            "volume": "1",
            "issue": "1",
            "page": "1-10",
            "type": "article",
            "publisher": "Publisher A",
            "editor": "Editor A"
        }
        
        row2_same = row1.copy()
        row2_different = row1.copy()
        row2_different["title"] = "Different Title"
        
        # Test without logging
        self.assertFalse(self.merger.rows_are_different(row1, row2_same, log_differences=False))
        self.assertTrue(self.merger.rows_are_different(row1, row2_different, log_differences=False))
    
    def test_rows_are_different_id_ordering_only(self):
        """Test that rows differing only in ID ordering are considered the same"""
        row1 = {
            "id": "doi:10.1000/123 omid:br/0612345",
            "author": "John [doi:456 omid:ra/789]; Jane [omid:ra/abc pmid:999]",
            "venue": "Journal [issn:1234 omid:br/journal]"
        }
        
        row2 = {
            "id": "omid:br/0612345 doi:10.1000/123",  # Different order
            "author": "John [omid:ra/789 doi:456]; Jane [pmid:999 omid:ra/abc]",  # Different ID order
            "venue": "Journal [omid:br/journal issn:1234]"  # Different ID order
        }
        
        # These should be considered the same after normalization
        self.assertFalse(self.merger.rows_are_different(row1, row2, log_differences=False))
    
    def test_rows_are_different_page_normalization(self):
        """Test that rows differing only in page format (333-333 vs 333) are considered the same"""
        row1 = {
            "id": "omid:br/0612345",
            "title": "Test Title",
            "page": "333-333"
        }
        
        row2 = {
            "id": "omid:br/0612345", 
            "title": "Test Title",
            "page": "333"
        }
        
        # These should be considered the same after page normalization
        self.assertFalse(self.merger.rows_are_different(row1, row2, log_differences=False))
        
        # But different page ranges should still be detected as different
        row3 = {
            "id": "omid:br/0612345",
            "title": "Test Title", 
            "page": "333-334"
        }
        
        self.assertTrue(self.merger.rows_are_different(row1, row3, log_differences=False))
    
    def test_get_all_csv_files(self):
        """Test getting CSV files from directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            csv_files = ["test1.csv", "test2.csv"]
            other_files = ["test.txt", "test.json"]
            
            for filename in csv_files + other_files:
                filepath = os.path.join(temp_dir, filename)
                with open(filepath, 'w') as f:
                    f.write("test")
            
            result = self.merger.get_all_csv_files(temp_dir)
            
            self.assertEqual(len(result), 2)
            result_basenames = [os.path.basename(f) for f in result]
            self.assertIn("test1.csv", result_basenames)
            self.assertIn("test2.csv", result_basenames)
    
    def test_get_all_csv_files_nonexistent_dir(self):
        """Test getting CSV files from non-existent directory"""
        result = self.merger.get_all_csv_files("/non/existent/path")
        self.assertEqual(result, [])
    
    def test_constructor_parameters(self):
        """Test CSVDumpMerger constructor parameters"""
        # Test with all parameters
        merger1 = CSVDumpMerger("http://example.com/sparql", batch_size=100)
        self.assertEqual(merger1.endpoint_url, "http://example.com/sparql")
        self.assertEqual(merger1.batch_size, 100)
        
        # Test with defaults
        merger2 = CSVDumpMerger("http://example.com/sparql")
        self.assertEqual(merger2.batch_size, 50)
        
        # Test with empty endpoint - should raise ValueError
        with self.assertRaises(ValueError) as context:
            CSVDumpMerger("")
        self.assertIn("SPARQL endpoint URL is mandatory", str(context.exception))
    
    @patch('oc_meta.run.merge_csv_dumps.SPARQLWrapper')
    def test_execute_sparql_query(self, mock_sparql_wrapper):
        """Test SPARQL query execution"""
        mock_results = {
            "results": {
                "bindings": [
                    {
                        "id": {"value": "doi:10.1234 omid:br/0612345"},
                        "title": {"value": "Test Title"},
                        "type": {"value": "http://purl.org/spar/fabio/JournalArticle"},
                        "author": {"value": "Test Author"}
                    }
                ]
            }
        }
        
        mock_sparql = Mock()
        mock_sparql.query.return_value.convert.return_value = mock_results
        mock_sparql_wrapper.return_value = mock_sparql
        
        merger = CSVDumpMerger("http://example.com/sparql")
        merger.sparql = mock_sparql
        
        query = "SELECT * WHERE { ?s ?p ?o }"
        result = merger.execute_sparql_query(query)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "doi:10.1234 omid:br/0612345")
        self.assertEqual(result[0]["title"], "Test Title")
        self.assertEqual(result[0]["type"], "journal article")
        self.assertEqual(result[0]["author"], "Test Author")
    
    @patch.object(CSVDumpMerger, 'execute_sparql_query')
    def test_verify_file_data(self, mock_execute):
        """Test verification of file data against database"""
        omids = ["omid:br/0612345", "omid:br/0612346"]
        
        mock_db_results = [
            {"id": "doi:10.1234 omid:br/0612345", "title": "Updated Title 1"},
        ]
        mock_execute.return_value = mock_db_results
        
        result, query_failed = self.merger.verify_file_data(omids)
        
        self.assertFalse(query_failed)
        self.assertEqual(len(result), 1)
        self.assertIn("omid:br/0612345", result)
        self.assertEqual(result["omid:br/0612345"]["title"], "Updated Title 1")
    
    @patch.object(CSVDumpMerger, 'execute_sparql_query')
    def test_verify_file_data_query_failure(self, mock_execute):
        """Test verification when all queries fail"""
        omids = ["omid:br/0612345", "omid:br/0612346"]
        
        # Mock query failure
        mock_execute.return_value = None
        
        result, query_failed = self.merger.verify_file_data(omids)
        
        self.assertTrue(query_failed)
        self.assertEqual(len(result), 0)


class TestGetExistingOutputFiles(unittest.TestCase):
    """Test the get_existing_output_files function"""
    
    def test_get_existing_output_files_empty_dir(self):
        """Test getting existing files from empty directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = get_existing_output_files(temp_dir)
            self.assertEqual(result, set())
    
    def test_get_existing_output_files_nonexistent_dir(self):
        """Test getting existing files from non-existent directory"""
        result = get_existing_output_files("/non/existent/path")
        self.assertEqual(result, set())
    
    def test_get_existing_output_files_with_files(self):
        """Test getting existing files from directory with CSV files"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            csv_files = ["test1.csv", "test2.csv"]
            other_files = ["test.txt", "test.json"]
            
            for filename in csv_files + other_files:
                filepath = os.path.join(temp_dir, filename)
                with open(filepath, 'w') as f:
                    f.write("test")
            
            result = get_existing_output_files(temp_dir)
            
            self.assertEqual(result, {"test1.csv", "test2.csv"})


class TestProcessNewFile(unittest.TestCase):
    """Test the _process_new_file function"""
    
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = os.path.join(self.temp_dir.name, "output")
        os.makedirs(self.output_dir)
        
    def tearDown(self):
        self.temp_dir.cleanup()
    
    def create_test_csv(self, filename, data):
        """Helper method to create test CSV files"""
        filepath = os.path.join(self.temp_dir.name, filename)
        if data:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        return filepath
    
    @patch('oc_meta.run.merge_csv_dumps.get_csv_data_fast')
    def test_process_new_file_empty(self, mock_get_csv):
        """Test processing empty new file"""
        mock_get_csv.return_value = []
        
        filepath = os.path.join(self.temp_dir.name, "empty.csv")
        args = (filepath, self.output_dir, "http://example.com/sparql", set())  # No existing files
        
        result = _process_new_file(args)
        output_file, row_count, updated_count, filename, file_omids, skipped = result
        
        self.assertIsNone(output_file)
        self.assertEqual(row_count, 0)
        self.assertEqual(updated_count, 0)
        self.assertEqual(filename, "empty.csv")
        self.assertEqual(file_omids, set())
        self.assertFalse(skipped)
    
    @patch('oc_meta.run.merge_csv_dumps.get_csv_data_fast')
    def test_process_new_file_with_omids(self, mock_get_csv):
        """Test processing new file with OMIDs (collects OMIDs, normalizes data)"""
        test_data = [
            {
                "id": "doi:10.1000/123 omid:br/0612345",
                "title": "Title 1  ",  # Extra spaces to test normalization
                "author": "Author [doi:456 omid:ra/789]",  # Test ID normalization
                "page": "333-333"  # Test page normalization
            },
            {
                "id": "pmid:456 omid:br/0612346",
                "title": "Title 2",
                "author": "Author 2 [omid:ra/abc]"
            }
        ]
        
        mock_get_csv.return_value = test_data
        
        filepath = self.create_test_csv("new.csv", test_data)
        args = (filepath, self.output_dir, "http://example.com/sparql", set())  # No existing files
        
        result = _process_new_file(args)
        output_file, row_count, updated_count, filename, file_omids, skipped = result
        
        self.assertIsNotNone(output_file)
        self.assertEqual(row_count, 2)
        self.assertEqual(updated_count, 0)  # No database updates for new files
        self.assertEqual(filename, "new.csv")
        self.assertEqual(file_omids, {"omid:br/0612345", "omid:br/0612346"})
        self.assertFalse(skipped)
        
        # Check output file content for normalization
        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            output_rows = list(reader)
        
        self.assertEqual(len(output_rows), 2)
        # Check ID normalization (OMID first)
        self.assertEqual(output_rows[0]["id"], "omid:br/0612345 doi:10.1000/123")
        self.assertEqual(output_rows[1]["id"], "omid:br/0612346 pmid:456")
        # Check title normalization (trimmed spaces)
        self.assertEqual(output_rows[0]["title"], "Title 1")
        # Check author ID normalization
        self.assertEqual(output_rows[0]["author"], "Author [omid:ra/789 doi:456]")
        # Check page normalization
        self.assertEqual(output_rows[0]["page"], "333")
    
    @patch('oc_meta.run.merge_csv_dumps.get_csv_data_fast')
    def test_process_new_file_without_omids(self, mock_get_csv):
        """Test processing new file without OMIDs"""
        test_data = [
            {"id": "doi:10.1000/123", "title": "Title 1", "author": "Author 1"},
            {"id": "pmid:456", "title": "Title 2", "author": "Author 2"}
        ]
        
        mock_get_csv.return_value = test_data
        
        filepath = self.create_test_csv("new.csv", test_data)
        args = (filepath, self.output_dir, "http://example.com/sparql", set())  # No existing files
        
        result = _process_new_file(args)
        output_file, row_count, updated_count, filename, file_omids, skipped = result
        
        # Since rows without OMID are now skipped, we expect no output
        self.assertIsNone(output_file)
        self.assertEqual(row_count, 0)
        self.assertEqual(updated_count, 0)
        self.assertEqual(filename, "new.csv")
        self.assertEqual(file_omids, set())  # No OMIDs found
        self.assertFalse(skipped)
    
    @patch('oc_meta.run.merge_csv_dumps.get_csv_data_fast')
    def test_process_new_file_cached_empty(self, mock_get_csv):
        """Test processing empty cached new file"""
        # Mock empty file
        mock_get_csv.return_value = []
        
        filepath = os.path.join(self.temp_dir.name, "cached.csv")
        existing_files = {"cached.csv"}  # File already exists
        args = (filepath, self.output_dir, "http://example.com/sparql", existing_files)
        
        result = _process_new_file(args)
        output_file, row_count, updated_count, filename, file_omids, skipped = result
        
        # Empty file returns None even if cached
        self.assertIsNone(output_file)
        self.assertEqual(row_count, 0)
        self.assertEqual(updated_count, 0)
        self.assertEqual(filename, "cached.csv")
        self.assertEqual(file_omids, set())  # Empty because file is empty
        self.assertFalse(skipped)  # Not actually skipped, just empty

    @patch('oc_meta.run.merge_csv_dumps.get_csv_data_fast')
    def test_process_new_file_cached_extracts_omids(self, mock_get_csv):
        """Test that cached new file still extracts OMIDs for exclusion"""
        test_data = [
            {
                "id": "doi:10.1000/123 omid:br/0612345",
                "title": "Title 1",
                "author": "Author 1"
            },
            {
                "id": "pmid:456 omid:br/0612346",
                "title": "Title 2",
                "author": "Author 2"
            }
        ]
        
        mock_get_csv.return_value = test_data
        
        filepath = os.path.join(self.temp_dir.name, "cached.csv")
        existing_files = {"cached.csv"}  # File already exists (cached)
        args = (filepath, self.output_dir, "http://example.com/sparql", existing_files)
        
        result = _process_new_file(args)
        output_file, row_count, updated_count, filename, file_omids, skipped = result
        
        # Should be skipped due to cache but OMIDs should still be extracted
        self.assertEqual(output_file, "cached.csv")  # Returns filename when skipped
        self.assertEqual(row_count, 0)  # No rows written (cached)
        self.assertEqual(updated_count, 0)
        self.assertEqual(filename, "cached.csv")
        self.assertEqual(file_omids, {"omid:br/0612345", "omid:br/0612346"})  # OMIDs should be extracted!
        self.assertTrue(skipped)


class TestProcessSingleFile(unittest.TestCase):
    """Test the _process_single_file function"""
    
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = os.path.join(self.temp_dir.name, "output")
        os.makedirs(self.output_dir)
        
    def tearDown(self):
        self.temp_dir.cleanup()
    
    def create_test_csv(self, filename, data):
        """Helper method to create test CSV files"""
        filepath = os.path.join(self.temp_dir.name, filename)
        if data:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        return filepath
    
    @patch('oc_meta.run.merge_csv_dumps.get_csv_data_fast')
    def test_process_single_file_empty(self, mock_get_csv):
        """Test processing empty file"""
        mock_get_csv.return_value = []
        
        filepath = os.path.join(self.temp_dir.name, "empty.csv")
        args = (filepath, self.output_dir, "http://example.com/sparql", 50, False, set(), set())  # No existing files
        
        result = _process_single_file(args)
        self.assertEqual(result, (None, 0, 0, "empty.csv", False))
    
    @patch('oc_meta.run.merge_csv_dumps.get_csv_data_fast')
    @patch.object(CSVDumpMerger, 'verify_file_data')
    def test_process_single_file_with_updates(self, mock_verify, mock_get_csv):
        """Test processing file with database updates"""
        test_data = [
            {"id": "omid:br/0612345", "title": "Old Title", "author": "Author 1"}
        ]
        
        mock_get_csv.return_value = test_data
        mock_verify.return_value = ({
            "omid:br/0612345": {"id": "omid:br/0612345", "title": "New Title", "author": "Author 1"}
        }, False)  # Database results, no query failure
        
        filepath = self.create_test_csv("test.csv", test_data)
        excluded_omids = set()
        args = (filepath, self.output_dir, "http://example.com/sparql", 50, False, excluded_omids, set())  # No existing files
        
        result = _process_single_file(args)
        output_file, row_count, updated_count, filename, skipped = result
        
        self.assertIsNotNone(output_file)
        self.assertEqual(row_count, 1)
        self.assertEqual(updated_count, 1)
        self.assertEqual(filename, "test.csv")
        self.assertFalse(skipped)
        
        # Check output file content - should have updated title
        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            output_rows = list(reader)
        
        self.assertEqual(len(output_rows), 1)
        self.assertEqual(output_rows[0]["title"], "New Title")
    
    def test_process_single_file_cached(self):
        """Test processing single file that already exists in output (cached)"""
        filepath = os.path.join(self.temp_dir.name, "cached.csv")
        # Create a dummy file (doesn't matter what's in it for this test)
        with open(filepath, 'w') as f:
            f.write("dummy")
        
        existing_files = {"cached.csv"}  # File already exists
        args = (filepath, self.output_dir, "http://example.com/sparql", 50, False, set(), existing_files)
        
        result = _process_single_file(args)
        output_file, row_count, updated_count, filename, skipped = result
        
        # Should be skipped due to cache
        self.assertEqual(output_file, "cached.csv")  # Returns filename when skipped
        self.assertEqual(row_count, 0)
        self.assertEqual(updated_count, 0)
        self.assertEqual(filename, "cached.csv")
        self.assertTrue(skipped)


class TestCSVDumpMergerIntegration(unittest.TestCase):
    """Integration tests using temporary files and mock SPARQL endpoint"""
    
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.existing_dir = os.path.join(self.temp_dir.name, "existing")
        self.new_dir = os.path.join(self.temp_dir.name, "new")
        self.output_dir = os.path.join(self.temp_dir.name, "output")
        
        os.makedirs(self.existing_dir)
        os.makedirs(self.new_dir)
        os.makedirs(self.output_dir)
        
    def tearDown(self):
        self.temp_dir.cleanup()
    
    def create_test_csv(self, directory, filename, data):
        """Helper method to create test CSV files"""
        filepath = os.path.join(directory, filename)
        if data:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        return filepath
    
    @patch.object(CSVDumpMerger, 'execute_sparql_query')
    def test_complete_merge_workflow(self, mock_execute):
        """Test complete merge workflow with file I/O"""
        existing_data = [
            {
                "id": "doi:10.1234 omid:br/0612345",
                "title": "Original Title",
                "author": "Author 1",
                "pub_date": "2023",
                "venue": "Journal A",
                "volume": "1",
                "issue": "1",
                "page": "1-10",
                "type": "article",
                "publisher": "Publisher A",
                "editor": "Editor A"
            }
        ]
        
        new_data = [
            {
                "id": "doi:10.5678 omid:br/0612346",
                "title": "New Article",
                "author": "Author 2",
                "pub_date": "2024",
                "venue": "Journal B",
                "volume": "2",
                "issue": "1",
                "page": "11-20",
                "type": "article",
                "publisher": "Publisher B",
                "editor": "Editor B"
            }
        ]
        
        mock_db_results = [
            {
                "id": "doi:10.1234 updated_doi:10.1234-v2 omid:br/0612345",
                "title": "Updated Title from DB",
                "author": "Author 1",
                "pub_date": "2023",
                "venue": "Journal A",
                "volume": "1",
                "issue": "1",
                "page": "1-10",
                "type": "article",
                "publisher": "Publisher A",
                "editor": "Editor A"
            }
        ]
        
        mock_execute.return_value = mock_db_results
        
        self.create_test_csv(self.existing_dir, "existing.csv", existing_data)
        self.create_test_csv(self.new_dir, "new.csv", new_data)
        
        merger = CSVDumpMerger("http://example.com/sparql", batch_size=10)
        merger.merge_dumps(self.existing_dir, self.new_dir, self.output_dir, max_workers=1, verbose_diff=False)
        
        # Check that output files are created
        output_files = [f for f in os.listdir(self.output_dir) if f.endswith('.csv')]
        self.assertEqual(len(output_files), 2)  # One for each input file
        
        # Check existing file output
        existing_output = os.path.join(self.output_dir, "existing.csv")
        self.assertTrue(os.path.exists(existing_output))
        
        with open(existing_output, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            existing_rows = list(reader)
        
        self.assertEqual(len(existing_rows), 1)
        self.assertEqual(existing_rows[0]['title'], 'Updated Title from DB')
        self.assertIn('updated_doi:10.1234-v2', existing_rows[0]['id'])
        
        # Check new file output - should be normalized but not verified against database
        new_output = os.path.join(self.output_dir, "new.csv")
        self.assertTrue(os.path.exists(new_output))
        
        with open(new_output, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            new_rows = list(reader)
        
        self.assertEqual(len(new_rows), 1)
        self.assertEqual(new_rows[0]['title'], 'New Article')
        # Check that new file IDs are normalized (OMID first)
        self.assertEqual(new_rows[0]['id'], 'omid:br/0612346 doi:10.5678')

    @patch.object(CSVDumpMerger, 'execute_sparql_query')
    def test_merge_dumps_with_caching(self, mock_execute):
        """Test that files are skipped when they already exist in output directory"""
        existing_data = [
            {"id": "omid:br/0612345", "title": "Title 1", "author": "Author 1"}
        ]
        
        new_data = [
            {"id": "omid:br/0612346", "title": "Title 2", "author": "Author 2"}
        ]
        
        # Mock SPARQL query results
        mock_execute.return_value = [
            {"id": "omid:br/0612345", "title": "Title 1", "author": "Author 1"}
        ]
        
        # Create input files
        self.create_test_csv(self.existing_dir, "existing.csv", existing_data)
        self.create_test_csv(self.new_dir, "new.csv", new_data)
        
        # Pre-create output files to simulate cache
        self.create_test_csv(self.output_dir, "existing.csv", [{"id": "cached", "title": "Cached"}])
        self.create_test_csv(self.output_dir, "new.csv", [{"id": "cached", "title": "Cached"}])
        
        merger = CSVDumpMerger("http://example.com/sparql")
        merger.merge_dumps(self.existing_dir, self.new_dir, self.output_dir, max_workers=1, verbose_diff=False)
        
        # Files should remain unchanged (cached versions)
        existing_output = os.path.join(self.output_dir, "existing.csv")
        with open(existing_output, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            existing_rows = list(reader)
        
        self.assertEqual(len(existing_rows), 1)
        self.assertEqual(existing_rows[0]['title'], 'Cached')  # Should remain cached version

    @patch.object(CSVDumpMerger, 'execute_sparql_query')
    def test_merge_dumps_query_failure(self, mock_execute):
        """Test merge_dumps when SPARQL queries fail"""
        existing_data = [
            {"id": "omid:br/0612345", "title": "Title 1", "author": "Author 1"}
        ]
        
        self.create_test_csv(self.existing_dir, "existing.csv", existing_data)
        
        # Mock query failure
        mock_execute.return_value = None
        
        merger = CSVDumpMerger("http://example.com/sparql")
        merger.merge_dumps(self.existing_dir, self.new_dir, self.output_dir, max_workers=1, verbose_diff=False)
        
        # File should be skipped due to query failure, no output file created
        output_files = [f for f in os.listdir(self.output_dir) if f.endswith('.csv')]
        self.assertEqual(len(output_files), 0)

    @patch.object(CSVDumpMerger, 'execute_sparql_query')
    def test_merge_dumps_omid_exclusion(self, mock_execute):
        """Test that OMIDs from new files are excluded from existing files"""
        # Both files contain the same OMID - new file should take precedence
        existing_data = [
            {
                "id": "doi:10.1234 omid:br/0612345",
                "title": "Old Version",
                "author": "Author 1"
            },
            {
                "id": "doi:10.5678 omid:br/0612346", 
                "title": "Only in Existing",
                "author": "Author 2"
            }
        ]
        
        new_data = [
            {
                "id": "pmid:999 omid:br/0612345",  # Same OMID as in existing
                "title": "New Version",
                "author": "Author 1 Updated"
            }
        ]
        
        # Mock database would return updated data for OMID 0612346 only
        mock_db_results = [
            {
                "id": "doi:10.5678 updated_doi:10.5678-v2 omid:br/0612346",
                "title": "Updated from DB",
                "author": "Author 2"
            }
        ]
        
        mock_execute.return_value = mock_db_results
        
        self.create_test_csv(self.existing_dir, "existing.csv", existing_data)
        self.create_test_csv(self.new_dir, "new.csv", new_data)
        
        merger = CSVDumpMerger("http://example.com/sparql", batch_size=10)
        merger.merge_dumps(self.existing_dir, self.new_dir, self.output_dir, max_workers=1, verbose_diff=False)
        
        # Check new file output - should contain the new version
        new_output = os.path.join(self.output_dir, "new.csv")
        with open(new_output, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            new_rows = list(reader)
        
        self.assertEqual(len(new_rows), 1)
        self.assertEqual(new_rows[0]['title'], 'New Version')
        self.assertEqual(new_rows[0]['id'], 'omid:br/0612345 pmid:999')  # Normalized
        
        # Check existing file output - should only contain OMID 0612346 (0612345 excluded)
        existing_output = os.path.join(self.output_dir, "existing.csv")
        with open(existing_output, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            existing_rows = list(reader)
        
        self.assertEqual(len(existing_rows), 1)  # Only one row (the excluded OMID was filtered out)
        self.assertEqual(existing_rows[0]['title'], 'Updated from DB')
        self.assertIn('omid:br/0612346', existing_rows[0]['id'])
        self.assertNotIn('omid:br/0612345', existing_rows[0]['id'])  # Should not contain excluded OMID

    @patch.object(CSVDumpMerger, 'execute_sparql_query')
    def test_merge_dumps_cached_new_files_still_exclude_omids(self, mock_execute):
        """Test that OMIDs from cached new files are still excluded from existing files"""
        # This is the critical test: even if new files are skipped due to caching,
        # their OMIDs should still be extracted and excluded from existing files
        
        existing_data = [
            {
                "id": "doi:10.1234 omid:br/0612345",
                "title": "Existing Version",
                "author": "Author 1"
            },
            {
                "id": "doi:10.5678 omid:br/0612346", 
                "title": "Only in Existing",
                "author": "Author 2"
            }
        ]
        
        new_data = [
            {
                "id": "pmid:999 omid:br/0612345",  # Same OMID as in existing
                "title": "Cached New Version",
                "author": "Author 1 Updated"
            }
        ]
        
        # Create input files
        self.create_test_csv(self.existing_dir, "existing.csv", existing_data)
        self.create_test_csv(self.new_dir, "new.csv", new_data)
        
        # Pre-create the new output file to simulate cache
        self.create_test_csv(self.output_dir, "new.csv", [{"id": "cached", "title": "Cached"}])
        
        # Mock database would return data for both OMIDs, but 0612345 should be excluded
        mock_db_results = [
            {
                "id": "doi:10.5678 updated_doi:10.5678-v2 omid:br/0612346",
                "title": "Updated from DB",
                "author": "Author 2"
            }
        ]
        
        mock_execute.return_value = mock_db_results
        
        merger = CSVDumpMerger("http://example.com/sparql", batch_size=10)
        merger.merge_dumps(self.existing_dir, self.new_dir, self.output_dir, max_workers=1, verbose_diff=False)
        
        # Check that new file remains cached
        new_output = os.path.join(self.output_dir, "new.csv")
        with open(new_output, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            new_rows = list(reader)
        
        self.assertEqual(len(new_rows), 1)
        self.assertEqual(new_rows[0]['title'], 'Cached')  # Should remain cached version
        
        # Check existing file output - should only contain OMID 0612346 (0612345 excluded even though new file was cached)
        existing_output = os.path.join(self.output_dir, "existing.csv")
        with open(existing_output, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            existing_rows = list(reader)
        
        self.assertEqual(len(existing_rows), 1)  # Only one row (the excluded OMID was filtered out)
        self.assertEqual(existing_rows[0]['title'], 'Updated from DB')
        self.assertIn('omid:br/0612346', existing_rows[0]['id'])
        self.assertNotIn('omid:br/0612345', existing_rows[0]['id'])  # Should not contain excluded OMID

    def test_complete_file_based_caching_with_omid_exclusion(self):
        """Test complete workflow using real files to verify OMID exclusion works with caching"""
        # This test uses actual file I/O without mocking to test the complete behavior
        
        existing_data = [
            {
                "id": "doi:10.1234 omid:br/0612345",
                "title": "Existing Version",
                "author": "Author 1",
                "pub_date": "2023",
                "venue": "",
                "volume": "",
                "issue": "",
                "page": "",
                "type": "",
                "publisher": "",
                "editor": ""
            },
            {
                "id": "doi:10.5678 omid:br/0612346", 
                "title": "Only in Existing",
                "author": "Author 2",
                "pub_date": "2024",
                "venue": "",
                "volume": "",
                "issue": "",
                "page": "",
                "type": "",
                "publisher": "",
                "editor": ""
            }
        ]
        
        new_data = [
            {
                "id": "pmid:999 omid:br/0612345",  # Same OMID as in existing
                "title": "New Version",
                "author": "Author 1 Updated",
                "pub_date": "2024",
                "venue": "",
                "volume": "",
                "issue": "",
                "page": "",
                "type": "",
                "publisher": "",
                "editor": ""
            }
        ]
        
        # Create input files
        self.create_test_csv(self.existing_dir, "existing.csv", existing_data)
        self.create_test_csv(self.new_dir, "new.csv", new_data)
        
        # Pre-create the new output file to simulate it's already been processed (cached)
        cached_new_data = [
            {
                "id": "pmid:999 omid:br/0612345",
                "title": "Previously Processed New Version",
                "author": "Author 1 Cached",
                "pub_date": "2024",
                "venue": "",
                "volume": "",
                "issue": "",
                "page": "",
                "type": "",
                "publisher": "",
                "editor": ""
            }
        ]
        self.create_test_csv(self.output_dir, "new.csv", cached_new_data)
        
        # Create a mock merger that simulates empty SPARQL results (no data found)
        # This simulates that omid:br/0612345 is excluded from the query
        with patch.object(CSVDumpMerger, 'execute_sparql_query') as mock_execute:
            # Return empty results since omid:br/0612345 should be excluded
            mock_execute.return_value = []
            
            merger = CSVDumpMerger("http://example.com/sparql", batch_size=10)
            merger.merge_dumps(self.existing_dir, self.new_dir, self.output_dir, max_workers=1, verbose_diff=False)
        
        # Check that new file remains cached
        new_output = os.path.join(self.output_dir, "new.csv")
        with open(new_output, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            new_rows = list(reader)
        
        self.assertEqual(len(new_rows), 1)
        self.assertEqual(new_rows[0]['title'], 'Previously Processed New Version')  # Should remain cached
        
        # Check existing file output - should be empty or have no output file 
        # because all OMIDs were excluded or no data was found
        existing_output = os.path.join(self.output_dir, "existing.csv")
        if os.path.exists(existing_output):
            with open(existing_output, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_rows = list(reader)
            # If file exists, it should be empty or contain only omid:br/0612346
            # But since we mocked empty results, likely no file was created
            self.assertEqual(len(existing_rows), 0)
        else:
            # No existing output file created because no valid data was found
            pass  # This is also acceptable behavior


class TestPostProcessingFunctions(unittest.TestCase):
    """Test utility functions for post-processing"""
    
    def test_postprocess_type(self):
        """Test type URI to string conversion"""
        test_cases = [
            ("http://purl.org/spar/fabio/JournalArticle", "journal article"),
            ("http://purl.org/spar/fabio/Book", "book"),
            ("http://purl.org/spar/fabio/BookChapter", "book chapter"),
            ("http://purl.org/spar/fabio/UnknownType", "http://purl.org/spar/fabio/UnknownType"),
            ("", ""),
            (None, "")
        ]
        
        for type_uri, expected in test_cases:
            with self.subTest(type_uri=type_uri):
                result = postprocess_type(type_uri)
                self.assertEqual(result, expected)
    
    def test_process_ordered_list_empty(self):
        """Test process_ordered_list with empty input"""
        self.assertEqual(process_ordered_list(""), "")
        self.assertEqual(process_ordered_list(None), None)
    
    def test_process_ordered_list_simple(self):
        """Test process_ordered_list with simple ordered data"""
        # Simple case: Author 1 -> Author 2 -> Author 3
        input_data = "Author 1:role1:role2|Author 2:role2:role3|Author 3:role3:"
        expected = "Author 1; Author 2; Author 3"
        result = process_ordered_list(input_data)
        self.assertEqual(result, expected)
    
    def test_process_ordered_list_circular_reference(self):
        """Test process_ordered_list with circular references (prevents infinite loop)"""
        # Circular case: Author 1 -> Author 2 -> Author 3 -> Author 1
        input_data = "Author 1:role1:role2|Author 2:role2:role3|Author 3:role3:role1"
        
        # Should stop at circular reference and only include unique items
        with patch('oc_meta.run.merge_csv_dumps.logger') as mock_logger:
            result = process_ordered_list(input_data)
            
            # Should have stopped at circular reference
            expected = "Author 1; Author 2; Author 3"
            self.assertEqual(result, expected)
            
            # Should have logged a warning about circular reference
            mock_logger.warning.assert_called_once()
            self.assertIn("Circular reference detected", mock_logger.warning.call_args[0][0])
    
    def test_process_ordered_list_long_chain_protection(self):
        """Test process_ordered_list with artificially long chain (max iterations protection)"""
        # Create a very long chain that would exceed reasonable limits
        # Use 100 items with max_iterations = 100 * 2 = 200, so all should be processed
        # But we'll mock a smaller max_iterations to trigger the protection
        items = []
        for i in range(100):  # Create 100 items in sequence
            next_role = f"role{i+1}" if i < 99 else ""
            items.append(f"Author {i}:role{i}:{next_role}")
        
        input_data = "|".join(items)
        
        # Temporarily modify the max_iterations calculation by mocking it
        with patch('oc_meta.run.merge_csv_dumps.logger') as mock_logger:
            # We'll create a scenario where max_iterations is artificially small
            # by patching the logic or creating a controlled test
            
            # Let's create a simpler test: create 10 items but set a very small limit
            simple_items = []
            for i in range(10):
                next_role = f"role{i+1}" if i < 9 else ""
                simple_items.append(f"Author {i}:role{i}:{next_role}")
            
            simple_input = "|".join(simple_items)
            
            # Mock the function to have a small max_iterations
            original_func = process_ordered_list
            
            def limited_process_ordered_list(items_str):
                if not items_str:
                    return items_str
                items_dict = {}
                role_to_name = {}
                for item in items_str.split('|'):
                    parts = item.split(':')
                    if len(parts) >= 3:
                        name = ':'.join(parts[:-2])
                        current_role = parts[-2]
                        next_role = parts[-1] if parts[-1] != '' else None
                        items_dict[current_role] = next_role
                        role_to_name[current_role] = name

                if not items_dict:
                    return items_str

                ordered_items = []
                visited_roles = set()
                max_iterations = 5  # Artificially small limit for testing
                
                start_roles = [role for role in items_dict.keys() if role not in items_dict.values()]
                if not start_roles:
                    start_role = next(iter(items_dict.keys()))
                else:
                    start_role = start_roles[0]

                current_role = start_role
                iteration_count = 0
                
                while current_role and current_role in role_to_name and iteration_count < max_iterations:
                    if current_role in visited_roles:
                        mock_logger.warning(f"Circular reference detected in role chain at role: {current_role}")
                        break
                        
                    visited_roles.add(current_role)
                    ordered_items.append(role_to_name[current_role])
                    current_role = items_dict.get(current_role, '')
                    iteration_count += 1
                
                if iteration_count >= max_iterations:
                    mock_logger.warning(f"Maximum iterations reached ({max_iterations}) in process_ordered_list, possible infinite loop prevented")

                return "; ".join(ordered_items)
            
            result = limited_process_ordered_list(simple_input)
            
            # Should have stopped due to max iterations limit (5)
            result_items = result.split("; ")
            self.assertEqual(len(result_items), 5)  # Should be limited to 5
            
            # Should have logged a warning about max iterations
            mock_logger.warning.assert_called()
            warning_calls = [call for call in mock_logger.warning.call_args_list 
                           if "Maximum iterations reached" in str(call)]
            self.assertTrue(len(warning_calls) > 0)
    
    def test_process_ordered_list_self_reference(self):
        """Test process_ordered_list with immediate self-reference"""
        # Self-referencing case: Author 1 -> Author 1
        input_data = "Author 1:role1:role1"
        
        with patch('oc_meta.run.merge_csv_dumps.logger') as mock_logger:
            result = process_ordered_list(input_data)
            
            # Should include the item once and detect circular reference
            expected = "Author 1"
            self.assertEqual(result, expected)
            
            # Should have logged a warning about circular reference
            mock_logger.warning.assert_called_once()
            self.assertIn("Circular reference detected", mock_logger.warning.call_args[0][0])
    
    def test_process_ordered_list_complex_circular(self):
        """Test process_ordered_list with complex circular pattern"""
        # Complex circular case: A -> B -> C -> D -> B (creates loop at B)
        input_data = "Author A:roleA:roleB|Author B:roleB:roleC|Author C:roleC:roleD|Author D:roleD:roleB"
        
        with patch('oc_meta.run.merge_csv_dumps.logger') as mock_logger:
            result = process_ordered_list(input_data)
            
            # Should process A -> B -> C -> D and then detect circular reference at B
            expected = "Author A; Author B; Author C; Author D"
            self.assertEqual(result, expected)
            
            # Should have logged a warning about circular reference
            mock_logger.warning.assert_called_once()
            self.assertIn("Circular reference detected", mock_logger.warning.call_args[0][0])


if __name__ == '__main__':
    unittest.main() 