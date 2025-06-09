#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import multiprocessing
import os
import re
import shutil
import tempfile
import unittest

from oc_meta.run.merge_csv_dumps_light import (
    CSVDumpMergerLight, extract_omid_from_id_field, get_all_csv_files,
    merge_sorted_temp_files, normalize_row_data,
    process_csv_file_to_temp)


class TestMergeCSVLight(unittest.TestCase):
    """Test suite for CSV Dump Merger Light functionality with streaming architecture"""
    
    def setUp(self):
        """Set up test environment before each test"""
        self.base_dir = os.path.dirname(__file__)
        self.test_data_dir = os.path.join(self.base_dir, 'merge_csv_light_test')
        self.existing_dir = os.path.join(self.test_data_dir, 'existing_csv')
        self.new_dir = os.path.join(self.test_data_dir, 'new_csv')
        self.output_dir = os.path.join(self.test_data_dir, 'test_output')
        
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        
        # Test with single worker for predictable results by default
        self.merger = CSVDumpMergerLight(max_workers=1)
    
    def tearDown(self):
        """Clean up after each test"""
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
    
    def _run_merge_and_load_data(self, rows_per_file=2, max_workers=1):
        """Helper method to run merge and load all data for comprehensive testing"""
        merger = CSVDumpMergerLight(max_workers=max_workers)
        merger.merge_dumps_light(self.existing_dir, self.new_dir, self.output_dir, rows_per_file)
        
        output_files = sorted([f for f in os.listdir(self.output_dir) if f.endswith('.csv')])
        all_rows = []
        for file_name in output_files:
            file_path = os.path.join(self.output_dir, file_name)
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                all_rows.extend(list(reader))
        
        return output_files, all_rows
    
    def _run_minimal_memory_and_load_data(self, rows_per_file=2):
        """Helper method to run minimal memory merge and load all data"""
        self.merger.merge_dumps_minimal_memory(self.existing_dir, self.new_dir, self.output_dir, rows_per_file)
        
        output_files = sorted([f for f in os.listdir(self.output_dir) if f.endswith('.csv')])
        all_rows = []
        for file_name in output_files:
            file_path = os.path.join(self.output_dir, file_name)
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                all_rows.extend(list(reader))
        
        return output_files, all_rows
    
    def test_extract_omid_from_id_field(self):
        """Test OMID extraction from ID field"""
        # Test valid OMID
        self.assertEqual(extract_omid_from_id_field("omid:br/001 doi:123"), "omid:br/001")
        self.assertEqual(extract_omid_from_id_field("doi:123 omid:ra/456"), "omid:ra/456")
        
        # Test invalid cases
        self.assertEqual(extract_omid_from_id_field(""), "")
        self.assertEqual(extract_omid_from_id_field("doi:123"), "")
        self.assertEqual(extract_omid_from_id_field(None), "")
    
    def test_normalize_row_data(self):
        """Test row data normalization"""
        test_row = {
            'id': 'doi:123 omid:br/001',
            'author': 'John Doe [orcid:123 omid:ra/001]',
            'page': '333-333',
            'title': 'Test Article',
            'venue': 'Test Journal [omid:br/venue]'
        }
        
        normalized = normalize_row_data(test_row)
        
        # ID should have OMID first
        self.assertTrue(normalized['id'].startswith('omid:br/001'))
        
        # Page range should be simplified
        self.assertEqual(normalized['page'], '333')
        
        # Other fields should be normalized but not empty
        self.assertIsNotNone(normalized['author'])
        self.assertIsNotNone(normalized['venue'])
    
    def test_get_all_csv_files(self):
        """Test CSV file discovery"""
        existing_files = get_all_csv_files(self.existing_dir)
        new_files = get_all_csv_files(self.new_dir)
        
        self.assertGreater(len(existing_files), 0, "Should find existing CSV files")
        self.assertGreater(len(new_files), 0, "Should find new CSV files")
        
        # All files should end with .csv
        for file_path in existing_files + new_files:
            self.assertTrue(file_path.endswith('.csv'))
        
        # Test non-existent directory
        non_existent_files = get_all_csv_files('/non/existent/path')
        self.assertEqual(len(non_existent_files), 0)
    
    def test_process_csv_file_to_temp(self):
        """Test processing CSV file to temporary sorted file"""
        existing_files = get_all_csv_files(self.existing_dir)
        self.assertGreater(len(existing_files), 0, "Need files to test")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path, temp_file, row_count = process_csv_file_to_temp(
                (existing_files[0], temp_dir, False)
            )
            
            self.assertEqual(file_path, existing_files[0])
            self.assertTrue(os.path.exists(temp_file))
            self.assertGreater(row_count, 0)
            
            # Verify temp file content
            with open(temp_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                temp_rows = list(reader)
                self.assertEqual(len(temp_rows), row_count)
    
    def test_merge_sorted_temp_files(self):
        """Test merging sorted temporary files"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create some test temp files
            temp_files = []
            
            # Create first temp file
            temp_file1 = tempfile.NamedTemporaryFile(mode='w', delete=False, 
                                                   dir=temp_dir, suffix='.csv', encoding='utf-8')
            fieldnames = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
            writer1 = csv.DictWriter(temp_file1, fieldnames=fieldnames)
            writer1.writeheader()
            writer1.writerow({'id': 'omid:br/001', 'title': 'Article 1', 'author': 'Author 1', 'pub_date': '2021', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''})
            writer1.writerow({'id': 'omid:br/003', 'title': 'Article 3', 'author': 'Author 3', 'pub_date': '2021', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''})
            temp_file1.close()
            temp_files.append(temp_file1.name)
            
            # Create second temp file
            temp_file2 = tempfile.NamedTemporaryFile(mode='w', delete=False, 
                                                   dir=temp_dir, suffix='.csv', encoding='utf-8')
            writer2 = csv.DictWriter(temp_file2, fieldnames=fieldnames)
            writer2.writeheader()
            writer2.writerow({'id': 'omid:br/002', 'title': 'Article 2', 'author': 'Author 2', 'pub_date': '2021', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''})
            temp_file2.close()
            temp_files.append(temp_file2.name)
            
            # Merge files
            merge_output_dir = os.path.join(temp_dir, 'merge_output')
            temp_files_with_priority = [(f, 1) for f in temp_files]  # Default priority
            merge_sorted_temp_files(temp_files_with_priority, merge_output_dir, total_rows=3, rows_per_file=2)
            
            # Verify merged output
            self.assertTrue(os.path.exists(merge_output_dir))
            output_files = [f for f in os.listdir(merge_output_dir) if f.endswith('.csv')]
            self.assertGreater(len(output_files), 0)
            
            # Check that rows are properly sorted
            first_file = os.path.join(merge_output_dir, sorted(output_files)[0])
            with open(first_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if len(rows) >= 2:
                    self.assertLessEqual(rows[0]['id'], rows[1]['id'], "Rows should be sorted by OMID")
    
    def test_initialization_with_workers(self):
        """Test CSVDumpMergerLight initialization with different worker counts"""
        # Test default initialization
        merger_default = CSVDumpMergerLight()
        self.assertEqual(merger_default.max_workers, multiprocessing.cpu_count())
        
        # Test custom worker count
        merger_custom = CSVDumpMergerLight(max_workers=2)
        self.assertEqual(merger_custom.max_workers, 2)
        
        # Test None (should default to CPU count)
        merger_none = CSVDumpMergerLight(max_workers=None)
        self.assertEqual(merger_none.max_workers, multiprocessing.cpu_count())
    
    def test_multiprocessing_functionality(self):
        """Test that multiprocessing works correctly with multiple workers"""
        # Test with multiple workers
        output_files_mp, all_rows_mp = self._run_merge_and_load_data(rows_per_file=2, max_workers=2)
        
        # Test with single worker for comparison
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        output_files_sp, all_rows_sp = self._run_merge_and_load_data(rows_per_file=2, max_workers=1)
        
        # Results should be identical regardless of worker count
        self.assertEqual(len(output_files_mp), len(output_files_sp))
        self.assertEqual(len(all_rows_mp), len(all_rows_sp))
        
        # Compare sorted data (order might differ due to parallel processing)
        mp_ids = sorted([row['id'] for row in all_rows_mp])
        sp_ids = sorted([row['id'] for row in all_rows_sp])
        self.assertEqual(mp_ids, sp_ids)
    
    def test_basic_functionality(self):
        """Test basic CSV merge functionality"""
        rows_per_file = 2
        
        self.merger.merge_dumps_light(self.existing_dir, self.new_dir, self.output_dir, rows_per_file)
        
        self.assertTrue(os.path.exists(self.output_dir))
        
        output_files = [f for f in os.listdir(self.output_dir) if f.endswith('.csv')]
        self.assertGreater(len(output_files), 0, "No output files were created")
        
        expected_files = ['oc_meta_data_001.csv', 'oc_meta_data_002.csv', 'oc_meta_data_003.csv']
        for expected_file in expected_files:
            self.assertIn(expected_file, output_files, f"Expected file {expected_file} not found")
        
        for filename in sorted(output_files):
            file_path = os.path.join(self.output_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                self.assertGreater(len(lines), 1, f"File {filename} should have header + data rows")
    
    def test_minimal_memory_mode(self):
        """Test minimal memory mode functionality"""
        rows_per_file = 2
        
        output_files_minimal, all_rows_minimal = self._run_minimal_memory_and_load_data(rows_per_file)
        
        # Reset and test regular mode
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        output_files_regular, all_rows_regular = self._run_merge_and_load_data(rows_per_file)
        
        # Results should be identical between minimal memory and regular modes
        self.assertEqual(len(output_files_minimal), len(output_files_regular))
        self.assertEqual(len(all_rows_minimal), len(all_rows_regular))
        
        # Compare sorted data
        minimal_ids = sorted([row['id'] for row in all_rows_minimal])
        regular_ids = sorted([row['id'] for row in all_rows_regular])
        self.assertEqual(minimal_ids, regular_ids)
    
    def test_omid_ordering_and_normalization(self):
        """Test OMID ordering and ID field normalization"""        
        rows_per_file = 2
        self.merger.merge_dumps_light(self.existing_dir, self.new_dir, self.output_dir, rows_per_file)
        
        first_file = os.path.join(self.output_dir, 'oc_meta_data_001.csv')
        self.assertTrue(os.path.exists(first_file), "First output file should exist")
        
        with open(first_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            self.assertGreater(len(lines), 1, "First file should have data rows")
            
            for i, line in enumerate(lines):
                if i == 0:  # Header
                    self.assertIn('id', line.lower(), "Header should contain 'id' field")
                else:
                    parts = line.split(',')
                    self.assertGreater(len(parts), 0, f"Row {i} should have CSV fields")
                    
                    id_field = parts[0].strip('"')
                    
                    id_parts = id_field.split()
                    omid_parts = [part for part in id_parts if part.startswith('omid:')]
                    self.assertGreater(len(omid_parts), 0, f"Row {i} should contain OMID")
                    self.assertTrue(id_parts[0].startswith('omid:'), f"Row {i}: OMID should be first in ID field")
    
    def test_file_structure(self):
        """Test that required test data files exist"""
        self.assertTrue(os.path.exists(self.existing_dir), f"Existing CSV directory should exist: {self.existing_dir}")
        self.assertTrue(os.path.exists(self.new_dir), f"New CSV directory should exist: {self.new_dir}")

        existing_files = [f for f in os.listdir(self.existing_dir) if f.endswith('.csv')]
        new_files = [f for f in os.listdir(self.new_dir) if f.endswith('.csv')]
        
        self.assertGreater(len(existing_files), 0, "Should have existing CSV files for testing")
        self.assertGreater(len(new_files), 0, "Should have new CSV files for testing")
        
    def test_merge_with_default_rows_per_file(self):
        """Test merge with default rows per file (3000)"""        
        self.merger.merge_dumps_light(self.existing_dir, self.new_dir, self.output_dir)
        
        output_files = [f for f in os.listdir(self.output_dir) if f.endswith('.csv')]
        self.assertEqual(len(output_files), 1, "With default rows per file, should create only one output file")
        
        expected_file = 'oc_meta_data_001.csv'
        self.assertIn(expected_file, output_files, f"Should create {expected_file}")
    
    def test_omid_precedence(self):
        """Test OMID precedence (new files override existing)"""
        output_files, all_rows = self._run_merge_and_load_data()
        
        omid_002_row = next((row for row in all_rows if 'omid:br/002' in row['id']), None)
        self.assertIsNotNone(omid_002_row, "Should find OMID br/002 in output")
        
        self.assertIn("Updated", omid_002_row['title'], "New file should override existing file")
    
    def test_omid_alphabetical_ordering(self):
        """Test OMID alphabetical ordering"""
        output_files, all_rows = self._run_merge_and_load_data()
        
        omids_found = []
        for row in all_rows:
            omid = None
            for id_part in row['id'].split():
                if id_part.startswith('omid:'):
                    omid = id_part
                    break
            if omid:
                omids_found.append(omid)
        
        sorted_omids = sorted(omids_found)
        self.assertEqual(omids_found, sorted_omids, "OMIDs should be sorted alphabetically")
    
    def test_id_field_normalization(self):
        """Test ID field normalization (OMID first, others sorted)"""
        output_files, all_rows = self._run_merge_and_load_data()

        for i, row in enumerate(all_rows[:3]):  # Check first 3 rows
            id_field = row['id']
            id_parts = id_field.split()
            omid_parts = [part for part in id_parts if part.startswith('omid:')]
            other_parts = [part for part in id_parts if not part.startswith('omid:')]
            
            self.assertGreater(len(omid_parts), 0, f"Row {i+1} should have OMID")
            self.assertIn(id_parts[0], omid_parts, f"Row {i+1}: OMID should be first")
            self.assertEqual(sorted(other_parts), other_parts, f"Row {i+1}: Other IDs should be sorted")
    
    def test_page_field_normalization(self):
        """Test page field normalization (333-333 -> 333)"""
        output_files, all_rows = self._run_merge_and_load_data()
        
        page_333_row = next((row for row in all_rows if row['page'] == '333'), None)
        self.assertIsNotNone(page_333_row, "Should find normalized page field '333'")
        
    def test_people_field_id_normalization(self):
        """Test people field ID normalization (OMID first in brackets)"""
        output_files, all_rows = self._run_merge_and_load_data()
        
        for i, row in enumerate(all_rows[:2]):  # Check first 2 rows
            author_field = row['author']            
            # Check if OMIDs come before other IDs in brackets
            if '[omid:ra/' in author_field and 'orcid:' in author_field:
                brackets = re.findall(r'\[([^\]]+)\]', author_field)
                for bracket_content in brackets:
                    ids_in_bracket = bracket_content.split()
                    omid_ids = [id for id in ids_in_bracket if id.startswith('omid:')]
                    
                    if omid_ids:
                        self.assertIn(ids_in_bracket[0], omid_ids, f"OMID should be first in brackets: {bracket_content}")
    
    def test_progressive_file_naming(self):
        """Test progressive file naming"""
        output_files, all_rows = self._run_merge_and_load_data()
        
        expected_names = ['oc_meta_data_001.csv', 'oc_meta_data_002.csv', 'oc_meta_data_003.csv']
        for expected_name in expected_names:
            self.assertIn(expected_name, output_files, f"Expected file {expected_name} should exist")
    
    def test_file_row_counts(self):
        """Test file row counts and total rows"""
        output_files, all_rows = self._run_merge_and_load_data()
        
        total_rows = 0
        for file_name in output_files:
            file_path = os.path.join(self.output_dir, file_name)
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                row_count = len(list(reader))
                total_rows += row_count
        
        # Verify we have the expected total number of rows (6 from test data)
        self.assertEqual(total_rows, 6, "Should have 6 total rows from test data")
        self.assertEqual(len(all_rows), 6, "Loaded rows should match total count")
    
    def test_data_consistency(self):
        """Test data consistency and completeness"""
        output_files, all_rows = self._run_merge_and_load_data()
        
        # Verify all rows have required fields
        required_fields = ['id', 'title', 'author', 'pub_date', 'venue']
        for i, row in enumerate(all_rows):
            for field in required_fields:
                self.assertIn(field, row, f"Row {i+1} should have field '{field}'")
        
        # Verify no duplicate OMIDs
        omids = []
        for row in all_rows:
            for id_part in row['id'].split():
                if id_part.startswith('omid:'):
                    omids.append(id_part)
                    break
        
        unique_omids = set(omids)
        self.assertEqual(len(omids), len(unique_omids), "Should have no duplicate OMIDs")
    
    def test_error_handling_with_multiprocessing(self):
        """Test error handling with multiprocessing (non-existent directories)"""
        non_existent_dir = os.path.join(self.test_data_dir, 'non_existent')
        
        # Should handle gracefully without crashing
        merger = CSVDumpMergerLight(max_workers=2)
        merger.merge_dumps_light(non_existent_dir, self.new_dir, self.output_dir)
        
        # Should still process new_dir files
        self.assertTrue(os.path.exists(self.output_dir))
        output_files = [f for f in os.listdir(self.output_dir) if f.endswith('.csv')]
        self.assertGreater(len(output_files), 0, "Should create files from new_dir despite missing existing_dir")
    
    def test_streaming_vs_minimal_memory_consistency(self):
        """Test that streaming and minimal memory modes produce identical results"""
        # Run streaming mode
        output_files_streaming, all_rows_streaming = self._run_merge_and_load_data(rows_per_file=2)
        
        # Reset and run minimal memory mode
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        output_files_minimal, all_rows_minimal = self._run_minimal_memory_and_load_data(rows_per_file=2)
        
        # Compare results
        self.assertEqual(len(all_rows_streaming), len(all_rows_minimal), "Both modes should produce same number of rows")
        
        # Compare content (sort to handle potential ordering differences)
        streaming_content = sorted([row['id'] + '|' + row['title'] for row in all_rows_streaming])
        minimal_content = sorted([row['id'] + '|' + row['title'] for row in all_rows_minimal])
        self.assertEqual(streaming_content, minimal_content, "Both modes should produce identical content")
    
    def test_deduplication_functionality(self):
        """Test that duplicate OMIDs are properly handled"""
        output_files, all_rows = self._run_merge_and_load_data()
        
        # Extract all OMIDs
        omids_found = []
        for row in all_rows:
            for id_part in row['id'].split():
                if id_part.startswith('omid:'):
                    omids_found.append(id_part)
                    break
        
        # Check for duplicates
        unique_omids = set(omids_found)
        self.assertEqual(len(omids_found), len(unique_omids), 
                        f"Found duplicate OMIDs: {[omid for omid in omids_found if omids_found.count(omid) > 1]}")
    
    def test_priority_handling_with_temp_files(self):
        """Test priority handling in merge_sorted_temp_files_with_priority"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test temp files with different priorities
            temp_files_with_priority = []
            
            # Create higher priority file (0 = new file)
            temp_file1 = tempfile.NamedTemporaryFile(mode='w', delete=False, 
                                                   dir=temp_dir, suffix='.csv', encoding='utf-8')
            fieldnames = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
            writer1 = csv.DictWriter(temp_file1, fieldnames=fieldnames)
            writer1.writeheader()
            writer1.writerow({'id': 'omid:br/002', 'title': 'Updated Article', 'author': 'New Author', 'pub_date': '2023', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''})
            temp_file1.close()
            temp_files_with_priority.append((temp_file1.name, 0))  # Higher priority (new file)
            
            # Create lower priority file (1 = existing file)
            temp_file2 = tempfile.NamedTemporaryFile(mode='w', delete=False, 
                                                   dir=temp_dir, suffix='.csv', encoding='utf-8')
            writer2 = csv.DictWriter(temp_file2, fieldnames=fieldnames)
            writer2.writeheader()
            writer2.writerow({'id': 'omid:br/002', 'title': 'Original Article', 'author': 'Old Author', 'pub_date': '2022', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''})
            temp_file2.close()
            temp_files_with_priority.append((temp_file2.name, 1))  # Lower priority (existing file)
            
            # Merge files
            merge_output_dir = os.path.join(temp_dir, 'merge_output')
            merge_sorted_temp_files(temp_files_with_priority, merge_output_dir, total_rows=2, rows_per_file=10)
            
            # Verify that higher priority (new file) won
            output_files = [f for f in os.listdir(merge_output_dir) if f.endswith('.csv')]
            self.assertGreater(len(output_files), 0)
            
            first_file = os.path.join(merge_output_dir, sorted(output_files)[0])
            with open(first_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                # Should only have one row (the higher priority one)
                self.assertEqual(len(rows), 1)
                # Should be the updated article (from higher priority file)
                self.assertIn('Updated', rows[0]['title'])
                self.assertIn('New Author', rows[0]['author'])


if __name__ == '__main__':
    unittest.main(verbosity=2) 