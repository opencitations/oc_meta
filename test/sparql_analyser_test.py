#!/usr/bin/python
# Copyright 2025, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import sys
import unittest
from unittest.mock import MagicMock, patch

from oc_meta.run.analyser.sparql_analyser import OCMetaSPARQLAnalyser


class TestOCMetaSPARQLAnalyser(unittest.TestCase):
    """Test cases for OCMetaSPARQLAnalyser class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_endpoint = "http://test.example.com/sparql"
        self.analyser = OCMetaSPARQLAnalyser(sparql_endpoint=self.test_endpoint)
    
    def test_initialization_with_endpoint(self):
        """Test that analyser initializes correctly with provided endpoint."""
        self.assertEqual(self.analyser.sparql_endpoint, self.test_endpoint)
        self.assertIsNotNone(self.analyser.client)
    
    def test_initialization_requires_endpoint(self):
        """Test that analyser requires an endpoint parameter."""
        with self.assertRaises(TypeError):
            OCMetaSPARQLAnalyser()
    
    def test_execute_sparql_query_success(self):
        """Test successful SPARQL query execution."""
        mock_results = {
            "results": {
                "bindings": [
                    {"count": {"value": "100"}}
                ]
            }
        }

        with patch.object(self.analyser.client, 'query', return_value=mock_results):
            result = self.analyser._execute_sparql_query("SELECT * WHERE { ?s ?p ?o }")
            self.assertEqual(result, mock_results)
    
    def test_execute_sparql_query_failure(self):
        """Test SPARQL query execution failure."""
        with patch.object(self.analyser.client, 'query', side_effect=Exception("Query failed")):
            with self.assertRaises(Exception) as context:
                self.analyser._execute_sparql_query("INVALID QUERY")
            self.assertIn("SPARQL query failed after multiple retries", str(context.exception))
    
    def test_count_expressions(self):
        """Test counting fabio:Expression entities."""
        mock_results = {
            "results": {
                "bindings": [
                    {"count": {"value": "1500"}}
                ]
            }
        }
        
        with patch.object(self.analyser, '_execute_sparql_query', return_value=mock_results):
            count = self.analyser.count_expressions()
            self.assertEqual(count, 1500)
    
    def test_count_role_entities(self):
        """Test counting pro role entities."""
        mock_results = {
            "results": {
                "bindings": [
                    {"role": {"value": "http://purl.org/spar/pro/author"}, "count": {"value": "800"}},
                    {"role": {"value": "http://purl.org/spar/pro/publisher"}, "count": {"value": "200"}},
                    {"role": {"value": "http://purl.org/spar/pro/editor"}, "count": {"value": "100"}}
                ]
            }
        }
        
        with patch.object(self.analyser, '_execute_sparql_query', return_value=mock_results):
            counts = self.analyser.count_role_entities()
            expected = {
                'pro:author': 800,
                'pro:publisher': 200,
                'pro:editor': 100
            }
            self.assertEqual(counts, expected)
    
    def test_count_role_entities_empty_results(self):
        """Test counting pro role entities with empty results."""
        mock_results = {
            "results": {
                "bindings": []
            }
        }
        
        with patch.object(self.analyser, '_execute_sparql_query', return_value=mock_results):
            counts = self.analyser.count_role_entities()
            expected = {
                'pro:author': 0,
                'pro:publisher': 0,
                'pro:editor': 0
            }
            self.assertEqual(counts, expected)
    
    def test_count_venues(self):
        """Test counting distinct venues (simple count)."""
        mock_results = {
            "results": {
                "bindings": [
                    {"count": {"value": "350"}}
                ]
            }
        }
        
        with patch.object(self.analyser, '_execute_sparql_query', return_value=mock_results):
            count = self.analyser.count_venues()
            self.assertEqual(count, 350)
    
    def test_count_venues_disambiguated(self):
        """Test counting disambiguated venues."""
        mock_results = {
            "results": {
                "bindings": [
                    {
                        "venue": {"value": "http://venue1"},
                        "venueName": {"value": "Nature"},
                        "has_identifier": {"value": "true"}
                    },
                    {
                        "venue": {"value": "http://venue2"},
                        "venueName": {"value": "Science"},
                        "has_identifier": {"value": "false"}
                    },
                    {
                        "venue": {"value": "http://venue3"},
                        "venueName": {"value": "Cell"},
                        "has_identifier": {"value": "true"}
                    }
                ]
            }
        }
        
        with patch.object(self.analyser, '_execute_sparql_query', return_value=mock_results):
            count = self.analyser.count_venues_disambiguated()
            # venue1 and venue3 have external IDs (count by OMID = 2)
            # venue2 has no external IDs (count by name = 1)
            # Total = 3 distinct venues
            self.assertEqual(count, 3)
    
    @patch('builtins.print')
    def test_run_all_analyses_success(self, mock_print):
        """Test running all analyses successfully."""
        with patch.object(self.analyser, 'count_expressions', return_value=1500), \
             patch.object(self.analyser, 'count_role_entities', return_value={'pro:author': 800, 'pro:publisher': 200, 'pro:editor': 100}), \
             patch.object(self.analyser, 'count_venues_disambiguated', return_value=350), \
             patch.object(self.analyser, 'count_venues', return_value=400):
            
            results = self.analyser.run_all_analyses()
            
            expected = {
                'fabio_expressions': 1500,
                'roles': {'pro:author': 800, 'pro:publisher': 200, 'pro:editor': 100},
                'venues_disambiguated': 350,
                'venues_simple': 400
            }
            self.assertEqual(results, expected)
    
    @patch('builtins.print')
    def test_run_selected_analyses_br_only(self, mock_print):
        """Test running only bibliographic resources analysis."""
        with patch.object(self.analyser, 'count_expressions', return_value=1500):
            results = self.analyser.run_selected_analyses(analyze_br=True, analyze_ar=False, analyze_venues=False)
            
            expected = {
                'fabio_expressions': 1500
            }
            self.assertEqual(results, expected)
    
    @patch('builtins.print')
    def test_run_selected_analyses_venues_only(self, mock_print):
        """Test running only venues analysis."""
        with patch.object(self.analyser, 'count_venues_disambiguated', return_value=350), \
             patch.object(self.analyser, 'count_venues', return_value=400):
            
            results = self.analyser.run_selected_analyses(analyze_br=False, analyze_ar=False, analyze_venues=True)
            
            expected = {
                'venues_disambiguated': 350,
                'venues_simple': 400
            }
            self.assertEqual(results, expected)
    
    @patch('builtins.print')
    def test_run_all_analyses_with_errors(self, mock_print):
        """Test running all analyses with some errors."""
        with patch.object(self.analyser, 'count_expressions', side_effect=Exception("Connection error")), \
             patch.object(self.analyser, 'count_role_entities', return_value={'pro:author': 800, 'pro:publisher': 200, 'pro:editor': 100}), \
             patch.object(self.analyser, 'count_venues_disambiguated', return_value=350), \
             patch.object(self.analyser, 'count_venues', return_value=400):
            
            results = self.analyser.run_all_analyses()
            
            expected = {
                'fabio_expressions': None,
                'roles': {'pro:author': 800, 'pro:publisher': 200, 'pro:editor': 100},
                'venues_disambiguated': 350,
                'venues_simple': 400
            }
            self.assertEqual(results, expected)


class TestMainFunction(unittest.TestCase):
    """Test cases for the main function."""
    
    @patch('sys.argv', ['sparql_analyser.py', 'http://test.example.com/sparql'])
    @patch('oc_meta.run.analyser.sparql_analyser.OCMetaSPARQLAnalyser')
    @patch('builtins.print')
    def test_main_success(self, mock_print, mock_analyser_class):
        """Test main function with successful analysis."""
        mock_analyser = MagicMock()
        mock_results = {
            'fabio_expressions': 1500,
            'roles': {'pro:author': 800, 'pro:publisher': 200, 'pro:editor': 100},
            'venues_disambiguated': 350,
            'venues_simple': 400
        }
        mock_analyser.run_selected_analyses.return_value = mock_results
        mock_analyser_class.return_value = mock_analyser
        mock_analyser_class.return_value.__enter__.return_value = mock_analyser

        from oc_meta.run.analyser.sparql_analyser import main
        result = main()

        self.assertEqual(result, mock_results)
        mock_analyser_class.assert_called_once_with('http://test.example.com/sparql')
        mock_analyser.run_selected_analyses.assert_called_once_with(True, True, True)
    
    @patch('sys.argv', ['sparql_analyser.py', 'http://test.example.com/sparql'])
    @patch('oc_meta.run.analyser.sparql_analyser.OCMetaSPARQLAnalyser')
    @patch('builtins.print')
    def test_main_failure(self, mock_print, mock_analyser_class):
        """Test main function with analysis failure."""
        mock_analyser_class.side_effect = Exception("Initialization failed")
        
        from oc_meta.run.analyser.sparql_analyser import main
        result = main()
        
        self.assertIsNone(result)
    
    @patch('sys.argv', ['sparql_analyser.py'])
    def test_main_missing_argument(self):
        """Test main function with missing endpoint argument."""
        from oc_meta.run.analyser.sparql_analyser import main
        
        with self.assertRaises(SystemExit):
            main()
    
    @patch('sys.argv', ['sparql_analyser.py', 'http://test.example.com/sparql', '--br'])
    @patch('oc_meta.run.analyser.sparql_analyser.OCMetaSPARQLAnalyser')
    @patch('builtins.print')
    def test_main_br_only(self, mock_print, mock_analyser_class):
        """Test main function with --br option only."""
        mock_analyser = MagicMock()
        mock_results = {'fabio_expressions': 1500}
        mock_analyser.run_selected_analyses.return_value = mock_results
        mock_analyser_class.return_value = mock_analyser
        mock_analyser_class.return_value.__enter__.return_value = mock_analyser

        from oc_meta.run.analyser.sparql_analyser import main
        result = main()

        self.assertEqual(result, mock_results)
        mock_analyser.run_selected_analyses.assert_called_once_with(True, False, False)
    
    @patch('sys.argv', ['sparql_analyser.py', 'http://test.example.com/sparql', '--venues'])
    @patch('oc_meta.run.analyser.sparql_analyser.OCMetaSPARQLAnalyser')
    @patch('builtins.print')
    def test_main_venues_only(self, mock_print, mock_analyser_class):
        """Test main function with --venues option only."""
        mock_analyser = MagicMock()
        mock_results = {'venues_disambiguated': 350, 'venues_simple': 400}
        mock_analyser.run_selected_analyses.return_value = mock_results
        mock_analyser_class.return_value = mock_analyser
        mock_analyser_class.return_value.__enter__.return_value = mock_analyser

        from oc_meta.run.analyser.sparql_analyser import main
        result = main()

        self.assertEqual(result, mock_results)
        mock_analyser.run_selected_analyses.assert_called_once_with(False, False, True)


if __name__ == '__main__':
    """
    Run the test suite.
    
    This will execute all test cases and provide a summary of results.
    To run specific tests, use:
        python sparql_analyser_test.py TestOCMetaSPARQLAnalyser.test_count_expressions
    """
    print("Starting OCMetaSPARQLAnalyser test suite...")
    print("="*60)
    
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFailures:")
        for test, failure in result.failures:
            print(f"  - {test}: {failure}")
    
    if result.errors:
        print("\nErrors:")
        for test, error in result.errors:
            print(f"  - {test}: {error}")
    
    if result.wasSuccessful():
        print("\nAll tests passed successfully! ✅")
    else:
        print("\nSome tests failed. ❌")
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1) 