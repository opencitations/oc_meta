import unittest
from unittest.mock import patch
import tempfile
import zipfile
import shutil
from pathlib import Path
from rdflib import ConjunctiveGraph, URIRef, Literal, Graph


from oc_meta.run import provenance_conversion

SAMPLE_JSONLD = '''
{
  "@context": "https://schema.org",
  "@id": "http://example.org/entity1",
  "@type": "CreativeWork",
  "name": "Test Entity"
}
'''
EXPECTED_NQUADS_CONTENT = '<http://example.org/entity1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/CreativeWork> .\n<http://example.org/entity1> <https://schema.org/name> "Test Entity" .\n'
INVALID_JSONLD = "{\"@context\": \"bad context\", \"@id\": \"bad_id\"}"

class TestProvenanceConversionIntegration(unittest.TestCase):
    """Integration test suite for provenance_conversion.py script using real files."""

    def setUp(self):
        """Create temporary directories and a sample zip file for testing."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.input_dir = self.test_dir / "input"
        self.output_dir = self.test_dir / "output"
        self.input_dir.mkdir()
        self.output_dir.mkdir()

        # Create a nested structure and the zip file
        self.prov_dir = self.input_dir / "ra" / "0610" / "10000" / "1000" / "prov"
        self.prov_dir.mkdir(parents=True)
        self.zip_path = self.prov_dir / "se.zip"
        self.json_filename = "data.json"
        with zipfile.ZipFile(self.zip_path, 'w') as zf:
            zf.writestr(self.json_filename, SAMPLE_JSONLD)

    def tearDown(self):
        """Remove the temporary directory after tests."""
        shutil.rmtree(self.test_dir)

    def test_count_quads(self):
        """Test the count_quads function."""
        graph = ConjunctiveGraph()
        graph.add((URIRef("ex:s1"), URIRef("ex:p1"), Literal("o1")))
        graph.add((URIRef("ex:s2"), URIRef("ex:p2"), Literal("o2"), URIRef("ex:g1")))
        self.assertEqual(provenance_conversion.count_quads(graph), 2)
        self.assertEqual(provenance_conversion.count_quads(ConjunctiveGraph()), 0)

    def test_convert_jsonld_to_nquads_success(self):
        """Test successful conversion from JSON-LD to N-Quads."""
        graph, nquads = provenance_conversion.convert_jsonld_to_nquads(SAMPLE_JSONLD)
        self.assertIsNotNone(graph)
        self.assertIsNotNone(nquads)
        self.assertIsInstance(graph, ConjunctiveGraph)

        expected_graph = Graph()
        subj = URIRef("http://example.org/entity1")
        type_pred = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        schema_type = URIRef("http://schema.org/CreativeWork")
        name_pred = URIRef("http://schema.org/name")
        name_obj = Literal("Test Entity")
        expected_graph.add((subj, type_pred, schema_type))
        expected_graph.add((subj, name_pred, name_obj))

        self.assertEqual(len(graph), len(expected_graph))
        self.assertTrue(graph.isomorphic(expected_graph))

    def test_convert_jsonld_to_nquads_failure(self):
        """Test conversion failure with invalid JSON-LD."""
        graph, nquads = provenance_conversion.convert_jsonld_to_nquads(INVALID_JSONLD)
        self.assertIsNone(graph)
        self.assertIsNone(nquads)

    def test_process_zip_file_success_integration(self):
        """Test successful processing using real files and directories."""
        result = provenance_conversion.process_zip_file(self.zip_path, self.output_dir, self.input_dir)

        self.assertTrue(result, "process_zip_file should return True on success")

        expected_output_filename = "ra-0610-10000-1000-prov-se.nq"
        expected_output_path = self.output_dir / expected_output_filename
        self.assertTrue(expected_output_path.exists(), f"Output file {expected_output_path} was not created")
        self.assertTrue(expected_output_path.is_file())

        output_graph = ConjunctiveGraph()
        try:
            output_graph.parse(expected_output_path, format='nquads')
        except Exception as e:
            self.fail(f"Failed to parse the generated N-Quads file {expected_output_path}: {e}")

        input_graph_for_check = ConjunctiveGraph()
        input_graph_for_check.parse(data=SAMPLE_JSONLD, format='json-ld')

        self.assertEqual(len(output_graph), len(input_graph_for_check),
                         f"Quad count mismatch: Output={len(output_graph)}, Expected={len(input_graph_for_check)}")
        self.assertTrue(output_graph.isomorphic(input_graph_for_check),
                        "Output graph content does not match expected content")

    def test_process_zip_file_no_json_integration(self):
        """Test processing a zip file with no JSON content."""
        no_json_zip_path = self.prov_dir / "no_json_se.zip"
        with zipfile.ZipFile(no_json_zip_path, 'w') as zf:
            zf.writestr("readme.txt", "This is not json")

        result = provenance_conversion.process_zip_file(no_json_zip_path, self.output_dir, self.input_dir)
        self.assertFalse(result)
        expected_output_filename = "ra-0610-10000-1000-prov-no_json_se.nq"
        self.assertFalse((self.output_dir / expected_output_filename).exists())

    def test_process_zip_file_bad_zip_integration(self):
        """Test processing a corrupt zip file."""
        bad_zip_path = self.prov_dir / "bad_se.zip"
        with open(bad_zip_path, 'wb') as f:
            f.write(b"This is not a zip file content")

        result = provenance_conversion.process_zip_file(bad_zip_path, self.output_dir, self.input_dir)
        self.assertFalse(result)
        expected_output_filename = "ra-0610-10000-1000-prov-bad_se.nq"
        self.assertFalse((self.output_dir / expected_output_filename).exists())

    def test_process_zip_file_conversion_fail_integration(self):
        """Test processing a zip file with invalid JSON-LD content."""
        invalid_json_zip_path = self.prov_dir / "invalid_json_se.zip"
        with zipfile.ZipFile(invalid_json_zip_path, 'w') as zf:
            zf.writestr("data.json", INVALID_JSONLD)

        result = provenance_conversion.process_zip_file(invalid_json_zip_path, self.output_dir, self.input_dir)
        self.assertFalse(result)
        expected_output_filename = "ra-0610-10000-1000-prov-invalid_json_se.nq"
        self.assertFalse((self.output_dir / expected_output_filename).exists())

    @patch('oc_meta.run.provenance_conversion.count_quads')
    def test_process_zip_file_checksum_fail_mocked_count(self, mock_count_quads):
        """Test checksum failure by mocking the second count_quads call."""
        # Let the real conversion and file writing happen
        # Mock only the quad counting to force a mismatch
        mock_count_quads.side_effect = [2, 1] # Input=2 (from real JSON-LD), Output=1 (mocked)

        # Use the standard zip created in setUp
        result = provenance_conversion.process_zip_file(self.zip_path, self.output_dir, self.input_dir)

        self.assertFalse(result, "process_zip_file should return False when checksum fails")
        self.assertEqual(mock_count_quads.call_count, 2)

        # Verify the output file WAS created (as checksum fails after writing)
        expected_output_filename = "ra-0610-10000-1000-prov-se.nq"
        expected_output_path = self.output_dir / expected_output_filename
        self.assertTrue(expected_output_path.exists(), f"Output file {expected_output_path} should still exist after checksum failure")


if __name__ == '__main__':
    unittest.main() 