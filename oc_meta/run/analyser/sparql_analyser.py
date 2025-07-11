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

from __future__ import annotations

import argparse
import sys
import time
from SPARQLWrapper import SPARQLWrapper, JSON
from typing import Dict, Any


class OCMetaSPARQLAnalyser:
    """
    System for analyzing dataset statistics through SPARQL queries.
    
    This analyser connects to a SPARQL endpoint and performs various statistical
    analyses on the OpenCitations Meta dataset automatically.
    """
    
    def __init__(self, sparql_endpoint: str, max_retries: int = 3, retry_delay: int = 5):
        """
        Initialize the SPARQL analyser.
        
        Args:
            sparql_endpoint: SPARQL endpoint URL (required)
            max_retries: Maximum number of retries for a failing query. Defaults to 3.
            retry_delay: Delay in seconds between retries. Defaults to 5.
        """
        self.sparql_endpoint = sparql_endpoint
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.sparql = SPARQLWrapper(self.sparql_endpoint)
        self.sparql.setReturnFormat(JSON)
    
    def _execute_sparql_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a SPARQL query with retry logic and return results.
        
        Args:
            query: SPARQL query string
            
        Returns:
            Query results as dictionary
            
        Raises:
            Exception: If query execution fails after all retries.
        """
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                self.sparql.setQuery(query)
                return self.sparql.queryAndConvert()
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    print(
                        f"Query failed on attempt {attempt + 1} of {self.max_retries}. "
                        f"No more retries left.",
                        file=sys.stderr
                    )
        raise Exception("SPARQL query failed after multiple retries.") from last_exception
    
    def count_expressions(self) -> int:
        """
        Count the number of fabio:Expression entities.
        
        Returns:
            Number of fabio:Expression entities
        """
        query = """
        PREFIX fabio: <http://purl.org/spar/fabio/>
        
        SELECT (COUNT(DISTINCT ?expression) AS ?count)
        WHERE {
            ?expression a fabio:Expression .
        }
        """
        
        results = self._execute_sparql_query(query)
        return int(results["results"]["bindings"][0]["count"]["value"])
    
    def count_role_entities(self) -> Dict[str, int]:
        """
        Count the number of pro:author, pro:publisher and pro:editor roles.
        
        Returns:
            Dictionary with counts for each role type
        """
        query = """
        PREFIX pro: <http://purl.org/spar/pro/>
        
        SELECT ?role (COUNT(DISTINCT ?roleInTime) AS ?count)
        WHERE {
            ?roleInTime pro:withRole ?role .
            FILTER(?role IN (pro:author, pro:publisher, pro:editor))
        }
        GROUP BY ?role
        """
        
        results = self._execute_sparql_query(query)
        
        role_counts = {
            'pro:author': 0,
            'pro:publisher': 0,
            'pro:editor': 0
        }
        
        for binding in results["results"]["bindings"]:
            role_uri = binding["role"]["value"]
            count = int(binding["count"]["value"])
            
            if role_uri == "http://purl.org/spar/pro/author":
                role_counts['pro:author'] = count
            elif role_uri == "http://purl.org/spar/pro/publisher":
                role_counts['pro:publisher'] = count
            elif role_uri == "http://purl.org/spar/pro/editor":
                role_counts['pro:editor'] = count
        
        return role_counts
    
    def count_venues_disambiguated(self) -> int:
        """
        Count the number of distinct venues with disambiguation.
        
        Venues are disambiguated based on external identifiers:
        - If venue has only OMID identifier, count by name
        - If venue has external identifiers (ISBN, ISSN, DOI, etc.), count by OMID
        
        This follows the same logic as the original analyser.py
        
        Returns:
            Number of distinct disambiguated venues
        """
        # Get all venues with their identifiers and names
        query = """
        PREFIX fabio: <http://purl.org/spar/fabio/>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX datacite: <http://purl.org/spar/datacite/>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        
        SELECT ?venue ?venueName (COUNT(?identifier) > 0 AS ?has_identifier)
        WHERE {
            ?expression a fabio:Expression ;
                       frbr:partOf ?venue .
            ?venue dcterms:title ?venueName .
            OPTIONAL {
                ?venue datacite:hasIdentifier ?identifier .
            }
        }
        GROUP BY ?venue ?venueName
        """
        
        results = self._execute_sparql_query(query)
        
        disambiguated_venues = set()
        for binding in results["results"]["bindings"]:
            venue_uri = binding["venue"]["value"]
            venue_name = binding["venueName"]["value"].lower()
            has_external_ids = binding["has_identifier"]["value"] == 'true'
            
            if not has_external_ids:
                disambiguated_venues.add(venue_name)
            else:
                disambiguated_venues.add(venue_uri)
        
        return len(disambiguated_venues)

    def count_venues(self) -> int:
        """
        Count the number of distinct venues (simple count without disambiguation).
        
        Returns:
            Number of distinct venues
        """
        query = """
        PREFIX fabio: <http://purl.org/spar/fabio/>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        
        SELECT (COUNT(DISTINCT ?venue) AS ?count)
        WHERE {
            ?expression a fabio:Expression ;
                       frbr:partOf ?venue .
        }
        """
        
        results = self._execute_sparql_query(query)
        return int(results["results"]["bindings"][0]["count"]["value"])
    
    def run_selected_analyses(self, analyze_br: bool, analyze_ar: bool, analyze_venues: bool) -> Dict[str, Any]:
        """
        Execute selected statistical analyses and return results.
        
        Args:
            analyze_br: Whether to analyze bibliographic resources (fabio:Expression)
            analyze_ar: Whether to analyze author roles (pro:author, pro:publisher, pro:editor)
            analyze_venues: Whether to analyze venues
        
        Returns:
            Dictionary containing selected analysis results
        """
        print("Starting SPARQL dataset analysis...")
        print(f"Connected to endpoint: {self.sparql_endpoint}")
        print()
        
        results = {}
        
        if analyze_br:
            print("1. Counting fabio:Expression entities...")
            try:
                expressions_count = self.count_expressions()
                results['fabio_expressions'] = expressions_count
                print(f"   Found {expressions_count:,} fabio:Expression entities")
            except Exception as e:
                print(f"   Error: {e}")
                results['fabio_expressions'] = None
            print()
        
        if analyze_ar:
            print("2. Counting pro:author, pro:publisher and pro:editor roles...")
            try:
                role_counts = self.count_role_entities()
                results['roles'] = role_counts
                print(f"   Found {role_counts['pro:author']:,} pro:author roles")
                print(f"   Found {role_counts['pro:publisher']:,} pro:publisher roles")
                print(f"   Found {role_counts['pro:editor']:,} pro:editor roles")
            except Exception as e:
                print(f"   Error: {e}")
                results['roles'] = None
            print()
        
        if analyze_venues:
            print("3. Counting venues (with disambiguation)...")
            try:
                venues_count = self.count_venues_disambiguated()
                results['venues_disambiguated'] = venues_count
                print(f"   Found {venues_count:,} distinct disambiguated venues")
                
                simple_count = self.count_venues()
                results['venues_simple'] = simple_count
                print(f"   (Simple count without disambiguation: {simple_count:,})")
            except Exception as e:
                print(f"   Error: {e}")
                results['venues_disambiguated'] = None
                results['venues_simple'] = None
            print()
        
        print("Analysis completed!")
        return results

    def run_all_analyses(self) -> Dict[str, Any]:
        """
        Execute all statistical analyses and return comprehensive results.
        
        Returns:
            Dictionary containing all analysis results
        """
        return self.run_selected_analyses(analyze_br=True, analyze_ar=True, analyze_venues=True)


def main():
    """
    Main function to run analyses with command line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Analyze OpenCitations Meta dataset statistics via SPARQL queries',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all analyses
  python sparql_analyser.py http://localhost:8890/sparql
  
  # Analyze only bibliographic resources
  python sparql_analyser.py http://localhost:8890/sparql --br
  
  # Analyze only author roles
  python sparql_analyser.py http://localhost:8890/sparql --ar
  
  # Analyze only venues  
  python sparql_analyser.py http://localhost:8890/sparql --venues
  
  # Analyze multiple specific types
  python sparql_analyser.py http://localhost:8890/sparql --br --venues

This tool will execute the following analyses based on options:
  --br: Count fabio:Expression entities
  --ar: Count pro:author, pro:publisher and pro:editor roles  
  --venues: Count distinct disambiguated venues
  
If no specific options are provided, all analyses will be executed.
        """
    )
    
    parser.add_argument(
        'sparql_endpoint',
        help='SPARQL endpoint URL (required)'
    )
    
    parser.add_argument(
        '--br',
        action='store_true',
        help='Analyze bibliographic resources (fabio:Expression entities)'
    )
    
    parser.add_argument(
        '--ar', 
        action='store_true',
        help='Analyze author roles (pro:author, pro:publisher, pro:editor)'
    )
    
    parser.add_argument(
        '--venues',
        action='store_true', 
        help='Analyze venues with disambiguation'
    )
        
    args = parser.parse_args()
    
    analyze_br = args.br or not (args.br or args.ar or args.venues)
    analyze_ar = args.ar or not (args.br or args.ar or args.venues) 
    analyze_venues = args.venues or not (args.br or args.ar or args.venues)
    
    try:
        analyser = OCMetaSPARQLAnalyser(args.sparql_endpoint)
        results = analyser.run_selected_analyses(analyze_br, analyze_ar, analyze_venues)
        
        print("\n" + "="*50)
        print("SUMMARY OF RESULTS")
        print("="*50)
        
        if results.get('fabio_expressions') is not None:
            print(f"fabio:Expression entities: {results['fabio_expressions']:,}")
        
        if results.get('roles'):
            print(f"pro:author roles: {results['roles']['pro:author']:,}")
            print(f"pro:publisher roles: {results['roles']['pro:publisher']:,}")
            print(f"pro:editor roles: {results['roles']['pro:editor']:,}")
        
        if results.get('venues_disambiguated') is not None:
            print(f"Distinct venues (disambiguated): {results['venues_disambiguated']:,}")
            if results.get('venues_simple') is not None:
                print(f"Distinct venues (simple count): {results['venues_simple']:,}")
        
        return results
        
    except Exception as e:
        print(f"Analysis failed: {e}")
        return None


if __name__ == "__main__":
    main() 