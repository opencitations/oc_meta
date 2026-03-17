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
import os
import re
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, Set

from rich_argparse import RichHelpFormatter
from sparqlite import SPARQLClient

from oc_meta.lib.console import create_progress
from oc_meta.lib.file_manager import get_csv_data
from oc_meta.lib.master_of_regex import name_and_ids


def _count_venues_in_file(filepath: str) -> Set[str]:
    csv_data = get_csv_data(filepath)
    venues = set()
    for row in csv_data:
        if row['venue']:
            ven_name_and_ids = re.search(name_and_ids, row['venue'])
            if ven_name_and_ids:
                venue_name = ven_name_and_ids.group(1).lower()
                venue_ids = set(ven_name_and_ids.group(2).split())
                venue_metaid = [identifier for identifier in venue_ids if identifier.split(':', maxsplit=1)[0] == 'omid'][0]
                if not venue_ids.difference({venue_metaid}):
                    venues.add(venue_name)
                else:
                    venues.add(venue_metaid)
    return venues


class OCMetaStatistics:
    def __init__(self, sparql_endpoint: str, csv_dump_path: str | None = None, max_retries: int = 3, retry_delay: int = 5):
        self.sparql_endpoint = sparql_endpoint
        self.csv_dump_path = csv_dump_path
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.client = SPARQLClient(sparql_endpoint, max_retries=max_retries, backoff_factor=retry_delay, timeout=3600)

    def _execute_sparql_query(self, query: str) -> Dict:
        try:
            return self.client.query(query)
        except Exception as e:
            print(f"Query failed after {self.max_retries} retries.", file=sys.stderr)
            raise Exception("SPARQL query failed after multiple retries.") from e

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        self.client.close()

    def count_expressions(self) -> int:
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

    def count_venues_from_csv(self) -> int:
        if not self.csv_dump_path:
            raise ValueError("CSV dump path is required to count venues")

        filenames = sorted(os.listdir(self.csv_dump_path))
        filepaths = [os.path.join(self.csv_dump_path, f) for f in filenames if f.endswith('.csv')]

        all_venues: Set[str] = set()

        with create_progress() as progress:
            task = progress.add_task("Counting venues from CSV files...", total=len(filepaths))

            with ProcessPoolExecutor() as executor:
                futures = {executor.submit(_count_venues_in_file, fp): fp for fp in filepaths}
                for future in as_completed(futures):
                    venues = future.result()
                    all_venues.update(venues)
                    progress.update(task, advance=1)

        return len(all_venues)

    def run_selected_analyses(self, analyze_br: bool, analyze_ar: bool, analyze_venues: bool) -> Dict:
        print("Starting dataset statistics...")
        print(f"Connected to endpoint: {self.sparql_endpoint}")
        if self.csv_dump_path:
            print(f"CSV dump path: {self.csv_dump_path}")
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
            print("3. Counting venues from CSV dump...")
            if not self.csv_dump_path:
                print("   Error: CSV dump path is required for venue counting")
                results['venues'] = None
            else:
                try:
                    venues_count = self.count_venues_from_csv()
                    results['venues'] = venues_count
                    print(f"   Found {venues_count:,} distinct venues")
                except Exception as e:
                    print(f"   Error: {e}")
                    results['venues'] = None
            print()

        print("Statistics completed!")
        return results

    def run_all_analyses(self) -> Dict:
        return self.run_selected_analyses(analyze_br=True, analyze_ar=True, analyze_venues=True)


def main():
    parser = argparse.ArgumentParser(
        description='Compute OpenCitations Meta dataset statistics',
        formatter_class=RichHelpFormatter,
        epilog="""
Examples:
  # Run all statistics
  python -m oc_meta.run.count.meta_entities http://localhost:8890/sparql --csv /path/to/csv/dump

  # Count only bibliographic resources
  python -m oc_meta.run.count.meta_entities http://localhost:8890/sparql --br

  # Count only roles
  python -m oc_meta.run.count.meta_entities http://localhost:8890/sparql --ar

  # Count only venues (requires CSV dump)
  python -m oc_meta.run.count.meta_entities http://localhost:8890/sparql --venues --csv /path/to/csv/dump

Statistics computed:
  --br: Count fabio:Expression entities (via SPARQL)
  --ar: Count pro:author, pro:publisher and pro:editor roles (via SPARQL)
  --venues: Count distinct venues with disambiguation (via CSV dump)

If no specific options are provided, all statistics will be computed.
        """
    )

    parser.add_argument(
        'sparql_endpoint',
        help='SPARQL endpoint URL'
    )

    parser.add_argument(
        '--csv',
        dest='csv_dump_path',
        help='Path to CSV dump directory (required for venue counting)'
    )

    parser.add_argument(
        '--br',
        action='store_true',
        help='Count bibliographic resources (fabio:Expression entities)'
    )

    parser.add_argument(
        '--ar',
        action='store_true',
        help='Count roles (pro:author, pro:publisher, pro:editor)'
    )

    parser.add_argument(
        '--venues',
        action='store_true',
        help='Count distinct venues (requires --csv)'
    )

    args = parser.parse_args()

    analyze_br = args.br or not (args.br or args.ar or args.venues)
    analyze_ar = args.ar or not (args.br or args.ar or args.venues)
    analyze_venues = args.venues or not (args.br or args.ar or args.venues)

    if analyze_venues and not args.csv_dump_path:
        print("Error: --csv is required for venue counting", file=sys.stderr)
        sys.exit(1)

    try:
        with OCMetaStatistics(args.sparql_endpoint, args.csv_dump_path) as stats:
            results = stats.run_selected_analyses(analyze_br, analyze_ar, analyze_venues)

            print("\n" + "="*50)
            print("SUMMARY")
            print("="*50)

            if results.get('fabio_expressions') is not None:
                print(f"fabio:Expression entities: {results['fabio_expressions']:,}")

            if results.get('roles'):
                print(f"pro:author roles: {results['roles']['pro:author']:,}")
                print(f"pro:publisher roles: {results['roles']['pro:publisher']:,}")
                print(f"pro:editor roles: {results['roles']['pro:editor']:,}")

            if results.get('venues') is not None:
                print(f"Distinct venues: {results['venues']:,}")

            return results

    except Exception as e:
        print(f"Statistics failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
