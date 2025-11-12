#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate synthetic test data for Meta processing benchmarks.

Creates CSV files with configurable sizes containing realistic bibliographic metadata
for testing the OpenCitations Meta processing pipeline.
"""

import argparse
import csv
import random
from pathlib import Path
from typing import Dict


class BenchmarkDataGenerator:
    """Generate synthetic bibliographic metadata for benchmark testing."""

    SAMPLE_TITLES = [
        "The impact of machine learning on scientific research",
        "A comprehensive review of climate change models",
        "Novel approaches to protein folding prediction",
        "Statistical methods for large-scale data analysis",
        "Understanding neural network architectures",
        "Advances in quantum computing algorithms",
        "Systematic analysis of gene expression patterns",
        "Deep learning applications in medical imaging",
        "Comparative study of optimization techniques",
        "Theoretical foundations of distributed systems",
    ]

    SAMPLE_AUTHORS = [
        "Smith, John [0000-0001-2345-6789]",
        "Johnson, Emily [0000-0002-3456-7890]",
        "Williams, David [0000-0003-4567-8901]",
        "Brown, Sarah [0000-0004-5678-9012]",
        "Garcia, Maria [0000-0005-6789-0123]",
        "Chen, Wei [0000-0006-7890-1234]",
        "Kumar, Raj [0000-0007-8901-2345]",
        "Anderson, Lisa [0000-0008-9012-3456]",
        "Martinez, Carlos [0000-0009-0123-4567]",
        "Lee, Yuki [0000-0010-1234-5678]",
    ]

    SAMPLE_VENUES = [
        "Nature [issn:0028-0836]",
        "Science [issn:0036-8075]",
        "Cell [issn:0092-8674]",
        "The Lancet [issn:0140-6736]",
        "PLOS ONE [issn:1932-6203]",
        "BMC Biology [issn:1741-7007]",
        "Scientific Reports [issn:2045-2322]",
        "IEEE Transactions [issn:0018-9340]",
        "ACM Computing Surveys [issn:0360-0300]",
        "Journal of Computational Biology [issn:1066-5277]",
    ]

    SAMPLE_PUBLISHERS = [
        "Springer Nature [crossref:297]",
        "Elsevier [crossref:78]",
        "Wiley [crossref:311]",
        "Oxford University Press [crossref:286]",
        "Cambridge University Press [crossref:56]",
    ]

    ARTICLE_TYPES = [
        "journal article",
        "review article",
        "research article",
        "conference paper",
        "book chapter",
    ]

    def __init__(self, size: int, output_path: str, seed: int = 42):
        self.size = size
        self.output_path = output_path
        random.seed(seed)

    def _generate_doi(self, index: int) -> str:
        """Generate a synthetic DOI."""
        prefix = random.choice(["10.1038", "10.1016", "10.1371", "10.1109", "10.1093"])
        return f"doi:{prefix}/benchmark.{index:06d}"

    def _generate_pmid(self) -> str:
        """Generate a synthetic PMID."""
        return f"pmid:{random.randint(10000000, 39999999)}"

    def _generate_pub_date(self) -> str:
        """Generate a publication date."""
        year = random.randint(2015, 2024)
        month = random.randint(1, 12)
        return f"{year}-{month:02d}"

    def _generate_pages(self) -> str:
        """Generate page range."""
        start = random.randint(1, 500)
        end = start + random.randint(5, 30)
        return f"{start}-{end}"

    def _generate_authors(self) -> str:
        """Generate author list."""
        num_authors = random.randint(1, 5)
        authors = random.sample(self.SAMPLE_AUTHORS, num_authors)
        return "; ".join(authors)

    def _generate_record(self, index: int) -> Dict[str, str]:
        """Generate a single bibliographic record."""
        base_id = self._generate_doi(index)

        has_pmid = random.random() > 0.3
        if has_pmid:
            base_id = f"{base_id} {self._generate_pmid()}"

        return {
            "id": base_id,
            "title": random.choice(self.SAMPLE_TITLES),
            "author": self._generate_authors(),
            "pub_date": self._generate_pub_date(),
            "venue": random.choice(self.SAMPLE_VENUES),
            "volume": str(random.randint(1, 50)),
            "issue": str(random.randint(1, 12)),
            "page": self._generate_pages(),
            "type": random.choice(self.ARTICLE_TYPES),
            "publisher": random.choice(self.SAMPLE_PUBLISHERS),
            "editor": "",
        }

    def generate(self):
        """Generate CSV file with synthetic records."""
        output_file = Path(self.output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        records = [self._generate_record(i) for i in range(self.size)]

        fieldnames = [
            "id", "title", "author", "pub_date", "venue",
            "volume", "issue", "page", "type", "publisher", "editor"
        ]

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        print(f"Generated {self.size} records in {output_file}")
        return str(output_file)


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic test data for Meta benchmarks"
    )
    parser.add_argument(
        "-s", "--size",
        type=int,
        default=100,
        help="Number of records to generate (default: 100)"
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Output CSV file path"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible data generation (default: 42)"
    )

    args = parser.parse_args()

    generator = BenchmarkDataGenerator(
        size=args.size,
        output_path=args.output,
        seed=args.seed
    )
    generator.generate()


if __name__ == "__main__":
    main()
