#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utility functions for preloading high-author-count test data.

Used by benchmark.py to generate and load BRs with thousands of authors.
"""

import os
import random

import yaml

from oc_meta.run.meta_process import run_meta_process

def generate_atlas_paper_csv(output_path: str, num_authors: int = 2869, seed: int = 42) -> None:
    """
    Generate CSV file with a single BR having many authors (simulating ATLAS paper).

    Args:
        output_path: Path where CSV file will be created
        num_authors: Number of authors to generate (default: 2869, matching real ATLAS paper)
        seed: Random seed for reproducible ORCID generation
    """
    
    random.seed(seed)

    authors = []
    for i in range(num_authors):
        orcid = f"0000-000{i // 10000:1d}-{(i % 10000):04d}-{random.randint(1000, 9999):04d}X"
        author_name = f"Author_{i+1:04d}, Test"
        authors.append(f"{author_name} [orcid:{orcid}]")

    author_field = "; ".join(authors)

    header = '"id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"'

    row = (
        '"doi:10.1140/epjc/s10052-016-4041-9 pmid:28280425",'
        '"Probing Lepton Flavour Violation Via Neutrinoless Tau Decays With The ATLAS Detector",'
        f'"{author_field}",'
        '"2016-04-26",'
        '"The European Physical Journal C [issn:1434-6044]",'
        '"76","5","","journal article",'
        '"Springer Science And Business Media LLC [crossref:297]",""'
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(header + '\n')
        f.write(row + '\n')

    print(f"Generated CSV with {num_authors} authors: {output_path}")


def generate_atlas_update_csv(output_path: str) -> None:
    """
    Generate CSV for update scenario: same BR as atlas_paper but with modified title and empty author field.

    Args:
        output_path: Path where update CSV file will be created
    """
    header = '"id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"'

    row = (
        '"doi:10.1140/epjc/s10052-016-4041-9 pmid:28280425",'
        '"Probing Lepton Flavour Violation Via Neutrinoless Tau Decays With The ATLAS Detector - UPDATED TITLE",'
        '"",'
        '"2016-04-26",'
        '"The European Physical Journal C [issn:1434-6044]",'
        '"76","5","","journal article",'
        '"Springer Science And Business Media LLC [crossref:297]",""'
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(header + '\n')
        f.write(row + '\n')

    print(f"Generated update CSV: {output_path}")


def preload_data(config_path: str, csv_path: str) -> None:
    """
    Load test data into triplestore using MetaProcess.

    Args:
        config_path: Path to meta_config.yaml
        csv_path: Path to CSV file to process
    """
    print(f"\n{'='*60}")
    print("Preloading High-Author Test Data")
    print(f"{'='*60}")
    print(f"Config: {config_path}")
    print(f"CSV: {csv_path}")
    print(f"{'='*60}\n")

    with open(config_path) as f:
        settings = yaml.safe_load(f)

    original_input_dir = settings["input_csv_dir"]
    test_input_dir = os.path.dirname(csv_path)
    settings["input_csv_dir"] = test_input_dir

    temp_config_path = os.path.join(test_input_dir, "_temp_preload_config.yaml")
    with open(temp_config_path, 'w') as f:
        yaml.dump(settings, f)

    try:
        print("Starting meta_process to load data...")
        print(f"Input directory: {test_input_dir}")
        print(f"CSV file: {os.path.basename(csv_path)}\n")

        run_meta_process(settings, temp_config_path)

        print(f"\n{'='*60}")
        print("Preload Complete")
        print(f"{'='*60}\n")

    finally:
        if os.path.exists(temp_config_path):
            os.remove(temp_config_path)

        settings["input_csv_dir"] = original_input_dir
