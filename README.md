[<img src="https://img.shields.io/badge/powered%20by-OpenCitations-%239931FC?labelColor=2D22DE" />](http://opencitations.net)
[![Run tests](https://github.com/opencitations/oc_meta/actions/workflows/run_tests.yml/badge.svg)](https://github.com/opencitations/oc_meta/actions/workflows/run_tests.yml)
[![Coverage](https://byob.yarr.is/arcangelo7/badges/opencitations-oc_meta_coverage)](https://opencitations.github.io/oc_meta/)
![PyPI](https://img.shields.io/pypi/pyversions/oc_meta?logo=python&logoColor=white&label=python&color=blue)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/opencitations/oc_meta)

# OpenCitations Meta

OpenCitations Meta processes bibliographic metadata for the [OpenCitations](https://opencitations.net/) infrastructure. It curates data from CSV files and generates RDF compliant with the [OpenCitations Data Model](http://opencitations.net/model).

## Documentation

Full documentation: **https://opencitations.github.io/oc_meta/**

## Installation

```bash
pip install oc_meta
```

## Quick start

```bash
uv run python -m oc_meta.run.meta_process -c meta_config.yaml
```

See the [getting started guide](https://opencitations.github.io/oc_meta/guides/getting_started/) for configuration details.

## Development

```bash
git clone https://github.com/opencitations/oc_meta.git
cd oc_meta
uv sync
```

Run tests:

```bash
./test/start-test-databases.sh
uv run coverage run --rcfile=test/coverage/.coveragerc
./test/stop-test-databases.sh
```

## How to cite

If you use OpenCitations Meta in your research, please cite:

Arcangelo Massari, Fabio Mariani, Ivan Heibi, Silvio Peroni, David Shotton; OpenCitations Meta. *Quantitative Science Studies* 2024; 5 (1): 50-75. doi: [https://doi.org/10.1162/qss_a_00292](https://doi.org/10.1162/qss_a_00292)
