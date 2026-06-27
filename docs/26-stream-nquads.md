<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Stream N-Quads

Converts JSON-LD ZIP archives to N-Quads using rdflib. By default it writes to stdout for pipe-based tools such as QLever. It can also write chunked `.nq` or `.nq.gz` files for loaders such as Virtuoso.

## Usage

```bash
uv run python -m oc_meta.run.migration.stream_nquads <rdf_dir> [options]
```

## Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `rdf_dir` | Yes | - | Root directory containing RDF ZIP archives |
| `-m`, `--mode` | No | `all` | Mode: `all` for all ZIP files, `data` for entity data only, `prov` for provenance only |
| `-w`, `--workers` | No | min(8, CPU count) | Number of worker processes |
| `-o`, `--output-dir` | No | - | Directory where chunked N-Quads files are written instead of stdout |
| `--lines-per-file` | No | `10000000` | Number of N-Quads lines per output file when `--output-dir` is used |
| `--gzip` | No | disabled | Compress chunked output files with gzip when `--output-dir` is used |
| `--prefix` | No | `output` | Output file prefix when `--output-dir` is used |

## Modes

### All mode (default)

Processes all ZIP files, both entity data and provenance.

```bash
uv run python -m oc_meta.run.migration.stream_nquads /srv/oc_meta/rdf > output.nq
```

### Data mode

Processes numeric ZIP files (e.g., `1000.zip`, `2000.zip`) excluding `se.zip` and files inside `prov/` directories.

```bash
uv run python -m oc_meta.run.migration.stream_nquads /srv/oc_meta/rdf --mode data > data.nq
```

### Provenance mode

Processes `se.zip` files in `prov/` directories.

```bash
uv run python -m oc_meta.run.migration.stream_nquads /srv/oc_meta/rdf --mode prov > prov.nq
```

## Piping into QLever

The primary use case is piping directly into QLever's indexer via Docker:

```bash
uv run python -m oc_meta.run.migration.stream_nquads /srv/oc_meta/rdf --mode data | \
  docker run --rm -i -u $(id -u):$(id -g) \
    --mount type=bind,src=$(pwd),target=/index -w /index \
    --entrypoint qlever-index \
    docker.io/adfreiburg/qlever:latest \
    -i index-name -s index-name.settings.json -F nq -f - \
    --stxxl-memory 50G
```

See the `index.sh` scripts in the QLever data directories for ready-to-use examples.

## Writing chunked files for Virtuoso

Virtuoso's bulk loader reads RDF files from a directory registered with `ld_dir` or `ld_dir_all`. Use `--output-dir` with `--gzip` to write chunked files that can be loaded directly by Virtuoso:

```bash
uv run python -m oc_meta.run.migration.stream_nquads /srv/oc_meta/rdf \
  --mode data \
  --workers 12 \
  --output-dir /srv/virtuoso/bulk_load \
  --lines-per-file 10000000 \
  --gzip \
  --prefix meta-data
```

This writes files such as:

```text
/srv/virtuoso/bulk_load/meta-data.000000.nq.gz
/srv/virtuoso/bulk_load/meta-data.000001.nq.gz
```

The same options work for provenance by using `--mode prov` and a different prefix.
