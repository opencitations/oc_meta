---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Stream N-Quads
description: Stream N-Quads from JSON-LD ZIP archives directly to stdout
---

Converts JSON-LD ZIP archives to N-Quads using rdflib and writes the output to stdout. Designed to pipe directly into QLever's indexer, eliminating the need for intermediate files on disk.

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

## Output format

Each line is a valid N-Quads statement. Named graphs come from the `@id` field at the top level of the JSON-LD array:

- Data files produce triples in shared graphs like `<https://w3id.org/oc/meta/br/>`
- Provenance files produce triples in per-entity graphs like `<https://w3id.org/oc/meta/br/06790181/prov/>`

The N-Quads stream is written to stdout with no other output, so it can be piped directly into other tools.
