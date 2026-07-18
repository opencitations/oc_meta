<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Generate info dir

Scans zipped JSON-LD data and provenance files and creates filesystem counter files.

## Usage

```bash
uv run python -m oc_meta.run.infodir.gen <rdf_directory> <new_info_dir> [options]
```

## Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `rdf_directory` | Yes | Path to the RDF directory containing `br/`, `ra/`, and the other entity directories |
| `new_info_dir` | Yes | New directory where counter files will be published |
| `-o`, `--output` | No | Generation report path; defaults to `<new_info_dir>.generation-report.json` |
| `--workers` | No | Worker processes; defaults to `4` |
| `--max-examples` | No | Missing-provenance examples retained in the report; defaults to `100` |

The destination must not exist. This prevents an old or partially generated info dir from being updated in place.

## Process

1. Scans provenance ZIP files and writes snapshot counters to fixed-width temporary files.
2. Scans data ZIP files, records current entity maxima, and identifies current entities without provenance.
3. Sets each entity counter to the maximum identifier found in either data or provenance.
4. Converts the temporary counters sequentially to `prov_file_*.txt`.
5. Publishes the result from a sibling staging directory with a single rename.

The scan keeps at most twice the configured worker count in flight. Temporary storage uses four bytes per provenance counter position, in addition to the final text files.

## Example

```bash
uv run python -m oc_meta.run.infodir.gen \
  /srv/oc_meta/rdf \
  /srv/oc_meta/info_dir-new \
  --workers 8 \
  -o /srv/oc_meta/info-dir-generation.json
```

An invalid ZIP, invalid JSON-LD entity, or unsupported URI stops generation before the destination is published. Current entities without provenance do not stop generation; the report uses status `generated_with_warnings` and contains exact counts plus a bounded sample.
