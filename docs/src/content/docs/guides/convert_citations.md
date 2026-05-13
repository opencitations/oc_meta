---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Convert citations
description: Resolve temporary IDs in citation CSVs to OMIDs after Meta processing
---

Some data sources use temporary identifiers (`temp:` prefix) to link metadata rows to their citation relationships. These IDs have no meaning outside the processing context and are not persisted in RDF. After Meta processing, the output CSV preserves the mapping between each `temp:` ID and its assigned OMID (e.g., `temp:gesis-ssoar-34729_b1 omid:br/06019115518` in the `id` column). This script reads that mapping and rewrites citation CSVs with OMIDs, producing output ready for the [OpenCitations Index](https://github.com/opencitations/index) [`cnc.py`](https://github.com/opencitations/index/blob/master/oc_index/scripts/cnc.py).

The standard Index pipeline (`meta2redis.py` → `cnc.py`) only loads persistent identifiers (DOI, ISBN, ORCID, etc.) into Redis; `temp:` IDs are excluded to prevent collisions. When citation data uses `temp:` IDs, this script is a required pre-processing step: it resolves them to OMIDs before feeding the output to `cnc.py`. It also validates transitive closure by reporting any citation IDs that could not be mapped.

## Usage

```bash
uv run python -m oc_meta.run.meta.convert_citations \
  -m /path/to/meta/output/csv/dir \
  -c /path/to/citations/dir \
  -o /path/to/output/dir
```

| Argument | Description |
|----------|-------------|
| `-m`, `--meta-output` | Directory containing Meta output CSVs (the curated CSVs with OMIDs in the `id` column) |
| `-c`, `--citations` | Directory containing input citation CSVs with `citing_id` and `cited_id` columns |
| `-o`, `--output` | Directory where converted citation CSVs will be written |

## How the mapping works

Meta's output CSVs contain all assigned identifiers in the `id` column, space-separated:

```
temp:gesis-ssoar-34729_b1 omid:br/06019115518
doi:10.14361/9783839434291 isbn:9783839434291 omid:br/0622049481
```

The script maps every non-OMID identifier to its OMID. In the first row, `temp:gesis-ssoar-34729_b1` maps to `omid:br/06019115518`. In the second, both the DOI and the ISBN map to `omid:br/0622049481`.

## Input and output format

Input citation CSVs must have `citing_id` and `cited_id` columns. Extra columns are ignored.

```
citing_id,citing_publication_date,cited_id,cited_publication_date
temp:urn:nbn:de:0168-ssoar-347295,2000,temp:gesis-ssoar-34729_b1,1941
```

Output CSVs use `citing` and `cited` columns with OMIDs, matching the format expected by `cnc.py`:

```
citing,cited
omid:br/06019115517,omid:br/06019115518
```

## Unresolvable citations

If a citing or cited ID has no OMID mapping, the citation row is dropped. The script prints a summary with the count of resolved and unresolved citations, and lists any orphan IDs.

This typically happens when the metadata and citations files were not cleaned together. Both files should always be used as a matched pair from the same cleaning step.
