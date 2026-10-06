<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Select duplicate responsible agents for merging

The duplicate RA command selects groups for merging by reading local agent names and identifiers, while it sets aside groups that need review. This selection uses no external APIs.

## Select groups locally

```bash
uv run python -m oc_meta.run.patches.fix_duplicate_ras \
  -c config/meta_config.yaml \
  --dry-run \
  --duplicates duplicate_ras.csv \
  --report-file duplicate_ra_selection.json \
  --merge-file duplicate_ras_merge.csv \
  --review-file duplicate_ras_deferred.csv \
  --workers 4
```

A group qualifies when all names are present, all members share an identifier with the same scheme and value, and each scheme have only one value across the group. ORCID values must also pass format and checksum checks. The command normalizes ORCID values before comparing them, so a URL and its bare identifier count as the same value.

Group size alone does not cause exclusion. Names provide supporting evidence, but matching names without a shared identifier do not qualify a group for the merge CSV.

The merge CSV contains the original survivor and the other members of each qualifying group, with the columns that the merge command expects. The deferred CSV records the excluded groups and their reasons; the JSON summary contains counts and paths for both files. Default output paths are `REPORT.merge.csv` and `REPORT.deferred.csv` when their flags are omitted.

Use the merge CSV with the [entity merger](14-merge-entities.md), which applies the merges in a separate step.

## Plan corrections for deferred groups

External checks are an explicit task. To investigate selected deferred groups, keep their full rows and the header in a separate CSV, then run:

```bash
uv run python -m oc_meta.run.patches.fix_duplicate_ras \
  -c config/meta_config.yaml \
  --repair \
  --duplicates selected_deferred_ras.csv \
  --report-file duplicate_ra_corrections.json \
  --review-file duplicate_ra_decisions.csv \
  --cache-file duplicate_ra_api_cache.sqlite \
  --mailto name@example.org
```

This mode creates a correction plan without changing RDF. It reads each distinct valid ORCID profile first, and it seeks work evidence only for malformed ORCIDs or profiles that match another group member while conflicting with the holder's name. Missing profiles and unresolved names remain for review.

For those agents, the command selects up to five works each unless `--max-evidence-works` sets another limit. It queries Crossref first and tries DataCite when Crossref has no record. OpenAlex is queried only when the available evidence leaves the case unresolved.

A correction requires external confirmation of the agent's role through contributor names and order; malformed or ambiguous local role chains cannot supply that confirmation. The plan proposes detaching a wrong ORCID link. Roles, contributor order, and identifier entities remain unchanged, while cases without enough evidence stay in the report for review.

The SQLite cache stores successful responses and missing records, so a later run can reuse the same cache file. `--refresh-cache` requests fresh responses. Supply `--openalex-api-key` or set `OPENALEX_API_KEY` when using a key for OpenAlex.

## Apply reviewed corrections

Set `decision` to `approve` for each correction that should run, or use `reject` or an empty cell to leave it unapplied. Keep the other columns unchanged.

```bash
uv run python -m oc_meta.run.patches.fix_duplicate_ras \
  -c config/meta_config.yaml \
  --execute duplicate_ra_corrections.json \
  --review-file duplicate_ra_decisions.csv \
  --resp-agent https://orcid.org/0000-0002-8420-0696
```

Execution checks the stored inputs and confirming work links against the current dataset before it applies each group of approved corrections. It writes RDF and provenance files, then marks the dataset as needing re-indexing.
