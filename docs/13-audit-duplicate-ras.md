<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Audit duplicate responsible agents

The duplicate RA auditor checks whether clusters produced by `find.duplicates ras` are safe to merge. It separates the check into two stages:

1. Confirm that the local responsible agent is the contributor recorded for the work. The comparison uses names and contributor order from Crossref, DataCite, and OpenAlex. ORCID values are not used at this stage.
2. Check the ORCID attached to that agent against work metadata and the ORCID person record.

The dry run does not change RDF files or triplestores. It writes a JSON plan and a CSV in which every proposed operation must be reviewed.

## Generate a plan

```bash
uv run python -m oc_meta.run.patches.fix_duplicate_ras \
  -c config/meta_config.yaml \
  --dry-run \
  --duplicates duplicate_ras.csv \
  --report-file duplicate_ra_audit.json \
  --review-file duplicate_ra_review.csv \
  --cache-file duplicate_ra_api_cache.sqlite \
  --mailto name@example.org
```

The script reads RDF below `output_rdf_dir/rdf`. It first scans every cluster locally, in bounded batches, and sends API requests only for clusters with at least one of these signals:

- names that do not reach the confirmed match score;
- more than one ORCID in the cluster;
- a RA connected to more than one ORCID;
- at least 50 cluster members;
- a missing agent name.

`--all-api` disables this prefilter. On a large duplicate file this increases memory use, API traffic, and execution time.

Candidate clusters are written with `merge_status: blocked_pending_review`. The summary reports how many other clusters passed the local prefilter as `locally_consistent_clusters`; those rows are not expanded in the JSON unless `--all-api` is used.

Use `--max-evidence-works` to set the maximum number of works checked for each candidate RA. The default is five. API responses, including `404` results, are stored in the SQLite cache. `--refresh-cache` fetches them again.

If OpenAlex requires a key in the execution environment, pass `--openalex-api-key` or set `OPENALEX_API_KEY`.

## Local and external checks

For each candidate, the script reconstructs:

```text
responsible agent <- pro:isHeldBy - agent role
agent role <- pro:isDocumentContextFor - bibliographic resource
bibliographic resource -> DOI or OpenAlex identifier
```

Author and editor roles are ordered through `oco:hasNext`. A fork, cycle, disconnected segment, multiple head, link to another chain, or role with more than one holder is reported as an ambiguous chain and is not treated as confirmed responsibility.

The external contributor lists are aligned with local names while preserving order. A separate unordered name comparison can propose a `reorder_chain` operation when the same contributors occur in another order. A low-scoring role can produce a `reassign_role` proposal only when another existing RA in the same duplicate cluster matches that external position. The script does not create people that are missing locally.

ORCID assessment starts after this role check. A wrong or malformed ORCID produces a correction proposal only when at least one external work record confirms the local agent at that role. Replacement also requires the same different ORCID in at least two confirmed work records and a matching ORCID person name. Otherwise the proposal is to detach the wrong identifier without deleting its identifier entity.

Automatic identifier proposals are limited to ORCID. VIAF, Wikidata, Crossref member IDs, and other schemes remain visible in the cluster report for manual review.

When a role itself needs reassignment, the same dry run does not use that proposed future state to decide an identifier change. Apply the reviewed role correction, re-index, regenerate the duplicate CSV, and run the audit again. This preserves the order between responsibility and identifier validation.

## Provenance in the report

The JSON includes local provenance for every candidate RA and its identifiers:

- first and latest generation time;
- number and URI of snapshots;
- latest `prov:wasAttributedTo` value;
- latest primary source and description;
- latest stored update query, when present.

This makes recent identifier reassignment distinguishable from data that has remained unchanged since ingestion.

The report has `complete: false` if the process receives `SIGINT` or `SIGTERM` while querying external services. An incomplete plan cannot be executed.

## Review the operations

The review CSV contains one row for each proposed operation. Set `decision` to one of:

| Value | Effect |
|---|---|
| `approve` | Apply the operation during execution |
| `reject` | Record no change |
| empty | Record no change |

Do not edit the other columns. Execution compares them with the JSON plan and stops if they differ.

Available actions are:

| Action | Change |
|---|---|
| `detach_identifier` | Remove one `datacite:hasIdentifier` link from an RA |
| `replace_identifier` | Remove the old link and attach the existing or newly created ORCID identifier |
| `reassign_role` | Change the RA connected through `pro:isHeldBy` |
| `reorder_chain` | Replace the reviewed `oco:hasNext` links |

All rows start unapproved. Cases with weak, missing, or conflicting evidence remain in the JSON for manual inspection without an executable operation.

## Execute approved changes

```bash
uv run python -m oc_meta.run.patches.fix_duplicate_ras \
  -c config/meta_config.yaml \
  --execute duplicate_ra_audit.json \
  --review-file duplicate_ra_review.csv \
  --resp-agent https://orcid.org/0000-0002-8420-0696
```

Before each group of changes, execution checks the current triplestore values against the old RA, identifier, literal value, and `hasNext` links stored in the plan. Identifier operations also retain their confirming work evidence: the executor verifies the DOI or OpenAlex ID used for the lookup, that the BR still contains the AR, that the AR is still held by the reviewed RA, and that its next link has not changed. A stale precondition stops the run. Operations that share an RA, AR, ID, bibliographic resource, or replacement ORCID are applied in the same `GraphSet`; disjoint components remain separate. This prevents a later import from the same triplestore snapshot from overwriting an earlier change and ensures that one replacement ORCID creates at most one identifier entity. `MetaEditor` writes RDF and provenance for each group.

Progress is saved after each group. When at least one group is attempted, `reindex_required.out` is created beside the JSON plan. Re-index the triplestore from the RDF files and delete the sentinel before running another correction or merge. If execution stops cleanly between groups, keep the progress file, re-index, delete the sentinel, and run the same plan and review CSV again. Changing review decisions after progress has been recorded invalidates the resume state. An exception inside a group requires a new dry run after re-indexing.

The executor also refuses to start when:

- the configuration or duplicate CSV has changed since the dry run;
- the review file contains unknown, missing, repeated, or conflicting operations;
- the JSON plan is incomplete;
- another re-index sentinel is present.
