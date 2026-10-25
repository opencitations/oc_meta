<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Fix appended role chains

Some early merges of bibliographic resources (BRs) added the author, editor, or publisher roles of the deleted BR to the roles of the BR that survived. A BR can therefore hold two role chains of the same type, and it often lists the same people twice. This fixer removes every role of each doubled type and prepares Crossref or DataCite input, so that Meta can create those roles again from one record. Roles of other types, responsible agents, and identifiers stay in place.

## Generate the plan and CSV files

```bash
uv run python -m oc_meta.run.patches.fix_appended_roles \
  -c config/meta_config.yaml \
  --dry-run \
  --report-file appended_roles_plan.json \
  --csv-output-dir appended_roles_meta_input \
  --cache-file appended_roles_api_cache.sqlite \
  --mailto name@example.org
```

The dry run reads the RDF and provenance files below `output_rdf_dir/rdf` and does not change them. For each BR, it walks back through the provenance snapshots, starting from the current roles. A snapshot counts as an append when its update query added roles that are still present, while the BR already held roles that are also still present. If both groups include roles of the same type, for example authors, that type is doubled.

The same walk finds the identifiers that the BR held when it was created. The fixer queries the DOIs of the BR in this order: first the DOIs it held at creation, then the DOIs added later. For each DOI it asks Crossref and, when Crossref has no record, DataCite. The first record found supplies the doubled fields. The SQLite cache stores API responses, so a later run can reuse them.

The CSV files follow the layout of the [dangling AR fixer](13-fix-dangling-ars.md). Each row holds the BR OMID, the chosen DOI, the local title and type, and only the doubled fields. The other fields stay empty, so Meta leaves them unchanged. Since those roles are gone when Meta runs, Meta writes the new roles with the usual `silencer` settings. For a book chapter, a proceedings article, a reference entry, or another part of a container, Meta gives the editors to the container instead. When the container already has editors, the `silencer` skips them, so the BR gets no editor role.

## Blocked resources

A BR goes to the `blocked` list, and stays unchanged, when:

- a role is missing or has no single type;
- a role belongs to another BR, or a `oco:hasNext` link crosses the set of roles to remove;
- the BR has no DOI, or no DOI has a Crossref or DataCite record;
- the chosen record has no agents for a doubled type.

The summary counts operations, blocked BRs, each blocker, the chosen providers, and the roles to delete for each type.

## Execute the plan

```bash
uv run python -m oc_meta.run.patches.fix_appended_roles \
  -c config/meta_config.yaml \
  --execute appended_roles_plan.json \
  --resp-agent https://orcid.org/0000-0002-8420-0696
```

Execution first checks the CSV hashes and compares the BR and every planned role with the hashes in the plan. Then it writes the RDF and provenance files and saves its progress after each BR. If execution stops, run the same command again to resume. The run creates `reindex_required.out` beside the plan, and it refuses to start a new plan while that file exists. Re-index the triplestores, then run Meta on the Crossref and DataCite CSV directories.
