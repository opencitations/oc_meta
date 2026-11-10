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

A group qualifies when all names are present and every pair of names can belong to the same person. Before names are compared, they are converted to Latin letters. The comparison accepts initials, known nicknames, small spelling differences, extra given names or surnames, and a reversed name order. All members must also share an identifier that has the same scheme and value, while each scheme can have only one value across the group. ORCID values must pass format and checksum checks too. The command normalizes ORCID values before it compares them, so a URL and its bare identifier count as the same value.

Group size alone does not cause exclusion, and matching names without a shared identifier do not qualify a group for the merge CSV.

The merge CSV contains the original survivor and the other members of each qualifying group, with the columns that the merge command expects. The deferred CSV records the excluded groups and their reasons; the JSON summary contains counts and paths for both files. Default output paths are `REPORT.merge.csv` and `REPORT.deferred.csv` when their flags are omitted.

Use the merge CSV with the [entity merger](14-merge-entities.md), which applies the merges in a separate step.

## Plan corrections for deferred groups

External checks are an explicit task. To investigate the deferred groups, pass their CSV to the repair mode:

```bash
uv run python -m oc_meta.run.patches.fix_duplicate_ras \
  -c config/meta_config.yaml \
  --repair \
  --duplicates duplicate_ras_deferred.csv \
  --report-file duplicate_ra_corrections.json \
  --merge-file duplicate_ras_confirmed_merge.csv \
  --review-file duplicate_ra_decisions.csv \
  --cache-file duplicate_ra_api_cache.sqlite \
  --mailto name@example.org
```

This mode creates a correction plan and a merge CSV without changing RDF. It handles groups whose only risk is conflicting names and whose members all hold one ORCID. For each of those ORCIDs, the command reads the public record once, which provides the names on the profile and the DOIs of the listed works.

Members whose name can belong to the profile are confirmed. When a group has at least two confirmed members, they form a row of the merge CSV, and the original survivor stays in place if it is one of them.

A member has received the ORCID by mistake only if its name fits the profile nowhere and a work with a DOI in the record has a co-author who clearly is the profile owner. The member's name must share no word with the profile. The co-author must spell at least two words of a profile name exactly, or its only word when the rest are initials. The plan proposes detaching the ORCID from that member. Roles, contributor order, and identifier entities remain unchanged. Every other member stays in the report for review, and so does each group outside this rule.

The SQLite cache stores successful responses and unavailable records, so a later run can reuse the same cache file. `--refresh-cache` requests fresh responses.

ORCID limits how many records an anonymous client can read each day, and an access token raises that limit. The token takes two steps to obtain. First, open the [ORCID developer tools page](https://orcid.org/developer-tools) and copy your client ID and client secret, registering them there if your account has none. These two values are not the token, so exchange them for one:

```bash
curl -H 'Accept: application/json' \
  -d 'client_id=YOUR_CLIENT_ID' \
  -d 'client_secret=YOUR_CLIENT_SECRET' \
  -d 'scope=/read-public' \
  -d 'grant_type=client_credentials' \
  https://orcid.org/oauth/token
```

The token is the value of `access_token` in the JSON response. Add the line `ORCID_ACCESS_TOKEN=your-token` to a `.env` file in the directory where you run the command, or export a shell environment variable with the same name.

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
