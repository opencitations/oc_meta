---
title: Contributing
description: Development guidelines and release process
---

## Development setup

1. Clone the repository:

```bash
git clone https://github.com/opencitations/oc_meta.git
```

```bash
cd oc_meta
```

2. Install dependencies with UV:

```bash
uv sync
```

3. Start test databases:

```bash
./test/start-test-databases.sh
```

4. Run tests to verify setup:

```bash
uv run coverage run --rcfile=test/coverage/.coveragerc
```

## Code style

- Python 3.10+ compatible
- Type hints where practical
- Follow existing code patterns

## Commit messages

Use [conventional commits](https://www.conventionalcommits.org/):

```
feat: add new identifier schema support
fix: correct ORCID checksum validation
docs: update configuration reference
refactor: simplify curator logic
test: add tests for edge cases
```

Types:

| Type | Description |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation |
| `refactor` | Code change that doesn't fix bug or add feature |
| `test` | Adding or updating tests |
| `chore` | Maintenance tasks |

## Pull requests

1. Create a branch from `master`
2. Make your changes
3. Run tests locally
4. Push and open a pull request
5. Wait for CI checks to pass
6. Request review

## Release process

The project uses semantic-release for automated versioning and publishing.

### Creating a release

1. Make your changes with conventional commits
2. Include `[release]` in the final commit message
3. Push to master

```bash
git commit -m "feat: add new feature [release]"
```

```bash
git push origin master
```

### What happens automatically

1. Tests run via GitHub Actions
2. If tests pass and commit contains `[release]`:
   - semantic-release determines version bump from commits
   - CHANGELOG.md is updated
   - GitHub release is created
   - Package is built and published to PyPI

### Version bumping

| Commit type | Version bump |
|-------------|--------------|
| `fix:` | Patch (1.0.0 → 1.0.1) |
| `feat:` | Minor (1.0.0 → 1.1.0) |
| `BREAKING CHANGE:` | Major (1.0.0 → 2.0.0) |

## Project structure

```
oc_meta/
├── core/           # Curator, Creator
├── lib/            # Utilities (finder, cleaner, file_manager)
├── plugins/        # Multiprocess, editor, csv_generator
└── run/            # CLI scripts
    ├── fixer/      # Data repair tools
    ├── merge/      # Find duplicates and merge entities
    ├── meta/       # Processing scripts
    ├── patches/    # hasNext anomalies and fixer
    └── upload/     # Triplestore upload

test/               # Test files
docs/               # Documentation (this site)
```

## Adding new features

### New identifier schema

1. Add validation logic to `oc_meta/lib/cleaner.py`
2. Add schema to `master_of_regex.py`
3. Update `oc_meta/core/curator.py` if needed
4. Add tests to `test/cleaner_test.py`
5. Update documentation

### New CLI script

1. Create script in appropriate `oc_meta/run/` subdirectory
2. Add argument parsing with argparse
3. Add tests
4. Document in this site

### New fixer

1. Create in `oc_meta/run/fixer/`
2. Follow existing patterns (dry-run support, provenance tracking)
3. Add tests
4. Document in patches section

## Getting help

- [GitHub Issues](https://github.com/opencitations/oc_meta/issues) - Bug reports and feature requests
- [OpenCitations](https://opencitations.net/) - Project information
