---
title: Testing
description: Running tests and CI/CD setup
---

## Running tests locally

Tests require Docker. Redis and QLever containers are managed automatically by pytest fixtures in `test/conftest.py`.

### 1. Install dependencies

```bash
uv sync
```

### 2. Run tests

```bash
uv run coverage run --rcfile=test/coverage/.coveragerc
```

Docker containers start automatically at the beginning of the test session and stop when tests complete.

Test infrastructure:

- Redis on port 6381 (databases 0 and 5)
- QLever data triplestore on port 8805
- QLever provenance triplestore on port 8806

View coverage report:

```bash
uv run coverage report
```

Generate HTML report:

```bash
uv run coverage html -d htmlcov
```

## Running specific tests

Run a single test file:

```bash
uv run python -m pytest test/curator_test.py -v
```

Run tests matching a pattern:

```bash
uv run python -m pytest test/ -k "test_doi" -v
```

## Test structure

Tests are in the `test/` directory:

| File | Tests |
|------|-------|
| `curator_test.py` | Data validation and normalization |
| `creator_test.py` | RDF generation |
| `meta_process_test.py` | End-to-end pipeline |
| `editor_test.py` | Post-processing modifications |
| `finder_test.py` | Entity lookup |
| `group_entities_test.py` | Merge grouping algorithm |

Test fixtures use minimal datasets in `test/` subdirectories.

## CI/CD

### GitHub Actions workflow

Tests run automatically on push and pull request via `.github/workflows/run_tests.yml`:

```yaml
name: Run tests

on:
  push:
    branches: [master]
  pull_request:
    branches: [master]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11", "3.12", "3.13"]
```

The workflow:

1. Sets up Python with UV
2. Installs dependencies
3. Runs tests with coverage (Docker containers managed by pytest)
4. Uploads coverage report

### Test matrix

Tests run on Python 3.10, 3.11, 3.12, and 3.13.

## Writing tests

### Test naming

Test files: `*_test.py`
Test functions: `test_*`

### Fixtures

Use pytest fixtures for common setup:

```python
@pytest.fixture
def redis_handler():
    return RedisCounterHandler(
        host="localhost",
        port=6381,
        db=5,
        supplier_prefix="060"
    )

def test_counter_increment(redis_handler):
    initial = redis_handler.read_counter("br")
    redis_handler.increment_counter("br")
    assert redis_handler.read_counter("br") == initial + 1
```

### Triplestore tests

Tests that need SPARQL use the test QLever instance:

```python
SPARQL_ENDPOINT = "http://localhost:8805"

def test_sparql_query():
    finder = ResourceFinder(ts=SPARQL_ENDPOINT, base_iri="https://w3id.org/oc/meta")
    # Test queries...
```

### Cleanup

Tests should clean up after themselves:

```python
def test_with_cleanup(redis_handler):
    try:
        # Test code...
    finally:
        # Cleanup
        redis_handler.delete_counter("br")
```

Or use fixtures with cleanup:

```python
@pytest.fixture
def temp_graph():
    g = Graph()
    yield g
    # Cleanup happens automatically after test
```

## Coverage requirements

Aim for high coverage on:

- `oc_meta.core.curator` - Data validation logic
- `oc_meta.core.creator` - RDF generation
- `oc_meta.lib.finder` - Entity lookup
- `oc_meta.lib.cleaner` - Identifier normalization

Lower coverage is acceptable for:

- CLI scripts (tested via integration tests)
- Error handling paths (hard to trigger in tests)
