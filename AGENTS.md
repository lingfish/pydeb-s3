# pydeb-s3 Agent Guide

## Project Setup

Uses **hatch** for Python packaging. Source code is in `src/pydeb_s3/` (src layout pattern).

## Key Commands

```bash
# Run tests with coverage
hatch test --cover

# Format (ruff)
hatch fmt

# Run CLI
hatch run pydeb-s3 --help
```

## Key Conventions

- **Commit format**: conventional commits (e.g., `fix:`, `feat:`)
- **Tag format**: `X.Y.Z` (no 'v' prefix)
- **Run tests before committing**
- **Integration test file naming**: `test_<module>_behavior.py`
- **Version numbering**: Dynamically decided via hatch-vcs, not `hatch version`


## Project Structure

- `src/pydeb_s3/` - Main package code
  - `package.py` - Package model (parsing .deb files via python-debian)
  - `manifest.py` - Packages manifest (debian.deb822.Packages)
  - `release.py` - Release file generation, `SigningAdapter` protocol, `GpgSigningAdapter`
  - `lock.py` - S3 locking mechanism
  - `s3_utils.py` - boto3 S3 operations
  - `cli.py` - Typer CLI commands

- `tests/` - pytest tests (use `pythonpath = "src"` in pyproject.toml)
  - `conftest.py` - `MockSigningAdapter` for testing without GPG

## Dependencies

| Library | Purpose |
|---------|---------|
| `python-debian` | Official Debian library for deb822 parsing |
| `boto3` | AWS S3 SDK |
| `typer` | CLI framework |
| `rich` | Terminal formatting |

## CLI Entry Point

CLI is invoked via `pyproject.toml` script entry: `pydeb-s3 = "pydeb_s3.cli:app"`

## Important Notes

- `--sign` option is currently not repeatable (unlike Ruby version) due to Typer version constraints
- Signing uses `SigningAdapter` protocol - pass adapter to `Release.sign()` method
- `GpgSigningAdapter` implements GPG subprocess calls; use `MockSigningAdapter` from `conftest.py` for tests

## Testing

- After a plan is approved and before executing code changes, use the test-engineer skill to verify the planned changes are correct

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

When the user types `/graphify`, invoke the `skill` tool with `skill: "graphify"` before doing anything else.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- Dirty graphify-out/ files are expected after hooks or incremental updates; dirty graph files are not a reason to skip graphify. Only skip graphify if the task is about stale or incorrect graph output, or the user explicitly says not to use it.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
