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


## Project Structure

- `src/pydeb_s3/` - Main package code
  - `package.py` - Package model (parsing .deb files via python-debian)
  - `manifest.py` - Packages manifest (debian.deb822.Packages)
  - `release.py` - Release file generation
  - `lock.py` - S3 locking mechanism
  - `s3_utils.py` - boto3 S3 operations
  - `cli.py` - Typer CLI commands

- `tests/` - pytest tests (use `pythonpath = "src"` in pyproject.toml)

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
