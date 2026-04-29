# pydeb-s3

[![PyPI version](https://img.shields.io/pypi/v/pydeb-s3.svg)](https://pypi.org/project/pydeb-s3/)
[![License](https://img.shields.io/pypi/l/pydeb-s3.svg)](LICENSE)
[![Python versions](https://img.shields.io/pypi/pyversions/pydeb-s3.svg)](https://pypi.org/project/pydeb-s3/)
[![GitHub stars](https://img.shields.io/github/stars/lingfish/pydeb-s3.svg)](https://github.com/lingfish/pydeb-s3/stargazers)

**pydeb-s3** is a Python port of [deb-s3](https://github.com/deb-s3/deb-s3), a simple utility to make creating and managing APT repositories on S3.

Most existing guides on using S3 to host an APT repository have you using something like [reprepro](http://mirrorer.alioth.debian.org/) to generate the repository file structure, and then [s3cmd](http://s3tools.org/s3cmd) to sync the files to S3.

The annoying thing about this process is it requires you to maintain a local copy of the file tree for regenerating and syncing the next time. Personally, my process is to use one-off virtual machines with [Vagrant](http://vagrantup.com), script out the build process, and then would prefer to just upload the final `.deb` from my Mac.

With **pydeb-s3**, there is no need for this. pydeb-s3 features:

- Downloads the existing package manifest and parses it.
- Updates it with the new package, replacing the existing entry if already there or adding a new one if not.
- Uploads the package itself, the Packages manifest, and the Packages.gz manifest. It will skip the uploading if the package is already there.
- Updates the Release file with the new hashes and file sizes.

## Updated Features

pydeb-s3 has been rewritten in Python with modern tooling and additional capabilities:

- Parses `.deb` files using the official `python-debian` library
- Updates package manifests, replacing existing entries or adding new ones
- Uploads packages, Packages manifest, and compressed manifests (`.gz`, `.bz2`, `.xz`)
- Updates Release file with new hashes and file sizes
- **GPG signing** of Release files for secure APT repositories
- **S3-compatible storage** support (AWS S3, Google Cloud Storage, MinIO, etc.)
- **Concurrent operation locking** to prevent conflicting uploads
- **Dry-run mode** for clean/verify operations
- Modern CLI with Typer, featuring help text and shell completion

## Installation

Install via pip:

```bash
$ pip install pydeb-s3
```

For isolated installation, use [pipx](https://pipx.pypa.io/):

```bash
$ pipx install pydeb-s3
```

## Quick Start

Upload a package to S3:

```bash
$ pydeb-s3 upload --bucket my-bucket my-deb-package-1.0.0_amd64.deb
```

For S3-compatible endpoints (e.g., Google Cloud Storage, MinIO):

```bash
$ pydeb-s3 upload --bucket my-bucket \
    --endpoint https://storage.googleapis.com \
    --checksum-when-required \
    --visibility nil \
    my-deb-package-1.0.0_amd64.deb
```

## Usage

pydeb-s3 provides the following commands:

```bash
$ pydeb-s3 --help
Usage: pydeb-s3 [OPTIONS] COMMAND [ARGS]...

  Easily create and manage an APT repository on S3

Options:
  --quiet               Only show errors
  --debug               Enable debug output
  --install-completion  Install completion for the current shell.
  --show-completion     Show completion for the current shell, to copy it or
                        customize the installation.
  --help                Show this message and exit.

Commands:
  upload   Upload the given files to a S3 bucket as an APT repository.
  list     List packages in given codename, component, and optionally architecture.
  show     Show information about a package.
  exists   Check if a package exists in the repository.
  copy     Copy a package to another codename and component.
  delete   Remove a package from the repository.
  verify   Verify that the files in the package manifests exist.
  clean    Remove orphaned package files.
```

For detailed options per command, run `pydeb-s3 <command> --help`.

## Common Command Examples

### List packages
```bash
$ pydeb-s3 list --bucket my-bucket --codename stable
```

### Show package info
```bash
$ pydeb-s3 show mypackage --bucket my-bucket --version 1.0.0
```

### Check if package exists
```bash
$ pydeb-s3 exists mypackage --bucket my-bucket --version 1.0.0
```

### Copy package to another codename
```bash
$ pydeb-s3 copy mypackage --bucket my-bucket --to-codename jammy --to-component main
```

### Verify repository integrity
```bash
$ pydeb-s3 verify --bucket my-bucket --fix-manifests
```

### Clean orphaned packages (dry-run first!)
```bash
$ pydeb-s3 clean --bucket my-bucket --dry-run
$ pydeb-s3 clean --bucket my-bucket  # Actually remove orphans
```

## Configuration

### AWS Credentials

pydeb-s3 uses standard `boto3` credential resolution:

1. **Command-line options**: `--access-key-id`, `--secret-access-key`, `--session-token`
2. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
3. **AWS config file**: `~/.aws/credentials` and `~/.aws/config`

### S3 Bucket

The `--bucket` option is required for all commands. Use `--prefix` to add a path prefix to all S3 objects.

### Visibility / ACL

Control uploaded file permissions with `--visibility`:
- `public` (default): public-read ACL
- `private`: private ACL
- `authenticated`: authenticated-read ACL
- `nil`: No ACL (for S3-compatible storage that doesn't support ACLs)

### GPG Signing

Sign Release files with `--sign <KEY_ID>`. You can specify multiple keys if needed (though repeatable `--sign` is limited by Typer version constraints).

## Development

pydeb-s3 uses [hatch](https://hatch.pypa.io/latest/) for packaging and dependency management.

## License

MIT License - see LICENSE file for details.

## Credits

- Original [deb-s3](https://github.com/deb-s3/deb-s3) by [Ken Robertson](https://github.com/krobertson)
