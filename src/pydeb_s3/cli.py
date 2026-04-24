"""CLI for pydeb-s3."""

import glob
import os
from typing import Annotated, Optional

import typer
from rich.console import Console

from pydeb_s3 import lock as lock_module
from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3 import s3_utils

app = typer.Typer(name="pydeb-s3", help="Easily create and manage an APT repository on S3")
console = Console()


def log(message: str) -> None:
    """Log a message to the console."""
    if not _get_quiet():
        console.print(f"[bold white]{message}[/bold white]")


def sublog(message: str) -> None:
    """Log a sub-message to the console."""
    if not _get_quiet():
        console.print(f"  {message}")


def error(message: str) -> None:
    """Log an error and exit."""
    console.print(f"[bold red]Error:[/bold red] {message}")
    raise typer.Exit(code=1)


def _get_quiet() -> bool:
    """Check if quiet mode is enabled."""
    return _QUIET


def _set_quiet(q: bool) -> None:
    """Set quiet mode."""
    global _QUIET
    _QUIET = q


_QUIET = False


def _configure_s3(
    bucket: str,
    prefix: Optional[str] = None,
    region: str = "us-east-1",
    endpoint: Optional[str] = None,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    visibility: str = "public",
    signing_key: Optional[list[str]] = None,
    gpg_provider: str = "gpg",
    gpg_options: str = "",
    encryption: bool = False,
    proxy_uri: Optional[str] = None,
    force_path_style: bool = False,
    checksum_when_required: bool = False,
    quiet: bool = False,
) -> None:
    """Configure S3 connection."""
    _set_quiet(quiet)
    s3_utils.configure_s3(
        bucket=bucket,
        prefix=prefix,
        region=region,
        endpoint=endpoint,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        visibility=visibility,
        signing_key=signing_key,
        gpg_provider=gpg_provider,
        gpg_options=gpg_options,
        encryption=encryption,
        proxy_uri=proxy_uri,
        force_path_style=force_path_style,
        checksum_when_required=checksum_when_required,
    )


@app.command("upload")
def upload_command(
    files: Annotated[list[str], typer.Argument(help="Files to upload")],
    arch: Annotated[Optional[str], typer.Option("-a", "--arch", help="The architecture of the package in the APT repository.")] = None,
    preserve_versions: Annotated[bool, typer.Option("-p", "--preserve-versions", help="Whether to preserve other versions of a package in the repository when uploading one.")] = False,
    lock: Annotated[bool, typer.Option("-l", "--lock", help="Use a lock file to prevent concurrent uploads.")] = False,
    fail_if_exists: Annotated[bool, typer.Option("--fail-if-exists", help="Fail if the package already exists.")] = False,
    skip_package_upload: Annotated[bool, typer.Option("--skip-package-upload", help="Don't upload the package files, only update the manifest.")] = False,
    bucket: Annotated[Optional[str], typer.Option("-b", "--bucket", help="The name of the S3 bucket to upload to.")] = None,
    prefix: Annotated[Optional[str], typer.Option("--prefix", help="The path prefix to use when storing on S3.")] = None,
    origin: Annotated[Optional[str], typer.Option("-o", "--origin", help="The origin to use in the repository Release file.")] = None,
    suite: Annotated[Optional[str], typer.Option("--suite", help="The suite to use in the repository Release file.")] = None,
    codename: Annotated[str, typer.Option("-c", "--codename", help="The codename of the APT repository.")] = "stable",
    component: Annotated[str, typer.Option("-m", "--component", help="The component of the APT repository.")] = "main",
    section: Annotated[Optional[str], typer.Option("-s", "--section", help="(deprecated, please use component)", hidden=True)] = None,
    access_key_id: Annotated[Optional[str], typer.Option("--access-key-id", help="The access key for connecting to S3.")] = None,
    secret_access_key: Annotated[Optional[str], typer.Option("--secret-access-key", help="The secret key for connecting to S3.")] = None,
    session_token: Annotated[Optional[str], typer.Option("--session-token", help="The session token for connecting to S3.")] = None,
    endpoint: Annotated[Optional[str], typer.Option("--endpoint", help="The URL endpoint to the S3 API.")] = None,
    s3_region: Annotated[str, typer.Option("--s3-region", help="The region for connecting to S3.")] = "us-east-1",
    force_path_style: Annotated[bool, typer.Option("--force-path-style", help="Use S3 path style instead of subdomains.")] = False,
    proxy_uri: Annotated[Optional[str], typer.Option("--proxy-uri", help="The URI of the proxy to send service requests through.")] = None,
    visibility: Annotated[str, typer.Option("-v", "--visibility", help="The access policy for the uploaded files. Can be public, private, or authenticated.")] = "public",
    sign: Annotated[Optional[list[str]], typer.Option("--sign", help="GPG Sign the Release file. Use --sign with your GPG key ID to use a specific key.")] = None,
    gpg_options: Annotated[str, typer.Option("--gpg-options", help="Additional command line options to pass to GPG when signing.")] = "",
    gpg_provider: Annotated[str, typer.Option("--gpg-provider", help="GPG provider to use.")] = "gpg",
    encryption: Annotated[bool, typer.Option("-e", "--encryption", help="Use S3 server side encryption.")] = False,
    quiet: Annotated[bool, typer.Option("-q", "--quiet", help="Doesn't output information, just returns status appropriately.")] = False,
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
    checksum_when_required: Annotated[bool, typer.Option("--checksum-when-required", help="Disable SDK upload checksums for S3-compatible endpoints.")] = False,
):
    """Upload the given files to a S3 bucket as an APT repository."""
    if not bucket:
        error("No value provided for required option '--bucket'")
    if not files:
        error("You must specify at least one file to upload")

    for pattern in files:
        if not glob.glob(pattern):
            error(f"File '{pattern}' doesn't exist")

    _configure_s3(
        bucket=bucket,
        prefix=prefix,
        region=s3_region,
        endpoint=endpoint,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        visibility=visibility,
        signing_key=sign,
        gpg_provider=gpg_provider,
        gpg_options=gpg_options,
        encryption=encryption,
        proxy_uri=proxy_uri,
        force_path_style=force_path_style,
        checksum_when_required=checksum_when_required,
        quiet=quiet,
    )

    comp = component
    if section:
        log("Warning: --section is deprecated, use --component instead")
        if not comp:
            comp = section

    if lock:
        lock_module.lock()

    try:
        release = release_module.Release.retrieve(codename, origin, suite)
        log(f"Retrieving existing manifests for {codename}")

        existing_arch = None
        if arch:
            existing_arch = [arch]
        else:
            existing_arch = release.architectures

        for architecture in existing_arch:
            sublog(f"Checking {codename}/{comp}/{architecture}")
            manifest = manifest_module.Manifest.retrieve(
                codename, comp, architecture, cache_control, fail_if_exists
            )

            for pattern in files:
                for filepath in glob.glob(pattern):
                    log(f"Uploading {filepath}")
                    pkg = package_module.Package.from_path(filepath)

                    if arch and pkg.arch != arch:
                        error(
                            f"Package architecture {pkg.arch} does not match specified architecture {arch}"
                        )

                    if fail_if_exists and pkg.name in manifest.packages:
                        error(f"Package {pkg.name} already exists")

                    manifest.add(pkg, preserve_versions)

                    if not skip_package_upload:
                        sublog("  Uploading package to S3")
                        s3_utils.s3_store(
                            filepath,
                            s3_utils.s3_path(f"pool/{pkg.name[0]}/{pkg.name}/{os.path.basename(filepath)}"),
                            visibility,
                            encryption,
                        )

            log(f"Uploading new manifest to S3 for {codename}/{comp}/{architecture}")
            manifest.upload(visibility)

            if fail_if_exists:
                log(f"Uploaded {codename}/{comp}/{architecture}")

        if sign:
            log(f"Signing Release file for {codename}")
            release.sign(sign, gpg_provider, gpg_options)
            release.upload(visibility)

        log("Update complete.")
    finally:
        if lock:
            lock_module.unlock()


@app.command("list")
def list_command(
    long: Annotated[bool, typer.Option("-l", "--long", help="Show more detail.")] = False,
    arch: Annotated[Optional[str], typer.Option("-a", "--arch", help="The architecture to filter to.")] = None,
    bucket: Annotated[Optional[str], typer.Option("-b", "--bucket", help="The name of the S3 bucket to upload to.")] = None,
    prefix: Annotated[Optional[str], typer.Option("--prefix", help="The path prefix to use when storing on S3.")] = None,
    codename: Annotated[str, typer.Option("-c", "--codename", help="The codename of the APT repository.")] = "stable",
    component: Annotated[str, typer.Option("-m", "--component", help="The component of the APT repository.")] = "main",
    s3_region: Annotated[str, typer.Option("--s3-region", help="The region for connecting to S3.")] = "us-east-1",
    access_key_id: Annotated[Optional[str], typer.Option("--access-key-id", help="The access key for connecting to S3.")] = None,
    secret_access_key: Annotated[Optional[str], typer.Option("--secret-access-key", help="The secret key for connecting to S3.")] = None,
    session_token: Annotated[Optional[str], typer.Option("--session-token", help="The session token for connecting to S3.")] = None,
    endpoint: Annotated[Optional[str], typer.Option("--endpoint", help="The URL endpoint to the S3 API.")] = None,
    quiet: Annotated[bool, typer.Option("-q", "--quiet", help="Doesn't output information, just returns status appropriately.")] = False,
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """List packages in given codename, component, and optionally architecture."""
    if not bucket:
        error("No value provided for required option '--bucket'")

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint, quiet=quiet)

    release = release_module.Release.retrieve(codename)
    archs = release.architectures
    if arch and arch != "all":
        archs = [arch]

    widths = [0, 0]
    rows = []

    for architecture in archs:
        manifest = manifest_module.Manifest.retrieve(codename, component, architecture, cache_control)

        if manifest.packages:
            for pkg in sorted(manifest.packages.values()):
                rows.append(
                    [
                        f"{pkg.name}",
                        f"{pkg.version}",
                        f"{architecture}",
                        f"{pkg.section}",
                        f"{pkg.description[:62]}...",
                    ]
                )

                widths[0] = max(widths[0], len(pkg.name))
                widths[1] = max(widths[1], len(pkg.version))

    if rows:
        console.print("[bold]Package[/bold]" + " " * (widths[0] - 7) + "[bold]Version[/bold]" + " " * (widths[1] - 7) + "[bold]Architecture[/bold]  [bold]Section[/bold]  [bold]Description[/bold]")

        for row in rows:
            name, version, arch, section, desc = row
            console.print(
                f"{name}"
                + " " * (widths[0] - len(name) + 1)
                + f"{version}"
                + " " * (widths[1] - len(version) + 2)
                + f"{arch}  {section}  {desc[:62]}"
            )


@app.command("show")
def show_command(
    package: Annotated[str, typer.Argument(help="The name of the package.")],
    version: Annotated[Optional[str], typer.Option("--version", help="The version of the package.")] = None,
    arch: Annotated[Optional[str], typer.Option("-a", "--arch", help="The architecture of the package.")] = None,
    bucket: Annotated[Optional[str], typer.Option("-b", "--bucket", help="The name of the S3 bucket to upload to.")] = None,
    prefix: Annotated[Optional[str], typer.Option("--prefix", help="The path prefix to use when storing on S3.")] = None,
    codename: Annotated[str, typer.Option("-c", "--codename", help="The codename of the APT repository.")] = "stable",
    component: Annotated[str, typer.Option("-m", "--component", help="The component of the APT repository.")] = "main",
    s3_region: Annotated[str, typer.Option("--s3-region", help="The region for connecting to S3.")] = "us-east-1",
    access_key_id: Annotated[Optional[str], typer.Option("--access-key-id", help="The access key for connecting to S3.")] = None,
    secret_access_key: Annotated[Optional[str], typer.Option("--secret-access-key", help="The secret key for connecting to S3.")] = None,
    session_token: Annotated[Optional[str], typer.Option("--session-token", help="The session token for connecting to S3.")] = None,
    endpoint: Annotated[Optional[str], typer.Option("--endpoint", help="The URL endpoint to the S3 API.")] = None,
    quiet: Annotated[bool, typer.Option("-q", "--quiet", help="Doesn't output information, just returns status appropriately.")] = False,
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Show information about a package."""
    if not bucket:
        error("No value provided for required option '--bucket'")

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint, quiet=quiet)

    if not arch:
        arch = "amd64"
    if not version:
        versions = []

    manifest = manifest_module.Manifest.retrieve(codename, component, arch, cache_control)

    pkg = manifest.packages.get(package)
    if not pkg:
        error(f"Package {package} not found.")

    if version:
        if version in pkg.versions:
            error(f"Version {version} not found.")
        else:
            console.print(pkg.version)
    else:
        console.print(pkg.full_description)


@app.command("exists")
def exists_command(
    package: Annotated[str, typer.Argument(help="The name of the package.")],
    version: Annotated[Optional[str], typer.Option("--version", help="The version of the package.")] = None,
    arch: Annotated[Optional[str], typer.Option("-a", "--arch", help="The architecture of the package.")] = None,
    bucket: Annotated[Optional[str], typer.Option("-b", "--bucket", help="The name of the S3 bucket to upload to.")] = None,
    prefix: Annotated[Optional[str], typer.Option("--prefix", help="The path prefix to use when storing on S3.")] = None,
    codename: Annotated[str, typer.Option("-c", "--codename", help="The codename of the APT repository.")] = "stable",
    component: Annotated[str, typer.Option("-m", "--component", help="The component of the APT repository.")] = "main",
    s3_region: Annotated[str, typer.Option("--s3-region", help="The region for connecting to S3.")] = "us-east-1",
    access_key_id: Annotated[Optional[str], typer.Option("--access-key-id", help="The access key for connecting to S3.")] = None,
    secret_access_key: Annotated[Optional[str], typer.Option("--secret-access-key", help="The secret key for connecting to S3.")] = None,
    session_token: Annotated[Optional[str], typer.Option("--session-token", help="The session token for connecting to S3.")] = None,
    endpoint: Annotated[Optional[str], typer.Option("--endpoint", help="The URL endpoint to the S3 API.")] = None,
    quiet: Annotated[bool, typer.Option("-q", "--quiet", help="Doesn't output information, just returns status appropriately.")] = False,
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Check if a package exists in the repository."""
    if not bucket:
        error("No value provided for required option '--bucket'")

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint, quiet=quiet)

    if not arch:
        arch = "amd64"

    manifest = manifest_module.Manifest.retrieve(codename, component, arch, cache_control)

    if package in manifest.packages:
        if version:
            if version in manifest.packages[package].versions:
                console.print("1")
            else:
                console.print("0")
        else:
            console.print("1")
    else:
        console.print("0")


@app.command("copy")
def copy_command(
    package: Annotated[str, typer.Argument(help="The name of the package to copy.")],
    to_codename: Annotated[str, typer.Option("--to-codename", help="The codename to copy the package to.")],
    to_component: Annotated[str, typer.Option("--to-component", help="The component to copy the package to.")],
    versions: Annotated[Optional[list[str]], typer.Option("--versions", help="The space-delimited versions to copy. If not specified, all versions will be copied.")] = None,
    arch: Annotated[Optional[str], typer.Option("-a", "--arch", help="The architecture of the package.")] = None,
    bucket: Annotated[Optional[str], typer.Option("-b", "--bucket", help="The name of the S3 bucket to upload to.")] = None,
    prefix: Annotated[Optional[str], typer.Option("--prefix", help="The path prefix to use when storing on S3.")] = None,
    codename: Annotated[str, typer.Option("-c", "--codename", help="The codename of the APT repository.")] = "stable",
    component: Annotated[str, typer.Option("-m", "--component", help="The component of the APT repository.")] = "main",
    s3_region: Annotated[str, typer.Option("--s3-region", help="The region for connecting to S3.")] = "us-east-1",
    access_key_id: Annotated[Optional[str], typer.Option("--access-key-id", help="The access key for connecting to S3.")] = None,
    secret_access_key: Annotated[Optional[str], typer.Option("--secret-access-key", help="The secret key for connecting to S3.")] = None,
    session_token: Annotated[Optional[str], typer.Option("--session-token", help="The session token for connecting to S3.")] = None,
    endpoint: Annotated[Optional[str], typer.Option("--endpoint", help="The URL endpoint to the S3 API.")] = None,
    quiet: Annotated[bool, typer.Option("-q", "--quiet", help="Doesn't output information, just returns status appropriately.")] = False,
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Copy a package to another codename and component."""
    if not bucket:
        error("No value provided for required option '--bucket'")

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint, quiet=quiet)

    if not arch:
        arch = "amd64"

    log(f"Retrieving existing manifests for {codename}")

    from_manifest = manifest_module.Manifest.retrieve(codename, component, arch, cache_control)

    if package not in from_manifest.packages:
        error(f"Package {package} not found in repository.")

    to_release = release_module.Release.retrieve(to_codename)
    if arch not in to_release.architectures:
        error(f"Architecture {arch} not available in target codename.")

    to_manifest = manifest_module.Manifest.retrieve(to_codename, to_component, arch, cache_control)

    pkg = from_manifest.packages[package]

    if versions:
        for version in versions:
            if version not in pkg.versions:
                error(f"Version {version} not found in package.")

        for version in list(pkg.versions):
            if version not in versions:
                del pkg.versions[version]
    else:
        log(f"Copying all versions of {package}")

    if package in to_manifest.packages:
        to_manifest.packages[package].versions.update(pkg.versions)
    else:
        to_manifest.packages[package] = pkg

    log(f"Uploading new manifest to S3 for {to_codename}/{to_component}/{arch}")
    to_manifest.upload()

    log("Copy complete.")


@app.command("delete")
def delete_command(
    package: Annotated[str, typer.Argument(help="The name of the package to remove.")],
    versions: Annotated[Optional[list[str]], typer.Option("--versions", help="The space-delimited versions to delete. If not specified, ALL VERSIONS will be deleted.")] = None,
    arch: Annotated[Optional[str], typer.Option("-a", "--arch", help="The architecture of the package.")] = None,
    bucket: Annotated[Optional[str], typer.Option("-b", "--bucket", help="The name of the S3 bucket to upload to.")] = None,
    prefix: Annotated[Optional[str], typer.Option("--prefix", help="The path prefix to use when storing on S3.")] = None,
    codename: Annotated[str, typer.Option("-c", "--codename", help="The codename of the APT repository.")] = "stable",
    component: Annotated[str, typer.Option("-m", "--component", help="The component of the APT repository.")] = "main",
    s3_region: Annotated[str, typer.Option("--s3-region", help="The region for connecting to S3.")] = "us-east-1",
    access_key_id: Annotated[Optional[str], typer.Option("--access-key-id", help="The access key for connecting to S3.")] = None,
    secret_access_key: Annotated[Optional[str], typer.Option("--secret-access-key", help="The secret key for connecting to S3.")] = None,
    session_token: Annotated[Optional[str], typer.Option("--session-token", help="The session token for connecting to S3.")] = None,
    endpoint: Annotated[Optional[str], typer.Option("--endpoint", help="The URL endpoint to the S3 API.")] = None,
    quiet: Annotated[bool, typer.Option("-q", "--quiet", help="Doesn't output information, just returns status appropriately.")] = False,
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Remove a package from the repository."""
    if not bucket:
        error("No value provided for required option '--bucket'")

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint, quiet=quiet)

    if not arch:
        arch = "amd64"

    log("Retrieving existing manifests")
    manifest = manifest_module.Manifest.retrieve(codename, component, arch, cache_control)

    if package not in manifest.packages:
        error(f"Package {package} not found.")

    log(f"Deleting {package} version {versions or 'all'}")
    deleted = manifest.delete_package(package, versions)

    log("Uploading new manifests to S3")
    log(f"Transferring dists/{codename}/{component}/binary-{arch}/Packages")
    manifest.upload()

    log("Update complete.")


@app.command("verify")
def verify_command(
    fix_manifests: Annotated[bool, typer.Option("-f", "--fix-manifests", help="Whether to fix problems in manifests when verifying.")] = False,
    sign: Annotated[Optional[list[str]], typer.Option("--sign", help="GPG Sign the Release file. Use --sign with your GPG key ID to use a specific key.")] = None,
    bucket: Annotated[Optional[str], typer.Option("-b", "--bucket", help="The name of the S3 bucket to upload to.")] = None,
    prefix: Annotated[Optional[str], typer.Option("--prefix", help="The path prefix to use when storing on S3.")] = None,
    origin: Annotated[Optional[str], typer.Option("-o", "--origin", help="The origin to use in the repository Release file.")] = None,
    suite: Annotated[Optional[str], typer.Option("--suite", help="The suite to use in the repository Release file.")] = None,
    codename: Annotated[str, typer.Option("-c", "--codename", help="The codename of the APT repository.")] = "stable",
    component: Annotated[str, typer.Option("-m", "--component", help="The component of the APT repository.")] = "main",
    s3_region: Annotated[str, typer.Option("--s3-region", help="The region for connecting to S3.")] = "us-east-1",
    access_key_id: Annotated[Optional[str], typer.Option("--access-key-id", help="The access key for connecting to S3.")] = None,
    secret_access_key: Annotated[Optional[str], typer.Option("--secret-access-key", help="The secret key for connecting to S3.")] = None,
    session_token: Annotated[Optional[str], typer.Option("--session-token", help="The session token for connecting to S3.")] = None,
    endpoint: Annotated[Optional[str], typer.Option("--endpoint", help="The URL endpoint to the S3 API.")] = None,
    force_path_style: Annotated[bool, typer.Option("--force-path-style", help="Use S3 path style instead of subdomains.")] = False,
    encryption: Annotated[bool, typer.Option("-e", "--encryption", help="Use S3 server side encryption.")] = False,
    quiet: Annotated[bool, typer.Option("-q", "--quiet", help="Doesn't output information, just returns status appropriately.")] = False,
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Verify that the files in the package manifests exist."""
    if not bucket:
        error("No value provided for required option '--bucket'")

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint, force_path_style=force_path_style,
                  encryption=encryption, quiet=quiet)

    log("Retrieving existing manifests")
    release = release_module.Release.retrieve(codename, origin, suite)

    components = component.split(",")
    if not components:
        components = ["main"]

    architectures = release.architectures

    for comp in components:
        for arch in architectures:
            if arch == "all":
                continue
            log(f"Checking for missing packages in: {codename}/{comp} {arch}")
            manifest = manifest_module.Manifest.retrieve(codename, comp, arch, cache_control, false)
            if manifest.packages:
                for pkg_name, pkg in sorted(manifest.packages.items()):
                    for version, version_info in list(pkg.versions.items()):
                        path = version_info.get("filename", f"pool/{pkg_name[0]}/{pkg_name}/{pkg_name}_{version}_{arch}.deb")
                        if not s3_utils.s3_exists(path):
                            log(f"Missing file: {path}")
                            if fix_manifests:
                                log(f"Deleting reference to {pkg_name} {version}")
                                del pkg.versions[version]
                                if not pkg.versions:
                                    del manifest.packages[pkg_name]

                if fix_manifests and (len(manifest.packages) > 0):
                    log(f"Uploading fixed manifest for {codename}/{comp}/{arch}")
                    manifest.upload()

    if sign:
        log(f"Signing Release file for {codename}")
        release.sign(sign, "gpg", "")
        release.upload("public")

    log("Verify complete.")


@app.command("clean")
def clean_command(
    bucket: Annotated[Optional[str], typer.Option("-b", "--bucket", help="The name of the S3 bucket to upload to.")] = None,
    prefix: Annotated[Optional[str], typer.Option("--prefix", help="The path prefix to use when storing on S3.")] = None,
    origin: Annotated[Optional[str], typer.Option("-o", "--origin", help="The origin to use in the repository Release file.")] = None,
    suite: Annotated[Optional[str], typer.Option("--suite", help="The suite to use in the repository Release file.")] = None,
    codename: Annotated[str, typer.Option("-c", "--codename", help="The codename of the APT repository.")] = "stable",
    component: Annotated[str, typer.Option("-m", "--component", help="The component of the APT repository.")] = "main",
    s3_region: Annotated[str, typer.Option("--s3-region", help="The region for connecting to S3.")] = "us-east-1",
    access_key_id: Annotated[Optional[str], typer.Option("--access-key-id", help="The access key for connecting to S3.")] = None,
    secret_access_key: Annotated[Optional[str], typer.Option("--secret-access-key", help="The secret key for connecting to S3.")] = None,
    session_token: Annotated[Optional[str], typer.Option("--session-token", help="The session token for connecting to S3.")] = None,
    endpoint: Annotated[Optional[str], typer.Option("--endpoint", help="The URL endpoint to the S3 API.")] = None,
    force_path_style: Annotated[bool, typer.Option("--force-path-style", help="Use S3 path style instead of subdomains.")] = False,
    encryption: Annotated[bool, typer.Option("-e", "--encryption", help="Use S3 server side encryption.")] = False,
    quiet: Annotated[bool, typer.Option("-q", "--quiet", help="Doesn't output information, just returns status appropriately.")] = False,
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Remove orphaned package files."""
    if not bucket:
        error("No value provided for required option '--bucket'")

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint, force_path_style=force_path_style,
                  encryption=encryption, quiet=quiet)

    log("Retrieving existing manifests")
    release = release_module.Release.retrieve(codename, origin, suite)

    components = component.split(",")
    if not components:
        components = ["main"]

    architectures = release.architectures
    all_pkgs = {}

    for comp in components:
        for arch in architectures:
            if arch == "all":
                continue
            manifest = manifest_module.Manifest.retrieve(codename, comp, arch, cache_control, False)
            if manifest.packages:
                for pkg in manifest.packages.values():
                    for version, info in list(pkg.versions.items()):
                        path = info.get("filename")
                        if not path:
                            path = f"pool/{pkg.name[0]}/{pkg.name}/{pkg.name}_{version}_{arch}.deb"
                        if path not in all_pkgs:
                            all_pkgs[path] = []

                        all_pkgs[path].append(f"{codename}/{comp}/{arch}")

    log("Searching for unreferenced packages")
    prefix_path = f"{prefix}/pool/" if prefix else "pool/"

    objects = s3_utils.s3_list_objects(prefix_path)
    removed_count = 0

    for obj in objects:
        path = obj.get("Key", "")
        if path and path not in all_pkgs:
            log(f"Removing {path}")
            s3_utils.s3_remove(path)
            removed_count += 1

    if removed_count > 0:
        log(f"Removed {removed_count} orphaned package(s).")
    else:
        log("No orphaned packages found.")
