"""CLI for pydeb-s3."""

import glob
import os
import sys
from typing import Annotated, Optional

import typer
from loguru import logger

from pydeb_s3 import lock as lock_module
from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3 import s3_utils


def cli_callback(
    ctx: typer.Context,
    quiet: Annotated[bool, typer.Option("--quiet", help="Only show errors")] = False,
    debug: Annotated[bool, typer.Option("--debug", help="Enable debug output")] = False,
):
    level = "DEBUG" if debug else ("ERROR" if quiet else "INFO")
    logger.remove()
    logger.add(sys.stderr, level=level)
    ctx.obj = {"quiet": quiet, "debug": debug}


app = typer.Typer(
    name="pydeb-s3",
    help="Easily create and manage an APT repository on S3",
    rich_markup_mode=None,
    callback=cli_callback,
)


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
) -> None:
    """Configure S3 connection."""
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
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
    checksum_when_required: Annotated[bool, typer.Option("--checksum-when-required", help="Disable SDK upload checksums for S3-compatible endpoints.")] = False,
):
    """Upload the given files to a S3 bucket as an APT repository."""
    if not bucket:
        logger.error("No value provided for required option '--bucket'")
        raise typer.Exit(code=1)
    if not files:
        logger.error("You must specify at least one file to upload")
        raise typer.Exit(code=1)

    for pattern in files:
        if not glob.glob(pattern):
            logger.error(f"File '{pattern}' doesn't exist")
            raise typer.Exit(code=1)

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
    )

    comp = component
    if section:
        logger.warning("--section is deprecated, use --component instead")
        if not comp:
            comp = section

    if lock:
        lock_module.lock()

    try:
        release = release_module.Release.retrieve(codename, origin, suite)
        logger.info("Retrieving existing manifests")

        manifests = {}

        if arch and arch != "all":
            manifests.setdefault(arch, manifest_module.Manifest.retrieve(
                codename, comp, arch, cache_control, fail_if_exists, skip_package_upload
            ))
        elif arch == "all" and not release.architectures:
            manifests.setdefault("amd64", manifest_module.Manifest.retrieve(
                codename, comp, "amd64", cache_control, fail_if_exists, skip_package_upload
            ))
            manifests.setdefault("i386", manifest_module.Manifest.retrieve(
                codename, comp, "i386", cache_control, fail_if_exists, skip_package_upload
            ))
            manifests.setdefault("armhf", manifest_module.Manifest.retrieve(
                codename, comp, "armhf", cache_control, fail_if_exists, skip_package_upload
            ))
            manifests.setdefault("arm64", manifest_module.Manifest.retrieve(
                codename, comp, "arm64", cache_control, fail_if_exists, skip_package_upload
            ))
        else:
            for arch_item in release.architectures:
                manifests.setdefault(arch_item, manifest_module.Manifest.retrieve(
                    codename, comp, arch_item, cache_control, fail_if_exists, skip_package_upload
                ))

        packages_arch_all = []

        for pattern in files:
            for filepath in glob.glob(pattern):
                logger.info(f"Examining package file {os.path.basename(filepath)}")
                pkg = package_module.Package.parse_file(filepath)

                pkg_arch = arch if arch else pkg.architecture

                if not pkg_arch:
                    logger.error(
                        f"No architecture given and unable to determine one for {filepath}. "
                        "Please specify one with --arch [i386|amd64|armhf|arm64]."
                    )
                    raise typer.Exit(code=1)

                if arch and arch != pkg_arch:
                    logger.warning(
                        f"You specified architecture {arch} but package {pkg.name} has architecture type of {pkg_arch}"
                    )

                manifests.setdefault(pkg_arch, manifest_module.Manifest.retrieve(
                    codename, comp, pkg_arch, cache_control, fail_if_exists, skip_package_upload
                ))

                manifests[pkg_arch].add(pkg, preserve_versions)

                if pkg_arch == "all":
                    packages_arch_all.append(pkg)

        for arch_key, manifest in manifests.items():
            if arch_key == "all":
                continue
            for pkg in packages_arch_all:
                manifest.add(pkg, preserve_versions, False)

        logger.info("Uploading packages and new manifests to S3")

        logger.debug(f"Uploading manifests for architectures: {list(manifests.keys())}")
        for arch_key, manifest in manifests.items():
            logger.debug(f"  Before write_to_s3: arch_key={arch_key}, packages count={len(manifest.packages)}")
            logger.info(f"  Transferring dists/{codename}/{comp}/binary-{arch_key}/Packages")
            manifest.write_to_s3()
            logger.debug(f"  After write_to_s3: release.files={list(release.files.keys())}")
            release.update_manifest(manifest)

        release.write_to_s3()

        if sign:
            logger.info(f"Signing Release file for {codename}")
            release.sign(sign, gpg_provider, gpg_options, visibility)

        logger.info("Update complete.")
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
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """List packages in given codename, component, and optionally architecture."""
    if not bucket:
        logger.error("No value provided for required option '--bucket'")
        raise typer.Exit(code=1)

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint)

    release = release_module.Release.retrieve(codename)
    archs = release.architectures
    if arch and arch != "all":
        archs = [arch]

    widths = [0, 0]
    rows = []

    for architecture in archs:
        manifest = manifest_module.Manifest.retrieve(codename, component, architecture, cache_control)

        if manifest.packages:
            for pkg in sorted(manifest.packages):
                rows.append(
                    [
                        f"{pkg.name}",
                        f"{pkg.version}",
                        f"{architecture}",
                        f"{pkg.category}",
                        f"{pkg.description[:62]}...",
                    ]
                )

                widths[0] = max(widths[0], len(pkg.name))
                widths[1] = max(widths[1], len(pkg.version))

    if rows:
        logger.info("[bold]Package[/bold]" + " " * (widths[0] - 7) + "[bold]Version[/bold]" + " " * (widths[1] - 7) + "[bold]Architecture[/bold]  [bold]Section[/bold]  [bold]Description[/bold]")

        for row in rows:
            name, version, arch, section, desc = row
            logger.info(
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
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Show information about a package."""
    if not bucket:
        logger.error("No value provided for required option '--bucket'")
        raise typer.Exit(code=1)

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint)

    if not arch:
        arch = "amd64"

    manifest = manifest_module.Manifest.retrieve(codename, component, arch, cache_control)

    pkg = manifest.packages.get(package)
    if not pkg:
        logger.error(f"Package {package} not found.")
        raise typer.Exit(code=1)

    if version:
        if version in pkg.versions:
            logger.error(f"Version {version} not found.")
            raise typer.Exit(code=1)
        logger.info(pkg.version)
    else:
        logger.info(pkg.full_description)


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
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Check if a package exists in the repository."""
    if not bucket:
        logger.error("No value provided for required option '--bucket'")
        raise typer.Exit(code=1)

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint)

    if not arch:
        arch = "amd64"

    manifest = manifest_module.Manifest.retrieve(codename, component, arch, cache_control)

    if package in manifest.packages:
        if version:
            if version in manifest.packages[package].versions:
                logger.info("1")
            else:
                logger.info("0")
        else:
            logger.info("1")
    else:
        logger.info("0")


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
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Copy a package to another codename and component."""
    if not bucket:
        logger.error("No value provided for required option '--bucket'")
        raise typer.Exit(code=1)

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint)

    if not arch:
        arch = "amd64"

    logger.info(f"Retrieving existing manifests for {codename}")

    from_manifest = manifest_module.Manifest.retrieve(codename, component, arch, cache_control)

    if package not in from_manifest.packages:
        logger.error(f"Package {package} not found in repository.")
        raise typer.Exit(code=1)

    to_release = release_module.Release.retrieve(to_codename)
    if arch not in to_release.architectures:
        logger.error(f"Architecture {arch} not available in target codename.")
        raise typer.Exit(code=1)

    to_manifest = manifest_module.Manifest.retrieve(to_codename, to_component, arch, cache_control)

    pkg = from_manifest.packages[package]

    if versions:
        for version in versions:
            if version not in pkg.versions:
                logger.error(f"Version {version} not found in package.")
                raise typer.Exit(code=1)

        for version in list(pkg.versions):
            if version not in versions:
                del pkg.versions[version]
    else:
        logger.info(f"Copying all versions of {package}")

    if package in to_manifest.packages:
        to_manifest.packages[package].versions.update(pkg.versions)
    else:
        to_manifest.packages[package] = pkg

    logger.info(f"Uploading new manifest to S3 for {to_codename}/{to_component}/{arch}")
    to_manifest.upload()

    logger.info("Copy complete.")


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
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Remove a package from the repository."""
    if not bucket:
        logger.error("No value provided for required option '--bucket'")
        raise typer.Exit(code=1)

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint)

    if not arch:
        arch = "amd64"

    logger.info("Retrieving existing manifests")
    manifest = manifest_module.Manifest.retrieve(codename, component, arch, cache_control)

    if package not in manifest.packages:
        logger.error(f"Package {package} not found.")
        raise typer.Exit(code=1)

    logger.info(f"Deleting {package} version {versions or 'all'}")
    deleted = manifest.delete_package(package, versions)

    logger.info("Uploading new manifests to S3")
    logger.info(f"Transferring dists/{codename}/{component}/binary-{arch}/Packages")
    manifest.upload()

    logger.info("Update complete.")


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
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Verify that the files in the package manifests exist."""
    if not bucket:
        logger.error("No value provided for required option '--bucket'")
        raise typer.Exit(code=1)

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint, force_path_style=force_path_style,
                  encryption=encryption)

    logger.info("Retrieving existing manifests")
    release = release_module.Release.retrieve(codename, origin, suite)

    components = component.split(",")
    if not components:
        components = ["main"]

    architectures = release.architectures

    for comp in components:
        for arch in architectures:
            if arch == "all":
                continue
            logger.info(f"Checking for missing packages in: {codename}/{comp} {arch}")
            manifest = manifest_module.Manifest.retrieve(codename, comp, arch, cache_control, False)
            if manifest.packages:
                for pkg_name, pkg in sorted(manifest.packages.items()):
                    for version, version_info in list(pkg.versions.items()):
                        path = version_info.get("filename", f"pool/{pkg_name[0]}/{pkg_name}/{pkg_name}_{version}_{arch}.deb")
                        if not s3_utils.s3_exists(path):
                            logger.warning(f"Missing file: {path}")
                            if fix_manifests:
                                logger.info(f"Deleting reference to {pkg_name} {version}")
                                del pkg.versions[version]
                                if not pkg.versions:
                                    del manifest.packages[pkg_name]

                if fix_manifests and (len(manifest.packages) > 0):
                    logger.info(f"Uploading fixed manifest for {codename}/{comp}/{arch}")
                    manifest.upload()

    if sign:
        logger.info(f"Signing Release file for {codename}")
        release.sign(sign, "gpg", "")

    logger.info("Verify complete.")


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
    cache_control: Annotated[Optional[str], typer.Option("-C", "--cache-control", help="Add cache-control headers to S3 objects.")] = None,
):
    """Remove orphaned package files."""
    if not bucket:
        logger.error("No value provided for required option '--bucket'")
        raise typer.Exit(code=1)

    _configure_s3(bucket=bucket, prefix=prefix, region=s3_region,
                  access_key_id=access_key_id, secret_access_key=secret_access_key,
                  session_token=session_token, endpoint=endpoint, force_path_style=force_path_style,
                  encryption=encryption)

    logger.info("Retrieving existing manifests")
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
                for pkg in manifest.packages:
                    # Get filename from package - prefer url_filename (Filename field in Packages)
                    path = pkg.url_filename
                    if not path:
                        # Fallback: construct path from package attributes
                        path = f"pool/{pkg.name[0]}/{pkg.name}/{pkg.name}_{pkg.version}_{arch}.deb"
                    if path:
                        if path not in all_pkgs:
                            all_pkgs[path] = []

                        all_pkgs[path].append(f"{codename}/{comp}/{arch}")

    logger.info("Searching for unreferenced packages")
    prefix_path = f"{prefix}/pool/" if prefix else "pool/"

    result = s3_utils.s3_list_objects(prefix_path)
    # s3_list_objects returns tuple of (objects list, continuation token)
    objects = result[0] if isinstance(result, tuple) else result
    removed_count = 0

    for obj in objects:
        path = obj.get("Key", "")
        if path and path not in all_pkgs:
            logger.warning(f"Removing {path}")
            s3_utils.s3_remove(path)
            removed_count += 1

    if removed_count > 0:
        logger.info(f"Removed {removed_count} orphaned package(s).")
    else:
        logger.info("No orphaned packages found.")
