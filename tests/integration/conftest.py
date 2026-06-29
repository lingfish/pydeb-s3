"""Fixtures for moto-server-backed E2E integration tests with Docker apt client."""

import io
import os
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import boto3
import pytest
import requests
from moto.server import ThreadedMotoServer

BUCKET = "apt-repo"
DEB_FIXTURE = "tests/fixtures/hello_2.10-5_amd64.deb"


def _docker_available() -> Optional[str]:
    """Check which container runtime is available. Returns executable name or None."""
    for cmd in ("docker", "podman"):
        if shutil.which(cmd):
            return cmd
    return None


def _find_free_port() -> int:
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _docker_exec(cid: str, cmd: List[str]) -> Tuple[int, str]:
    """Run a command inside a container, return (exit_code, stdout)."""
    docker = _docker_available()
    assert docker, "No container runtime available"
    result = subprocess.run(
        [docker, "exec", cid] + cmd,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result.returncode, result.stdout


def _docker_run(image: str, cmd=None) -> str:
    """Start a container and return its ID."""
    docker = _docker_available()
    assert docker, "No container runtime available"
    args = [docker, "run", "-d", "--network", "host", image]
    if cmd:
        args.extend(cmd)
    return subprocess.check_output(args).decode().strip()


# ---------------------------------------------------------------------------
# Session-scoped moto server
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def moto_server():
    """Start a ThreadedMotoServer on a free port and yield its URL."""
    port = _find_free_port()
    server = ThreadedMotoServer(port=port)
    server.start()
    url = f"http://127.0.0.1:{port}"
    yield url
    server.stop()


# ---------------------------------------------------------------------------
# Per-test state reset
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_moto(moto_server):
    """Reset all moto state before each test so tests are isolated."""
    try:
        requests.post(f"{moto_server}/moto-api/reset", timeout=5)
    except requests.exceptions.RequestException:
        pass


# ---------------------------------------------------------------------------
# Bucket fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def bucket(moto_server):
    """Create an S3 bucket in moto and yield its name."""
    client = boto3.client(
        "s3",
        endpoint_url=moto_server,
        region_name="us-east-1",
    )
    client.create_bucket(Bucket=BUCKET)
    return BUCKET


# ---------------------------------------------------------------------------
# Repo population via pydeb-s3 CLI
# ---------------------------------------------------------------------------


@pytest.fixture
def unsigned_populated_repo(moto_server, bucket):
    """Upload a generated test-pkg .deb (no deps) to moto S3."""
    with tempfile.TemporaryDirectory() as tmpdir:
        deb_path = os.path.join(tmpdir, "test-pkg_1.0.0_amd64.deb")
        _create_fake_deb(deb_path, "test-pkg", "1.0.0", "amd64")
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pydeb_s3",
                "upload",
                "-b",
                bucket,
                "--endpoint",
                moto_server,
                "--visibility",
                "public",
                deb_path,
            ],
            check=True,
            capture_output=True,
        )


@pytest.fixture
def populated_repo_with_second_version(moto_server, bucket):
    """Upload two versions of test-pkg for upgrade tests.

    Version 1: test-pkg_1.0.0-1 (hello_2.10-5_amd64.deb)
    Version 2: test-pkg_2.0.0-1 (generated fake .deb)
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test-pkg_2.0.0_amd64.deb
        deb_path = os.path.join(tmpdir, "test-pkg_2.0.0_amd64.deb")
        _create_fake_deb(deb_path, "test-pkg", "2.0.0-1", "amd64")

        # Upload both versions (preserve versions so both are kept)
        for deb in [DEB_FIXTURE, deb_path]:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pydeb_s3",
                    "upload",
                    "-b",
                    bucket,
                    "--endpoint",
                    moto_server,
                    "--visibility",
                    "public",
                    "--preserve-versions",
                    deb,
                ],
                check=True,
                capture_output=True,
            )


def _create_fake_deb(path: str, name: str, version: str, arch: str) -> None:
    """Create a minimal .deb file at *path*."""
    control = (
        f"Package: {name}\n"
        f"Version: {version}\n"
        "Section: test\n"
        "Priority: optional\n"
        f"Architecture: {arch}\n"
        "Maintainer: Test <test@example.com>\n"
        "Description: Test package\n"
        f" Test package for pydeb-s3.\n"
    )
    # debian-binary
    debian_binary = b"2.0\n"
    # control.tar.gz
    ct = io.BytesIO()
    with tarfile.open(fileobj=ct, mode="w:gz") as tar:
        info = tarfile.TarInfo(name="control")
        data = control.encode()
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    control_tar = ct.getvalue()
    # data.tar.gz (empty)
    dt = io.BytesIO()
    with tarfile.open(fileobj=dt, mode="w:gz"):
        pass
    data_tar = dt.getvalue()

    with open(path, "wb") as f:
        f.write(b"!<arch>\n")
        _ar_append(f, b"debian-binary", debian_binary)
        _ar_append(f, b"control.tar.gz", control_tar)
        _ar_append(f, b"data.tar.gz", data_tar)


def _ar_append(f, name: bytes, data: bytes) -> None:
    """Append a BSD-ar member (left-justified numeric fields)."""
    header = name.ljust(16)
    header += b"0           "  # timestamp (12 bytes)
    header += b"0     "  # uid (6)
    header += b"0     "  # gid (6)
    header += b"100644  "  # mode (8)
    header += str(len(data)).encode().ljust(10)  # size (10)
    header += b"`\n"
    f.write(header)
    f.write(data)
    if len(data) % 2:
        f.write(b"\n")


# ---------------------------------------------------------------------------
# GPG fixtures for signed-repo tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def gpg_key_id():
    """Generate an ephemeral GPG key, yield its ID, then delete it."""
    import os as _os
    import random as _random
    import string as _string

    gnupghome = f"/tmp/pydeb-s3-gpghome-{''.join(_random.choices(_string.ascii_lowercase, k=8))}"
    _os.makedirs(gnupghome, mode=0o700, exist_ok=True)

    # Generate RSA key with no passphrase (--pinentry-mode loopback avoids gpg-agent)
    subprocess.run(
        [
            "gpg",
            "--batch",
            "--yes",
            "--pinentry-mode",
            "loopback",
            "--passphrase",
            "",
            "--gen-key",
            "--homedir",
            gnupghome,
        ],
        input=b"Key-Type: RSA\nKey-Length: 2048\nSubkey-Type: RSA\nSubkey-Length: 2048\n"
        b"Name-Real: pydeb-s3-test\nName-Email: test@pydeb-s3.local\n"
        b"Expire-Date: 0\n%no-protection\n%commit\n",
        check=True,
        capture_output=True,
    )

    # Extract key ID from --list-keys output
    result = subprocess.run(
        ["gpg", "--homedir", gnupghome, "--list-keys", "--keyid-format", "LONG", "pydeb-s3-test"],
        check=True,
        capture_output=True,
        text=True,
    )
    key_id = ""
    for line in result.stdout.splitlines():
        if line.startswith("pub"):
            key_id = line.split("/")[1].split()[0]
            break
    if not key_id:
        raise RuntimeError("Could not parse GPG key ID")

    # Export public key for use in apt container
    pubkey_path = gnupghome + ".pub"
    with open(pubkey_path, "w") as f:
        subprocess.run(
            ["gpg", "--homedir", gnupghome, "--export", "--armor", key_id],
            check=True,
            stdout=f,
        )

    yield {"id": key_id, "homedir": gnupghome, "pubkey": pubkey_path}

    # Cleanup
    subprocess.run(
        [
            "gpg",
            "--homedir",
            gnupghome,
            "--batch",
            "--yes",
            "--delete-secret-and-public-key",
            key_id,
        ],
        check=False,
        capture_output=True,
    )
    subprocess.run(["rm", "-rf", gnupghome, pubkey_path], check=False, capture_output=True)


@pytest.fixture
def signed_populated_repo(moto_server, bucket, gpg_key_id):
    """Upload .deb and sign Release file with ephemeral GPG key."""
    with tempfile.TemporaryDirectory() as tmpdir:
        deb_path = os.path.join(tmpdir, "test-pkg_1.0.0_amd64.deb")
        _create_fake_deb(deb_path, "test-pkg", "1.0.0", "amd64")
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pydeb_s3",
                "upload",
                "-b",
                bucket,
                "--endpoint",
                moto_server,
                "--visibility",
                "public",
                "--sign",
                gpg_key_id["id"],
                "--gpg-options",
                f"--homedir {gpg_key_id['homedir']}",
                deb_path,
            ],
            check=True,
            capture_output=True,
        )
    return gpg_key_id


# ---------------------------------------------------------------------------
# Docker exec helper fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def docker_exec():
    """Return a callable to run commands inside a container.

    Usage: exit_code, stdout = docker_exec(container_id, ["apt-get", "update"])
    """

    def _exec(cid: str, cmd: List[str]) -> Tuple[int, str]:
        d = _docker_available()
        if not d:
            pytest.skip("Docker or Podman not available")
        result = subprocess.run(
            [d, "exec", cid] + cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        return result.returncode, result.stdout

    return _exec


# ---------------------------------------------------------------------------
# Docker Debian container fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def debian_container(moto_server):
    """Start a Debian container with apt source pointing at moto."""
    docker = _docker_available()
    if not docker:
        pytest.skip("Docker or Podman not available")

    port = urlparse(moto_server).port
    cid = _docker_run("debian:12-slim", ["sleep", "3600"])

    try:
        # Write apt source pointing at moto
        _docker_exec(
            cid,
            [
                "sh",
                "-c",
                f"echo 'deb [trusted=yes] http://127.0.0.1:{port}/{BUCKET} stable main' > /etc/apt/sources.list.d/pydeb-s3.list",
            ],
        )
        yield cid
    finally:
        subprocess.run([docker, "rm", "-f", cid], check=False, capture_output=True)


@pytest.fixture
def debian_container_signed(moto_server, gpg_key_id):
    """Debian container configured with GPG public key (no [trusted=yes])."""
    docker = _docker_available()
    if not docker:
        pytest.skip("Docker or Podman not available")

    port = urlparse(moto_server).port
    cid = _docker_run("debian:12-slim", ["sleep", "3600"])

    try:
        # Install gnupg inside container
        _docker_exec(cid, ["apt-get", "update", "-qq"])
        _docker_exec(cid, ["apt-get", "install", "-y", "-qq", "gnupg"])

        # Copy public key into container
        subprocess.run(
            [docker, "cp", gpg_key_id["pubkey"], f"{cid}:/tmp/pydeb-s3.asc"],
            check=True,
            capture_output=True,
        )
        # Install key
        _docker_exec(
            cid,
            [
                "sh",
                "-c",
                "gpg --dearmor < /tmp/pydeb-s3.asc > /etc/apt/trusted.gpg.d/pydeb-s3.gpg",
            ],
        )
        # Write sources.list with signed-by (no trusted=yes)
        _docker_exec(
            cid,
            [
                "sh",
                "-c",
                f"echo 'deb [signed-by=/etc/apt/trusted.gpg.d/pydeb-s3.gpg] http://127.0.0.1:{port}/{BUCKET} stable main' > /etc/apt/sources.list.d/pydeb-s3.list",
            ],
        )
        yield cid
    finally:
        subprocess.run([docker, "rm", "-f", cid], check=False, capture_output=True)
