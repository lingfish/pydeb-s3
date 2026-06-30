"""End-to-end integration tests using moto server + Docker apt client.

These tests spin up a ThreadedMotoServer to mimic S3, upload packages with
pydeb-s3, then use actual ``apt`` inside a Debian container to verify the
repository works end-to-end (update, install, upgrade, dist-upgrade).
"""

import io
import shutil
import subprocess
import sys
import tarfile
import tempfile
from typing import Dict, Optional, Tuple

import pytest
import requests

pytestmark = pytest.mark.integration

BUCKET = "apt-repo"


# ---------------------------------------------------------------------------
# Helpers (duplicated from conftest to avoid cross-package imports)
# ---------------------------------------------------------------------------


def _docker_available() -> str:
    """Check which container runtime is available. Returns executable name or ''."""
    for cmd in ("docker", "podman"):
        if shutil.which(cmd):
            return cmd
    return ""


def _create_fake_deb(
    path: str,
    name: str,
    version: str,
    arch: str,
    depends: Optional[str] = None,
) -> None:
    """Create a minimal .deb file at *path*.

    If *depends* is provided, it is included as the Depends field (useful
    for testing dependency parsing with OR-alternation syntax like
    ``"systemd | systemd-standalone-sysusers"``).
    """
    control_lines = [
        f"Package: {name}",
        f"Version: {version}",
        "Section: test",
        "Priority: optional",
        f"Architecture: {arch}",
        "Maintainer: Test <test@example.com>",
    ]
    if depends:
        control_lines.append(f"Depends: {depends}")
    control_lines.append("Description: Test package")
    control_lines.append(" Test package for pydeb-s3.")
    control = "\n".join(control_lines) + "\n"
    debian_binary = b"2.0\n"
    ct = io.BytesIO()
    with tarfile.open(fileobj=ct, mode="w:gz") as tar:
        info = tarfile.TarInfo(name="control")
        data = control.encode()
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    control_tar = ct.getvalue()
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


def _http_get(url: str) -> Tuple[int, Dict[str, str], str]:
    """Fetch *url* with requests and return (status_code, headers_dict, text).

    Header keys are lowercased for case-insensitive access.
    """
    try:
        resp = requests.get(url, timeout=15, allow_redirects=True)
        headers = {k.lower(): v for k, v in resp.headers.items()}
        return resp.status_code, headers, resp.text
    except requests.RequestException as e:
        return 0, {}, str(e)


def _http_get_binary(url: str) -> Tuple[int, Dict[str, str], bytes]:
    """Fetch *url* with requests and return (status_code, headers_dict, content)."""
    try:
        resp = requests.get(url, timeout=15, allow_redirects=True)
        headers = {k.lower(): v for k, v in resp.headers.items()}
        return resp.status_code, headers, resp.content
    except requests.RequestException as e:
        return 0, {}, str(e).encode()


def _pydeb_upload(bucket: str, endpoint: str, deb_path: str, *extra_args: str) -> None:
    """Run pydeb-s3 upload with common args and optional extras."""
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pydeb_s3",
            "upload",
            "-b",
            bucket,
            "--endpoint",
            endpoint,
            "--visibility",
            "public",
            *extra_args,
            deb_path,
        ],
        check=True,
        capture_output=True,
    )


# ---------------------------------------------------------------------------
# HTTP-level compatibility
# ---------------------------------------------------------------------------


class TestHttpCompatibility:
    """Verify that moto-served S3 objects have apt-compatible HTTP responses."""

    def test_release_file_accessible(self, unsigned_populated_repo, moto_server):
        """Release file returns 200 with expected Content-Type."""
        code, headers, body = _http_get(f"{moto_server}/{BUCKET}/dists/stable/Release")
        assert code == 200, f"Expected 200, got {code}: {body[:200]}"
        assert "Codename:" in body, "Release file should contain Codename field"
        ctype = headers.get("content-type", "")
        assert "text/plain" in ctype, f"Expected text/plain, got {ctype}"

    def test_packages_file_accessible(self, unsigned_populated_repo, moto_server):
        """Packages file returns 200 with correct content."""
        code, headers, body = _http_get(
            f"{moto_server}/{BUCKET}/dists/stable/main/binary-amd64/Packages"
        )
        assert code == 200, f"Expected 200, got {code}: {body[:200]}"
        assert "Package:" in body, "Packages file should contain Package entries"
        assert "test-pkg" in body, f"Packages should contain test-pkg, got: {body[:200]}"

    def test_packages_gz_file_accessible(self, unsigned_populated_repo, moto_server):
        """Packages.gz returns 200 with gzip Content-Type."""
        code, headers, body = _http_get(
            f"{moto_server}/{BUCKET}/dists/stable/main/binary-amd64/Packages.gz"
        )
        assert code == 200, f"Expected 200, got {code}"
        ctype = headers.get("content-type", "")
        assert "gzip" in ctype, f"Expected gzip, got {ctype}"

    def test_deb_file_accessible(self, unsigned_populated_repo, moto_server):
        """.deb file returns 200 with binary Content-Type."""
        import boto3

        client = boto3.client("s3", endpoint_url=moto_server, region_name="us-east-1")
        objs = client.list_objects_v2(Bucket=BUCKET, Prefix="pool/")
        deb_key = None
        for obj in objs.get("Contents", []):
            if obj["Key"].endswith(".deb"):
                deb_key = obj["Key"]
                break
        assert deb_key, "No .deb found in pool"

        code, headers, content = _http_get_binary(f"{moto_server}/{BUCKET}/{deb_key}")
        assert code == 200, f"Expected 200 for .deb, got {code}"
        ctype = headers.get("content-type", "")
        assert "octet-stream" in ctype, f"Expected octet-stream, got {ctype}"
        length = int(headers.get("content-length", 0))
        assert length > 0, "Content-Length should be non-zero"
        assert len(content) == length, "Body length should match Content-Length"


# ---------------------------------------------------------------------------
# apt operations
# ---------------------------------------------------------------------------


class TestAptUpdate:
    """Test that apt can update from a pydeb-s3-served repo."""

    def test_apt_update_succeeds(self, unsigned_populated_repo, debian_container, docker_exec):
        """apt-get update reports our repo as a source."""
        code, out = docker_exec(debian_container, ["apt-get", "update", "-qq"])
        assert code == 0, f"apt update failed, stdout={out}"
        # apt shows hits for our repo in verbose output
        code, out = docker_exec(
            debian_container,
            [
                "apt-get",
                "update",
                "-o",
                "APT::Get::Show-User-Simulation-Note=false",
            ],
        )
        # Should at minimum not error
        assert code == 0


class TestAptInstall:
    """Test that packages from the repo can be installed."""

    def test_apt_installs_package(self, unsigned_populated_repo, debian_container, docker_exec):
        """A generated .deb (test-pkg) can be installed via apt."""
        code, out = docker_exec(debian_container, ["apt-get", "update", "-qq"])
        assert code == 0
        code, out = docker_exec(debian_container, ["apt-get", "install", "-y", "test-pkg"])
        assert code == 0, f"apt install failed, stdout={out}"

        code, out = docker_exec(debian_container, ["dpkg", "-l", "test-pkg"])
        assert code == 0
        assert "test-pkg" in out
        assert "1.0.0" in out


class TestAptUpgrade:
    """Test apt upgrade when a newer package version is uploaded."""

    def _upload_and_install(
        self,
        deb_path,
        bucket,
        endpoint,
        cid,
        docker_exec,
    ):
        _pydeb_upload(bucket, endpoint, deb_path, "--preserve-versions")
        code, out = docker_exec(cid, ["apt-get", "update", "-qq"])
        assert code == 0, f"apt update failed after upload: {out}"

    def test_apt_upgrade_newer_version(
        self,
        moto_server,
        bucket,
        debian_container,
        docker_exec,
    ):
        """apt upgrade picks up a newer package version."""
        docker = _docker_available()
        assert docker, "No container runtime"

        with tempfile.TemporaryDirectory() as tmpdir:
            v1_deb = f"{tmpdir}/test-pkg_1.0.0_amd64.deb"
            v2_deb = f"{tmpdir}/test-pkg_2.0.0_amd64.deb"
            _create_fake_deb(v1_deb, "test-pkg", "1.0.0", "amd64")
            _create_fake_deb(v2_deb, "test-pkg", "2.0.0", "amd64")

            # Upload v1, install it
            _pydeb_upload(BUCKET, moto_server, v1_deb)
            code, out = docker_exec(debian_container, ["apt-get", "update", "-qq"])
            assert code == 0
            code, out = docker_exec(
                debian_container,
                [
                    "apt-get",
                    "install",
                    "-y",
                    "test-pkg",
                ],
            )
            assert code == 0, f"install failed: {out}"

            # Upload v2 with preserve-versions
            _pydeb_upload(BUCKET, moto_server, v2_deb, "--preserve-versions")

            code, out = docker_exec(debian_container, ["apt-get", "update", "-qq"])
            assert code == 0, f"update failed: {out}"

            code, out = docker_exec(
                debian_container,
                [
                    "apt-get",
                    "upgrade",
                    "-y",
                ],
            )
            assert code == 0, f"upgrade failed: {out}"

            code, out = docker_exec(debian_container, ["dpkg", "-l", "test-pkg"])
            assert code == 0
            assert "2.0.0" in out, f"Expected 2.0.0, got {out}"


class TestAptDistUpgrade:
    """Test apt dist-upgrade with version bumps."""

    def test_apt_dist_upgrade(
        self,
        moto_server,
        bucket,
        debian_container,
        docker_exec,
    ):
        """apt dist-upgrade handles version transition."""
        with tempfile.TemporaryDirectory() as tmpdir:
            v1_deb = f"{tmpdir}/test-pkg_1.0.0_amd64.deb"
            v2_deb = f"{tmpdir}/test-pkg_2.0.0_amd64.deb"
            _create_fake_deb(v1_deb, "test-pkg", "1.0.0", "amd64")
            _create_fake_deb(v2_deb, "test-pkg", "2.0.0", "amd64")

            # Upload v1 and install
            _pydeb_upload(BUCKET, moto_server, v1_deb)
            code, out = docker_exec(debian_container, ["apt-get", "update", "-qq"])
            assert code == 0
            code, out = docker_exec(debian_container, ["apt-get", "install", "-y", "test-pkg"])
            assert code == 0, f"install failed: {out}"

            # Upload v2
            _pydeb_upload(BUCKET, moto_server, v2_deb, "--preserve-versions")
            code, out = docker_exec(debian_container, ["apt-get", "update", "-qq"])
            assert code == 0
            code, out = docker_exec(debian_container, ["apt-get", "dist-upgrade", "-y"])
            assert code == 0, f"dist-upgrade failed: {out}"

            code, out = docker_exec(debian_container, ["dpkg", "-l", "test-pkg"])
            assert code == 0
            assert "2.0.0" in out, f"Expected 2.0.0, got {out}"


class TestAptInstallRemove:
    """Test package removal after installation."""

    def test_apt_remove(self, unsigned_populated_repo, debian_container, docker_exec):
        """Apt can remove a package installed from the repo."""
        code, out = docker_exec(debian_container, ["apt-get", "update", "-qq"])
        assert code == 0
        code, out = docker_exec(debian_container, ["apt-get", "install", "-y", "test-pkg"])
        assert code == 0

        code, out = docker_exec(debian_container, ["apt-get", "remove", "-y", "test-pkg"])
        assert code == 0, f"remove failed: {out}"

        code, out = docker_exec(
            debian_container,
            [
                "sh",
                "-c",
                "dpkg-query -W -f '${Status}' test-pkg 2>/dev/null || echo 'not-installed'",
            ],
        )
        assert "deinstall" in out or "not-installed" in out, (
            f"Expected deinstalled status, got '{out}'"
        )


# ---------------------------------------------------------------------------
# GPG signed repo
# ---------------------------------------------------------------------------


class TestGpgSignedRepo:
    """Test that a GPG-signed repo works with apt + signed-by."""

    @pytest.mark.skipif(
        not _docker_available(),
        reason="Docker or Podman not available",
    )
    def test_apt_update_with_signed_repo(
        self,
        signed_populated_repo,
        debian_container_signed,
        docker_exec,
    ):
        """apt update succeeds against a GPG-signed repo without [trusted=yes]."""
        code, out = docker_exec(debian_container_signed, ["apt-get", "update", "-qq"])
        assert code == 0, f"signed apt update failed: {out}"

    @pytest.mark.skipif(
        not _docker_available(),
        reason="Docker or Podman not available",
    )
    def test_apt_install_from_signed_repo(
        self,
        signed_populated_repo,
        debian_container_signed,
        docker_exec,
    ):
        """apt install works from a signed repo."""
        code, out = docker_exec(debian_container_signed, ["apt-get", "update", "-qq"])
        assert code == 0
        code, out = docker_exec(
            debian_container_signed,
            [
                "apt-get",
                "install",
                "-y",
                "test-pkg",
            ],
        )
        assert code == 0, f"signed install failed: {out}"

        code, out = docker_exec(debian_container_signed, ["dpkg", "-l", "test-pkg"])
        assert code == 0
        assert "test-pkg" in out


# ---------------------------------------------------------------------------
# Dependency parsing round-trip
# ---------------------------------------------------------------------------


class TestDependencyParsing:
    """Verify OR-alternation dependencies survive the pydeb-s3 round-trip."""

    def test_or_alternation_deps_preserved_in_packages(
        self,
        moto_server,
        bucket,
    ):
        """Depends with ``|`` syntax are preserved verbatim in Packages file."""
        or_depends = "systemd | systemd-standalone-sysusers | systemd-sysusers"
        with tempfile.TemporaryDirectory() as tmpdir:
            deb_path = f"{tmpdir}/test-pkg_1.0.0_amd64.deb"
            _create_fake_deb(deb_path, "test-pkg", "1.0.0", "amd64", depends=or_depends)

            # Upload via pydeb-s3 CLI
            _pydeb_upload(BUCKET, moto_server, deb_path)

            # Fetch Packages file from moto
            _, headers, body = _http_get(
                f"{moto_server}/{BUCKET}/dists/stable/main/binary-amd64/Packages"
            )

            # Verify Depends field is preserved exactly
            assert f"Depends: {or_depends}" in body, (
                f"OR-alternation Depends not preserved.\n"
                f"Expected: Depends: {or_depends}\n"
                f"Got body snippet: {body[body.find('Depends') : body.find('Depends') + 100] if 'Depends' in body else '(no Depends found)'}"
            )

    def test_simple_dep_preserved_in_packages(
        self,
        moto_server,
        bucket,
    ):
        """Simple (non-alternation) Depends are also preserved."""
        simple_dep = "libc6 (>= 2.34)"
        with tempfile.TemporaryDirectory() as tmpdir:
            deb_path = f"{tmpdir}/test-pkg_1.0.0_amd64.deb"
            _create_fake_deb(deb_path, "test-pkg", "1.0.0", "amd64", depends=simple_dep)

            _pydeb_upload(BUCKET, moto_server, deb_path)

            _, _, body = _http_get(
                f"{moto_server}/{BUCKET}/dists/stable/main/binary-amd64/Packages"
            )
            assert f"Depends: {simple_dep}" in body, (
                f"Simple Depends not preserved.\n"
                f"Expected: Depends: {simple_dep}\n"
                f"Body: {body[body.find('Depends') : body.find('Depends') + 60] if 'Depends' in body else '(no Depends found)'}"
            )

    def test_no_depends_field_absent(
        self,
        moto_server,
        bucket,
    ):
        """Package without Depends does not get a Depends field in Packages."""
        with tempfile.TemporaryDirectory() as tmpdir:
            deb_path = f"{tmpdir}/test-pkg_1.0.0_amd64.deb"
            _create_fake_deb(deb_path, "test-pkg", "1.0.0", "amd64")

            _pydeb_upload(BUCKET, moto_server, deb_path)

            _, _, body = _http_get(
                f"{moto_server}/{BUCKET}/dists/stable/main/binary-amd64/Packages"
            )
            assert "Depends:" not in body, (
                "Package with no dependencies should not have a Depends field"
            )


# ---------------------------------------------------------------------------
# Repo structure invariants
# ---------------------------------------------------------------------------


class TestRepoStructure:
    """Walk the moto S3 tree and verify the repo has the expected layout."""

    def test_expected_s3_keys_exist(
        self,
        unsigned_populated_repo,
        moto_server,
    ):
        """All expected files exist in the S3 bucket."""
        import boto3

        client = boto3.client("s3", endpoint_url=moto_server, region_name="us-east-1")
        objs = client.list_objects_v2(Bucket=BUCKET)
        keys = {o["Key"] for o in objs.get("Contents", [])}

        assert "dists/stable/Release" in keys
        assert "dists/stable/main/binary-amd64/Packages" in keys
        assert "dists/stable/main/binary-amd64/Packages.gz" in keys
        pool_debs = [k for k in keys if k.startswith("pool/") and k.endswith(".deb")]
        assert len(pool_debs) > 0, f"No .deb files found in pool, keys={keys}"

    def test_release_file_has_hashes(
        self,
        unsigned_populated_repo,
        moto_server,
    ):
        """Release file contains SHA256 hashes for Packages files."""
        import boto3

        client = boto3.client("s3", endpoint_url=moto_server, region_name="us-east-1")
        release = client.get_object(Bucket=BUCKET, Key="dists/stable/Release")
        body = release["Body"].read().decode()

        assert "SHA256:" in body, "Release should have SHA256 section"
        assert "main/binary-amd64/Packages" in body, "Release should reference Packages file"
        assert "main/binary-amd64/Packages.gz" in body, "Release should reference Packages.gz file"


# ---------------------------------------------------------------------------
# External repo coexistence (ollama-deb via extrepo)
# ---------------------------------------------------------------------------


class TestExternalRepoIntegration:
    """Test that pydeb-s3 repo coexists with external repos (ollama-deb via extrepo).

    Uses ``extrepo`` inside the container to add the real ollama-deb repo (hosted
    at packages.lingfish.net).  Tests verify ollama installs correctly and that
    apt upgrade/dist-upgrade on our moto-served packages doesn't reinstall it.
    """

    @pytest.fixture
    def ollama_repo(self, debian_container, docker_exec):
        """Enable ollama-deb via extrepo inside the existing container.

        Skips the test on Debian < 12 (bullseye) because ollama requires
        ``libc6 >= 2.34``, which is not available there.
        """
        # Check Debian version — skip if too old for ollama
        code, out = docker_exec(debian_container, ["cat", "/etc/debian_version"])
        if code == 0:
            major = out.strip().split(".")[0]
            if int(major) < 12:
                pytest.skip(f"ollama requires libc6 >= 2.34, not available on Debian {out.strip()}")

        # Update may partially fail if the moto repo is empty — that's OK
        docker_exec(debian_container, ["apt-get", "update", "-qq"])
        code, out = docker_exec(debian_container, ["apt-get", "install", "-y", "-qq", "extrepo"])
        assert code == 0, f"extrepo install failed: {out}"
        # Enable non-free policy so extrepo allows the ollama source
        code, out = docker_exec(
            debian_container,
            [
                "sed",
                "-i",
                "-e",
                "s/^# - non-free$/- non-free/g",
                "/etc/extrepo/config.yaml",
            ],
        )
        assert code == 0, f"sed failed: {out}"
        code, out = docker_exec(debian_container, ["extrepo", "enable", "ollama"])
        assert code == 0, f"extrepo enable ollama failed: {out}"

    def test_ollama_installs_via_extrepo(
        self,
        debian_container,
        docker_exec,
        ollama_repo,
    ):
        """ollama installs from the external repo alongside our moto repo."""
        # Update may partially fail (empty moto repo) — ollama-deb is fine
        docker_exec(debian_container, ["apt-get", "update", "-qq"])
        code, out = docker_exec(debian_container, ["apt-get", "install", "-y", "ollama"])
        assert code == 0, f"ollama install failed: {out}"
        code, out = docker_exec(debian_container, ["which", "ollama"])
        assert code == 0, f"ollama binary not found: {out}"

    def test_upgrade_does_not_reinstall_ollama(
        self,
        unsigned_populated_repo,
        moto_server,
        bucket,
        debian_container,
        docker_exec,
        ollama_repo,
    ):
        """apt upgrade with a new pkg version in our repo doesn't touch ollama."""
        code, out = docker_exec(debian_container, ["apt-get", "update", "-qq"])
        assert code == 0
        code, out = docker_exec(
            debian_container, ["apt-get", "install", "-y", "ollama", "test-pkg"]
        )
        assert code == 0, f"install failed: {out}"

        with tempfile.TemporaryDirectory() as tmpdir:
            v2 = f"{tmpdir}/test-pkg_2.0.0_amd64.deb"
            _create_fake_deb(v2, "test-pkg", "2.0.0", "amd64")
            _pydeb_upload(BUCKET, moto_server, v2, "--preserve-versions")
            code, out = docker_exec(debian_container, ["apt-get", "update", "-qq"])
            assert code == 0
            code, out = docker_exec(debian_container, ["apt-get", "upgrade", "-y"])
            assert code == 0, f"upgrade failed: {out}"
            code, out = docker_exec(debian_container, ["dpkg", "-l", "ollama"])
            assert code == 0, f"ollama was removed: {out}"
            code, out = docker_exec(debian_container, ["dpkg", "-l", "test-pkg"])
            assert "2.0.0" in out, f"test-pkg not upgraded: {out}"

    def test_dist_upgrade_does_not_reinstall_ollama(
        self,
        unsigned_populated_repo,
        moto_server,
        bucket,
        debian_container,
        docker_exec,
        ollama_repo,
    ):
        """apt dist-upgrade with new pkgs doesn't reinstall ollama."""
        code, out = docker_exec(debian_container, ["apt-get", "update", "-qq"])
        assert code == 0
        code, out = docker_exec(
            debian_container, ["apt-get", "install", "-y", "ollama", "test-pkg"]
        )
        assert code == 0, f"install failed: {out}"

        with tempfile.TemporaryDirectory() as tmpdir:
            v2 = f"{tmpdir}/test-pkg_2.0.0_amd64.deb"
            _create_fake_deb(v2, "test-pkg", "2.0.0", "amd64")
            _pydeb_upload(BUCKET, moto_server, v2, "--preserve-versions")
            code, out = docker_exec(debian_container, ["apt-get", "update", "-qq"])
            assert code == 0
            code, out = docker_exec(debian_container, ["apt-get", "dist-upgrade", "-y"])
            assert code == 0, f"dist-upgrade failed: {out}"
            code, out = docker_exec(debian_container, ["dpkg", "-l", "ollama"])
            assert code == 0, f"ollama was removed: {out}"
            code, out = docker_exec(debian_container, ["dpkg", "-l", "test-pkg"])
            assert "2.0.0" in out, f"test-pkg not upgraded: {out}"
