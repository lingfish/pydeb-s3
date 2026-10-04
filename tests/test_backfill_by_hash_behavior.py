"""Integration tests for the backfill-by-hash command.

The command repairs repositories published before by-hash support existed:
it reads the existing Release file and copies each Packages/Packages.gz index
to the by-hash paths named there, without modifying or re-signing the Release.
"""

import os
import tempfile

import pytest
import typer

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3.cli import backfill_by_hash_command

PACKAGES = "dists/stable/main/binary-amd64/Packages"
PACKAGES_GZ = "dists/stable/main/binary-amd64/Packages.gz"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _all_keys(adapter) -> set:
    """Return all S3 keys (with prefix), handling pagination."""
    keys = set()
    token = None
    while True:
        contents, token = adapter.list_objects("", continuation_token=token)
        keys.update(obj["Key"] for obj in contents)
        if not token:
            break
    return keys


def _strip_prefix(adapter, key: str) -> str:
    if adapter.prefix:
        p = adapter.prefix.rstrip("/") + "/"
        if key.startswith(p):
            return key[len(p) :]
    return key


def _by_hash_keys(adapter) -> set:
    return {k for k in _all_keys(adapter) if "/by-hash/" in k}


def _object_bytes(adapter, key: str) -> bytes:
    """Download an object and return its raw bytes."""
    fd, tmp = tempfile.mkstemp()
    os.close(fd)
    try:
        adapter.download(key, tmp)
        with open(tmp, "rb") as f:
            return f.read()
    finally:
        os.unlink(tmp)


def _strip_by_hash(adapter) -> int:
    """Remove every by-hash object, emulating a pre-1.3.2 repository."""
    removed = 0
    for key in list(_all_keys(adapter)):
        stripped = _strip_prefix(adapter, key)
        if "/by-hash/" in stripped:
            adapter.remove(stripped)
            removed += 1
    return removed


def _seed_repo(
    adapter,
    deb_file: str,
    codename: str = "stable",
    component: str = "main",
    arch: str = "amd64",
    architectures=None,
    components=None,
):
    """Create a Release + manifest (which also writes by-hash on this version)."""
    release = release_module.Release(
        codename=codename,
        origin="TestRepo",
        architectures=architectures or [arch],
        components=components or [component],
    )
    pkg = package_module.Package.parse_file(deb_file)
    manifest = manifest_module.Manifest.retrieve(adapter, codename, component, arch)
    manifest.add(pkg)
    manifest.write_to_s3(adapter)
    release.update_manifest(manifest)
    release.write_to_s3(adapter)
    return release


def _expected_by_hash_keys(release, rel_name: str, index_key: str) -> list:
    directory = index_key.rsplit("/", 1)[0]
    hashes = release.files[rel_name]
    return [
        f"{directory}/by-hash/{name}/{hashes[algo]}"
        for algo, name in manifest_module._BY_HASH_DIRS
        if hashes.get(algo)
    ]


# ---------------------------------------------------------------------------
# Happy path and invariants
# ---------------------------------------------------------------------------


class TestBackfillByHashBehavior:
    """Tests for backfill-by-hash against a seeded (legacy) repo."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter, sample_deb_file):
        self.adapter = moto_s3_adapter
        self.sample_deb_file = sample_deb_file
        _seed_repo(self.adapter, self.sample_deb_file)
        # Emulate a repo published before by-hash support.
        assert _strip_by_hash(self.adapter) > 0
        assert _by_hash_keys(self.adapter) == set()

    def _run(self, **kwargs):
        return backfill_by_hash_command(bucket="test-bucket", **kwargs)

    def test_recreates_by_hash_for_packages_and_packages_gz(self):
        """Every advertised by-hash path is recreated with the index bytes."""
        self._run()
        release = release_module.Release.retrieve(self.adapter, "stable")

        for rel_name, index_key in (
            ("main/binary-amd64/Packages", PACKAGES),
            ("main/binary-amd64/Packages.gz", PACKAGES_GZ),
        ):
            for key in _expected_by_hash_keys(release, rel_name, index_key):
                assert self.adapter.exists(key), f"Missing by-hash key: {key}"
                assert _object_bytes(self.adapter, key) == _object_bytes(self.adapter, index_key)

    def test_release_bytes_unchanged(self):
        """Backfill must not modify the Release file."""
        before = self.adapter.read("dists/stable/Release")
        self._run()
        after = self.adapter.read("dists/stable/Release")
        assert before == after

    def test_creates_only_by_hash_keys(self):
        """The only new objects are by-hash copies; existing objects are untouched."""
        before = _all_keys(self.adapter)
        self._run()
        after = _all_keys(self.adapter)
        new = after - before
        assert new, "Expected new by-hash objects"
        assert all("/by-hash/" in _strip_prefix(self.adapter, k) for k in new)
        assert before <= after

    def test_idempotent_second_run_writes_nothing(self):
        """A second run adds no new objects."""
        self._run()
        keys_after_first = _all_keys(self.adapter)
        self._run()
        assert _all_keys(self.adapter) == keys_after_first

    def test_dry_run_writes_nothing(self):
        """dry_run reports but creates no objects."""
        before = _all_keys(self.adapter)
        self._run(dry_run=True)
        assert _all_keys(self.adapter) == before

    def test_content_type_matches_plain_index(self):
        """by-hash objects reuse the plain index's content type."""
        self._run()
        release = release_module.Release.retrieve(self.adapter, "stable")
        key = _expected_by_hash_keys(release, "main/binary-amd64/Packages", PACKAGES)[0]
        assert self.adapter.head(key)["ContentType"] == self.adapter.head(PACKAGES)["ContentType"]

    def test_aborts_on_index_mismatch_without_writing(self):
        """A tampered index aborts before any by-hash object is written."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(b"tampered Packages.gz bytes")
            tmp = f.name
        try:
            self.adapter.store_file(tmp, PACKAGES_GZ, content_type="application/x-gzip")
        finally:
            os.unlink(tmp)

        with pytest.raises(typer.Exit):
            self._run()

        assert _by_hash_keys(self.adapter) == set()


class TestBackfillMultipleTargets:
    """Backfill across multiple components, architectures and codenames."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter, sample_deb_file):
        self.adapter = moto_s3_adapter
        self.sample_deb_file = sample_deb_file

    def test_multiple_components_and_arches(self):
        """Packages indexes for several component/arch pairs are all backfilled."""
        release = release_module.Release(
            codename="stable",
            origin="TestRepo",
            architectures=["amd64", "arm64"],
            components=["main", "non-free"],
        )
        for component, arch in (("main", "amd64"), ("non-free", "arm64")):
            pkg = package_module.Package.parse_file(self.sample_deb_file)
            manifest = manifest_module.Manifest.retrieve(self.adapter, "stable", component, arch)
            manifest.add(pkg)
            manifest.write_to_s3(self.adapter)
            release.update_manifest(manifest)
        release.write_to_s3(self.adapter)

        _strip_by_hash(self.adapter)
        backfill_by_hash_command(bucket="test-bucket")

        for component, arch in (("main", "amd64"), ("non-free", "arm64")):
            for suffix in ("Packages", "Packages.gz"):
                key = f"dists/stable/{component}/binary-{arch}/{suffix}"
                assert self.adapter.exists(key)
                assert self.adapter.exists(
                    f"dists/stable/{component}/binary-{arch}/by-hash/SHA256/{self._sha256(key)}"
                )

    def test_all_codenames(self):
        """--all-codenames backfills every codename under dists/."""
        _seed_repo(self.adapter, self.sample_deb_file, codename="stable")
        _seed_repo(self.adapter, self.sample_deb_file, codename="rc")
        _strip_by_hash(self.adapter)

        backfill_by_hash_command(bucket="test-bucket", all_codenames=True)

        for codename in ("stable", "rc"):
            release = release_module.Release.retrieve(self.adapter, codename)
            key = f"dists/{codename}/main/binary-amd64/Packages"
            for by_hash in _expected_by_hash_keys(release, "main/binary-amd64/Packages", key):
                assert self.adapter.exists(by_hash)

    def _sha256(self, key: str) -> str:
        fd, tmp = tempfile.mkstemp()
        os.close(fd)
        try:
            self.adapter.download(key, tmp)
            return manifest_module.hash_file(tmp)["sha256"]
        finally:
            os.unlink(tmp)


class TestBackfillWithPrefix:
    """Backfill against a repo stored under a prefix."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter_with_prefix, sample_deb_file):
        self.adapter = moto_s3_adapter_with_prefix
        self.sample_deb_file = sample_deb_file
        _seed_repo(self.adapter, self.sample_deb_file)
        _strip_by_hash(self.adapter)

    def test_recreates_under_prefix(self):
        backfill_by_hash_command(bucket="test-bucket", prefix="apt")
        release = release_module.Release.retrieve(self.adapter, "stable")
        keys = _expected_by_hash_keys(release, "main/binary-amd64/Packages", PACKAGES)
        assert keys
        for key in keys:
            # exists() applies the adapter prefix, proving it landed under apt/.
            assert self.adapter.exists(key)


# ---------------------------------------------------------------------------
# Whitelisting and edge cases
# ---------------------------------------------------------------------------


class TestBackfillEdgeCases:
    """Tests for whitelist filtering and error handling."""

    def test_skips_non_packages_indexes(self, moto_s3_adapter, tmp_path):
        """Only Packages/Packages.gz entries are backfilled, not other indexes."""
        adapter = moto_s3_adapter
        pkg_file = tmp_path / "Packages"
        pkg_file.write_bytes(b"Package: test-pkg\nVersion: 1.0.0\n")
        contents_file = tmp_path / "Contents-amd64"
        contents_file.write_bytes(b"Contents-amd64\nenterprise\n")

        pkg_hashes = manifest_module.hash_file(str(pkg_file))
        contents_hashes = manifest_module.hash_file(str(contents_file))

        adapter.store_file(
            str(pkg_file),
            "dists/stable/main/binary-amd64/Packages",
            content_type="text/plain",
        )
        adapter.store_file(
            str(contents_file),
            "dists/stable/main/Contents-amd64",
            content_type="text/plain",
        )
        adapter.store_content(
            "Origin: TestRepo\n"
            "Codename: stable\n"
            "Architectures: amd64\n"
            "Components: main\n"
            "Acquire-By-Hash: yes\n"
            "SHA256:\n"
            f" {pkg_hashes['sha256']} {pkg_hashes['size']} main/binary-amd64/Packages\n"
            f" {contents_hashes['sha256']} {contents_hashes['size']} main/Contents-amd64\n",
            "dists/stable/Release",
        )

        backfill_by_hash_command(bucket="test-bucket")

        by_hash = _by_hash_keys(adapter)
        # Only the Packages index has an advertised hash, so exactly one copy.
        assert by_hash == {f"dists/stable/main/binary-amd64/by-hash/SHA256/{pkg_hashes['sha256']}"}
        assert not any("Contents-amd64" in k and "/by-hash/" in k for k in _all_keys(adapter))

    def test_fails_when_release_missing(self, moto_s3_adapter):
        """A missing Release is a hard error."""
        with pytest.raises(typer.Exit):
            backfill_by_hash_command(bucket="test-bucket")

    def test_no_indexes_exits_cleanly(self, moto_s3_adapter):
        """A Release with no index entries exits without error."""
        moto_s3_adapter.store_content(
            "Origin: TestRepo\nCodename: stable\n", "dists/stable/Release"
        )
        backfill_by_hash_command(bucket="test-bucket")
        assert _by_hash_keys(moto_s3_adapter) == set()

    def test_requires_bucket(self):
        """backfill-by-hash fails without --bucket."""
        with pytest.raises(typer.Exit):
            backfill_by_hash_command(bucket=None)
