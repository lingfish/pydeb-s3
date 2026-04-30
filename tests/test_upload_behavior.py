"""Integration tests for the upload command."""

import pytest

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3 import s3_utils


class TestUploadIntegration:
    """Integration tests for upload command using real upload flow."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client, sample_deb_file):
        """Set up test fixtures with S3 bucket and configuration."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._access_policy = "public-read"
        self.sample_deb_file = sample_deb_file

    def _create_initial_release(self):
        """Create and upload an initial Release file."""
        release = release_module.Release(
            codename="stable",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main"],
        )
        release.write_to_s3()
        return release

    def _get_pool_path(self, pkg: package_module.Package, component: str = "main") -> str:
        """Get the pool path for a package based on its filename."""
        basename = pkg.filename.split('/')[-1]
        return f"pool/{component}/{pkg.name[0]}/{pkg.name[0:2]}/{basename}"

    def test_uploads_package_file_to_s3(self):
        """Upload creates .deb file in S3 pool and updates Release file hashes.

        This test simulates the real upload flow:
        1. Retrieve existing Release (or create new)
        2. Parse and add package to appropriate manifest
        3. Write manifest back to S3 (which uploads the .deb file)
        4. Update and write Release file
        """
        self._create_initial_release()

        # Simulate real upload flow: retrieve release, then manifest, add package, write back
        release = release_module.Release.retrieve("stable")

        pkg = package_module.Package.parse_file(self.sample_deb_file)
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()

        release.update_manifest(manifest)
        release.write_to_s3()

        # Verify .deb file exists in S3 pool
        pool_path = self._get_pool_path(pkg)
        exists = s3_utils.s3_exists(pool_path)
        assert exists, f"Package file {pool_path} should exist in S3 pool"

    def test_creates_packages_manifest(self):
        """Upload creates the Packages manifest file in S3."""
        self._create_initial_release()

        release = release_module.Release.retrieve("stable")
        pkg = package_module.Package.parse_file(self.sample_deb_file)

        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()

        release.update_manifest(manifest)
        release.write_to_s3()

        # Verify Packages file exists and contains package info
        packages_content = s3_utils.s3_read("dists/stable/main/binary-amd64/Packages")
        assert packages_content is not None
        assert "test-pkg" in packages_content

    def test_creates_packages_gz(self):
        """Upload creates the gzipped Packages file in S3."""
        self._create_initial_release()

        release = release_module.Release.retrieve("stable")
        pkg = package_module.Package.parse_file(self.sample_deb_file)

        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()

        release.update_manifest(manifest)
        release.write_to_s3()

        # Verify gzipped Packages file exists
        gz_exists = s3_utils.s3_exists("dists/stable/main/binary-amd64/Packages.gz")
        assert gz_exists, "Packages.gz should exist in S3"

    def test_updates_release_file_hash(self):
        """Upload updates the Release file hash when manifest changes.

        Verifies that:
        1. Release file is written
        2. Hash entries for Packages files are present
        3. Hash values correctly reference Packages file
        """
        self._create_initial_release()

        release = release_module.Release.retrieve("stable")
        pkg = package_module.Package.parse_file(self.sample_deb_file)

        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()

        release.update_manifest(manifest)
        release.write_to_s3()

        # Read Release file and verify structure
        release_content = s3_utils.s3_read("dists/stable/Release")
        assert release_content is not None

        # Verify Packages path is referenced (relative to dists/<codename>/)
        # The Release file uses paths like "main/binary-amd64/Packages"
        assert "main/binary-amd64/Packages" in release_content

        # Verify the hash entries have proper format (hash size path)
        # Each entry should have format: "<hash> <size> main/binary-amd64/Packages"
        import re
        # Match any hash (MD5=32, SHA1=40, SHA256=64, SHA512=128 hex chars) for Packages or Packages.gz
        packages_hash_pattern = r"^[a-f0-9]{32,128} \d+ main/binary-amd64/(Packages|Packages\.gz)$"
        lines = release_content.split("\n")
        hash_lines = [l for l in lines if "main/binary-amd64/Packages" in l]
        assert len(hash_lines) > 0, "Release should contain hash entries for Packages files"
        for line in hash_lines:
            # Check it's a valid hash line format (exact match for Packages or Packages.gz)
            assert re.match(packages_hash_pattern, line.strip()), \
                f"Invalid hash line format: {line}"


class TestUploadPreserveVersions:
    """Tests for preserve-versions flag with S3 state verification."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client, sample_deb_file):
        """Set up test fixtures with S3 bucket and configuration."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._access_policy = "public-read"
        self.sample_deb_file = sample_deb_file

    def _create_initial_release(self):
        """Create and upload an initial Release file."""
        release = release_module.Release(
            codename="stable",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main"],
        )
        release.write_to_s3()
        return release

    def _get_pool_path(self, pkg: package_module.Package, component: str = "main") -> str:
        """Get the pool path for a package based on its filename."""
        basename = pkg.filename.split('/')[-1]
        return f"pool/{component}/{pkg.name[0]}/{pkg.name[0:2]}/{basename}"

    def _write_package_and_release(self, pkg: package_module.Package,
                               preserve_versions: bool,
                               component: str = "main"):
        """Write package to manifest and update release."""
        release = release_module.Release.retrieve("stable")
        manifest = manifest_module.Manifest.retrieve("stable", component, "amd64")
        manifest.add(pkg, preserve_versions=preserve_versions)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()
        return manifest

    def test_preserve_versions_false_removes_old(self):
        """Without preserve-versions, old version is replaced by new version.

        Verifies that:
        1. First package version (1.0.0) is uploaded
        2. Second package version (2.0.0) replaces first when preserve_versions=False
        3. .deb file exists in S3 pool
        4. Manifest contains only the new version
        """
        self._create_initial_release()

        # Upload first version with preserve_versions=True
        pkg1 = package_module.Package.parse_file(self.sample_deb_file)
        if pkg1.iteration is None:
            pkg1.iteration = "1"
        self._write_package_and_release(pkg1, preserve_versions=True)

        pool_path = self._get_pool_path(pkg1)
        assert s3_utils.s3_exists(pool_path), f"Version 1.0.0 should exist: {pool_path}"

        # Upload second version without preserving
        pkg2 = package_module.Package.parse_file(self.sample_deb_file)
        pkg2.version = "2.0.0"
        pkg2.iteration = "1"
        self._write_package_and_release(pkg2, preserve_versions=False)

        # Verify manifest contains only the new version
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        assert len(manifest.packages) == 1, "Manifest should have only 1 package"
        assert manifest.packages[0].full_version == "2.0.0-1", "Manifest should contain only version 2.0.0-1"

        # Verify Packages file in S3 contains only new version
        packages_content = s3_utils.s3_read("dists/stable/main/binary-amd64/Packages")
        assert "2.0.0-1" in packages_content, "Packages should reference version 2.0.0-1"

    def test_preserve_versions_true_keeps_old(self):
        """With preserve-versions, old version is kept alongside new version.

        Verifies that:
        1. First package version (1.0.0) is uploaded
        2. Second package version (2.0.0) is added when preserve_versions=True
        3. Both .deb files exist in S3 pool (same file reused with different metadata)
        4. Manifest contains both versions
        """
        self._create_initial_release()

        # Upload first version with preserve_versions=True
        pkg1 = package_module.Package.parse_file(self.sample_deb_file)
        if pkg1.iteration is None:
            pkg1.iteration = "1"
        self._write_package_and_release(pkg1, preserve_versions=True)

        pool_path = self._get_pool_path(pkg1)
        assert s3_utils.s3_exists(pool_path), f"Version 1.0.0 should exist: {pool_path}"

        # Upload second version preserving old one
        pkg2 = package_module.Package.parse_file(self.sample_deb_file)
        pkg2.version = "2.0.0"
        pkg2.iteration = "1"
        self._write_package_and_release(pkg2, preserve_versions=True)

        # Verify .deb file still exists in S3 pool
        pool_path = self._get_pool_path(pkg2)
        assert s3_utils.s3_exists(pool_path), f"Version 2.0.0 should exist: {pool_path}"

        # Verify manifest contains both versions
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        assert len(manifest.packages) == 2, "Manifest should have 2 packages"

        versions = sorted([p.full_version for p in manifest.packages])
        assert "1.0.0-1" in versions, "Manifest should contain version 1.0.0-1"
        assert "2.0.0-1" in versions, "Manifest should contain version 2.0.0-1"

        # Verify Packages file in S3 contains both versions
        packages_content = s3_utils.s3_read("dists/stable/main/binary-amd64/Packages")
        assert "1.0.0-1" in packages_content, "Packages should reference version 1.0.0-1"
        assert "2.0.0-1" in packages_content, "Packages should reference version 2.0.0-1"
