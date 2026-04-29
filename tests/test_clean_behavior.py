"""Integration tests for the clean command."""

import os
import sys
import tempfile

import pytest

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3 import s3_utils
from pydeb_s3.cli import clean_command


def setup_logger():
    """Configure loguru to output to captured stderr."""
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, format="{message}")


class TestCleanIntegration:
    """Integration tests for clean command using mocked S3."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures with S3 bucket and configuration."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._access_policy = "public-read"

    def _create_release(self, codename="stable", architectures=None, components=None):
        """Create and upload a Release file."""
        if architectures is None:
            architectures = ["amd64"]
        if components is None:
            components = ["main"]
        release = release_module.Release(
            codename=codename,
            origin="TestRepo",
            architectures=architectures,
            components=components,
        )
        release.write_to_s3()
        return release

    def _add_packages_to_manifest(self, release, deb_file, component="main", arch="amd64"):
        """Add packages to manifest and update release."""
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve("stable", component, arch)
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()
        return pkg

    def _upload_deb_to_pool(self, deb_file_path, component="main"):
        """Upload a .deb file directly to the pool in S3 using s3_store."""
        # Create a temp file to use with s3_store
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(open(deb_file_path, "rb").read())
            tmp_path = tmp.name

        try:
            filename = os.path.basename(deb_file_path)
            # Extract package name from filename (e.g., hello_2.10-5_amd64.deb -> hello)
            name = filename.rsplit("_", 2)[0]
            first_letter = name[0]
            # Store in pool with component path
            key = f"pool/{component}/{first_letter}/{name}/{filename}"
            s3_utils.s3_store(tmp_path, key, "application/x-debian-package")
        finally:
            os.unlink(tmp_path)

    def _get_pool_files(self):
        """Get list of .deb files in pool from S3."""
        result = s3_utils.s3_list_objects("pool/")
        objects = result[0] if isinstance(result, tuple) else result
        return [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

    def test_clean_removes_orphaned_files(self, capfd):
        """Clean command removes .deb files not referenced by any Packages file."""
        setup_logger()

        # Create release with packages
        release = self._create_release()

        # Add a package to manifest (this creates the reference)
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
        )

        # Upload an orphaned .deb file to the pool (not in manifest)
        # This simulates a package that was removed from manifest but file remains
        self._upload_deb_to_pool("tests/fixtures/hello_2.10-5_amd64.deb")

        # Verify the orphan exists in S3
        pool_files_before = self._get_pool_files()
        assert any("hello" in f for f in pool_files_before), "Orphan file should exist before clean"

        # Run clean command
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Verify the orphan was removed
        pool_files_after = self._get_pool_files()

        # The hello package should be removed as it's not in manifest
        assert not any("hello" in f for f in pool_files_after), "Orphan file should be removed by clean"
        assert "Removing" in output or "Removed" in output, "Clean should report removal"

    def test_clean_does_not_remove_referenced_files(self, capfd):
        """Clean command keeps .deb files that are referenced by Packages files."""
        setup_logger()

        # Create release with packages
        release = self._create_release()

        # Add a package to manifest (this creates the reference)
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
        )

        # Verify the package exists in S3 pool
        pool_files_before = self._get_pool_files()
        assert any("test-pkg" in f for f in pool_files_before), "Referenced file should exist before clean"

        # Run clean command
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Verify the referenced file still exists
        pool_files_after = self._get_pool_files()

        # The test-pkg should still exist as it's in the manifest
        assert any("test-pkg" in f for f in pool_files_after), "Referenced file should remain after clean"
        assert "No orphaned packages found" in output, "Clean should report no orphans"

    def test_clean_no_orphans_no_op(self, capfd):
        """Clean command handles case with no orphaned packages."""
        setup_logger()

        # Create release with packages
        release = self._create_release()

        # Add a package to manifest
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
        )

        # Run clean command
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Should report no orphans found
        assert "No orphaned packages found" in output, "Clean should report no orphans when none exist"

    def test_clean_multiple_orphans_removed(self, capfd):
        """Clean command removes multiple orphaned files."""
        setup_logger()

        # Create release with one package
        release = self._create_release()
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
        )

        # Upload multiple orphaned packages
        orphan_files = [
            "tests/fixtures/hello_2.10-5_amd64.deb",
            "tests/fixtures/test-pkg_1.0.0_arm64.deb",
        ]

        for deb_file in orphan_files:
            self._upload_deb_to_pool(deb_file)

        # Run clean command
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Verify orphans were removed
        pool_files_after = self._get_pool_files()

        # Only test-pkg should remain (the one in manifest)
        assert any("test-pkg_1.0.0_amd64" in f for f in pool_files_after), "Referenced file should remain"
        assert not any("hello" in f for f in pool_files_after), "Orphan hello should be removed"
        assert not any("test-pkg_1.0.0_arm64" in f for f in pool_files_after), "Orphan arm64 should be removed"
        assert "Removed 2 orphaned" in output, "Should report removing 2 orphans"

    def test_clean_with_multiple_components(self, capfd):
        """Clean command works across multiple components."""
        setup_logger()

        # Create release with multiple components
        release = self._create_release(components=["main", "non-free"])

        # Add package to main component
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
            component="main",
        )

        # Add package to non-free component
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/hello_2.10-5_amd64.deb",
            component="non-free",
        )

        # Upload an orphan to pool (in main component area)
        self._upload_deb_to_pool("tests/fixtures/test-pkg-full_1.0.0_all.deb")

        # Run clean for main component only
        # Note: clean scans entire pool and removes files not referenced in
        # the specified component's manifest - it doesn't preserve files from other components
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # The test-pkg-full orphan should be removed
        pool_files_after = self._get_pool_files()

        # Main referenced package should remain
        assert any("test-pkg_1.0.0_amd64" in f for f in pool_files_after), "main package should remain"
        # The orphan should be removed
        assert not any("test-pkg-full" in f for f in pool_files_after), "Orphan should be removed"


    def test_clean_dry_run_does_not_remove_orphans(self, capfd):
        """Dry-run mode reports orphans but does not delete them."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
        )
        self._upload_deb_to_pool("tests/fixtures/hello_2.10-5_amd64.deb")

        pool_files_before = self._get_pool_files()
        assert any("hello" in f for f in pool_files_before)

        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
            dry_run=True,
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        pool_files_after = self._get_pool_files()
        assert any("hello" in f for f in pool_files_after), "Orphan file should still exist in dry-run"
        assert "Would remove" in output, "Dry-run should report would-be removals"
        assert "Would remove 1 orphaned package(s)." in output, "Dry-run should report correct summary"

    def test_clean_dry_run_no_orphans(self, capfd):
        """Dry-run mode with no orphans reports nothing to remove."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
        )

        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
            dry_run=True,
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        assert "No orphaned packages found" in output
        assert "Would remove" not in output

    def test_clean_dry_run_multiple_orphans(self, capfd):
        """Dry-run mode reports multiple would-be removals."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
        )

        orphan_files = [
            "tests/fixtures/hello_2.10-5_amd64.deb",
            "tests/fixtures/test-pkg_1.0.0_arm64.deb",
        ]
        for deb_file in orphan_files:
            self._upload_deb_to_pool(deb_file)

        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
            dry_run=True,
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        pool_files_after = self._get_pool_files()
        assert any("hello" in f for f in pool_files_after), "Orphan should still exist in dry-run"
        assert any("test-pkg_1.0.0_arm64" in f for f in pool_files_after), "Orphan should still exist in dry-run"
        assert "Would remove 2 orphaned package(s)." in output

    def test_clean_normal_run_still_works(self, capfd):
        """Normal (non-dry-run) clean still removes orphans."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
        )
        self._upload_deb_to_pool("tests/fixtures/hello_2.10-5_amd64.deb")

        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
            dry_run=False,
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        pool_files_after = self._get_pool_files()
        assert not any("hello" in f for f in pool_files_after), "Orphan should be removed in normal run"
        assert "Removed 1 orphaned package(s)." in output


class TestCleanErrors:
    """Tests for error handling in clean command."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures with S3 bucket."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._access_policy = "public-read"

    def test_clean_requires_bucket(self):
        """Clean command requires bucket option."""
        import typer

        with pytest.raises(typer.Exit):
            clean_command(
                bucket=None,
                codename="stable",
                component="main",
            )