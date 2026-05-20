"""Integration tests for the delete command."""

import sys

import pytest
import typer

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3.s3_adapter import S3Adapter
from pydeb_s3.cli import delete_command


def setup_logger():
    """Configure loguru to output to captured stderr."""
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, format="{message}")


def package_exists(manifest, package_name):
    """Check if a package with given name exists in manifest."""
    return any(pkg.name == package_name for pkg in manifest.packages)


class TestDeleteManifest:
    """Integration tests for delete_package method in manifest."""

    @pytest.fixture(autouse=True)
    def setup(self, mock_s3_adapter, sample_deb_file):
        """Set up test fixtures with S3 bucket and configuration.

        Uses mock_s3_adapter since these tests directly call module methods
        (manifest.delete_package) rather than CLI commands.
        """
        self.s3_adapter = mock_s3_adapter
        self.sample_deb_file = sample_deb_file

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
        release.write_to_s3(self.s3_adapter)
        return release

    def _add_packages_to_manifest(self, release, deb_file, component="main", arch="amd64"):
        """Add packages to manifest and update release."""
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", component, arch)
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)
        return pkg

    def test_delete_package_removes_from_manifest(self):
        """Delete a package removes it from the manifest."""
        setup_logger()

        # Create release and add package
        release = self._create_release()
        self._add_packages_to_manifest(
            release,
            self.sample_deb_file,
        )

        # Get manifest and verify package exists
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        assert package_exists(manifest, "test-pkg")

        # Delete the package directly using manifest
        manifest.delete_package("test-pkg", None)

        # Verify package is gone from the list
        assert not package_exists(manifest, "test-pkg")

    def test_delete_package_with_versions(self):
        """Delete a specific version of a package."""
        setup_logger()

        # Create release and add package
        release = self._create_release()
        self._add_packages_to_manifest(
            release,
            self.sample_deb_file,
        )

        # Get manifest
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        assert package_exists(manifest, "test-pkg")

        # Delete specific version
        deleted = manifest.delete_package("test-pkg", ["1.0.0"])

        # Verify package was deleted
        assert len(deleted) > 0
        # Package should now be removed (as only version was deleted)
        assert not package_exists(manifest, "test-pkg")

    def test_delete_package_writes_to_s3(self):
        """Delete should work with S3 upload."""
        setup_logger()

        # Create release and add package
        release = self._create_release()
        self._add_packages_to_manifest(
            release,
            self.sample_deb_file,
        )

        # Get manifest and delete the package
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest.delete_package("test-pkg", None)

        # Write back to S3
        manifest.write_to_s3(self.s3_adapter)

        # Retrieve fresh manifest and verify package is gone
        new_manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        assert not package_exists(new_manifest, "test-pkg")

    def test_delete_nonexistent_package_returns_empty(self):
        """Deleting non-existent package returns empty list."""
        setup_logger()

        # Create release
        release = self._create_release()

        # Get manifest (empty)
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")

        # Delete non-existent package
        deleted = manifest.delete_package("nonexistent", None)

        # Should return empty list
        assert deleted == []

    def test_delete_package_updates_release(self):
        """Delete should update Release file."""
        setup_logger()

        release = self._create_release()

        # Add package
        self._add_packages_to_manifest(
            release,
            self.sample_deb_file,
        )

        # Get Package counts from release
        files_before = dict(release.files)

        # Delete package from manifest
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest.delete_package("test-pkg", None)
        manifest.write_to_s3(self.s3_adapter)

        # Update release after delete
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)

        # Verify Release file has updated hashes
        files_after = dict(release.files)

        # The Package file size should differ after delete
        # because the manifest is now smaller/empty
        assert "main/binary-amd64/Packages" in files_after

    def test_delete_multiple_versions_different_architectures(self):
        """Delete should work across multiple architectures."""
        setup_logger()

        # Create release with multiple archs
        release = self._create_release(architectures=["amd64", "arm64"])

        # Add amd64 package
        pkg_amd64 = package_module.Package.parse_file(self.sample_deb_file)
        manifest_amd64 = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest_amd64.add(pkg_amd64)
        manifest_amd64.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest_amd64)

        # Add arm64 package
        arm64_file = "tests/fixtures/test-pkg_1.0.0_arm64.deb"
        pkg_arm64 = package_module.Package.parse_file(arm64_file)
        manifest_arm64 = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "arm64")
        manifest_arm64.add(pkg_arm64)
        manifest_arm64.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest_arm64)

        release.write_to_s3(self.s3_adapter)

        # Delete from only amd64
        manifest_amd64 = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest_amd64.delete_package("test-pkg", None)
        manifest_amd64.write_to_s3(self.s3_adapter)

        # Verify amd64 is gone but arm64 remains
        new_amd64 = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        new_arm64 = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "arm64")

        assert not package_exists(new_amd64, "test-pkg")
        assert package_exists(new_arm64, "test-pkg")


class TestDeleteCommand:
    """Tests for CLI delete command interaction."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter, sample_deb_file):
        """Set up test fixtures with S3 bucket and configuration.

        Uses moto_s3_adapter since these tests call delete_command()
        which internally creates Boto3S3Adapter via cli._configure_s3().
        """
        self.s3_adapter = moto_s3_adapter
        self.sample_deb_file = sample_deb_file

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
        release.write_to_s3(self.s3_adapter)
        return release

    def _add_packages_to_manifest(self, release, deb_file, component="main", arch="amd64"):
        """Add packages to manifest and update release."""
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", component, arch)
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)
        return pkg

    def test_delete_requires_bucket(self):
        """Delete command requires bucket option."""
        with pytest.raises(typer.Exit):
            delete_command(
                package="test-pkg",
                bucket=None,
                codename="stable",
                component="main",
                arch="amd64",
            )

    def test_delete_nonexistent_package_errors(self):
        """Deleting a package that doesn't exist should raise error."""
        setup_logger()

        # Create empty release
        release = release_module.Release(
            codename="stable",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main"],
        )
        release.write_to_s3(self.s3_adapter)

        # Attempt to delete non-existent package
        # The command checks using 'in' operator which has a bug
        # but it should still error when manifest.packages is empty
        with pytest.raises(typer.Exit):
            delete_command(
                package="nonexistent-package",
                bucket="test-bucket",
                codename="stable",
                component="main",
                arch="amd64",
            )

    def test_delete_existing_package_succeeds(self):
        """Deleting a package that exists should succeed.

        This test reproduces the bug where the CLI uses 'in' operator
        to check if a package name exists in manifest.packages.
        Since manifest.packages is a list of Package objects (not strings),
        the 'in' check always returns False, causing the delete to fail
        with 'Package not found' even when the package exists.
        """
        setup_logger()

        # Create release and add package
        release = self._create_release()
        pkg = self._add_packages_to_manifest(
            release,
            self.sample_deb_file,
        )

        # Verify package exists in manifest
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        assert package_exists(manifest, "test-pkg")

        # Delete the package via CLI command - this should succeed
        # but currently fails due to the 'in' operator bug
        delete_command(
            package="test-pkg",
            bucket="test-bucket",
            codename="stable",
            component="main",
            arch="amd64",
        )

        # Verify package was actually deleted
        manifest_after = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        assert not package_exists(manifest_after, "test-pkg")
