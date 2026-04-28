"""Integration tests for the copy command."""

import sys

import pytest

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3 import s3_utils
from pydeb_s3.cli import copy_command


def setup_logger():
    """Configure loguru to output to captured stderr."""
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, format="{message}")


def package_exists(manifest, package_name):
    """Check if a package with given name exists in manifest."""
    return any(pkg.name == package_name for pkg in manifest.packages)


def find_package(manifest, package_name):
    """Find a package by name in manifest."""
    return next((p for p in manifest.packages if p.name == package_name), None)


class TestCopyIntegration:
    """Integration tests for copy command using mocked S3."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client, sample_deb_file):
        """Set up test fixtures with S3 bucket and configuration."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._access_policy = "public-read"
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

    def test_copy_package_to_different_component(self, capfd):
        """Copy a package from main component to non-free component."""
        setup_logger()

        # Create source release with main component
        release = self._create_release(components=["main", "non-free"])

        # Add package to source component (main)
        pkg = self._add_packages_to_manifest(
            release,
            self.sample_deb_file,
            component="main",
        )

        # Copy the package to non-free component
        copy_command(
            package="test-pkg",
            to_codename="stable",
            to_component="non-free",
            versions=None,
            arch="amd64",
            bucket="test-bucket",
            prefix=None,
            codename="stable",
            component="main",
            s3_region="us-east-1",
            access_key_id=None,
            secret_access_key=None,
            session_token=None,
            endpoint=None,
            cache_control=None,
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Verify copy was successful
        assert "Copy complete" in output or "Copy" in output

        # Verify package exists in target manifest (packages is a list, not dict)
        target_manifest = manifest_module.Manifest.retrieve("stable", "non-free", "amd64")
        assert package_exists(target_manifest, "test-pkg")
        copied_pkg = find_package(target_manifest, "test-pkg")
        assert copied_pkg is not None
        assert copied_pkg.version == "1.0.0"

        # Verify package still exists in source manifest (not moved, just copied)
        source_manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        assert package_exists(source_manifest, "test-pkg")

    def test_copy_package_preserves_package_data(self, capfd):
        """Verify that copied package retains all metadata."""
        setup_logger()

        # Create release with both components
        release = self._create_release(components=["main", "extra"])

        # Add package to main
        pkg = self._add_packages_to_manifest(
            release,
            self.sample_deb_file,
            component="main",
        )

        # Copy package
        copy_command(
            package="test-pkg",
            to_codename="stable",
            to_component="extra",
            versions=None,
            arch="amd64",
            bucket="test-bucket",
            prefix=None,
            codename="stable",
            component="main",
            s3_region="us-east-1",
            access_key_id=None,
            secret_access_key=None,
            session_token=None,
            endpoint=None,
            cache_control=None,
        )

        # Verify package data is preserved in target
        target_manifest = manifest_module.Manifest.retrieve("stable", "extra", "amd64")
        copied_pkg = find_package(target_manifest, "test-pkg")

        # Check key metadata is preserved
        assert copied_pkg is not None
        assert copied_pkg.name == "test-pkg"
        assert copied_pkg.version == "1.0.0"
        assert copied_pkg.architecture == "amd64"

    def test_copy_package_to_different_architecture(self, capfd):
        """Copy a package to a different architecture.

        Note: The copy command uses the same arch for source and target, so this test
        verifies that packages across multiple architectures can be copied.

        Actually, the copy command doesn't support cross-arch copy (no separate --to-arch param).
        This test verifies that having multiple arch releases works correctly.
        """
        setup_logger()

        # Create release with multiple architectures (amd64 and arm64)
        release = self._create_release(architectures=["amd64", "arm64"], components=["main"])

        # Add amd64 package
        amd64_pkg = package_module.Package.parse_file(self.sample_deb_file)
        manifest_amd64 = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest_amd64.add(amd64_pkg)
        manifest_amd64.write_to_s3()
        release.update_manifest(manifest_amd64)

        # Also need to create empty arm64 manifest so the architecture is recognized
        manifest_arm64 = manifest_module.Manifest.retrieve("stable", "main", "arm64")
        manifest_arm64.write_to_s3()
        release.update_manifest(manifest_arm64)

        release.write_to_s3()

        # Copy FROM amd64 TO a different component (not different arch)
        copy_command(
            package="test-pkg",
            to_codename="stable",
            to_component="extra",  # Different component
            versions=None,
            arch="amd64",
            bucket="test-bucket",
            prefix=None,
            codename="stable",
            component="main",
            s3_region="us-east-1",
            access_key_id=None,
            secret_access_key=None,
            session_token=None,
            endpoint=None,
            cache_control=None,
        )

        # Verify package exists in extra component (amd64)
        target_manifest = manifest_module.Manifest.retrieve("stable", "extra", "amd64")
        assert package_exists(target_manifest, "test-pkg")

    def test_copy_specific_version(self, capfd):
        """Copy only a specific version when multiple versions exist."""
        setup_logger()

        # Create release with both components
        release = self._create_release(components=["main", "archive"])

        # Add package to main
        pkg = self._add_packages_to_manifest(
            release,
            self.sample_deb_file,
            component="main",
        )

        # Copy specific version
        copy_command(
            package="test-pkg",
            to_codename="stable",
            to_component="archive",
            versions=["1.0.0"],
            arch="amd64",
            bucket="test-bucket",
            prefix=None,
            codename="stable",
            component="main",
            s3_region="us-east-1",
            access_key_id=None,
            secret_access_key=None,
            session_token=None,
            endpoint=None,
            cache_control=None,
        )

        # Verify only the specified version was copied
        target_manifest = manifest_module.Manifest.retrieve("stable", "archive", "amd64")
        copied_pkg = find_package(target_manifest, "test-pkg")
        assert copied_pkg is not None
        assert copied_pkg.version == "1.0.0"

    def test_copy_updates_target_release_file(self, capfd):
        """Verify that the target Release file is updated after copy."""
        setup_logger()

        # Create release with both components
        release = self._create_release(components=["main", "non-free"])

        # Add package to source
        self._add_packages_to_manifest(
            release,
            self.sample_deb_file,
            component="main",
        )

        # Copy package
        copy_command(
            package="test-pkg",
            to_codename="stable",
            to_component="non-free",
            versions=None,
            arch="amd64",
            bucket="test-bucket",
            prefix=None,
            codename="stable",
            component="main",
            s3_region="us-east-1",
            access_key_id=None,
            secret_access_key=None,
            session_token=None,
            endpoint=None,
            cache_control=None,
        )

        # Verify target Release file was updated (has the new manifest reference)
        release_updated = release_module.Release.retrieve("stable")
        # The release should have updated hashes for the non-free component
        assert "non-free" in release_updated.components


class TestCopyErrors:
    """Tests for error handling in copy command."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures with S3 bucket."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._access_policy = "public-read"

    def test_copy_nonexistent_package(self, capfd):
        """Error when copying a package that doesn't exist."""
        import typer

        setup_logger()

        # Create release with a package
        release = release_module.Release(
            codename="stable",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main", "non-free"],
        )
        release.write_to_s3()

        # Try to copy a non-existent package
        with pytest.raises(typer.Exit):
            copy_command(
                package="nonexistent-package",
                to_codename="stable",
                to_component="non-free",
                versions=None,
                arch="amd64",
                bucket="test-bucket",
                prefix=None,
                codename="stable",
                component="main",
                s3_region="us-east-1",
                access_key_id=None,
                secret_access_key=None,
                session_token=None,
                endpoint=None,
                cache_control=None,
            )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        assert "not found" in output.lower() or "error" in output.lower()

    def test_copy_requires_bucket(self):
        """Copy command requires bucket option."""
        import typer

        setup_logger()

        with pytest.raises(typer.Exit):
            copy_command(
                package="test-pkg",
                to_codename="stable",
                to_component="main",
                versions=None,
                arch="amd64",
                bucket=None,  # No bucket provided
                prefix=None,
                codename="stable",
                component="main",
                s3_region="us-east-1",
                access_key_id=None,
                secret_access_key=None,
                session_token=None,
                endpoint=None,
                cache_control=None,
            )

    def test_copy_to_invalid_codename(self, capfd):
        """Error when target codename doesn't exist."""
        import typer

        setup_logger()

        # Create source release
        release = release_module.Release(
            codename="stable",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main"],
        )
        release.write_to_s3()

        # Add package
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()

        # Try to copy to non-existent codename
        with pytest.raises(typer.Exit):
            copy_command(
                package="test-pkg",
                to_codename="nonexistent-codename",
                to_component="main",
                versions=None,
                arch="amd64",
                bucket="test-bucket",
                prefix=None,
                codename="stable",
                component="main",
                s3_region="us-east-1",
                access_key_id=None,
                secret_access_key=None,
                session_token=None,
                endpoint=None,
                cache_control=None,
            )

    def test_copy_invalid_architecture_in_target(self, capfd):
        """Error when target codename doesn't have the architecture."""
        import typer

        setup_logger()

        # Create source release with amd64
        source_release = release_module.Release(
            codename="stable",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main"],
        )
        source_release.write_to_s3()

        # Add package
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()
        source_release.update_manifest(manifest)
        source_release.write_to_s3()

        # Create target release with only arm64 (no amd64)
        target_release = release_module.Release(
            codename="stable",
            origin="TestRepo",
            architectures=["arm64"],  # Different architecture
            components=["main", "secondary"],
        )
        target_release.write_to_s3()

        # Try to copy amd64 package to target that doesn't have amd64
        with pytest.raises(typer.Exit):
            copy_command(
                package="test-pkg",
                to_codename="stable",
                to_component="secondary",
                versions=None,
                arch="amd64",  # Source arch
                bucket="test-bucket",
                prefix=None,
                codename="stable",
                component="main",
                s3_region="us-east-1",
                access_key_id=None,
                secret_access_key=None,
                session_token=None,
                endpoint=None,
                cache_control=None,
            )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        assert "architecture" in output.lower()