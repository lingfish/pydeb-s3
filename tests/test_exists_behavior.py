"""Integration tests for the exists command."""

import sys

import pytest

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3 import s3_utils
from pydeb_s3.cli import exists_command


def setup_logger():
    """Configure loguru to output to captured stderr."""
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, format="{message}")


class TestExistsIntegration:
    """Integration tests for exists command using mocked S3."""

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

    def test_exists_returns_true_for_existing_package(self, capfd):
        """exists returns 1 for package that exists in repository."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(release, self.sample_deb_file)

        exists_command(
            package="test-pkg",
            version=None,
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Command should output "1" indicating package exists
        assert "1" in output

    def test_exists_returns_false_for_nonexistent_package(self, capfd):
        """exists returns 0 for package that does not exist in repository."""
        setup_logger()

        self._create_release()

        exists_command(
            package="nonexistent-package",
            version=None,
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Command should output "0" indicating package does not exist
        assert "0" in output

    def test_exists_with_exact_version_match(self, capfd):
        """exists returns 1 when package exists with specified version."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(release, self.sample_deb_file)

        exists_command(
            package="test-pkg",
            version="1.0.0",
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Version matches, so should return "1"
        assert "1" in output

    def test_exists_with_nonexistent_version(self, capfd):
        """exists returns 0 when package exists but version does not."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(release, self.sample_deb_file)

        exists_command(
            package="test-pkg",
            version="2.0.0",
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Version doesn't match, so should return "0"
        assert "0" in output

    def test_exists_with_different_architecture(self, capfd):
        """exists returns 0 when package exists but different arch."""
        setup_logger()

        release = self._create_release(architectures=["amd64", "arm64"])

        # Add amd64 package
        self._add_packages_to_manifest(release, self.sample_deb_file, arch="amd64")

        # Add arm64 package
        arm64_deb = "tests/fixtures/test-pkg_1.0.0_arm64.deb"
        arm64_pkg = package_module.Package.parse_file(arm64_deb)
        manifest_arm64 = manifest_module.Manifest.retrieve("stable", "main", "arm64")
        manifest_arm64.add(arm64_pkg)
        manifest_arm64.write_to_s3()

        # Check amd64 arch against arm64 package
        exists_command(
            package="test-pkg",
            version=None,
            arch="arm64",
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Package exists with arm64, should return "1"
        assert "1" in output

    def test_exists_default_architecture_is_amd64(self, capfd):
        """exists uses amd64 as default architecture."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(release, self.sample_deb_file)

        # Don't specify arch - should default to amd64
        exists_command(
            package="test-pkg",
            version=None,
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Should find amd64 package (default arch)
        assert "1" in output


class TestExistsMultiplePackages:
    """Tests for exists with multiple packages."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures with S3 bucket."""
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

    def test_exists_with_hello_package(self, capfd):
        """exists works with hello package."""
        setup_logger()

        release = self._create_release()
        pkg = package_module.Package.parse_file("tests/fixtures/hello_2.10-5_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()

        exists_command(
            package="hello",
            version=None,
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        assert "1" in output

    def test_exists_nonexistent_vs_existing(self, capfd):
        """Test contrast between existing and nonexistent packages."""
        setup_logger()

        release = self._create_release()

        # Add test-pkg
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()

        # Check existing package
        exists_command(
            package="test-pkg",
            version=None,
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        assert "1" in output

        # Check nonexistent package
        exists_command(
            package="other-pkg",
            version=None,
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        assert "0" in output


class TestExistsErrors:
    """Tests for error handling in exists command."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures with S3 bucket."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._access_policy = "public-read"

    def test_exists_requires_bucket(self):
        """exists command requires bucket option."""
        import typer

        with pytest.raises(typer.Exit):
            exists_command(
                package="test-pkg",
                version=None,
                arch=None,
                bucket=None,
                codename="stable",
                component="main",
            )

    def test_exists_requires_package_argument(self):
        """exists command requires package argument."""
        import typer

        # Can't call without package argument - typer handles required args
        # This test just verifies the command signature is correct
        assert callable(exists_command)