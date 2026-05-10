"""Integration tests for the show command."""

import sys

import pytest

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3.s3_adapter import S3Adapter
from pydeb_s3.cli import show_command


def setup_logger():
    """Configure loguru to output to captured stderr."""
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, format="{message}")


class TestShowIntegration:
    """Integration tests for show command using mocked S3."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter, sample_deb_file):
        """Set up test fixtures with S3 bucket and configuration.

        Uses moto_s3_adapter since these tests call show_command()
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

    def test_show_displays_package_details(self, capfd):
        """show displays package details when no version specified."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(release, self.sample_deb_file)

        show_command(
            package="test-pkg",
            version=None,
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Should output some package info (not empty)
        assert output.strip() != "", f"Expected package info in output, got: {output}"

    def test_show_with_package_name(self, capfd):
        """show outputs package name in description."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(release, self.sample_deb_file)

        show_command(
            package="test-pkg",
            version=None,
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Should contain package name
        assert "test-pkg" in output

    def test_show_with_version_filter(self, capfd):
        """show displays specific version when requested."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(release, self.sample_deb_file)

        show_command(
            package="test-pkg",
            version="1.0.0",
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Should output version info (not error)
        assert "test-pkg" in output

    def test_show_with_nonexistent_version(self, capfd):
        """show returns error for nonexistent version."""
        import typer

        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(release, self.sample_deb_file)

        with pytest.raises(typer.Exit):
            show_command(
                package="test-pkg",
                version="999.0.0",
                arch=None,
                bucket="test-bucket",
                codename="stable",
                component="main",
            )

    def test_show_with_architecture_filter(self, capfd):
        """show works with explicit architecture."""
        setup_logger()

        release = self._create_release(architectures=["amd64", "arm64"])
        self._add_packages_to_manifest(release, self.sample_deb_file, arch="amd64")

        show_command(
            package="test-pkg",
            version=None,
            arch="amd64",
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        assert "test-pkg" in output

    def test_show_with_different_architecture(self, capfd):
        """show returns error for package not in requested arch."""
        import typer

        setup_logger()

        release = self._create_release(architectures=["amd64"])
        self._add_packages_to_manifest(release, self.sample_deb_file, arch="amd64")

        with pytest.raises(typer.Exit):
            show_command(
                package="test-pkg",
                version=None,
                arch="arm64",
                bucket="test-bucket",
                codename="stable",
                component="main",
            )


class TestShowErrors:
    """Tests for error handling in show command."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter):
        """Set up test fixtures with S3 bucket.

        Uses moto_s3_adapter since these tests may call show_command().
        """
        self.s3_adapter = moto_s3_adapter

    def test_show_requires_bucket(self):
        """show command requires bucket option."""
        import typer

        with pytest.raises(typer.Exit):
            show_command(
                package="test-pkg",
                version=None,
                arch=None,
                bucket=None,
                codename="stable",
                component="main",
            )

    def test_show_returns_error_for_nonexistent_package(self, capfd):
        """show returns error when package not found."""
        import typer

        # Create release file (but don't add any packages)
        release = release_module.Release(
            codename="stable",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main"],
        )
        release.write_to_s3(self.s3_adapter)

        setup_logger()

        with pytest.raises(typer.Exit):
            show_command(
                package="nonexistent-package",
                version=None,
                arch=None,
                bucket="test-bucket",
                codename="stable",
                component="main",
            )


class TestShowQuietOutput:
    """Tests for show command output to stdout with --quiet flag."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter, sample_deb_file):
        """Set up test fixtures with S3 bucket and configuration.

        Uses moto_s3_adapter since these tests call show_command().
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

    def test_show_outputs_to_stdout_not_stderr(self, capfd):
        """show command output should go to stdout, not stderr."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(release, self.sample_deb_file)

        # Clear any setup output before running the command
        capfd.readouterr()

        show_command(
            package="test-pkg",
            version=None,
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        # Output should go to stdout
        assert "test-pkg" in captured.out
        # stderr should not contain the package info
        assert "test-pkg" not in captured.err

    def test_show_with_quiet_flag_outputs_nothing(self, capfd):
        """show command with --quiet should output nothing."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(release, self.sample_deb_file)

        # Clear any setup output before running the command
        capfd.readouterr()

        # Call the show command with quiet flag
        show_command(
            package="test-pkg",
            version=None,
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
            quiet=True,
        )

        captured = capfd.readouterr()
        # With --quiet, there should be no user-facing output
        assert "test-pkg" not in captured.out
        assert "1.0.0" not in captured.out

    def test_show_version_only_to_stdout(self, capfd):
        """show with version output should go to stdout."""
        setup_logger()

        release = self._create_release()
        self._add_packages_to_manifest(release, self.sample_deb_file)

        # Clear any setup output before running the command
        capfd.readouterr()

        show_command(
            package="test-pkg",
            version="1.0.0",
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        # Version output should go to stdout (not stderr)
        assert "1.0.0" in captured.out


class TestShowMultiplePackages:
    """Tests for show with multiple packages."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter):
        """Set up test fixtures with S3 bucket.

        Uses moto_s3_adapter since these tests call show_command().
        """
        self.s3_adapter = moto_s3_adapter

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

    def test_show_with_hello_package(self, capfd):
        """show works with hello package."""
        setup_logger()

        release = self._create_release()
        pkg = package_module.Package.parse_file("tests/fixtures/hello_2.10-5_amd64.deb")
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)

        show_command(
            package="hello",
            version=None,
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        assert "hello" in output.lower()

    def test_show_with_hello_version(self, capfd):
        """show displays hello package with specific version."""
        setup_logger()

        release = self._create_release()
        pkg = package_module.Package.parse_file("tests/fixtures/hello_2.10-5_amd64.deb")
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)

        show_command(
            package="hello",
            version="2.10-5",
            arch=None,
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Should output version info
        assert "hello" in output.lower() or "2.10" in output
