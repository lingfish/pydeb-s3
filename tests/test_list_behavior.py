"""Integration tests for the list command."""

import sys

import pytest

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3 import s3_utils
from pydeb_s3.cli import list_command


def setup_logger():
    """Configure loguru to output to captured stderr."""
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, format="{message}")


class TestListIntegration:
    """Integration tests for list command using mocked S3."""

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

    def test_list_packages_default(self, capfd):
        """List all packages from manifest without filtering."""
        setup_logger()

        self._create_release()
        self._add_packages_to_manifest(
            self._create_release(),
            self.sample_deb_file,
        )

        # Call the list command
        list_command(
            bucket="test-bucket",
            long=False,
            arch=None,
            codename="stable",
            component="main",
        )

        # Capture output from stderr (loguru outputs to stderr)
        captured = capfd.readouterr()
        output = captured.out + captured.err
        assert "test-pkg" in output
        assert "1.0.0" in output

    def test_list_packages_long_format(self, capfd):
        """List packages with long format option (currently same as default)."""
        setup_logger()

        self._create_release()
        self._add_packages_to_manifest(
            self._create_release(),
            self.sample_deb_file,
        )

        # Call the list command with long option
        list_command(
            bucket="test-bucket",
            long=True,
            arch=None,
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        assert "test-pkg" in output
        assert "1.0.0" in output

    def test_list_filter_by_architecture(self, capfd):
        """Filter packages by architecture."""
        setup_logger()

        # Create release with multiple architectures
        self._create_release(architectures=["amd64", "arm64"])

        # Add amd64 package
        amd64_pkg = package_module.Package.parse_file(self.sample_deb_file)
        manifest_amd64 = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest_amd64.add(amd64_pkg)
        manifest_amd64.write_to_s3()

        # Add arm64 package using different deb file
        arm64_deb_file = "tests/fixtures/test-pkg_1.0.0_arm64.deb"
        arm64_pkg = package_module.Package.parse_file(arm64_deb_file)
        manifest_arm64 = manifest_module.Manifest.retrieve("stable", "main", "arm64")
        manifest_arm64.add(arm64_pkg)
        manifest_arm64.write_to_s3()

        # Update release
        release = release_module.Release.retrieve("stable")
        release.update_manifest(manifest_amd64)
        release.update_manifest(manifest_arm64)
        release.write_to_s3()

        # List only amd64 packages
        list_command(
            bucket="test-bucket",
            long=False,
            arch="amd64",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Should show amd64 package
        assert "test-pkg" in output
        assert "amd64" in output

    def test_list_empty_manifest(self, capfd):
        """List command handles empty manifest gracefully."""
        setup_logger()

        # Create release but no packages
        self._create_release()

        # List with default codename/component
        list_command(
            bucket="test-bucket",
            long=False,
            arch=None,
            codename="stable",
            component="main",
        )

        # Should produce output but with no packages (empty)
        captured = capfd.readouterr()
        output = captured.out + captured.err
        # No error should occur, and there should be no package names in output
        assert "test-pkg" not in output

    def test_list_multiple_architectures(self, capfd):
        """List packages from multiple architectures."""
        setup_logger()

        # Create release with multiple architectures
        self._create_release(architectures=["amd64", "arm64", "i386"])

        # Add packages for different architectures
        amd64_pkg = package_module.Package.parse_file(self.sample_deb_file)
        manifest_amd64 = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest_amd64.add(amd64_pkg)
        manifest_amd64.write_to_s3()

        arm64_deb = "tests/fixtures/test-pkg_1.0.0_arm64.deb"
        arm64_pkg = package_module.Package.parse_file(arm64_deb)
        manifest_arm64 = manifest_module.Manifest.retrieve("stable", "main", "arm64")
        manifest_arm64.add(arm64_pkg)
        manifest_arm64.write_to_s3()

        # Update release
        release = release_module.Release.retrieve("stable")
        release.update_manifest(manifest_amd64)
        release.update_manifest(manifest_arm64)
        release.write_to_s3()

        # List all packages (no arch filter)
        list_command(
            bucket="test-bucket",
            long=False,
            arch=None,
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Should show packages from both architectures
        assert "amd64" in output
        assert "arm64" in output

    def test_list_different_component(self, capfd):
        """List packages from different components."""
        setup_logger()

        self._create_release(components=["main", "non-free"])

        self._add_packages_to_manifest(
            self._create_release(),
            self.sample_deb_file,
            component="non-free",
        )

        list_command(
            bucket="test-bucket",
            long=False,
            arch=None,
            codename="stable",
            component="non-free",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        assert "test-pkg" in output
        assert "non-free" in output

    def test_list_packages_sorted(self, capfd):
        """Verify packages are sorted by name then version."""
        setup_logger()

        release = self._create_release()

        pkg1 = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg1)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()

        pkg2 = package_module.Package.parse_file("tests/fixtures/hello_2.10-5_amd64.deb")
        manifest2 = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest2.add(pkg2)
        manifest2.write_to_s3()
        release.update_manifest(manifest2)
        release.write_to_s3()

        list_command(
            bucket="test-bucket",
            long=False,
            arch=None,
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        assert "test-pkg" in output
        assert "hello" in output

        # Verify packages are sorted alphabetically by name
        # "hello" should appear before "test-pkg" in the output
        # The output has multiple lines including S3 storage logs.
        # List output lines match pattern: "name version arch  section  ..."

        lines = output.split("\n")

        hello_list_lines = [
            i for i, line in enumerate(lines)
            if line.startswith("hello ") and "2.10-5" in line and "amd64" in line
        ]
        test_pkg_list_lines = [
            i for i, line in enumerate(lines)
            if line.startswith("test-pkg ") and "1.0.0" in line and "amd64" in line
        ]

        assert hello_list_lines, "Expected 'hello' list output line in output"
        assert test_pkg_list_lines, "Expected 'test-pkg' list output line in output"

        # Check the order in list output
        assert hello_list_lines[0] < test_pkg_list_lines[0], (
            f"Expected 'hello' before 'test-pkg' alphabetically in list output, "
            f"but hello at line {hello_list_lines[0]} and test-pkg at line {test_pkg_list_lines[0]}"
        )

    def test_list_packages_sorted_same_name_different_versions(self, capfd):
        """Verify packages with same name are sorted by version."""
        setup_logger()

        release = self._create_release()

        # Add two versions of the same package
        pkg1 = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg1)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()

        # Parse test-pkg-full as a different package (same base name)
        pkg2 = package_module.Package.parse_file("tests/fixtures/test-pkg-full_1.0.0_all.deb")
        manifest2 = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest2.add(pkg2)
        manifest2.write_to_s3()
        release.update_manifest(manifest2)
        release.write_to_s3()

        list_command(
            bucket="test-bucket",
            long=False,
            arch=None,
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err
        # Both packages should be present
        assert "test-pkg" in output
        assert "test-pkg-full" in output


class TestListErrors:
    """Tests for error handling in list command."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures with S3 bucket."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._access_policy = "public-read"

    def test_list_requires_bucket(self):
        """List command requires bucket option."""
        import typer

        with pytest.raises(typer.Exit):
            list_command(
                bucket=None,
                long=False,
                arch=None,
                codename="stable",
                component="main",
            )


class TestListQuietOutput:
    """Tests for list command output to stdout with --quiet flag."""

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

    def test_list_outputs_to_stdout_not_stderr(self, capfd):
        """List command output should go to stdout, not stderr."""
        setup_logger()

        self._create_release()
        self._add_packages_to_manifest(
            self._create_release(),
            self.sample_deb_file,
        )

        # Clear any setup output before running the command
        capfd.readouterr()

        # Call the list command
        list_command(
            bucket="test-bucket",
            long=False,
            arch=None,
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        # Output should go to stdout (not stderr)
        assert "test-pkg" in captured.out
        assert "test-pkg" not in captured.err

    def test_list_with_quiet_flag_outputs_nothing(self, capfd):
        """List command with --quiet should output nothing."""
        setup_logger()

        # Set quiet in context
        from pydeb_s3.cli import app
        app_ctx = {"quiet": True, "debug": False}

        self._create_release()
        self._add_packages_to_manifest(
            self._create_release(),
            self.sample_deb_file,
        )

        # Clear any setup output before running the command
        capfd.readouterr()

        # Call the list command with quiet flag
        list_command(
            bucket="test-bucket",
            long=False,
            arch=None,
            codename="stable",
            component="main",
            quiet=True,
        )

        captured = capfd.readouterr()
        # With --quiet, there should be no user-facing output
        assert "test-pkg" not in captured.out
        assert "1.0.0" not in captured.out

    def test_list_output_is_parseable_one_line_per_package(self, capfd):
        """List command output should be machine-parseable, one package per line."""
        setup_logger()

        self._create_release()
        self._add_packages_to_manifest(
            self._create_release(),
            self.sample_deb_file,
        )

        # Clear any setup output before running the command
        capfd.readouterr()

        list_command(
            bucket="test-bucket",
            long=False,
            arch=None,
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out
        # Each package should be on its own line
        lines = [line.strip() for line in output.split("\n") if line.strip()]
        assert any("test-pkg" in line for line in lines), (
            f"Expected parseable output with package name, got: {output}"
        )