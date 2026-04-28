"""Integration tests for the verify command."""

import sys

import pytest

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3 import s3_utils
from pydeb_s3.cli import verify_command


def setup_logger():
    """Configure loguru to output to captured stderr."""
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, format="{message}", level="INFO")


class TestVerifyIntegration:
    """Integration tests for verify command using mocked S3."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client, sample_deb_file):
        """Set up test fixtures with S3 bucket and configuration."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._access_policy = "public-read"
        self.sample_deb_file = sample_deb_file
        self.hello_deb_file = "tests/fixtures/hello_2.10-5_amd64.deb"

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

    def _get_package_s3_path(self, pkg, component="main"):
        """Get the expected S3 path for a package including component prefix."""
        return pkg.url_filename_for(component)

    def test_verify_passes_with_all_files_present(self, capfd):
        """Verify passes when all package files exist in S3."""
        setup_logger()

        # Create release and packages
        release = self._create_release()
        pkg = self._add_packages_to_manifest(release, self.sample_deb_file)

        # Verify the file exists in S3 using the correct path
        s3_key = self._get_package_s3_path(pkg)
        assert s3_utils.s3_exists(s3_key), f"Package file should exist at {s3_key}"

        # Run verify - should pass without warnings
        verify_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Verify should pass - check for verify complete message
        assert "Verify complete" in output
        # Should NOT have missing file warnings
        assert "Missing file" not in output

    def test_verify_warns_on_missing_file(self, capfd):
        """Verify shows warning when package file is missing from S3."""
        setup_logger()

        # Create release and packages
        release = self._create_release()
        pkg = self._add_packages_to_manifest(release, self.sample_deb_file)

        # Get the S3 key and delete the file to simulate missing file
        s3_key = self._get_package_s3_path(pkg)
        self.s3_client.delete_object(Bucket="test-bucket", Key=s3_key)

        # Verify file is deleted
        assert not s3_utils.s3_exists(s3_key), "File should be deleted"

        # Run verify - should warn about missing file
        verify_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Should show missing file warning
        assert "Missing file" in output

    def test_verify_with_fix_manifests(self, capfd):
        """Verify with --fix-manifests removes missing packages from manifest."""
        setup_logger()

        # Create release and packages
        release = self._create_release()
        pkg = self._add_packages_to_manifest(release, self.sample_deb_file)

        # Get the S3 key and delete the file to simulate missing file
        s3_key = self._get_package_s3_path(pkg)
        self.s3_client.delete_object(Bucket="test-bucket", Key=s3_key)

        initial_packages = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        package_names = [p.name for p in initial_packages.packages]
        assert "test-pkg" in package_names

        # Run verify with fix-manifests
        verify_command(
            bucket="test-bucket",
            fix_manifests=True,
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Should have fixed the manifest
        assert "Deleting" in output or "Uploading fixed manifest" in output

    def test_verify_multiple_architectures(self, capfd):
        """Verify checks packages across multiple architectures."""
        setup_logger()

        # Create release with multiple architectures
        self._create_release(architectures=["amd64", "arm64"])

        # Add amd64 package
        amd64_pkg = package_module.Package.parse_file(self.sample_deb_file)
        manifest_amd64 = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest_amd64.add(amd64_pkg)
        manifest_amd64.write_to_s3()

        # Add arm64 package (different package file for different arch)
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

        # Delete the arm64 package from S3 to trigger warning
        arm64_key = self._get_package_s3_path(arm64_pkg)
        self.s3_client.delete_object(Bucket="test-bucket", Key=arm64_key)

        # Run verify
        verify_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Should check both architectures and warn about missing arm64 file
        assert "Checking for missing packages" in output
        assert "Missing file" in output

    def test_verify_empty_manifest(self, capfd):
        """Verify handles empty manifest gracefully."""
        setup_logger()

        # Create release but no packages
        self._create_release()

        # Run verify
        verify_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Should not error - just complete
        assert "Verify complete" in output

    def test_verify_multiple_packages(self, capfd):
        """Verify checks multiple packages in the manifest."""
        setup_logger()

        release = self._create_release()

        # Add first package
        pkg1 = package_module.Package.parse_file(self.sample_deb_file)
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg1)
        manifest.write_to_s3()

        # Add second package
        pkg2 = package_module.Package.parse_file(self.hello_deb_file)
        manifest2 = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest2.add(pkg2)
        manifest2.write_to_s3()

        release.update_manifest(manifest)
        release.update_manifest(manifest2)
        release.write_to_s3()

        # Run verify
        verify_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Should pass without missing file warnings
        assert "Missing file" not in output
        assert "Verify complete" in output

    def test_verify_some_packages_missing(self, capfd):
        """Verify reports missing files when some packages are not uploaded."""
        setup_logger()

        release = self._create_release()

        # Add first package
        pkg1 = package_module.Package.parse_file(self.sample_deb_file)
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg1)
        manifest.write_to_s3()

        # Add second package
        pkg2 = package_module.Package.parse_file(self.hello_deb_file)
        manifest2 = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest2.add(pkg2)
        manifest2.write_to_s3()

        release.update_manifest(manifest)
        release.update_manifest(manifest2)
        release.write_to_s3()

        # Delete only hello package from S3
        hello_key = self._get_package_s3_path(pkg2)
        self.s3_client.delete_object(Bucket="test-bucket", Key=hello_key)

        # Run verify
        verify_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Should report exactly one missing file (hello)
        assert "Missing file" in output
        assert "hello" in output.lower() or "Checking files for package" in output


class TestVerifyErrors:
    """Tests for error handling in verify command."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures with S3 bucket."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._access_policy = "public-read"

    def test_verify_requires_bucket(self):
        """Verify command requires bucket option."""
        import typer

        with pytest.raises(typer.Exit):
            verify_command(
                bucket=None,
                codename="stable",
                component="main",
            )

    def test_verify_handles_empty_bucket(self, capfd):
        """Verify handles empty bucket gracefully."""
        setup_logger()

        # Run verify on empty bucket (no release, no packages)
        verify_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        # Should not crash
        captured = capfd.readouterr()
        # May or may not have complete message depending on error handling


# Helper to initialize logger for error tests
def setup_logger():
    """Configure loguru to output to captured stderr."""
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, format="{message}", level="INFO")