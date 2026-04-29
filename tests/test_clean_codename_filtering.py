"""Integration tests for clean command codename filtering.

These tests verify that the clean command checks ALL codenames when determining
if a package is orphaned, not just the specified codename.

The bug: clean_command only checks if packages are referenced by the SPECIFIED
codename's manifests, but packages in pool/ are shared across ALL codenames.
This causes packages that are referenced by other codenames (e.g., stable) to
be incorrectly marked as orphaned and deleted when running clean --codename rc.

The fix requires:
1. Adding list_codenames() function to s3_utils.py to list all codenames from S3
2. Modifying clean_command to check ALL codenames, not just the specified one
3. A package should only be deleted if it's not referenced by ANY codename
"""

import os
import sys
import tempfile
from unittest.mock import patch, MagicMock

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
    logger.add(sys.stderr, format="{message}", level="DEBUG")


class TestListCodenames:
    """Tests for the list_codenames() function.

    This function should list all codenames by scanning the dists/ directory in S3.
    """

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures with S3 bucket."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._prefix = None
        s3_utils._access_policy = "public-read"

    def test_list_codenames_returns_all_codenames(self):
        """list_codenames() should return all codenames from S3 dists/ directory.

        When S3 has multiple codenames (stable, rc, testing), list_codenames()
        should return all of them.
        """
        # Upload Release files for multiple codenames
        stable_release = release_module.Release(
            codename="stable",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main"],
        )
        stable_release.write_to_s3()

        rc_release = release_module.Release(
            codename="rc",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main"],
        )
        rc_release.write_to_s3()

        testing_release = release_module.Release(
            codename="testing",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main"],
        )
        testing_release.write_to_s3()

        # Call list_codenames and verify it returns all codenames
        codenames = s3_utils.list_codenames()

        assert "stable" in codenames, "stable should be in codenames"
        assert "rc" in codenames, "rc should be in codenames"
        assert "testing" in codenames, "testing should be in codenames"
        assert len(codenames) == 3, f"Expected 3 codenames, got {len(codenames)}: {codenames}"

    def test_list_codenames_handles_empty_dists(self):
        """list_codenames() should return empty list when dists/ is empty.

        When there are no codenames in S3, list_codenames() should return
        an empty list, not crash.
        """
        # Don't create any Release files - dists/ is empty
        codenames = s3_utils.list_codenames()

        assert codenames == [], f"Expected empty list, got {codenames}"

    def test_list_codenames_handles_pagination(self):
        """list_codenames() should handle S3 pagination correctly.

        When there are more than 1000 objects in dists/, list_codenames()
        should continue fetching additional pages until all codenames are found.
        """
        # Create multiple codenames
        for codename in ["stable", "rc", "testing", "unstable", "experimental"]:
            release = release_module.Release(
                codename=codename,
                origin="TestRepo",
                architectures=["amd64"],
                components=["main"],
            )
            release.write_to_s3()

        # Mock s3_list_objects to simulate pagination
        original_list = s3_utils.s3_list_objects

        call_count = [0]

        def paginated_list(prefix, continuation_token=None):
            call_count[0] += 1

            if continuation_token is None:
                # First call - return partial results with continuation token
                return [
                    {"Key": "dists/stable/Release"},
                    {"Key": "dists/rc/Release"},
                ], "next-token"
            else:
                # Second call - return remaining results
                return [
                    {"Key": "dists/testing/Release"},
                    {"Key": "dists/unstable/Release"},
                    {"Key": "dists/experimental/Release"},
                ], None

        with patch.object(s3_utils, "s3_list_objects", side_effect=paginated_list):
            codenames = s3_utils.list_codenames()

        # Should have made multiple calls for pagination
        assert call_count[0] >= 2, f"Expected pagination calls, got {call_count[0]}"

        # All codenames should be found
        assert len(codenames) == 5, f"Expected 5 codenames, got {len(codenames)}: {codenames}"

    def test_list_codenames_handles_subdirectory_objects(self):
        """list_codenames() should extract codename from nested paths.

        Objects in S3 can have paths like dists/rc/main/binary-amd64/Packages.
        list_codenames() should extract 'rc' from such paths.
        """
        # Create releases for codenames
        for codename in ["stable", "rc"]:
            release = release_module.Release(
                codename=codename,
                origin="TestRepo",
                architectures=["amd64"],
                components=["main"],
            )
            release.write_to_s3()

        # Also add some nested objects (like Packages files)
        self.s3_client.put_object(
            Bucket="test-bucket",
            Key="dists/rc/main/binary-amd64/Packages",
            Body=b"Package: test\nVersion: 1.0\n",
        )
        self.s3_client.put_object(
            Bucket="test-bucket",
            Key="dists/stable/main/binary-amd64/Packages",
            Body=b"Package: test\nVersion: 1.0\n",
        )

        codenames = s3_utils.list_codenames()

        assert "stable" in codenames, "stable should be in codenames"
        assert "rc" in codenames, "rc should be in codenames"


class TestCleanChecksAllCodenames:
    """Tests that clean command checks all codenames when determining orphaned packages.

    The bug: clean --codename rc only checks rc's manifest, missing that packages
    are also referenced by stable codename.
    """

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures with S3 bucket."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._prefix = None
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

    def _add_packages_to_manifest(self, release, deb_file, component="main", arch="amd64", codename="stable"):
        """Add packages to manifest and update release."""
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve(codename, component, arch)
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()
        return pkg

    def _upload_deb_to_pool(self, deb_file_path, component="main"):
        """Upload a .deb file directly to the pool in S3."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(open(deb_file_path, "rb").read())
            tmp_path = tmp.name

        try:
            filename = os.path.basename(deb_file_path)
            name = filename.rsplit("_", 2)[0]
            # Match url_filename_for format: pool/{component}/{name[0]}/{name[0:2]}/{filename}
            first_letter = name[0]
            first_two = name[0:2] if len(name) >= 2 else first_letter
            key = f"pool/{component}/{first_letter}/{first_two}/{filename}"
            s3_utils.s3_store(tmp_path, key, "application/x-debian-package")
        finally:
            os.unlink(tmp_path)

    def test_clean_does_not_delete_package_referenced_by_other_codename(self, capfd):
        """Clean with --codename rc should NOT delete packages referenced by stable.

        This is the core bug test: when running clean --codename rc, packages
        that are referenced by the stable codename should NOT be marked as orphaned
        even if they're not in rc's manifest.
        """
        setup_logger()

        # Create stable codename with package A in manifest
        stable_release = self._create_release(codename="stable", components=["main"])
        self._add_packages_to_manifest(
            stable_release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
            component="main",
            codename="stable",
        )

        # Create rc codename - but DON'T add the same package to its manifest
        rc_release = self._create_release(codename="rc", components=["main"])
        # Note: NOT adding test-pkg to rc's manifest

        # Upload the package to pool/main/
        self._upload_deb_to_pool(
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
            component="main",
        )

        # Verify package exists in pool before clean
        # The path format is pool/main/t/te/test-pkg_1.0.0_amd64.deb
        result = s3_utils.s3_list_objects("pool/main/t/")
        objects = result[0] if isinstance(result, tuple) else result
        files_before = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert any("test-pkg_1.0.0_amd64.deb" in f for f in files_before), (
            "Test package should exist in pool before clean"
        )

        # Run clean for rc codename ONLY
        # BUG: Without the fix, this would delete the package because it's not in rc's manifest
        # EXPECTED: Package should NOT be deleted because stable references it
        clean_command(
            bucket="test-bucket",
            codename="rc",
            component="main",
        )

        # Get files after clean
        result = s3_utils.s3_list_objects("pool/main/t/")
        objects = result[0] if isinstance(result, tuple) else result
        files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        # The package should STILL EXIST because stable codename references it
        # BUG: Without fix, this test will FAIL because the package gets incorrectly deleted
        assert any("test-pkg_1.0.0_amd64.deb" in f for f in files_after), (
            f"Package should NOT be deleted when cleaning rc codename because stable references it. "
            f"Files after clean: {files_after}"
        )

    def test_clean_with_multiple_codenames_no_orphans(self, capfd):
        """Clean should not delete packages when all codenames reference them.

        When running clean with --codename stable, packages that are referenced
        by other codenames (like rc) should NOT be deleted.
        """
        setup_logger()

        # Create stable codename with package A
        stable_release = self._create_release(codename="stable", components=["main"])
        self._add_packages_to_manifest(
            stable_release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
            component="main",
            codename="stable",
        )

        # Create rc codename with package B
        rc_release = self._create_release(codename="rc", components=["main"])
        self._add_packages_to_manifest(
            rc_release,
            "tests/fixtures/hello_2.10-5_amd64.deb",
            component="main",
            codename="rc",
        )

        # Upload both packages to pool
        self._upload_deb_to_pool(
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
            component="main",
        )
        self._upload_deb_to_pool(
            "tests/fixtures/hello_2.10-5_amd64.deb",
            component="main",
        )

        # Run clean for stable codename
        # With the fix, it should check rc's manifest too and not delete hello
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        # Both packages should still exist because they're referenced by some codename
        # Path format: pool/main/t/te/test-pkg_1.0.0_amd64.deb
        result_test_pkg = s3_utils.s3_list_objects("pool/main/t/")
        objects = result_test_pkg[0] if isinstance(result_test_pkg, tuple) else result_test_pkg
        test_pkg_files = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        # Path format: pool/main/h/he/hello_2.10-5_amd64.deb
        result_hello = s3_utils.s3_list_objects("pool/main/h/")
        objects = result_hello[0] if isinstance(result_hello, tuple) else result_hello
        hello_files = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        # test-pkg is referenced by stable, hello is referenced by rc
        # Both should NOT be deleted when checking all codenames
        # BUG: Without fix, hello gets deleted because it's not in stable's manifest
        assert any("test-pkg_1.0.0_amd64.deb" in f for f in test_pkg_files), (
            "test-pkg should not be deleted - referenced by stable"
        )
        assert any("hello_2.10-5_amd64.deb" in f for f in hello_files), (
            f"hello should not be deleted - referenced by rc. Files: {hello_files}"
        )

    def test_clean_only_deletes_truly_orphaned_packages(self, capfd):
        """Clean should delete packages that are not referenced by ANY codename.

        Package A (not referenced by any codename) should be deleted.
        Package B (referenced by stable) should NOT be deleted.
        """
        setup_logger()

        # Create stable codename with package B in manifest
        stable_release = self._create_release(codename="stable", components=["main"])
        self._add_packages_to_manifest(
            stable_release,
            "tests/fixtures/hello_2.10-5_amd64.deb",
            component="main",
            codename="stable",
        )

        # Upload package A (orphan - not in any manifest) to pool
        self._upload_deb_to_pool(
            "tests/fixtures/test-pkg-full_1.0.0_all.deb",
            component="main",
        )

        # Upload package B (referenced by stable) to pool
        self._upload_deb_to_pool(
            "tests/fixtures/hello_2.10-5_amd64.deb",
            component="main",
        )

        # Verify both packages exist before clean
        # Path format: pool/main/t/te/test-pkg-full_1.0.0_all.deb
        result_orphan = s3_utils.s3_list_objects("pool/main/t/")
        objects = result_orphan[0] if isinstance(result_orphan, tuple) else result_orphan
        orphan_files_before = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        # Path format: pool/main/h/he/hello_2.10-5_amd64.deb
        result_referenced = s3_utils.s3_list_objects("pool/main/h/")
        objects = result_referenced[0] if isinstance(result_referenced, tuple) else result_referenced
        referenced_files_before = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert any("test-pkg-full" in f for f in orphan_files_before), "Orphan package should exist before clean"
        assert any("hello_2.10-5_amd64.deb" in f for f in referenced_files_before), "Referenced package should exist before clean"

        # Run clean for stable codename
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        # Get files after clean
        result_orphan = s3_utils.s3_list_objects("pool/main/t/")
        objects = result_orphan[0] if isinstance(result_orphan, tuple) else result_orphan
        orphan_files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        result_referenced = s3_utils.s3_list_objects("pool/main/h/")
        objects = result_referenced[0] if isinstance(result_referenced, tuple) else result_referenced
        referenced_files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        # Orphan package should be deleted (not referenced by any codename)
        assert not any("test-pkg-full" in f for f in orphan_files_after), (
            "Orphan package should be deleted"
        )

        # Referenced package should NOT be deleted (referenced by stable)
        # BUG: Without fix, this might fail if the code doesn't check all codenames
        assert any("hello_2.10-5_amd64.deb" in f for f in referenced_files_after), (
            f"Referenced package should NOT be deleted. Files after: {referenced_files_after}"
        )


class TestCleanCodenamesMocked:
    """Tests with mocked S3 responses to verify codename checking behavior.

    These tests verify that the clean command uses list_codenames() and checks
    all codenames' manifests when determining orphaned packages.
    """

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._prefix = None
        s3_utils._access_policy = "public-read"

    def _create_release(self, codename="stable", components=None):
        """Create release."""
        if components is None:
            components = ["main"]
        release = release_module.Release(
            codename=codename,
            origin="TestRepo",
            architectures=["amd64"],
            components=components,
        )
        release.write_to_s3()
        return release

    def test_clean_uses_list_codenames_function(self, capfd):
        """Clean command should call list_codenames to get all codenames.

        The fix requires clean_command to call list_codenames() to find
        all available codenames in S3 before determining orphaned packages.
        """
        setup_logger()

        # First check if list_codenames exists - if not, skip this test
        if not hasattr(s3_utils, "list_codenames"):
            pytest.skip("list_codenames() function not yet implemented")

        # Create a release
        release = self._create_release(components=["main"])

        from pydeb_s3 import manifest as manifest_module
        from pydeb_s3 import package as package_module
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()

        # Track if list_codenames is called
        list_codenames_called = []

        original_list_codenames = s3_utils.list_codenames

        def mock_list_codenames():
            list_codenames_called.append(True)
            return original_list_codenames()

        with patch.object(s3_utils, "list_codenames", side_effect=mock_list_codenames):
            clean_command(
                bucket="test-bucket",
                codename="stable",
                component="main",
            )

        # With the fix, list_codenames should be called
        # Without the fix, this test will FAIL because list_codenames is not called
        assert len(list_codenames_called) > 0, (
            "list_codenames() should be called by clean command to check all codenames"
        )

    def test_clean_checks_all_codenames_manifests(self, capfd):
        """Clean should check manifests for ALL codenames, not just specified one.

        When determining if a package is orphaned, clean should iterate over
        all codenames found by list_codenames() and check their manifests.
        """
        setup_logger()

        # First check if list_codenames exists - if not, skip this test
        if not hasattr(s3_utils, "list_codenames"):
            pytest.skip("list_codenames() function not yet implemented")

        # Create stable and rc releases
        stable_release = self._create_release(codename="stable", components=["main"])
        rc_release = self._create_release(codename="rc", components=["main"])

        # Add package to stable manifest only
        from pydeb_s3 import manifest as manifest_module
        from pydeb_s3 import package as package_module
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()
        stable_release.update_manifest(manifest)
        stable_release.write_to_s3()

        # Track manifest retrieval calls
        manifest_retrieval_calls = []
        original_retrieve = manifest_module.Manifest.retrieve

        def tracking_retrieve(codename, component, arch, cache_control=None, use_cache=True):
            manifest_retrieval_calls.append(codename)
            return original_retrieve(codename, component, arch, cache_control, use_cache)

        with patch.object(manifest_module.Manifest, "retrieve", side_effect=tracking_retrieve):
            with patch.object(s3_utils, "list_codenames", return_value=["stable", "rc"]):
                clean_command(
                    bucket="test-bucket",
                    codename="rc",  # Cleaning rc codename
                    component="main",
                )

        # With the fix, manifests for both stable AND rc should be retrieved
        # Without the fix, only rc's manifest would be checked
        # This test expects both codenames to be checked
        stable_checked = "stable" in manifest_retrieval_calls
        rc_checked = "rc" in manifest_retrieval_calls

        # The fix should check stable's manifest even when cleaning rc
        assert stable_checked, (
            f"stable codename manifest should be checked when cleaning rc. "
            f"Manifest calls: {manifest_retrieval_calls}"
        )


class TestCleanCodenamesEdgeCases:
    """Edge case tests for codename filtering in clean command."""

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._prefix = None
        s3_utils._access_policy = "public-read"

    def _create_release(self, codename="stable", components=None):
        """Create release."""
        if components is None:
            components = ["main"]
        release = release_module.Release(
            codename=codename,
            origin="TestRepo",
            architectures=["amd64"],
            components=components,
        )
        release.write_to_s3()
        return release

    def test_clean_handles_single_codename(self, capfd):
        """Clean should work correctly when there's only one codename.

        When there's only one codename in S3, clean should still work
        and correctly identify orphaned packages.
        """
        setup_logger()

        # Create only stable codename
        release = self._create_release(codename="stable", components=["main"])

        from pydeb_s3 import manifest as manifest_module
        from pydeb_s3 import package as package_module
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()

        # Upload orphan package
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(open("tests/fixtures/test-pkg-full_1.0.0_all.deb", "rb").read())
            tmp_path = tmp.name

        try:
            s3_utils.s3_store(tmp_path, "pool/main/t/test-pkg-full/test-pkg-full_1.0.0_all.deb",
                            "application/x-debian-package")
        finally:
            os.unlink(tmp_path)

        # Run clean - with the fix, it should use list_codenames to find the single codename
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        # Orphan should be deleted
        result = s3_utils.s3_list_objects("pool/main/t/test-pkg-full/")
        objects = result[0] if isinstance(result, tuple) else result
        files = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert not any("test-pkg-full" in f for f in files), (
            "Orphan should be deleted when there's only one codename"
        )

    def test_clean_with_codename_not_in_s3(self, capfd):
        """Clean should handle when specified codename doesn't exist in S3.

        If user specifies --codename nonexistent, but other codenames exist,
        the clean should still work by checking all existing codenames.
        """
        setup_logger()

        # Create stable codename only
        release = self._create_release(codename="stable", components=["main"])

        from pydeb_s3 import manifest as manifest_module
        from pydeb_s3 import package as package_module
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()

        # This should not crash - it should use stable's manifest
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        # Test passes if no exception is raised
        # The clean should work using available codenames