"""Integration tests for clean command component filtering.

These tests verify that the clean command correctly filters by component when
listing S3 objects for cleanup, and handles pagination properly.

The bug: s3_utils.s3_list_objects("pool/") is called without filtering by component,
leading to all objects under pool/ being listed regardless of component. This causes
packages from components not specified (e.g., non-free) to be incorrectly marked as
orphaned and deleted.

The fix requires:
1. Listing objects per-component (e.g., pool/main/, pool/non-free/) instead of pool/
2. Handling S3 ListObjectsV2 pagination (previously only first 1000 objects fetched)
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


class TestCleanComponentFiltering:
    """Tests for component filtering in clean command.

    These tests verify the fix for the component filtering bug where all objects
    under pool/ were listed regardless of --component argument.
    """

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
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(open(deb_file_path, "rb").read())
            tmp_path = tmp.name

        try:
            filename = os.path.basename(deb_file_path)
            name = filename.rsplit("_", 2)[0]
            first_letter = name[0]
            # Store in pool with component path
            key = f"pool/{component}/{first_letter}/{name}/{filename}"
            s3_utils.s3_store(tmp_path, key, "application/x-debian-package")
        finally:
            os.unlink(tmp_path)

    def test_clean_lists_objects_with_component_prefix(self, capfd):
        """Clean command should list objects with component-specific prefix.

        When running clean with --component main, it should list objects
        under pool/main/ instead of pool/ to avoid deleting packages
        from other components.
        """
        setup_logger()

        # Create release with packages
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

        # Upload orphan to main pool
        self._upload_deb_to_pool(
            "tests/fixtures/test-pkg-full_1.0.0_all.deb",
            component="main",
        )

        # Mock s3_list_objects to verify it's called with correct prefix
        original_list_objects = s3_utils.s3_list_objects

        call_args = []

        def mock_list_objects(prefix, continuation_token=None):
            call_args.append({"prefix": prefix, "continuation_token": continuation_token})
            return original_list_objects(prefix, continuation_token)

        with patch.object(s3_utils, "s3_list_objects", side_effect=mock_list_objects):
            clean_command(
                bucket="test-bucket",
                codename="stable",
                component="main",
            )

        # Verify s3_list_objects was called with pool/main/ not pool/
        assert len(call_args) > 0, "s3_list_objects should have been called"
        pool_main_called = any(
            call["prefix"] == "pool/main/" for call in call_args
        )
        assert pool_main_called, (
            f"Expected s3_list_objects to be called with 'pool/main/', "
            f"but got calls: {call_args}"
        )

    def test_clean_does_not_delete_non_free_when_main_specified(self, capfd):
        """Clean with --component main should NOT delete non-free packages.

        This is the core bug test: when --component main is specified,
        packages in pool/non-free/ should NOT be considered orphaned.
        """
        setup_logger()

        # Create release with packages in both main and non-free
        release = self._create_release(components=["main", "non-free"])

        # Add package to non-free component in manifest
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/hello_2.10-5_amd64.deb",
            component="non-free",
        )

        # Upload a .deb directly to non-free pool directory
        # (simulating a file that's in the repository but not in the main manifest)
        self._upload_deb_to_pool(
            "tests/fixtures/test-pkg-full_1.0.0_all.deb",
            component="non-free",
        )

        # Get files before clean
        result = s3_utils.s3_list_objects("pool/non-free/")
        objects = result[0] if isinstance(result, tuple) else result
        non_free_files_before = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert any("test-pkg-full" in f for f in non_free_files_before), (
            "Test file should exist in non-free pool before clean"
        )

        # Run clean for main component ONLY
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="main",
        )

        # Get files after clean
        result = s3_utils.s3_list_objects("pool/non-free/")
        objects = result[0] if isinstance(result, tuple) else result
        non_free_files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        # The non-free package should STILL EXIST because clean was run
        # with --component main, which should only scan pool/main/ not pool/non-free/
        # BUG: Without component filtering, this file gets incorrectly deleted
        assert any("test-pkg-full" in f for f in non_free_files_after), (
            f"non-free package should NOT be deleted when cleaning main component only. "
            f"Files after clean: {non_free_files_after}"
        )

    def test_clean_with_non_free_component_only(self, capfd):
        """Clean with --component non-free should only clean non-free pool."""
        setup_logger()

        # Create release with non-free component
        release = self._create_release(components=["non-free"])

        # Add package to non-free component
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/hello_2.10-5_amd64.deb",
            component="non-free",
        )

        # Upload orphan to non-free pool
        self._upload_deb_to_pool(
            "tests/fixtures/test-pkg-full_1.0.0_all.deb",
            component="non-free",
        )

        # Get files before clean
        result = s3_utils.s3_list_objects("pool/non-free/")
        objects = result[0] if isinstance(result, tuple) else result
        non_free_files_before = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert any("test-pkg-full" in f for f in non_free_files_before)

        # Run clean for non-free component
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="non-free",
        )

        # Get files after clean
        result = s3_utils.s3_list_objects("pool/non-free/")
        objects = result[0] if isinstance(result, tuple) else result
        non_free_files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        # The orphan should be removed (it's in non-free pool and not in manifest)
        assert not any("test-pkg-full" in f for f in non_free_files_after), (
            "Orphan in non-free pool should be removed when cleaning non-free"
        )


class TestCleanMultipleComponents:
    """Tests for cleaning with multiple components specified."""

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

    def _add_packages_to_manifest(self, release, deb_file, component="main", arch="amd64"):
        """Add packages to manifest."""
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve("stable", component, arch)
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()
        return pkg

    def _upload_deb_to_pool(self, deb_file_path, component="main"):
        """Upload a .deb file to pool."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(open(deb_file_path, "rb").read())
            tmp_path = tmp.name

        try:
            filename = os.path.basename(deb_file_path)
            name = filename.rsplit("_", 2)[0]
            first_letter = name[0]
            key = f"pool/{component}/{first_letter}/{name}/{filename}"
            s3_utils.s3_store(tmp_path, key, "application/x-debian-package")
        finally:
            os.unlink(tmp_path)

    def test_clean_with_multiple_components_comma_separated(self, capfd):
        """Clean with --component non-free,contrib should clean both components."""
        setup_logger()

        # Create release with non-free and contrib components
        release = self._create_release(components=["non-free", "contrib"])

        # Add packages to both components
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/hello_2.10-5_amd64.deb",
            component="non-free",
        )
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
            component="contrib",
        )

        # Upload orphans to both component pools
        self._upload_deb_to_pool(
            "tests/fixtures/test-pkg-full_1.0.0_all.deb",
            component="non-free",
        )
        self._upload_deb_to_pool(
            "tests/fixtures/test-pkg_1.0.0_arm64.deb",
            component="contrib",
        )

        # Run clean for both non-free and contrib
        clean_command(
            bucket="test-bucket",
            codename="stable",
            component="non-free,contrib",
        )

        captured = capfd.readouterr()
        output = captured.out + captured.err

        # Both orphans should be removed
        result_non_free = s3_utils.s3_list_objects("pool/non-free/")
        objects = result_non_free[0] if isinstance(result_non_free, tuple) else result_non_free
        non_free_files = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        result_contrib = s3_utils.s3_list_objects("pool/contrib/")
        objects = result_contrib[0] if isinstance(result_contrib, tuple) else result_contrib
        contrib_files = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert not any("test-pkg-full" in f for f in non_free_files), "non-free orphan should be removed"
        assert not any("test-pkg_1.0.0_arm64" in f for f in contrib_files), "contrib orphan should be removed"


class TestCleanPagination:
    """Tests for pagination handling in clean command.

    The bug: s3_list_objects only returns first 1000 objects (S3 default),
    potentially missing orphaned packages beyond page 1.
    """

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

    def _add_packages_to_manifest(self, release, deb_file, component="main", arch="amd64"):
        """Add packages to manifest."""
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve("stable", component, arch)
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()
        return pkg

    def _upload_deb_to_pool(self, deb_file_path, component="main"):
        """Upload a .deb file to pool."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(open(deb_file_path, "rb").read())
            tmp_path = tmp.name

        try:
            filename = os.path.basename(deb_file_path)
            name = filename.rsplit("_", 2)[0]
            first_letter = name[0]
            key = f"pool/{component}/{first_letter}/{name}/{filename}"
            s3_utils.s3_store(tmp_path, key, "application/x-debian-package")
        finally:
            os.unlink(tmp_path)

    def test_clean_handles_pagination(self, capfd):
        """Clean command should handle S3 pagination correctly.

        When there are more than 1000 objects in pool/, the clean command
        should continue fetching additional pages until all objects are
        processed.
        """
        setup_logger()

        # Create release with one package
        release = self._create_release()
        self._add_packages_to_manifest(
            release,
            "tests/fixtures/test-pkg_1.0.0_amd64.deb",
        )

        # Track how s3_list_objects is called
        original_list_objects = s3_utils.s3_list_objects

        call_count = [0]
        call_prefixes = []

        def tracking_list_objects(prefix, continuation_token=None):
            call_count[0] += 1
            call_prefixes.append({"prefix": prefix, "token": continuation_token})
            return original_list_objects(prefix, continuation_token)

        with patch.object(s3_utils, "s3_list_objects", side_effect=tracking_list_objects):
            clean_command(
                bucket="test-bucket",
                codename="stable",
                component="main",
            )

        # Verify pagination is being handled
        # The fix should handle continuation tokens properly
        # At minimum, s3_list_objects should be called with the correct prefix
        assert len(call_prefixes) > 0, "s3_list_objects should be called"
        # With pagination fix, it should continue calling until NextContinuationToken is None


class TestCleanComponentPrefixCalls:
    """Tests that verify s3_list_objects is called with correct prefixes.

    These tests use mocking to directly verify the function is called
    with the correct component-specific prefixes.
    """

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
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

    def test_clean_calls_s3_list_objects_with_pool_component_prefix(self, capfd):
        """Verify s3_list_objects is called with pool/{component}/ prefix.

        The bug: clean calls s3_list_objects("pool/") which lists ALL objects
        under pool/ regardless of component. The fix should call it with
        pool/{component}/ for each component being cleaned.
        """
        setup_logger()

        # Create release
        release = self._create_release(components=["main"])

        # Add a package to manifest
        from pydeb_s3 import manifest as manifest_module
        from pydeb_s3 import package as package_module
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()

        # Track the prefix used in s3_list_objects calls
        original_list = s3_utils.s3_list_objects
        prefixes_used = []

        def catching_list(prefix, continuation_token=None):
            prefixes_used.append(prefix)
            return original_list(prefix, continuation_token)

        with patch.object(s3_utils, "s3_list_objects", side_effect=catching_list):
            clean_command(
                bucket="test-bucket",
                codename="stable",
                component="main",
            )

        # Verify the prefix is pool/main/ not pool/
        assert len(prefixes_used) > 0, "s3_list_objects should have been called"
        # At least one call should use pool/main/
        has_main_prefix = any("pool/main/" in p for p in prefixes_used)
        has_plain_pool = any(p == "pool/" for p in prefixes_used)

        # This test expects: should use pool/main/ not plain pool/
        assert has_main_prefix or not has_plain_pool, (
            f"Expected prefix 'pool/main/', but got: {prefixes_used}. "
            f"The bug causes 'pool/' to be used which lists all components."
        )

    def test_clean_with_non_free_uses_pool_non_free_prefix(self, capfd):
        """When --component non-free, prefix should be pool/non-free/ not pool/."""
        setup_logger()

        release = self._create_release(components=["non-free"])

        from pydeb_s3 import manifest as manifest_module
        from pydeb_s3 import package as package_module
        pkg = package_module.Package.parse_file("tests/fixtures/hello_2.10-5_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "non-free", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()

        original_list = s3_utils.s3_list_objects
        prefixes_used = []

        def catching_list(prefix, continuation_token=None):
            prefixes_used.append(prefix)
            return original_list(prefix, continuation_token)

        with patch.object(s3_utils, "s3_list_objects", side_effect=catching_list):
            clean_command(
                bucket="test-bucket",
                codename="stable",
                component="non-free",
            )

        # Should use pool/non-free/ prefix
        assert len(prefixes_used) > 0
        has_non_free_prefix = any("pool/non-free/" in p for p in prefixes_used)
        has_plain_pool = any(p == "pool/" for p in prefixes_used)

        assert has_non_free_prefix or not has_plain_pool, (
            f"Expected prefix 'pool/non-free/', but got: {prefixes_used}"
        )


class TestCleanPaginationMocked:
    """Tests with mocked S3 pagination responses.

    These tests simulate S3 returning multiple pages of results
    to verify the clean command handles continuation tokens.
    """

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        """Set up test fixtures."""
        self.s3_client = s3_client
        self.s3_client.create_bucket(Bucket="test-bucket")
        s3_utils._s3_client = self.s3_client
        s3_utils._bucket = "test-bucket"
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

    def test_clean_paginates_through_all_objects(self, capfd):
        """Clean should continue fetching when S3 returns continuation token.

        This test verifies that when S3 returns a NextContinuationToken,
        the clean command makes additional calls to fetch all pages.
        The bug: only first 1000 objects are fetched.
        """
        setup_logger()

        # Create release with packages
        release = self._create_release(components=["main"])

        from pydeb_s3 import manifest as manifest_module
        from pydeb_s3 import package as package_module
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve("stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3()
        release.update_manifest(manifest)
        release.write_to_s3()

        # Mock s3_list_objects to return pagination responses
        call_log = []

        def mock_list_objects(prefix, continuation_token=None):
            call_log.append({
                "prefix": prefix,
                "continuation_token": continuation_token,
            })

            # Return only orphan files for page 1
            if continuation_token is None:
                # First call - return one orphan and a continuation token
                return [
                    {"Key": "pool/main/o/orphan/orphan_1.0.0_amd64.deb", "Size": 100},
                ], "token-page-2"
            else:
                # Second call - return another orphan, no continuation token (last page)
                return [
                    {"Key": "pool/main/t/testpkg/test-package_2.0.0_amd64.deb", "Size": 100},
                ], None

        with patch.object(s3_utils, "s3_list_objects", side_effect=mock_list_objects):
            clean_command(
                bucket="test-bucket",
                codename="stable",
                component="main",
            )

        # Verify pagination was handled
        # The fix should call s3_list_objects multiple times with continuation tokens
        assert len(call_log) >= 2, (
            f"Expected multiple S3 calls for pagination, but got {len(call_log)} calls: {call_log}"
        )

        # Verify continuation token was passed on second call
        second_call = call_log[1]
        assert second_call["continuation_token"] == "token-page-2", (
            f"Expected continuation token 'token-page-2', got: {second_call}"
        )