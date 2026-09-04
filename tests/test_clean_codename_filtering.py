"""Integration tests for clean command codename filtering.

The --codename flag now actually filters which codename's manifest
is checked when determining orphaned packages:

- When --codename is explicitly passed: only check that codename
- When --codename is NOT passed: check all codenames (safety default)

This allows users to clean a specific codename without affecting
packages referenced by other codenames they don't care about.
"""

import os
import tempfile
from unittest.mock import patch

import pytest

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3 import s3_utils
from pydeb_s3.cli import clean_command


class TestListCodenames:
    """Tests for the list_codenames() function.

    This function should list all codenames by scanning the dists/ directory in S3.
    """

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter):
        """Set up test fixtures with S3 bucket."""
        self.s3_adapter = moto_s3_adapter

    def test_list_codenames_returns_all_codenames(self):
        """list_codenames() should return all codenames from S3 dists/ directory."""
        stable_release = release_module.Release(
            codename="stable", origin="TestRepo", architectures=["amd64"], components=["main"]
        )
        stable_release.write_to_s3(self.s3_adapter)

        rc_release = release_module.Release(
            codename="rc", origin="TestRepo", architectures=["amd64"], components=["main"]
        )
        rc_release.write_to_s3(self.s3_adapter)

        codenames = s3_utils.list_codenames(self.s3_adapter)

        assert "stable" in codenames
        assert "rc" in codenames
        assert len(codenames) == 2

    def test_list_codenames_handles_empty_dists(self):
        """list_codenames() should return empty list when dists/ is empty."""
        codenames = s3_utils.list_codenames(self.s3_adapter)
        assert codenames == []


class TestCleanFiltersByCodenameWhenPassed:
    """Tests that when --codename is explicitly passed, only that codename's manifest is checked."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter):
        """Set up test fixtures with S3 bucket."""
        self.s3_adapter = moto_s3_adapter

    def _create_release(self, codename="stable", architectures=None, components=None):
        """Create and upload a Release file."""
        if architectures is None:
            architectures = ["amd64"]
        if components is None:
            components = ["main"]
        release = release_module.Release(
            codename=codename, origin="TestRepo", architectures=architectures, components=components
        )
        release.write_to_s3(self.s3_adapter)
        return release

    def _add_packages_to_manifest(self, release, deb_file, component="main", arch="amd64", codename="stable"):
        """Add packages to manifest and update release."""
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, codename, component, arch)
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)
        return pkg

    def _upload_deb_to_pool(self, deb_file_path, component="main"):
        """Upload a .deb file directly to the pool in S3."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(open(deb_file_path, "rb").read())
            tmp_path = tmp.name
        try:
            filename = os.path.basename(deb_file_path)
            name = filename.rsplit("_", 2)[0]
            first_letter = name[0]
            first_two = name[0:2] if len(name) >= 2 else first_letter
            key = f"pool/{component}/{first_letter}/{first_two}/{filename}"
            self.s3_adapter.store_file(tmp_path, key, "application/x-debian-package")
        finally:
            os.unlink(tmp_path)

    def test_clean_with_codename_only_checks_specified_codename(self, capfd):
        """When --codename rc is passed, clean should ONLY check rc's manifest.

        Packages referenced by stable but NOT by rc should be deleted.
        """
        # Create stable with package A
        stable_release = self._create_release(codename="stable", components=["main"])
        self._add_packages_to_manifest(
            stable_release, "tests/fixtures/test-pkg_1.0.0_amd64.deb",
            component="main", codename="stable",
        )

        # Create rc WITHOUT package A
        rc_release = self._create_release(codename="rc", components=["main"])

        # Upload package A to pool
        self._upload_deb_to_pool("tests/fixtures/test-pkg_1.0.0_amd64.deb", component="main")

        # Run clean for rc codename ONLY
        clean_command(bucket="test-bucket", codename="rc", component="main")

        # Package A should be DELETED because it's not in rc's manifest
        result = self.s3_adapter.list_objects("pool/main/t/")
        objects = result[0] if isinstance(result, tuple) else result
        files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert not any("test-pkg_1.0.0_amd64.deb" in f for f in files_after), (
            f"Package should be deleted when cleaning rc because it's not in rc's manifest. "
            f"Files after clean: {files_after}"
        )

    def test_clean_with_codename_only_deletes_orphans(self, capfd):
        """When --codename rc is passed, truly orphaned packages should be deleted.

        Even if stable references them, clean with --codename rc won't see stable's manifest.
        """
        # Create rc with one package
        rc_release = self._create_release(codename="rc", components=["main"])
        self._add_packages_to_manifest(
            rc_release, "tests/fixtures/hello_2.10-5_amd64.deb",
            component="main", codename="rc",
        )

        # Upload orphan package not in rc
        self._upload_deb_to_pool("tests/fixtures/test-pkg-full_1.0.0_all.deb", component="main")

        # Run clean for rc
        clean_command(bucket="test-bucket", codename="rc", component="main")

        # Orphan should be deleted
        result = self.s3_adapter.list_objects("pool/main/t/")
        objects = result[0] if isinstance(result, tuple) else result
        files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert not any("test-pkg-full" in f for f in files_after), (
            "Orphan package should be deleted when not in specified codename's manifest"
        )

    def test_clean_with_codename_preserves_referenced_in_that_codename(self, capfd):
        """When --codename rc is passed, packages referenced by rc should NOT be deleted."""
        rc_release = self._create_release(codename="rc", components=["main"])
        self._add_packages_to_manifest(
            rc_release, "tests/fixtures/hello_2.10-5_amd64.deb",
            component="main", codename="rc",
        )

        # Upload package to pool
        self._upload_deb_to_pool("tests/fixtures/hello_2.10-5_amd64.deb", component="main")

        clean_command(bucket="test-bucket", codename="rc", component="main")

        result = self.s3_adapter.list_objects("pool/main/h/")
        objects = result[0] if isinstance(result, tuple) else result
        hello_files = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert any("hello_2.10-5_amd64.deb" in f for f in hello_files), (
            "Package referenced by rc should NOT be deleted"
        )


class TestCleanChecksAllCodenamesByDefault:
    """Tests that when --codename is NOT passed, all codenames are checked (safety default)."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter):
        """Set up test fixtures with S3 bucket."""
        self.s3_adapter = moto_s3_adapter

    def _create_release(self, codename="stable", architectures=None, components=None):
        """Create and upload a Release file."""
        if architectures is None:
            architectures = ["amd64"]
        if components is None:
            components = ["main"]
        release = release_module.Release(
            codename=codename, origin="TestRepo", architectures=architectures, components=components
        )
        release.write_to_s3(self.s3_adapter)
        return release

    def _add_packages_to_manifest(self, release, deb_file, component="main", arch="amd64", codename="stable"):
        """Add packages to manifest and update release."""
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, codename, component, arch)
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)
        return pkg

    def _upload_deb_to_pool(self, deb_file_path, component="main"):
        """Upload a .deb file directly to the pool in S3."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(open(deb_file_path, "rb").read())
            tmp_path = tmp.name
        try:
            filename = os.path.basename(deb_file_path)
            name = filename.rsplit("_", 2)[0]
            first_letter = name[0]
            first_two = name[0:2] if len(name) >= 2 else first_letter
            key = f"pool/{component}/{first_letter}/{first_two}/{filename}"
            self.s3_adapter.store_file(tmp_path, key, "application/x-debian-package")
        finally:
            os.unlink(tmp_path)

    def test_clean_without_codename_checks_all_codenames(self, capfd):
        """When no --codename is passed, clean should check ALL codenames.

        Packages referenced by any codename should NOT be deleted.
        """
        # Create stable with package A
        stable_release = self._create_release(codename="stable", components=["main"])
        self._add_packages_to_manifest(
            stable_release, "tests/fixtures/test-pkg_1.0.0_amd64.deb",
            component="main", codename="stable",
        )

        # Create rc WITHOUT package A
        rc_release = self._create_release(codename="rc", components=["main"])

        # Upload package A to pool
        self._upload_deb_to_pool("tests/fixtures/test-pkg_1.0.0_amd64.deb", component="main")

        # Run clean WITHOUT codename (default behavior - check all codenames)
        clean_command(bucket="test-bucket", component="main")

        # Package A should NOT be deleted because stable references it
        result = self.s3_adapter.list_objects("pool/main/t/")
        objects = result[0] if isinstance(result, tuple) else result
        files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert any("test-pkg_1.0.0_amd64.deb" in f for f in files_after), (
            "Package should NOT be deleted when no --codename passed because stable references it"
        )

    def test_clean_without_codename_deletes_truly_orphaned(self, capfd):
        """When no --codename is passed, truly orphaned packages should be deleted."""
        # Create stable with one package
        stable_release = self._create_release(codename="stable", components=["main"])
        self._add_packages_to_manifest(
            stable_release, "tests/fixtures/hello_2.10-5_amd64.deb",
            component="main", codename="stable",
        )

        # Upload orphan package not in any manifest
        self._upload_deb_to_pool("tests/fixtures/test-pkg-full_1.0.0_all.deb", component="main")

        # Run clean WITHOUT codename
        clean_command(bucket="test-bucket", component="main")

        # Orphan should be deleted (not referenced by any codename)
        result = self.s3_adapter.list_objects("pool/main/t/")
        objects = result[0] if isinstance(result, tuple) else result
        files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert not any("test-pkg-full" in f for f in files_after), (
            "Orphan package should be deleted when not referenced by any codename"
        )


class TestCleanCodenamesMocked:
    """Tests with mocked S3 responses to verify codename checking behavior."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter):
        """Set up test fixtures with S3 bucket."""
        self.s3_adapter = moto_s3_adapter

    def _create_release(self, codename="stable", components=None):
        """Create release."""
        if components is None:
            components = ["main"]
        release = release_module.Release(
            codename=codename, origin="TestRepo", architectures=["amd64"], components=components
        )
        release.write_to_s3(self.s3_adapter)
        return release

    def test_clean_with_codename_does_not_call_list_codenames(self, capfd):
        """When --codename is passed, list_codenames() should NOT be called."""
        if not hasattr(s3_utils, "list_codenames"):
            pytest.skip("list_codenames() function not yet implemented")

        release = self._create_release(components=["main"])

        from pydeb_s3 import manifest as manifest_module
        from pydeb_s3 import package as package_module
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)

        # Track if list_codenames is called
        list_codenames_called = []
        original_list_codenames = s3_utils.list_codenames

        def mock_list_codenames(adapter):
            list_codenames_called.append(True)
            return original_list_codenames(adapter)

        with patch.object(s3_utils, "list_codenames", side_effect=mock_list_codenames):
            clean_command(bucket="test-bucket", codename="stable", component="main")

        # With the fix, list_codenames should NOT be called when --codename is passed
        assert len(list_codenames_called) == 0, (
            "list_codenames() should NOT be called when --codename is explicitly passed"
        )

    def test_clean_without_codename_calls_list_codenames(self, capfd):
        """When no --codename is passed, list_codenames() SHOULD be called."""
        if not hasattr(s3_utils, "list_codenames"):
            pytest.skip("list_codenames() function not yet implemented")

        release = self._create_release(components=["main"])

        from pydeb_s3 import manifest as manifest_module
        from pydeb_s3 import package as package_module
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)

        list_codenames_called = []
        original_list_codenames = s3_utils.list_codenames

        def mock_list_codenames(adapter):
            list_codenames_called.append(True)
            return original_list_codenames(adapter)

        with patch.object(s3_utils, "list_codenames", side_effect=mock_list_codenames):
            clean_command(bucket="test-bucket", component="main")

        # With the fix, list_codenames SHOULD be called when no --codename is passed
        assert len(list_codenames_called) > 0, (
            "list_codenames() should be called when no --codename is passed"
        )


class TestCleanCodenamesEdgeCases:
    """Edge case tests for codename filtering in clean command."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter):
        """Set up test fixtures with S3 bucket."""
        self.s3_adapter = moto_s3_adapter

    def _create_release(self, codename="stable", components=None):
        """Create release."""
        if components is None:
            components = ["main"]
        release = release_module.Release(
            codename=codename, origin="TestRepo", architectures=["amd64"], components=components
        )
        release.write_to_s3(self.s3_adapter)
        return release

    def test_clean_with_single_codename_works(self, capfd):
        """Clean should work when there's only one codename in S3 and --codename is passed."""
        release = self._create_release(codename="stable", components=["main"])

        from pydeb_s3 import manifest as manifest_module
        from pydeb_s3 import package as package_module
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)

        # Upload orphan package
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(open("tests/fixtures/test-pkg-full_1.0.0_all.deb", "rb").read())
            tmp_path = tmp.name
        try:
            self.s3_adapter.store_file(tmp_path, "pool/main/t/test-pkg-full/test-pkg-full_1.0.0_all.deb",
                            "application/x-debian-package")
        finally:
            os.unlink(tmp_path)

        # Run clean with --codename stable
        clean_command(bucket="test-bucket", codename="stable", component="main")

        # Orphan should be deleted
        result = self.s3_adapter.list_objects("pool/main/t/test-pkg-full/")
        objects = result[0] if isinstance(result, tuple) else result
        files = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert not any("test-pkg-full" in f for f in files), (
            "Orphan should be deleted when there's only one codename and --codename is passed"
        )

    def test_clean_without_codename_handles_single_codename(self, capfd):
        """Clean should work when there's only one codename and no --codename is passed."""
        release = self._create_release(codename="stable", components=["main"])

        from pydeb_s3 import manifest as manifest_module
        from pydeb_s3 import package as package_module
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)

        # Upload orphan package
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(open("tests/fixtures/test-pkg-full_1.0.0_all.deb", "rb").read())
            tmp_path = tmp.name
        try:
            self.s3_adapter.store_file(tmp_path, "pool/main/t/test-pkg-full/test-pkg-full_1.0.0_all.deb",
                            "application/x-debian-package")
        finally:
            os.unlink(tmp_path)

        # Run clean WITHOUT codename
        clean_command(bucket="test-bucket", component="main")

        # Orphan should be deleted
        result = self.s3_adapter.list_objects("pool/main/t/test-pkg-full/")
        objects = result[0] if isinstance(result, tuple) else result
        files = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]

        assert not any("test-pkg-full" in f for f in files), (
            "Orphan should be deleted when there's only one codename and no --codename passed"
        )
