"""Integration tests for clean command codename filtering.

Default: clean checks ALL codenames (safe - don't delete packages referenced
by other codenames). With --force, only the specified codename is checked
(aggressive - may delete packages referenced elsewhere).
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
    """Tests for the list_codenames() function."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter):
        self.s3_adapter = moto_s3_adapter

    def test_list_codenames_returns_all_codenames(self):
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
        codenames = s3_utils.list_codenames(self.s3_adapter)
        assert codenames == []


class TestCleanWithForceFlag:
    """Tests that --force makes clean only check the specified codename."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter):
        self.s3_adapter = moto_s3_adapter

    def _create_release(self, codename="stable", architectures=None, components=None):
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
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, codename, component, arch)
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)
        return pkg

    def _upload_deb_to_pool(self, deb_file_path, component="main"):
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

    def test_clean_with_force_only_checks_specified_codename(self, capfd):
        """With --force --codename rc, packages only in stable should be deleted."""
        stable_release = self._create_release(codename="stable", components=["main"])
        self._add_packages_to_manifest(
            stable_release, "tests/fixtures/test-pkg_1.0.0_amd64.deb",
            component="main", codename="stable",
        )
        self._create_release(codename="rc", components=["main"])
        self._upload_deb_to_pool("tests/fixtures/test-pkg_1.0.0_amd64.deb", component="main")
        clean_command(bucket="test-bucket", codename="rc", component="main", force=True)
        result = self.s3_adapter.list_objects("pool/main/t/")
        objects = result[0] if isinstance(result, tuple) else result
        files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]
        assert not any("test-pkg_1.0.0_amd64.deb" in f for f in files_after)

    def test_clean_with_force_preserves_referenced_in_that_codename(self, capfd):
        rc_release = self._create_release(codename="rc", components=["main"])
        self._add_packages_to_manifest(
            rc_release, "tests/fixtures/hello_2.10-5_amd64.deb",
            component="main", codename="rc",
        )
        self._upload_deb_to_pool("tests/fixtures/hello_2.10-5_amd64.deb", component="main")
        clean_command(bucket="test-bucket", codename="rc", component="main", force=True)
        result = self.s3_adapter.list_objects("pool/main/h/")
        objects = result[0] if isinstance(result, tuple) else result
        hello_files = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]
        assert any("hello_2.10-5_amd64.deb" in f for f in hello_files)


class TestCleanDefaultChecksAllCodenames:
    """Default (no --force): clean checks ALL codenames (safe)."""

    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter):
        self.s3_adapter = moto_s3_adapter

    def _create_release(self, codename="stable", architectures=None, components=None):
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
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, codename, component, arch)
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)
        return pkg

    def _upload_deb_to_pool(self, deb_file_path, component="main"):
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

    def test_clean_default_checks_all_codenames(self, capfd):
        stable_release = self._create_release(codename="stable", components=["main"])
        self._add_packages_to_manifest(
            stable_release, "tests/fixtures/test-pkg_1.0.0_amd64.deb",
            component="main", codename="stable",
        )
        self._create_release(codename="rc", components=["main"])
        self._upload_deb_to_pool("tests/fixtures/test-pkg_1.0.0_amd64.deb", component="main")
        clean_command(bucket="test-bucket", codename="rc", component="main")
        result = self.s3_adapter.list_objects("pool/main/t/")
        objects = result[0] if isinstance(result, tuple) else result
        files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]
        assert any("test-pkg_1.0.0_amd64.deb" in f for f in files_after)

    def test_clean_default_deletes_truly_orphaned(self, capfd):
        stable_release = self._create_release(codename="stable", components=["main"])
        self._add_packages_to_manifest(
            stable_release, "tests/fixtures/hello_2.10-5_amd64.deb",
            component="main", codename="stable",
        )
        self._upload_deb_to_pool("tests/fixtures/test-pkg-full_1.0.0_all.deb", component="main")
        clean_command(bucket="test-bucket", codename="stable", component="main")
        result = self.s3_adapter.list_objects("pool/main/t/")
        objects = result[0] if isinstance(result, tuple) else result
        files_after = [obj["Key"] for obj in objects if obj.get("Key", "").endswith(".deb")]
        assert not any("test-pkg-full" in f for f in files_after)


class TestCleanCodenamesMocked:
    @pytest.fixture(autouse=True)
    def setup(self, moto_s3_adapter):
        self.s3_adapter = moto_s3_adapter

    def _create_release(self, codename="stable", components=None):
        if components is None:
            components = ["main"]
        release = release_module.Release(
            codename=codename, origin="TestRepo", architectures=["amd64"], components=components
        )
        release.write_to_s3(self.s3_adapter)
        return release

    def test_clean_with_force_does_not_call_list_codenames(self, capfd):
        if not hasattr(s3_utils, "list_codenames"):
            pytest.skip("list_codenames() not implemented")
        release = self._create_release(components=["main"])
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)
        called = []
        orig = s3_utils.list_codenames
        def mock(adapter):
            called.append(True)
            return orig(adapter)
        with patch.object(s3_utils, "list_codenames", side_effect=mock):
            clean_command(bucket="test-bucket", codename="stable", component="main", force=True)
        assert len(called) == 0

    def test_clean_without_force_calls_list_codenames(self, capfd):
        if not hasattr(s3_utils, "list_codenames"):
            pytest.skip("list_codenames() not implemented")
        release = self._create_release(components=["main"])
        pkg = package_module.Package.parse_file("tests/fixtures/test-pkg_1.0.0_amd64.deb")
        manifest = manifest_module.Manifest.retrieve(self.s3_adapter, "stable", "main", "amd64")
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)
        called = []
        orig = s3_utils.list_codenames
        def mock(adapter):
            called.append(True)
            return orig(adapter)
        with patch.object(s3_utils, "list_codenames", side_effect=mock):
            clean_command(bucket="test-bucket", codename="stable", component="main")
        assert len(called) > 0
