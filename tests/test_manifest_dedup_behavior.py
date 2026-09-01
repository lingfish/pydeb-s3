"""Integration tests for cross-component deduplication upload flow.

These tests exercise the full dedup lifecycle through Manifest.write_to_s3(),
verifying S3 state after multi-step operations. Every test is expected to
FAIL before the implementation exists.

The flow tested:
1. Upload package to component A (non-free).
2. Upload same package to component B (main) with dedupe_component="non-free".
3. Verify S3 pool paths, manifest content, and copy-vs-upload behavior.
"""

import os

import pytest

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module


class TestDedupeUploadFlow:
    """Integration tests for the full dedup upload lifecycle."""

    @pytest.fixture(autouse=True)
    def setup(self, mock_s3_adapter, sample_deb_file):
        """Set up shared fixtures."""
        self.s3_adapter = mock_s3_adapter
        self.sample_deb_file = sample_deb_file

    def _create_release(self, codename="stable"):
        """Create and upload a Release file with main + non-free."""
        release = release_module.Release(
            codename=codename,
            origin="TestRepo",
            architectures=["amd64"],
            components=["main", "non-free"],
        )
        release.write_to_s3(self.s3_adapter)
        return release

    def _upload_package(self, deb_file, component, codename="stable", arch="amd64"):
        """Upload a .deb to a specific component and update the release.

        Returns (package, manifest).
        """
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve(
            self.s3_adapter, codename, component, arch
        )
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)

        release = release_module.Release.retrieve(self.s3_adapter, codename)
        release.update_manifest(manifest)
        release.write_to_s3(self.s3_adapter)
        return pkg, manifest

    def _pool_path(self, pkg, component):
        """Build the expected S3 pool path for a package."""
        basename = os.path.basename(pkg.filename)
        return f"pool/{component}/{pkg.name[0]}/{pkg.name[0:2]}/{basename}"

    def _packages_content(self, component, codename="stable", arch="amd64"):
        """Read the Packages file content from S3."""
        path = f"dists/{codename}/{component}/binary-{arch}/Packages"
        return self.s3_adapter.read(path)

    def _tracking_adapter(self, adapter):
        """Wrap adapter methods to track which S3 operations were called.

        Returns (adapter, calls_dict).
        """
        calls = {"copy": [], "store_file": []}
        original_copy = adapter.copy
        original_store = adapter.store_file

        def tracking_copy(source, dest):
            calls["copy"].append((source, dest))
            return original_copy(source, dest)

        def tracking_store(filepath, key, **kwargs):
            if not key.endswith(("Packages", "Packages.gz")):
                calls["store_file"].append((filepath, key))
            return original_store(filepath, key, **kwargs)

        adapter.copy = tracking_copy
        adapter.store_file = tracking_store
        return adapter, calls

    # ------------------------------------------------------------------
    # Test 1: Full upload flow with dedup
    # ------------------------------------------------------------------
    def test_full_upload_flow_with_dedup(self):
        """Upload to non-free, then upload same to main with dedup.

        Verifies:
        1. pool/non-free/... exists after first upload.
        2. pool/main/... exists after dedup upload.
        3. Both pool files have identical content (server-side copy).
        4. s3_adapter.copy() was called (not store_file for .deb).
        5. main/Packages manifest contains the package.
        """
        self._create_release()

        # Step 1: upload to non-free
        pkg, _ = self._upload_package(self.sample_deb_file, "non-free")
        source_path = self._pool_path(pkg, "non-free")
        assert self.s3_adapter.exists(source_path), (
            f"non-free pool file should exist: {source_path}"
        )

        # Wrap adapter to track calls
        tracked, calls = self._tracking_adapter(self.s3_adapter)

        # Step 2: upload to main with dedup
        manifest_main = manifest_module.Manifest.retrieve(
            tracked, "stable", "main", "amd64"
        )
        manifest_main.dedupe_component = "non-free"  # <-- will fail
        manifest_main.add(pkg)
        manifest_main.write_to_s3(tracked)

        # Step 3: verify main pool file exists
        dest_path = self._pool_path(pkg, "main")
        assert tracked.exists(dest_path), (
            f"main pool file should exist after dedup: {dest_path}"
        )

        # Step 4: verify copy was used (not store_file)
        assert len(calls["copy"]) >= 1, (
            "s3_adapter.copy() should have been called for dedup"
        )
        assert len(calls["store_file"]) == 0, (
            "s3_adapter.store_file() should NOT have been called for .deb (dedup used)"
        )

        # Step 5: verify identical content (server-side copy)
        source_content = tracked._storage[tracked._s3_path(source_path)]
        dest_content = tracked._storage[tracked._s3_path(dest_path)]
        assert source_content == dest_content, (
            "Dedup copy must produce identical content"
        )

        # Step 6: verify main/Packages contains the package
        packages_content = self._packages_content("main")
        assert "test-pkg" in packages_content

    # ------------------------------------------------------------------
    # Test 2: Dedup with multiple packages
    # ------------------------------------------------------------------
    def test_dedup_with_multiple_packages(self):
        """Upload 2 to non-free, upload 3 to main (2 deduped + 1 new).

        Verifies:
        1. alpha and bravo exist in non-free pool.
        2. Deduped packages are copied to main pool via copy().
        3. New-only package (charlie) is uploaded via store_file().
        4. main/Packages contains all expected packages.
        """
        self._create_release()

        # Upload only alpha and bravo to non-free (charlie is new-only)
        pkg_a = package_module.Package.parse_file(self.sample_deb_file)
        pkg_a.name = "alpha"
        pkg_b = package_module.Package.parse_file(self.sample_deb_file)
        pkg_b.name = "bravo"
        pkg_c = package_module.Package.parse_file(self.sample_deb_file)
        pkg_c.name = "charlie"

        for pkg in [pkg_a, pkg_b]:
            manifest_nf = manifest_module.Manifest.retrieve(
                self.s3_adapter, "stable", "non-free", "amd64"
            )
            manifest_nf.add(pkg)
            manifest_nf.write_to_s3(self.s3_adapter)

        # Verify alpha and bravo exist in non-free
        for pkg in [pkg_a, pkg_b]:
            path = self._pool_path(pkg, "non-free")
            assert self.s3_adapter.exists(path), f"non-free should have {pkg.name}"

        # Wrap adapter to track calls
        tracked, calls = self._tracking_adapter(self.s3_adapter)

        # Upload to main with dedup: alpha and bravo exist in non-free, charlie is new
        manifest_main = manifest_module.Manifest.retrieve(
            tracked, "stable", "main", "amd64"
        )
        manifest_main.dedupe_component = "non-free"  # <-- will fail

        manifest_main.add(pkg_a)
        manifest_main.add(pkg_b)
        manifest_main.add(pkg_c)
        manifest_main.write_to_s3(tracked)

        # All 3 should exist in main
        for pkg in [pkg_a, pkg_b, pkg_c]:
            path = self._pool_path(pkg, "main")
            assert tracked.exists(path), (
                f"main should have {pkg.name} (dedup or upload)"
            )

        # alpha and bravo should be copied (they exist in non-free)
        # charlie should be uploaded normally
        assert len(calls["copy"]) == 2, (
            f"Expected 2 copy() calls (alpha, bravo), got {len(calls['copy'])}"
        )
        assert len(calls["store_file"]) >= 1, (
            "Expected at least 1 store_file() call for charlie"
        )

        # Verify content matches for the deduped packages
        for pkg in [pkg_a, pkg_b]:
            src = tracked._storage[tracked._s3_path(self._pool_path(pkg, "non-free"))]
            dst = tracked._storage[tracked._s3_path(self._pool_path(pkg, "main"))]
            assert src == dst, f"dedup content must match for {pkg.name}"

        # main/Packages should list all 3
        content = self._packages_content("main")
        assert "alpha" in content
        assert "bravo" in content
        assert "charlie" in content

    # ------------------------------------------------------------------
    # Test 3: Stale manifest scenario
    # ------------------------------------------------------------------
    def test_dedup_stale_manifest_scenario(self):
        """Upload to non-free, delete pool file, upload to main with dedup.

        When the .deb file has been deleted but the non-free manifest
        still references it, the dedup should detect the missing file
        and fall back to normal upload.

        Verifies:
        1. After deletion, non-free pool file is gone.
        2. Dedup detects missing file and falls back to upload.
        3. main pool file exists after fallback upload.
        4. copy() was NOT called (source missing).
        5. store_file() WAS called (fallback).
        6. main/Packages contains the package.
        """
        self._create_release()

        # Upload to non-free
        pkg, _ = self._upload_package(self.sample_deb_file, "non-free")
        source_path = self._pool_path(pkg, "non-free")
        assert self.s3_adapter.exists(source_path)

        # Delete the pool file (simulate stale state)
        self.s3_adapter.remove(source_path)
        assert not self.s3_adapter.exists(source_path)

        # Wrap adapter to track calls
        tracked, calls = self._tracking_adapter(self.s3_adapter)

        # Upload to main with dedup — should fall back to upload
        manifest_main = manifest_module.Manifest.retrieve(
            tracked, "stable", "main", "amd64"
        )
        manifest_main.dedupe_component = "non-free"  # <-- will fail
        manifest_main.add(pkg)
        manifest_main.write_to_s3(tracked)

        # main pool file should exist (via fallback upload)
        dest_path = self._pool_path(pkg, "main")
        assert tracked.exists(dest_path), (
            "Fallback upload should place file in main pool"
        )

        # copy was NOT called (source missing), store_file was called as fallback
        assert len(calls["copy"]) == 0, (
            "copy() should NOT have been called when source file is missing"
        )
        assert len(calls["store_file"]) >= 1, (
            "store_file() should have been called as fallback"
        )

        # main/Packages should contain the package
        content = self._packages_content("main")
        assert "test-pkg" in content
