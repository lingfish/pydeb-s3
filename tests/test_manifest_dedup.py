"""Unit tests for cross-component deduplication in Manifest.write_to_s3().

These tests exercise the dedupe_component feature at the Manifest level,
using MockS3Adapter for fast in-memory S3 simulation. Every test in this
file is expected to FAIL before the implementation exists — that's the
point of TDD.

Design decisions under test:
- Manifest.dedupe_component field (decision #10)
- S3 server-side copy via S3Adapter.copy() (decision #2)
- Stale-manifest guard via s3_adapter.exists() (decision #6)
- Fallback to upload on missing file (decision #7)
- skip_package_upload silences dedup (decision #8)
- Different version → no copy (general correctness)
"""

import os

import pytest

from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module
from pydeb_s3 import release as release_module
from pydeb_s3.s3_adapter import MockS3Adapter


class TestManifestDedup:
    """Unit tests for Manifest deduplication logic."""

    @pytest.fixture(autouse=True)
    def setup(self, mock_s3_adapter, sample_deb_file):
        """Set up shared fixtures.

        Uses mock_s3_adapter (MockS3Adapter) for fast in-memory testing.
        """
        self.s3_adapter = mock_s3_adapter
        self.sample_deb_file = sample_deb_file

    def _create_initial_release(self, codename="stable", architectures=None, components=None):
        """Create and upload an initial Release file."""
        if architectures is None:
            architectures = ["amd64"]
        if components is None:
            components = ["main", "non-free"]
        release = release_module.Release(
            codename=codename,
            origin="TestRepo",
            architectures=architectures,
            components=components,
        )
        release.write_to_s3(self.s3_adapter)
        return release

    def _upload_package_to_component(
        self, deb_file: str, component: str, arch: str = "amd64",
        codename: str = "stable",
    ) -> package_module.Package:
        """Upload a .deb package to a specific component.

        Returns the parsed Package object.
        """
        pkg = package_module.Package.parse_file(deb_file)
        manifest = manifest_module.Manifest.retrieve(
            self.s3_adapter, codename, component, arch
        )
        manifest.add(pkg)
        manifest.write_to_s3(self.s3_adapter)
        return pkg

    def _pool_path(self, pkg: package_module.Package, component: str) -> str:
        """Build the expected S3 pool path for a package."""
        basename = os.path.basename(pkg.filename)
        return f"pool/{component}/{pkg.name[0]}/{pkg.name[0:2]}/{basename}"

    def _tracking_adapter(self, adapter: MockS3Adapter):
        """Wrap adapter methods to track which ones were called.

        Returns (adapter, calls_dict) where calls_dict has keys
        'copy', 'store_file' mapping to lists of call tuples.
        """
        calls = {"copy": [], "store_file": []}
        original_copy = adapter.copy
        original_store = adapter.store_file

        def tracking_copy(source, dest):
            calls["copy"].append((source, dest))
            return original_copy(source, dest)

        def tracking_store(filepath, key, **kwargs):
            # Only track .deb file uploads (not Packages/Packages.gz)
            if not key.endswith(("Packages", "Packages.gz")):
                calls["store_file"].append((filepath, key))
            return original_store(filepath, key, **kwargs)

        adapter.copy = tracking_copy
        adapter.store_file = tracking_store
        return adapter, calls

    # ------------------------------------------------------------------
    # Test 1: Happy path — copy from dedupe component
    # ------------------------------------------------------------------
    def test_dedupe_copies_from_other_component(self):
        """Package in dedupe component with existing file → S3 copy used.

        Steps:
        1. Create release with main + non-free components.
        2. Upload test-pkg to non-free (puts .deb in pool/non-free/...).
        3. Create a manifest for main with dedupe_component="non-free".
        4. Add the same package and call write_to_s3().
        5. Assert pool/main/... exists.
        6. Assert s3_adapter.copy() was called (not store_file for .deb).
        """
        self._create_initial_release()

        # Step 2: upload to non-free
        pkg = self._upload_package_to_component(self.sample_deb_file, "non-free")
        source_path = self._pool_path(pkg, "non-free")
        assert self.s3_adapter.exists(source_path), (
            f"Source file should exist after upload: {source_path}"
        )

        # Wrap adapter to track method calls
        tracked, calls = self._tracking_adapter(self.s3_adapter)

        # Step 3 & 4: create main manifest with dedupe, add same package
        manifest = manifest_module.Manifest.retrieve(
            tracked, "stable", "main", "amd64"
        )
        manifest.dedupe_component = "non-free"  # <-- will fail before implementation
        manifest.add(pkg)
        manifest.write_to_s3(tracked)

        # Step 5: file should now exist in main pool (via copy)
        dest_path = self._pool_path(pkg, "main")
        assert tracked.exists(dest_path), (
            f"Copied file should exist in main: {dest_path}"
        )

        # Step 6: copy was used, not store_file for the .deb
        assert len(calls["copy"]) >= 1, (
            "s3_adapter.copy() should have been called for dedup"
        )
        assert len(calls["store_file"]) == 0, (
            "s3_adapter.store_file() should NOT have been called for .deb files "
            "(dedup copy should be used instead)"
        )

    # ------------------------------------------------------------------
    # Test 2: Fallback when .deb file is missing from dedupe component
    # ------------------------------------------------------------------
    def test_dedupe_falls_back_when_file_missing(self):
        """Dedupe manifest lists package but .deb file not in pool → normal upload.

        Steps:
        1. Create release with main + non-free.
        2. Upload to non-free to get a package listed in non-free manifest.
        3. Manually delete the .deb from pool/non-free/... (simulates
           stale manifest / deleted file).
        4. Create main manifest with dedupe_component="non-free", add pkg.
        5. write_to_s3() should fall back to store_file() for the .deb.
        6. Assert pool/main/... exists.
        7. Assert store_file() was called (not copy).
        """
        self._create_initial_release()

        # Upload to non-free
        pkg = self._upload_package_to_component(self.sample_deb_file, "non-free")
        source_path = self._pool_path(pkg, "non-free")

        # Delete the .deb file (manifest still references it)
        self.s3_adapter.remove(source_path)
        assert not self.s3_adapter.exists(source_path)

        # Wrap adapter to track method calls
        tracked, calls = self._tracking_adapter(self.s3_adapter)

        # Upload to main with dedup — should fall back to normal upload
        manifest = manifest_module.Manifest.retrieve(
            tracked, "stable", "main", "amd64"
        )
        manifest.dedupe_component = "non-free"  # <-- will fail before implementation
        manifest.add(pkg)
        manifest.write_to_s3(tracked)

        # File should exist in main pool via fallback upload
        dest_path = self._pool_path(pkg, "main")
        assert tracked.exists(dest_path), (
            f"Fallback upload should place file in main: {dest_path}"
        )

        # copy was NOT called (source missing), store_file was called as fallback
        assert len(calls["copy"]) == 0, (
            "s3_adapter.copy() should NOT have been called when source file is missing"
        )
        assert len(calls["store_file"]) >= 1, (
            "s3_adapter.store_file() should have been called as fallback"
        )

    # ------------------------------------------------------------------
    # Test 3: No dedupe manifest exists → normal upload, no errors
    # ------------------------------------------------------------------
    def test_dedupe_falls_back_when_manifest_missing(self):
        """No dedupe manifest in S3 → normal upload, no errors raised.

        Steps:
        1. Create release with main + non-free.
        2. Do NOT upload anything to non-free (no manifest exists).
        3. Create main manifest with dedupe_component="non-free", add pkg.
        4. write_to_s3() should succeed (normal upload path).
        5. Assert pool/main/... exists.
        6. Assert copy was NOT called, store_file was called.
        """
        self._create_initial_release()

        tracked, calls = self._tracking_adapter(self.s3_adapter)

        manifest = manifest_module.Manifest.retrieve(
            tracked, "stable", "main", "amd64"
        )
        manifest.dedupe_component = "non-free"  # <-- will fail before implementation
        pkg = package_module.Package.parse_file(self.sample_deb_file)
        manifest.add(pkg)
        manifest.write_to_s3(tracked)

        # File should exist in main pool via normal upload
        dest_path = self._pool_path(pkg, "main")
        assert tracked.exists(dest_path), (
            f"Normal upload should place file: {dest_path}"
        )

        # No copy, just normal upload
        assert len(calls["copy"]) == 0, (
            "s3_adapter.copy() should NOT have been called (no dedupe manifest)"
        )
        assert len(calls["store_file"]) >= 1, (
            "s3_adapter.store_file() should have been called for normal upload"
        )

    # ------------------------------------------------------------------
    # Test 4: skip_package_upload + dedupe → silently skip
    # ------------------------------------------------------------------
    def test_dedupe_skipped_when_skip_package_upload(self):
        """skip_package_upload=True + dedupe_component → nothing uploaded/copied.

        Steps:
        1. Create release with main + non-free.
        2. Upload to non-free (so .deb exists in pool).
        3. Create main manifest with skip_package_upload=True AND
           dedupe_component="non-free", add pkg.
        4. write_to_s3() should NOT copy or upload the .deb.
        5. Assert pool/main/... does NOT exist.
        6. Assert pool/non-free/... still exists (untouched).
        7. Assert neither copy() nor store_file() was called for .deb.
        """
        self._create_initial_release()

        # Upload to non-free
        pkg = self._upload_package_to_component(self.sample_deb_file, "non-free")

        tracked, calls = self._tracking_adapter(self.s3_adapter)

        # Attempt to add to main with skip + dedupe
        manifest = manifest_module.Manifest.retrieve(
            tracked, "stable", "main", "amd64",
            skip_package_upload=True,
        )
        manifest.dedupe_component = "non-free"  # <-- will fail before implementation
        manifest.add(pkg)
        manifest.write_to_s3(tracked)

        # .deb should NOT be in main pool
        dest_path = self._pool_path(pkg, "main")
        assert not tracked.exists(dest_path), (
            f"skip_package_upload should prevent copy/upload: {dest_path}"
        )

        # .deb should still be in non-free
        source_path = self._pool_path(pkg, "non-free")
        assert tracked.exists(source_path), (
            f"Source file should remain untouched: {source_path}"
        )

        # Neither copy nor store_file should have been called for .deb
        assert len(calls["copy"]) == 0, (
            "copy() should NOT have been called when skip_package_upload=True"
        )
        assert len(calls["store_file"]) == 0, (
            "store_file() should NOT have been called when skip_package_upload=True"
        )

    # ------------------------------------------------------------------
    # Test 5: Same package different version → no copy
    # ------------------------------------------------------------------
    def test_dedupe_same_package_different_version_not_copied(self):
        """non-free has v1.0, uploading v2.0 to main → normal upload, no copy.

        Steps:
        1. Create release with main + non-free.
        2. Upload test-pkg v1.0 to non-free.
        3. Create main manifest with dedupe_component="non-free".
        4. Add test-pkg v2.0 (different version).
        5. write_to_s3() should NOT copy v1.0 from non-free.
        6. Assert pool/main/... exists with v2.0 (via normal upload).
        7. Assert copy() was NOT called, store_file() was called.
        """
        self._create_initial_release()

        # Upload v1.0 to non-free
        pkg_v1 = package_module.Package.parse_file(self.sample_deb_file)
        manifest_nf = manifest_module.Manifest.retrieve(
            self.s3_adapter, "stable", "non-free", "amd64"
        )
        manifest_nf.add(pkg_v1)
        manifest_nf.write_to_s3(self.s3_adapter)

        # Create main manifest with dedupe
        tracked, calls = self._tracking_adapter(self.s3_adapter)
        manifest_main = manifest_module.Manifest.retrieve(
            tracked, "stable", "main", "amd64"
        )
        manifest_main.dedupe_component = "non-free"  # <-- will fail before implementation

        # Add v2.0 of the same package
        pkg_v2 = package_module.Package.parse_file(self.sample_deb_file)
        pkg_v2.version = "2.0.0"
        pkg_v2.iteration = "1"
        manifest_main.add(pkg_v2)
        manifest_main.write_to_s3(tracked)

        # v2.0 should exist in main pool (normal upload)
        dest_path_v2 = self._pool_path(pkg_v2, "main")
        assert tracked.exists(dest_path_v2), (
            f"v2.0 should be uploaded to main: {dest_path_v2}"
        )

        # v1.0 should NOT have been copied
        assert len(calls["copy"]) == 0, (
            "copy() should NOT have been called for different version"
        )
        assert len(calls["store_file"]) >= 1, (
            "store_file() should have been called for v2.0 normal upload"
        )

    # ------------------------------------------------------------------
    # Test 6: dedupe_component field defaults to None
    # ------------------------------------------------------------------
    def test_dedupe_component_defaults_to_none(self):
        """Manifest.dedupe_component defaults to None (no dedup)."""
        manifest = manifest_module.Manifest()
        assert manifest.dedupe_component is None, (  # <-- will fail before implementation
            "dedupe_component should default to None"
        )

    # ------------------------------------------------------------------
    # Test 7: Dedup within same codename only (cross-codename ignored)
    # ------------------------------------------------------------------
    def test_dedupe_does_not_cross_codenames(self):
        """Package in non-free/unstable is NOT copied to main/stable.

        Design decision #3 says: "Same codename only (search all codenames'
        manifests for visibility, but only copy within same codename)."

        Steps:
        1. Create release for 'unstable' and 'stable' with main + non-free.
        2. Upload test-pkg to unstable/non-free.
        3. Create stable/main manifest with dedupe_component="non-free".
        4. Add the same package and call write_to_s3().
        5. Assert copy() was NOT called (different codename).
        6. Assert store_file() was called (normal upload).
        """
        # Create releases for both codenames
        release_module.Release(
            codename="unstable",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main", "non-free"],
        ).write_to_s3(self.s3_adapter)
        release_module.Release(
            codename="stable",
            origin="TestRepo",
            architectures=["amd64"],
            components=["main", "non-free"],
        ).write_to_s3(self.s3_adapter)

        # Upload to unstable/non-free
        pkg = self._upload_package_to_component(
            self.sample_deb_file, "non-free", codename="unstable"
        )

        tracked, calls = self._tracking_adapter(self.s3_adapter)

        # Upload to stable/main with dedupe — should NOT copy from unstable
        manifest = manifest_module.Manifest.retrieve(
            tracked, "stable", "main", "amd64"
        )
        manifest.dedupe_component = "non-free"  # <-- will fail before implementation
        manifest.add(pkg)
        manifest.write_to_s3(tracked)

        dest_path = self._pool_path(pkg, "main")
        assert tracked.exists(dest_path), (
            f"File should be uploaded to stable/main: {dest_path}"
        )

        # No cross-codename copy should occur
        assert len(calls["copy"]) == 0, (
            "copy() should NOT be called across codenames"
        )
        assert len(calls["store_file"]) >= 1, (
            "store_file() should be called for normal upload to stable/main"
        )

        # Verify unstable/non-free source is still intact
        source_path = self._pool_path(pkg, "non-free")
        assert tracked.exists(source_path), (
            f"unstable/non-free source should be intact: {source_path}"
        )
