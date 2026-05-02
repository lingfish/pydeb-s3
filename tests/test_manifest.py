"""Tests for the Manifest model."""


from pydeb_s3 import manifest as manifest_module
from pydeb_s3 import package as package_module


class TestManifestAdd:
    """Tests for Manifest.add()."""

    def setup_method(self):
        """Set up test fixtures."""
        self.manifest = manifest_module.Manifest()

    def test_removes_packages_with_same_full_version(self):
        """Removes packages which have the same full version."""
        import time
        epoch = str(int(time.time()))

        existing_with_same = package_module.Package(
            name="discourse",
            version="0.9.8.3",
            epoch=epoch,
            iteration="1",
        )
        new_package = package_module.Package(
            name="discourse",
            version="0.9.8.3",
            epoch=epoch,
            iteration="1",
        )

        self.manifest.packages = [existing_with_same]
        self.manifest.add(new_package, preserve_versions=True)
        assert len(self.manifest.packages) == 1

    def test_does_not_remove_based_only_on_version(self):
        """Does not remove packages based only on the version."""
        existing_with_same = package_module.Package(
            name="discourse",
            version="0.9.8.3",
            iteration="1",
        )
        new_package = package_module.Package(
            name="discourse",
            version="0.9.8.3",
            iteration="2",
        )

        self.manifest.packages = [existing_with_same]
        self.manifest.add(new_package, preserve_versions=True)
        assert len(self.manifest.packages) == 2

    def test_removes_same_name_when_preserve_versions_false(self):
        """Removes packages with same name and version <= incoming when preserve_versions is false."""
        existing_packages = [
            package_module.Package(name="discourse", version="0.9.8.3", iteration="1"),
            package_module.Package(name="discourse", version="0.9.8.4"),
            package_module.Package(name="discourse", version="0.9.8.5", epoch="2"),
        ]
        # Upload 0.9.8.5 - should remove 0.9.8.3 and 0.9.8.4 but keep 0.9.8.5
        new_package = package_module.Package(name="discourse", version="0.9.8.5")

        self.manifest.packages = existing_packages
        self.manifest.add(new_package, preserve_versions=False)
        # Should have 0.9.8.5 (new) and 0.9.8.5 with epoch=2 (newer)
        # 0.9.8.3-1 and 0.9.8.4 are < 0.9.8.5 so removed
        assert len(self.manifest.packages) == 2

    def test_does_not_remove_newer_versions_when_uploading_older(self):
        """When uploading an older version with preserve_versions=False,
        newer versions should NOT be removed from manifest."""
        # Manifest has newer versions: 0.22.1~rc1, 0.22.1
        existing_packages = [
            package_module.Package(name="ollama", version="0.22.1", iteration="~rc1"),
            package_module.Package(name="ollama", version="0.22.1"),
        ]
        # Uploading older 0.21.1
        new_package = package_module.Package(name="ollama", version="0.21.1")

        self.manifest.packages = existing_packages
        self.manifest.add(new_package, preserve_versions=False)

        # Should have all 3: 0.22.1-~rc1, 0.22.1, and 0.21.1
        assert len(self.manifest.packages) == 3
        versions = sorted([p.full_version for p in self.manifest.packages])
        assert "0.22.1-~rc1" in versions
        assert "0.22.1" in versions
        assert "0.21.1" in versions

    def test_removes_older_versions_when_uploading_newer(self):
        """When uploading a newer version with preserve_versions=False,
        older versions should be removed from manifest."""
        existing_packages = [
            package_module.Package(name="ollama", version="0.21.1"),
            package_module.Package(name="ollama", version="0.22.1", iteration="~rc1"),
        ]
        # Uploading newer 0.22.1
        new_package = package_module.Package(name="ollama", version="0.22.1")

        self.manifest.packages = existing_packages
        self.manifest.add(new_package, preserve_versions=False)

        # Should only have 0.22.1 (older removed)
        assert len(self.manifest.packages) == 1
        assert self.manifest.packages[0].full_version == "0.22.1"

    def test_replaces_same_version_when_preserve_versions_false(self):
        """Re-uploading same version should work (remove then re-add)."""
        existing_packages = [
            package_module.Package(name="ollama", version="0.22.1"),
        ]
        new_package = package_module.Package(name="ollama", version="0.22.1")

        self.manifest.packages = existing_packages
        self.manifest.add(new_package, preserve_versions=False)

        # Should have exactly one (the new one)
        assert len(self.manifest.packages) == 1


class TestManifestDeletePackage:
    """Tests for Manifest.delete_package()."""

    def setup_method(self):
        """Set up test fixtures."""
        self.manifest = manifest_module.Manifest()

    def test_removes_matching_versions(self):
        """Removes packages which have the same version as one of the versions specified."""
        import time
        epoch = str(int(time.time()))

        existing_packages = [
            package_module.Package(
                name="discourse",
                epoch=epoch,
                version="0.9.8.3",
                iteration="1",
            ),
            package_module.Package(
                name="discourse",
                epoch=epoch,
                version="0.9.0.0",
                iteration="1",
            ),
            package_module.Package(
                name="discourse",
                epoch=epoch,
                version="0.9.0.0",
                iteration="2",
            ),
        ]

        versions_to_delete = [f"{epoch}:0.9.8.3-1"]

        self.manifest.packages = existing_packages
        deleted = self.manifest.delete_package("discourse", versions_to_delete)

        remaining_versions = [p.full_version for p in self.manifest.packages]
        assert f"{epoch}:0.9.0.0-2" in remaining_versions
        assert len(deleted) == 1

    def test_removes_all_if_no_versions_specified(self):
        """Removes all packages with the name if no versions specified."""
        existing_packages = [
            package_module.Package(name="discourse", version="0.9.8.3"),
            package_module.Package(name="discourse", version="0.9.0.0"),
        ]

        self.manifest.packages = existing_packages
        deleted = self.manifest.delete_package("discourse")

        assert len(self.manifest.packages) == 0
        assert len(deleted) == 2
