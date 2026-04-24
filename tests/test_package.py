"""Tests for the Package model."""


from pydeb_s3 import package as package_module

FIXTURES_PACKAGES = """Package: discourse
Version: 0.9.8.3-1396474125.12e4179.wheezy
License: unknown
Vendor: @ba8d89490f8f
Architecture: amd64
Maintainer: <@ba8d89490f8f>
Installed-Size: 264979
Depends: mysql-common, libpq5, libsqlite3-0, openssl, libxml2, libxslt1.1, libreadline5, libreadline6, libssl1.0.0, libmysqlclient18, libevent-2.0-5, libevent-core-2.0-5, libevent-extra-2.0-5
Provides: discourse
Section: default
Priority: extra
Homepage: http://www.discourse.org
Filename: pool/d/di/discourse_0.9.8.3-1396474125.12e4179.wheezy_amd64.deb
Size: 85733514
SHA1: 919b7b7860ed0f5850d031072c00a64c6f41657a
SHA256: ab79c879c498086b62289da77d770725939acababcbcd8d8af5cf46e1971fd0e
MD5sum: 11aa00eeb849212667d81ce1904daa4d
Description: A platform for community discussion. Free, open, simple.
 The description can have a continuation line.
 .
 And blank lines.
 .
 If it wants to.
"""


EXPECTED_DESCRIPTION = """A platform for community discussion. Free, open, simple.
 The description can have a continuation line.
 .
 And blank lines.
 .
 If it wants to."""


class TestPackageParseString:
    """Tests for Package.parse_string()."""

    def test_creates_package_with_right_attributes(self):
        """Creates a Package object with the right attributes."""
        pkg = package_module.parse_string(FIXTURES_PACKAGES)
        assert pkg.version == "0.9.8.3"
        assert pkg.epoch is None
        assert pkg.iteration == "1396474125.12e4179.wheezy"
        assert pkg.full_version == "0.9.8.3-1396474125.12e4179.wheezy"

    def test_parses_description(self):
        """Parses description correctly."""
        pkg = package_module.parse_string(FIXTURES_PACKAGES)
        assert "A platform for community discussion" in pkg.description
        assert "continuation line" in pkg.description


class TestPackageFullVersion:
    """Tests for Package.full_version."""

    def test_returns_none_if_no_version(self):
        """Returns nil if no version, epoch, iteration."""
        pkg = package_module.Package()
        assert pkg.full_version is None

    def test_returns_only_version_if_no_epoch_and_iteration(self):
        """Returns only the version if no epoch and no iteration."""
        pkg = package_module.Package(version="0.9.8")
        assert pkg.full_version == "0.9.8"

    def test_returns_epoch_version_if_epoch_and_version(self):
        """Returns epoch:version if epoch and version."""
        import time
        epoch = str(int(time.time()))
        pkg = package_module.Package(version="0.9.8", epoch=epoch)
        assert pkg.full_version == f"{epoch}:0.9.8"

    def test_returns_version_iteration_if_version_and_iteration(self):
        """Returns version-iteration if version and iteration."""
        pkg = package_module.Package(version="0.9.8", iteration="2")
        assert pkg.full_version == "0.9.8-2"

    def test_returns_full_version_if_all_present(self):
        """Returns epoch:version-iteration if epoch and version and iteration."""
        import time
        epoch = str(int(time.time()))
        pkg = package_module.Package(version="0.9.8", iteration="2", epoch=epoch)
        assert pkg.full_version == f"{epoch}:0.9.8-2"
