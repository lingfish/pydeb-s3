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

FIXTURE_HELLO_CONTROL_STRING = """Package: hello
Version: 2.10-5
Architecture: amd64
Maintainer: Santiago Vila <sanvila@debian.org>
Installed-Size: 280
Depends: libc6 (>= 2.38)
Conflicts: hello-traditional
Breaks: hello-debhelper (<< 2.9)
Replaces: hello-debhelper (<< 2.9), hello-traditional
Section: devel
Priority: optional
Homepage: https://www.gnu.org/software/hello/
Description: example package based on GNU hello
 The GNU hello program produces a familiar, friendly greeting. It
 allows non-programmers to use a classic computer science tool which
 would otherwise be unavailable to them.
 .
 Seriously, though: this is an example of how to do a Debian package.
 It is the Debian version of the GNU Project's `hello world' program
 (which is itself an example for the GNU Project)."""



class TestPackageParseString:
    """Tests for Package.parse_string()."""

    def test_creates_package_with_right_attributes(self):
        """Creates a Package object with the right attributes."""
        pkg = package_module.parse_string(FIXTURES_PACKAGES)
        assert pkg.version == "0.9.8.3"
        assert pkg.epoch is None
        assert pkg.iteration == "1396474125.12e4179.wheezy"
        assert pkg.full_version == "0.9.8.3-1396474125.12e4179.wheezy"
        assert pkg.license == "unknown"
        assert pkg.vendor == "@ba8d89490f8f"
        assert pkg.priority == "extra"

    def test_parses_description(self):
        """Parses description correctly."""
        pkg = package_module.parse_string(FIXTURES_PACKAGES)
        assert "A platform for community discussion" in pkg.description
        assert "The description can have a continuation line." in pkg.description

    def test_parses_hello_deb_fields(self):
        """Parses hello.deb fields correctly from a real .deb file using parse_file."""
        pkg = package_module.Package.parse_file("tests/fixtures/hello_2.10-5_amd64.deb")
        assert pkg.name == "hello"
        assert pkg.version == "2.10"
        assert pkg.architecture == "amd64"
        assert pkg.maintainer == "Santiago Vila <sanvila@debian.org>"
        assert pkg.attributes["deb_installed_size"] == "280"
        assert "libc6 (>= 2.38)" in ', '.join(pkg.dependencies)
        assert pkg.attributes["deb_conflicts"] == "hello-traditional"
        assert pkg.attributes["deb_breaks"] == "hello-debhelper (<< 2.9)"
        assert pkg.attributes["deb_replaces"] == "hello-debhelper (<< 2.9), hello-traditional"
        assert pkg.category == "devel"
        assert pkg.priority == "optional"
        assert pkg.url == "https://www.gnu.org/software/hello/"
        assert pkg.description.startswith("example package based on GNU hello")
        assert pkg.filename.endswith("hello_2.10-5_amd64.deb")

    def test_generates_description_last_and_formatted(self):
        """Generated Packages content should have Description as the last field, correctly formatted."""
        pkg = package_module.Package(
            name="test-pkg",
            version="1.0.0",
            architecture="amd64",
            maintainer="Test <test@example.com>",
            description=EXPECTED_DESCRIPTION,
            license="MIT",
            vendor="ExampleCorp",
            category="misc",
            priority="optional",
            url="http://example.com",
            dependencies=["depA (>= 1.0)", "depB"],
            attributes={
                "deb_installed_size": 1234,
                "deb_recommends": "dependencyA",
            },
            sha256="hash256",
            sha1="hash1",
            md5="hashmd5",
            size=5678,
            filename="/tmp/test-pkg_1.0.0_amd64.deb",
        )
        # Mock url_filename_for as it relies on os.path.basename(self.filename)
        # and component, which is not directly part of the Package object construction.
        # For this test, we just need a predictable value.
        pkg.url_filename_for = lambda component: f"pool/{component}/t/test-pkg_1.0.0_amd64.deb"

        generated_content = pkg.generate("main")

        # Split into lines for easier assertion
        lines = generated_content.strip().split("\n")

        # Verify Description is the very last field and correctly formatted
        # Using strip() on both sides to avoid issues with trailing spaces or newlines in fixture
        assert lines[-1].strip() == "If it wants to."
        assert lines[-2].strip() == "."
        assert lines[-3].strip() == "And blank lines."
        assert lines[-4].strip() == "."
        assert lines[-5].strip() == "The description can have a continuation line."
        assert lines[-6].strip() == "Description: A platform for community discussion. Free, open, simple."

        # Verify that other fields are present (not exhaustive check, mainly for order)
        assert "Package: test-pkg" in generated_content
        assert "Version: 1.0.0" in generated_content
        assert "License: MIT" in generated_content
        assert "Vendor: ExampleCorp" in generated_content
        assert "Architecture: amd64" in generated_content
        assert "Maintainer: Test <test@example.com>" in generated_content
        assert "Installed-Size: 1234" in generated_content
        assert "Depends: depA (>= 1.0), depB" in generated_content
        assert "Recommends: dependencyA" in generated_content
        assert "Section: misc" in generated_content
        assert "Priority: optional" in generated_content
        assert "Homepage: http://example.com" in generated_content
        assert "Filename: pool/main/t/test-pkg_1.0.0_amd64.deb" in generated_content
        assert "Size: 5678" in generated_content
        assert "SHA1: hash1" in generated_content
        assert "SHA256: hash256" in generated_content
        assert "MD5sum: hashmd5" in generated_content


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


class TestPackageParseFile:
    """Tests for Package.parse_file()."""

    def test_parses_amd64_deb(self):
        """Parses amd64 .deb file correctly."""
        pkg = package_module.Package.parse_file(
            "tests/fixtures/test-pkg_1.0.0_amd64.deb"
        )
        assert pkg.architecture == "amd64"
        assert pkg.name == "test-pkg"
        assert pkg.version == "1.0.0"
        assert pkg.filename.endswith("test-pkg_1.0.0_amd64.deb")

    def test_parses_arm64_deb(self):
        """Parses arm64 .deb file correctly."""
        pkg = package_module.Package.parse_file(
            "tests/fixtures/test-pkg_1.0.0_arm64.deb"
        )
        assert pkg.architecture == "arm64"

    def test_parses_all_arch_deb(self):
        """Parses all architecture .deb file correctly."""
        pkg = package_module.Package.parse_file(
            "tests/fixtures/test-pkg_1.0.0_all.deb"
        )
        assert pkg.architecture == "all"

    def test_extracts_name(self):
        """Extracts package name from .deb file."""
        pkg = package_module.Package.parse_file(
            "tests/fixtures/test-pkg_1.0.0_amd64.deb"
        )
        assert pkg.name == "test-pkg"

    def test_extracts_version(self):
        """Extracts package version from .deb file."""
        pkg = package_module.Package.parse_file(
            "tests/fixtures/test-pkg_1.0.0_amd64.deb"
        )
        assert pkg.version == "1.0.0"

    def test_has_filename(self):
        """Sets filename attribute from .deb file path."""
        pkg = package_module.Package.parse_file(
            "tests/fixtures/test-pkg_1.0.0_amd64.deb"
        )
        assert pkg.filename is not None
        assert "test-pkg" in pkg.filename


class TestPackagePoolPath:
    """Tests for package pool path generation."""

    def test_url_filename_uses_component(self):
        """Pool path should use component, not codename, for sharing across suites."""
        pkg = package_module.Package()
        pkg.name = "test-pkg"
        pkg.filename = "/path/to/test-pkg_1.0.0_amd64.deb"

        path = pkg.url_filename_for("main")
        assert path.startswith("pool/main/")
        assert "test-pkg" in path

    def test_url_filename_with_different_components(self):
        """Same package can have different paths for different components."""
        pkg = package_module.Package()
        pkg.name = "test-pkg"
        pkg.filename = "/path/to/test-pkg_1.0.0_amd64.deb"

        main_path = pkg.url_filename_for("main")
        contrib_path = pkg.url_filename_for("contrib")
        nonfree_path = pkg.url_filename_for("non-free")

        assert "pool/main/" in main_path
        assert "pool/contrib/" in contrib_path
        assert "pool/non-free/" in nonfree_path


class TestHelloPackage:
    """Tests for the hello package."""

    def test_generates_hello_deb_output(self):
        """Generates a Packages entry for hello.deb and parses it back."""
        original_pkg = package_module.Package.parse_file(
            "tests/fixtures/hello_2.10-5_amd64.deb"
        )
        original_pkg.url_filename_for = lambda component: f"pool/{component}/h/hello_2.10-5_amd64.deb"

        generated_content = original_pkg.generate("main")
        parsed_pkg = package_module.parse_string(generated_content)

        assert parsed_pkg.name == original_pkg.name
        assert parsed_pkg.version == original_pkg.version
        assert parsed_pkg.architecture == original_pkg.architecture
        assert parsed_pkg.maintainer == original_pkg.maintainer
        assert parsed_pkg.attributes["deb_installed_size"] == "280" # This comes from dpkg -I
        assert parsed_pkg.dependencies == original_pkg.dependencies
        assert parsed_pkg.attributes["deb_conflicts"] == original_pkg.attributes["deb_conflicts"]
        assert parsed_pkg.attributes["deb_breaks"] == original_pkg.attributes["deb_breaks"]
        assert parsed_pkg.attributes["deb_replaces"] == original_pkg.attributes["deb_replaces"]
        assert parsed_pkg.category == original_pkg.category
        assert parsed_pkg.priority == original_pkg.priority
        assert parsed_pkg.url == original_pkg.url
        assert parsed_pkg.description == original_pkg.description
        assert parsed_pkg.url_filename.endswith("hello_2.10-5_amd64.deb")
        # Hashes and size are dynamic, just check for presence after parsing back
        assert parsed_pkg.size is not None
        assert parsed_pkg.sha1 is not None
        assert parsed_pkg.sha256 is not None
        assert parsed_pkg.md5 is not None

