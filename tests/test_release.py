"""Tests for the Release model."""


from pydeb_s3 import release as release_module


class TestReleaseInterface:
    """Tests for Release class interface."""

    def test_release_has_sign_method(self):
        """Release should have sign() method."""
        r = release_module.Release()
        assert hasattr(r, "sign"), "Release missing sign() method"
        assert callable(r.sign), "sign is not callable"

    def test_release_has_upload_method(self):
        """Release should have upload() method."""
        r = release_module.Release()
        assert hasattr(r, "upload"), "Release missing upload() method"
        assert callable(r.upload), "upload is not callable"

    def test_release_has_generate_method(self):
        """Release should have generate() method."""
        r = release_module.Release()
        assert hasattr(r, "generate"), "Release missing generate() method"
        assert callable(r.generate), "generate is not callable"

    def test_release_has_retrieve_method(self):
        """Release should have retrieve() class method."""
        assert hasattr(release_module.Release, "retrieve"), "Release missing retrieve() class method"
        assert callable(release_module.Release.retrieve), "retrieve is not callable"


class TestReleaseGenerate:
    """Tests for Release.generate()."""

    def test_generates_codename(self):
        """Generates Codename field."""
        r = release_module.Release(codename="testing")
        content = r.generate()
        assert "Codename: testing" in content

    def test_generates_with_origin(self):
        """Generates Origin field."""
        r = release_module.Release(codename="stable", origin="MyRepo")
        content = r.generate()
        assert "Origin: MyRepo" in content

    def test_generates_with_suite(self):
        """Generates Suite field."""
        r = release_module.Release(codename="stable", suite="unstable")
        content = r.generate()
        assert "Suite: unstable" in content

    def test_generates_architectures(self):
        """Generates Architectures field."""
        r = release_module.Release(codename="stable", architectures=["amd64", "arm64"])
        content = r.generate()
        assert "Architectures: amd64 arm64" in content

    def test_generates_components(self):
        """Generates Components field."""
        r = release_module.Release(codename="stable", components=["main", "contrib"])
        content = r.generate()
        assert "Components: main contrib" in content


class TestReleaseFilename:
    """Tests for Release.filename property."""

    def test_returns_correct_path(self):
        """Returns correct filename path."""
        r = release_module.Release(codename="stable")
        assert r.filename == "dists/stable/Release"

    def test_returns_codename_in_path(self):
        """Codename is included in path."""
        r = release_module.Release(codename="testing")
        assert r.filename == "dists/testing/Release"


class TestReleaseSignatureFiles:
    """Tests for Release signature file exclusion."""

    def test_get_signature_files_returns_exact_paths(self):
        """_get_signature_files returns exact paths for codename."""
        r = release_module.Release(codename="stable")
        sig_files = r._get_signature_files()
        assert "dists/stable/InRelease" in sig_files
        assert "dists/stable/Release.gpg" in sig_files
        assert len(sig_files) == 2

    def test_get_signature_files_different_codename(self):
        """_get_signature_files returns correct paths for different codename."""
        r = release_module.Release(codename="testing")
        sig_files = r._get_signature_files()
        assert "dists/testing/InRelease" in sig_files
        assert "dists/testing/Release.gpg" in sig_files


class TestReleaseGenerateExcludesSignatureFiles:
    """Tests for Release.generate() excluding signature files."""

    def test_generate_excludes_inrelease(self):
        """generate() excludes InRelease from hash sections."""
        r = release_module.Release(
            codename="stable",
            files={
                "main/binary-amd64/Packages": {"sha256": "abc123", "size": 100},
                "dists/stable/InRelease": {"sha256": "wronghash", "size": 50},
            },
        )
        content = r.generate()
        assert "main/binary-amd64/Packages" in content
        assert "dists/stable/InRelease" not in content

    def test_generate_excludes_release_gpg(self):
        """generate() excludes Release.gpg from hash sections."""
        r = release_module.Release(
            codename="stable",
            files={
                "main/binary-amd64/Packages": {"sha256": "abc123", "size": 100},
                "dists/stable/Release.gpg": {"sha256": "wronghash", "size": 50},
            },
        )
        content = r.generate()
        assert "main/binary-amd64/Packages" in content
        assert "dists/stable/Release.gpg" not in content

    def test_generate_excludes_both_signature_files(self):
        """generate() excludes both InRelease and Release.gpg."""
        r = release_module.Release(
            codename="stable",
            files={
                "main/binary-amd64/Packages": {"sha256": "abc123", "size": 100},
                "dists/stable/InRelease": {"sha256": "hash1", "size": 50},
                "dists/stable/Release.gpg": {"sha256": "hash2", "size": 50},
            },
        )
        content = r.generate()
        assert "main/binary-amd64/Packages" in content
        assert "InRelease" not in content
        assert "Release.gpg" not in content

    def test_generate_excludes_self_hashes(self):
        """generate() excludes hash entries for Release file itself."""
        r = release_module.Release(
            codename="stable",
            files={
                "main/binary-amd64/Packages": {"sha256": "packagehash", "size": 100},
            },
        )
        content = r.generate()
        # Assert that there are no SHA256, SHA512, MD5sum entries for 'Release' file itself
        assert " Release" not in content # Generic check for any hash entry for Release
        assert "1106 Release" not in content # Specific check for previous content
        assert "1132 Release" not in content # Specific check for previous content

    def test_generate_excludes_self_hashes(self):
        """generate() excludes hash entries for Release file itself."""
        r = release_module.Release(
            codename="stable",
            files={
                "main/binary-amd64/Packages": {"sha256": "packagehash", "size": 100},
            },
        )
        content = r.generate()
        # Assert that there are no SHA256, SHA512, MD5sum entries for 'Release' file itself
        assert " Release" not in content # Generic check for any hash entry for Release
        assert "1106 Release" not in content # Specific check for previous content
        assert "1132 Release" not in content # Specific check for previous content


class TestReleaseParse:
    """Tests for Release._parse()."""

    def test_parse_extracts_codename(self):
        """Parses Codename from content."""
        content = "Codename: myrepo\n"
        r = release_module.Release()
        r._parse(content)
        assert r.codename == "myrepo"

    def test_parse_extracts_origin(self):
        """Parses Origin from content."""
        content = "Origin: TestRepo\nSuite: stable\n"
        r = release_module.Release()
        r._parse(content)
        assert r.origin == "TestRepo"

    def test_parse_extracts_architectures(self):
        """Parses Architectures from content."""
        content = "Architectures: amd64 i386\n"
        r = release_module.Release()
        r._parse(content)
        assert r.architectures == ["amd64", "i386"]

    def test_parse_extracts_components(self):
        """Parses Components from content."""
        content = "Components: main contrib\n"
        r = release_module.Release()
        r._parse(content)
        assert r.components == ["main", "contrib"]

    def test_parse_extracts_sha256_hash(self):
        """Parses SHA256 hash entries."""
        content = """Codename: stable
Architectures: amd64
Components: main

SHA256:
 abcdef123456 1898 main/binary-amd64/Packages
"""
        r = release_module.Release()
        r._parse(content)
        assert "main/binary-amd64/Packages" in r.files
        assert r.files["main/binary-amd64/Packages"]["sha256"] == "abcdef123456"
        assert r.files["main/binary-amd64/Packages"]["size"] == 1898

    def test_parse_extracts_sha1_hash(self):
        """Parses SHA1 hash entries."""
        content = """Codename: stable
Architectures: amd64

SHA1:
 abcdef1234567890 1898 main/binary-amd64/Packages
"""
        r = release_module.Release()
        r._parse(content)
        assert "main/binary-amd64/Packages" in r.files
        assert r.files["main/binary-amd64/Packages"]["sha1"] == "abcdef1234567890"

    def test_parse_extracts_md5_hash(self):
        """Parses MD5 hash entries."""
        content = """Codename: stable
MD5Sum:
 abcdef12 1898 main/binary-amd64/Packages
"""
        r = release_module.Release()
        r._parse(content)
        assert "main/binary-amd64/Packages" in r.files
        assert r.files["main/binary-amd64/Packages"]["md5"] == "abcdef12"

    def test_parse_extracts_all_hash_types(self):
        """Parses all hash types from Release content."""
        content = """Codename: stable
SHA256:
 hash256val 1898 main/binary-amd64/Packages
SHA1:
 hash1val 1898 main/binary-amd64/Packages
MD5Sum:
 hash5val 1898 main/binary-amd64/Packages
"""
        r = release_module.Release()
        r._parse(content)
        assert "main/binary-amd64/Packages" in r.files
        assert r.files["main/binary-amd64/Packages"]["sha256"] == "hash256val"
        assert r.files["main/binary-amd64/Packages"]["sha1"] == "hash1val"
        assert r.files["main/binary-amd64/Packages"]["md5"] == "hash5val"

    def test_parse_handles_packages_gz(self):
        """Parses Packages.gz hash entries."""
        content = """Codename: stable
SHA256:
 gzhash256 565 main/binary-amd64/Packages.gz
"""
        r = release_module.Release()
        r._parse(content)
        assert "main/binary-amd64/Packages.gz" in r.files
        assert r.files["main/binary-amd64/Packages.gz"]["sha256"] == "gzhash256"
