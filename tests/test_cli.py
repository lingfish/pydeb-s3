"""Tests for the CLI commands."""


from unittest.mock import patch

from typer.testing import CliRunner

from pydeb_s3.cli import app

runner = CliRunner()


class TestUploadValidation:
    """Tests for upload command validation."""

    def test_requires_bucket(self):
        """upload fails without --bucket."""
        result = runner.invoke(app, ["upload", "file.deb"])
        assert result.exit_code != 0

    def test_requires_files(self):
        """upload fails without files."""
        result = runner.invoke(app, ["upload", "-b", "mybucket"])
        assert result.exit_code != 0

    def test_file_not_found(self):
        """upload fails when file doesn't exist."""
        with patch("glob.glob", return_value=[]):
            result = runner.invoke(app, ["upload", "-b", "mybucket", "nonexistent.deb"])
            assert result.exit_code != 0

    def test_shows_help(self):
        """upload shows help."""
        result = runner.invoke(app, ["upload", "--help"])
        assert result.exit_code == 0


class TestListValidation:
    """Tests for list command validation."""

    def test_requires_bucket(self):
        """list fails without --bucket."""
        result = runner.invoke(app, ["list"])
        assert result.exit_code != 0

    def test_shows_help(self):
        """list shows help."""
        result = runner.invoke(app, ["list", "--help"])
        assert result.exit_code == 0


class TestShowValidation:
    """Tests for show command validation."""

    def test_requires_bucket(self):
        """show fails without --bucket."""
        result = runner.invoke(app, ["show", "mypackage"])
        assert result.exit_code != 0

    def test_requires_package_argument(self):
        """show requires package argument."""
        result = runner.invoke(app, ["show"])
        assert result.exit_code != 0

    def test_shows_help(self):
        """show shows help."""
        result = runner.invoke(app, ["show", "--help"])
        assert result.exit_code == 0


class TestExistsValidation:
    """Tests for exists command validation."""

    def test_requires_bucket(self):
        """exists fails without --bucket."""
        result = runner.invoke(app, ["exists", "mypackage"])
        assert result.exit_code != 0

    def test_requires_package_argument(self):
        """exists requires package argument."""
        result = runner.invoke(app, ["exists"])
        assert result.exit_code != 0

    def test_shows_help(self):
        """exists shows help."""
        result = runner.invoke(app, ["exists", "--help"])
        assert result.exit_code == 0


class TestCopyValidation:
    """Tests for copy command validation."""

    def test_requires_bucket(self):
        """copy fails without --bucket."""
        result = runner.invoke(app, ["copy", "mypackage", "--to-codename", "stable", "--to-component", "main"])
        assert result.exit_code != 0

    def test_requires_package_argument(self):
        """copy requires package argument."""
        result = runner.invoke(app, ["copy"])
        assert result.exit_code != 0

    def test_requires_to_codename(self):
        """copy requires --to-codename."""
        result = runner.invoke(app, ["copy", "mypackage", "-b", "mybucket"])
        assert result.exit_code != 0

    def test_requires_to_component(self):
        """copy requires --to-component."""
        result = runner.invoke(app, ["copy", "mypackage", "-b", "mybucket", "--to-codename", "stable"])
        assert result.exit_code != 0

    def test_shows_help(self):
        """copy shows help."""
        result = runner.invoke(app, ["copy", "--help"])
        assert result.exit_code == 0


class TestDeleteValidation:
    """Tests for delete command validation."""

    def test_requires_bucket(self):
        """delete fails without --bucket."""
        result = runner.invoke(app, ["delete", "mypackage"])
        assert result.exit_code != 0

    def test_requires_package_argument(self):
        """delete requires package argument."""
        result = runner.invoke(app, ["delete"])
        assert result.exit_code != 0

    def test_shows_help(self):
        """delete shows help."""
        result = runner.invoke(app, ["delete", "--help"])
        assert result.exit_code == 0


class TestVerifyValidation:
    """Tests for verify command validation."""

    def test_requires_bucket(self):
        """verify fails without --bucket."""
        result = runner.invoke(app, ["verify"])
        assert result.exit_code != 0

    def test_shows_help(self):
        """verify shows help."""
        result = runner.invoke(app, ["verify", "--help"])
        assert result.exit_code == 0


class TestCleanValidation:
    """Tests for clean command validation."""

    def test_requires_bucket(self):
        """clean fails without --bucket."""
        result = runner.invoke(app, ["clean"])
        assert result.exit_code != 0

    def test_shows_help(self):
        """clean shows help."""
        result = runner.invoke(app, ["clean", "--help"])
        assert result.exit_code == 0


class TestCLICommands:
    """Tests for CLI command registration."""

    def test_all_commands_registered(self):
        """All expected commands are registered."""
        res = runner.invoke(app, ["--help"])
        assert res.exit_code == 0
        assert "upload" in res.output
        assert "list" in res.output
        assert "show" in res.output
        assert "exists" in res.output
        assert "copy" in res.output
        assert "delete" in res.output
        assert "verify" in res.output
        assert "clean" in res.output
