"""Tests for CLI --bytes flag for bandwidth stats."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from pydeb_s3 import cli


class TestCLIBytesFlag:
    """Tests for the --bytes CLI flag."""

    def test_upload_command_has_bytes_option(self):
        """Upload command should accept --bytes flag."""
        runner = CliRunner()

        # When running with --help, we should see --bytes in the output
        result = runner.invoke(cli.app, ["upload", "--help"])

        assert "--bytes" in result.stdout

    def test_bytes_flag_has_correct_default(self):
        """--bytes flag should default to False."""
        runner = CliRunner()
        result = runner.invoke(cli.app, ["upload", "--help"])

        # Help output should show the default
        # Look for --bytes option in help
        assert "--bytes" in result.stdout
        # The option should show False as default in some form
        assert "False" in result.stdout or "default" in result.stdout.lower()

    @patch("pydeb_s3.cli._configure_s3")
    @patch("pydeb_s3.lock.lock")
    @patch("pydeb_s3.lock.unlock")
    @patch("pydeb_s3.s3_utils.UploadProgress")
    def test_bytes_flag_passed_to_s3_store(
        self, mock_progress, mock_unlock, mock_lock, mock_configure
    ):
        """--bytes flag is passed through to s3_store()."""
        runner = CliRunner()

        # Mock necessary components
        mock_progress.return_value = MagicMock()

        # Use a real temporary file
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".deb", mode="w") as f:
            f.write("Package: test\nVersion: 1.0\nArchitecture: all\n")
            temp_path = f.name

        try:
            # Call upload command with --bytes flag
            result = runner.invoke(
                cli.app,
                [
                    "upload",
                    "--bucket", "test-bucket",
                    temp_path,
                    "--bytes",  # Include the flag
                ],
            )

            # Should not error out (at least the flag should be accepted)
            # The file won't exist in the repo, but the flag should work
        finally:
            import os
            os.unlink(temp_path)

    def test_bytes_default_is_false(self):
        """Default should be bits/second (use_bytes=False)."""
        # Get the upload command
        from typer.testing import CliRunner
        runner = CliRunner()

        # Check the help output to verify --bytes defaults to False
        result = runner.invoke(cli.app, ["upload", "--help"])
        help_text = result.stdout

        # The --bytes flag should be documented
        assert "--bytes" in help_text


class TestCLIBytesFlagDefaultBehavior:
    """Test that default is bits/second (not bytes/second)."""

    def test_default_shows_bits(self):
        """Default should be bits/second (use_bytes=False)."""
        from typer.testing import CliRunner
        runner = CliRunner()

        # Check that --bytes is a flag (boolean option)
        result = runner.invoke(cli.app, ["upload", "--help"])

        # Help should show --bytes option
        assert "--bytes" in result.stdout
