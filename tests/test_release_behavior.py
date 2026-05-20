"""Integration tests for Release signing behavior."""

import os
import tempfile
from unittest.mock import patch, MagicMock

import pytest

from pydeb_s3 import release as release_module


class TestGpgSigningAdapterCommands:
    """Tests for GpgSigningAdapter GPG command construction."""

    @pytest.fixture
    def temp_file(self):
        """Create a temporary file for testing."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".Release", delete=False) as f:
            f.write("Test release content")
            temp_path = f.name
        yield temp_path
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        asc_path = temp_path + ".asc"
        if os.path.exists(asc_path):
            os.unlink(asc_path)

    def test_clearsign_includes_batch_flags(self, temp_file):
        """clearsign() should include --batch --no-tty --yes flags for non-interactive GPG.

        These flags are required for CI environments where /dev/tty is not available.
        Without them, GPG prompts for confirmation (e.g., overwrite) which fails in CI.
        """
        adapter = release_module.GpgSigningAdapter(keys=["test-key-123"])

        # Mock subprocess.run to capture the command and create output file
        with patch("subprocess.run") as mock_run, \
             patch("os.path.exists") as mock_exists, \
             patch("os.rename"):
            # Make the mock return success
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = b""
            mock_run.return_value = mock_result

            # Mock os.path.exists to return True for the .asc file
            def exists_side_effect(path):
                return path.endswith(".asc")
            mock_exists.side_effect = exists_side_effect

            output_path = temp_file + ".asc"
            adapter.clearsign(temp_file, output_path)

            # Verify subprocess.run was called
            assert mock_run.called, "subprocess.run should have been called"

            # Get the command that was executed
            call_args = mock_run.call_args
            cmd = call_args[0][0]  # First positional argument

            # Verify batch flags are present
            assert "--batch" in cmd, "clearsign should include --batch flag"
            assert "--no-tty" in cmd, "clearsign should include --no-tty flag"
            assert "--yes" in cmd, "clearsign should include --yes flag"

    def test_detach_sign_includes_batch_flags(self, temp_file):
        """detach_sign() should include --batch --no-tty --yes flags for non-interactive GPG.

        These flags are required for CI environments where /dev/tty is not available.
        Without them, GPG prompts for confirmation (e.g., overwrite) which fails in CI.
        """
        adapter = release_module.GpgSigningAdapter(keys=["test-key-123"])

        # Mock subprocess.run to capture the command and create output file
        with patch("subprocess.run") as mock_run, \
             patch("os.path.exists") as mock_exists, \
             patch("os.rename"):
            # Make the mock return success
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = b""
            mock_run.return_value = mock_result

            # Mock os.path.exists to return True for the .asc file
            def exists_side_effect(path):
                return path.endswith(".asc")
            mock_exists.side_effect = exists_side_effect

            output_path = temp_file + ".asc"
            adapter.detach_sign(temp_file, output_path)

            # Verify subprocess.run was called
            assert mock_run.called, "subprocess.run should have been called"

            # Get the command that was executed
            call_args = mock_run.call_args
            cmd = call_args[0][0]  # First positional argument

            # Verify batch flags are present
            assert "--batch" in cmd, "detach_sign should include --batch flag"
            assert "--no-tty" in cmd, "detach_sign should include --no-tty flag"
            assert "--yes" in cmd, "detach_sign should include --yes flag"

    def test_clearsign_batch_flags_before_options(self, temp_file):
        """Batch flags should come before user-provided options.

        This allows user options to override batch flags if needed.
        """
        adapter = release_module.GpgSigningAdapter(
            keys=["test-key-123"],
            options="--pinentry-mode loopback"
        )

        with patch("subprocess.run") as mock_run, \
             patch("os.path.exists") as mock_exists, \
             patch("os.rename"):
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = b""
            mock_run.return_value = mock_result

            # Mock os.path.exists to return True for the .asc file
            def exists_side_effect(path):
                return path.endswith(".asc")
            mock_exists.side_effect = exists_side_effect

            output_path = temp_file + ".asc"
            adapter.clearsign(temp_file, output_path)

            call_args = mock_run.call_args
            cmd = call_args[0][0]

            # Verify batch flags come before options
            batch_idx = cmd.index("--batch")
            options_idx = cmd.index("--pinentry-mode")
            assert batch_idx < options_idx, "Batch flags should come before user options"

    def test_detach_sign_batch_flags_before_options(self, temp_file):
        """Batch flags should come before user-provided options.

        This allows user options to override batch flags if needed.
        """
        adapter = release_module.GpgSigningAdapter(
            keys=["test-key-123"],
            options="--pinentry-mode loopback"
        )

        with patch("subprocess.run") as mock_run, \
             patch("os.path.exists") as mock_exists, \
             patch("os.rename"):
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = b""
            mock_run.return_value = mock_result

            # Mock os.path.exists to return True for the .asc file
            def exists_side_effect(path):
                return path.endswith(".asc")
            mock_exists.side_effect = exists_side_effect

            output_path = temp_file + ".asc"
            adapter.detach_sign(temp_file, output_path)

            call_args = mock_run.call_args
            cmd = call_args[0][0]

            # Verify batch flags come before options
            batch_idx = cmd.index("--batch")
            options_idx = cmd.index("--pinentry-mode")
            assert batch_idx < options_idx, "Batch flags should come before user options"


class TestReleaseSignMethod:
    """Tests for Release.sign() method."""

    @pytest.fixture
    def mock_s3_adapter(self):
        """Create a mock S3 adapter."""
        adapter = MagicMock()
        adapter.store_file = MagicMock()
        return adapter

    def test_sign_calls_clearsign_then_detach_sign(self, mock_s3_adapter):
        """sign() should call clearsign followed by detach_sign.

        This tests the sequential signing flow where both clearsigned and
        detached signatures are created from the same Release file.
        """
        # Create a mock signing adapter that tracks calls
        mock_signing = MagicMock()
        mock_signing.get_key_info.return_value = {"keys": ["test-key"]}

        release = release_module.Release(
            codename="stable",
            files={"main/binary-amd64/Packages": {"sha256": "abc123", "size": 100}},
        )

        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            # Setup mock temp file
            mock_file = MagicMock()
            mock_file.name = "/tmp/test.Release"
            mock_file.close = MagicMock()
            mock_temp.return_value.__enter__.return_value = mock_file

            with patch("os.unlink"):
                release.sign(mock_s3_adapter, mock_signing)

        # Verify both signing methods were called
        assert mock_signing.clearsign.called, "clearsign should be called"
        assert mock_signing.detach_sign.called, "detach_sign should be called"

        # Verify clearsign was called before detach_sign
        calls = mock_signing.method_calls
        clearsign_idx = next(i for i, c in enumerate(calls) if c[0] == "clearsign")
        detach_idx = next(i for i, c in enumerate(calls) if c[0] == "detach_sign")
        assert clearsign_idx < detach_idx, "clearsign should be called before detach_sign"