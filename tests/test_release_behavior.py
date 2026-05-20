"""Integration tests for Release signing behavior."""

import os
import tempfile
from unittest.mock import patch, MagicMock, PropertyMock

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

    def test_sign_calls_detach_sign_before_clearsign(self, mock_s3_adapter):
        """sign() should call detach_sign BEFORE clearsign.

        This is critical: GpgSigningAdapter produces input_path.asc then renames
        it to output_path. To ensure .asc ends up as clearsigned content:
        1. Run detach_sign first -> produces temp.asc, renames to temp.detached
        2. Run clearsign second -> produces fresh temp.asc, renames to temp.asc

        If clearsign runs last, its output gets overwritten by detach_sign.
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

        # Verify detach_sign was called BEFORE clearsign (critical for the fix)
        calls = mock_signing.method_calls
        clearsign_idx = next(i for i, c in enumerate(calls) if c[0] == "clearsign")
        detach_idx = next(i for i, c in enumerate(calls) if c[0] == "detach_sign")
        assert detach_idx < clearsign_idx, "detach_sign must be called before clearsign"

    def test_detach_sign_and_clearsign_use_distinct_paths(self, mock_s3_adapter):
        """sign() should use distinct paths for clearsign and detach_sign.

        This is a critical bug fix: both methods were using the same output path,
        causing detach_sign to overwrite the clearsigned output. The InRelease
        file would then contain a detached signature instead of a clearsigned
        message, causing APT to reject it with 'Clearsigned file isn't valid'.
        """
        mock_signing = MagicMock()
        mock_signing.get_key_info.return_value = {"keys": ["test-key"]}

        release = release_module.Release(
            codename="stable",
            files={"main/binary-amd64/Packages": {"sha256": "abc123", "size": 100}},
        )

        # Don't mock tempfile — use a real temp file so path concatenation works
        # (MagicMock's __add__ returns MagicMock, not real strings)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".Release", delete=False) as f:
            f.write("test content")
            temp_path = f.name

        try:
            release.sign(mock_s3_adapter, mock_signing)
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            asc_path = temp_path + ".asc"
            if os.path.exists(asc_path):
                os.unlink(asc_path)
            detached_path = temp_path + ".detached"
            if os.path.exists(detached_path):
                os.unlink(detached_path)

        # Get the paths passed to each method
        clearsign_call = mock_signing.clearsign.call_args
        detach_call = mock_signing.detach_sign.call_args

        clearsign_output_path = clearsign_call[0][1]  # Second positional arg
        detach_output_path = detach_call[0][1]  # Second positional arg

        # CRITICAL: These paths must be different
        # The fix uses: detached_path = temp + ".detached" and clearsigned_path = temp + ".asc"
        assert clearsign_output_path != detach_output_path, (
            f"clearsign and detach_sign must use distinct output paths, "
            f"but both got: {clearsign_output_path}"
        )

        # Verify the expected suffixes
        assert clearsign_output_path.endswith(".asc"), (
            f"clearsign output should end with .asc, got: {clearsign_output_path}"
        )
        assert detach_output_path.endswith(".detached"), (
            f"detach_sign output should end with .detached, got: {detach_output_path}"
        )

    def test_inrelease_gets_clearsigned_content(self, mock_s3_adapter):
        """InRelease upload should read from the clearsign output path.

        The InRelease file must contain a clearsigned message
        (-----BEGIN PGP SIGNED MESSAGE-----), not a detached signature.
        """
        mock_signing = MagicMock()
        mock_signing.get_key_info.return_value = {"keys": ["test-key"]}

        release = release_module.Release(
            codename="stable",
            files={"main/binary-amd64/Packages": {"sha256": "abc123", "size": 100}},
        )

        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_file = MagicMock()
            mock_file.name = "/tmp/test.Release"
            mock_file.close = MagicMock()
            mock_temp.return_value.__enter__.return_value = mock_file

            with patch("os.unlink"):
                release.sign(mock_s3_adapter, mock_signing)

        # Get the paths passed to store_file calls
        store_calls = mock_s3_adapter.store_file.call_args_list

        # Find the InRelease upload (should be the second store_file call)
        # First call is unsigned Release, second is InRelease, third is Release.gpg
        inrelease_call = store_calls[1]
        inrelease_local_path = inrelease_call[0][0]  # First positional arg

        # Get the clearsign output path
        clearsign_output_path = mock_signing.clearsign.call_args[0][1]

        # InRelease should read from the clearsign output path
        assert inrelease_local_path == clearsign_output_path, (
            f"InRelease should read from clearsign output path ({clearsign_output_path}), "
            f"but got: {inrelease_local_path}"
        )

    def test_release_gpg_gets_detached_signature(self, mock_s3_adapter):
        """Release.gpg upload should read from the detach_sign output path."""
        mock_signing = MagicMock()
        mock_signing.get_key_info.return_value = {"keys": ["test-key"]}

        release = release_module.Release(
            codename="stable",
            files={"main/binary-amd64/Packages": {"sha256": "abc123", "size": 100}},
        )

        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_file = MagicMock()
            mock_file.name = "/tmp/test.Release"
            mock_file.close = MagicMock()
            mock_temp.return_value.__enter__.return_value = mock_file

            with patch("os.unlink"):
                release.sign(mock_s3_adapter, mock_signing)

        # Get the paths passed to store_file calls
        store_calls = mock_s3_adapter.store_file.call_args_list

        # Release.gpg is the third store_file call
        gpg_call = store_calls[2]
        gpg_local_path = gpg_call[0][0]  # First positional arg

        # Get the detach_sign output path
        detach_output_path = mock_signing.detach_sign.call_args[0][1]

        # Release.gpg should read from the detach_sign output path
        assert gpg_local_path == detach_output_path, (
            f"Release.gpg should read from detach_sign output path ({detach_output_path}), "
            f"but got: {gpg_local_path}"
        )

    def test_detach_sign_runs_before_clearsign(self, mock_s3_adapter):
        """detach_sign must run BEFORE clearsign so .asc ends up as clearsigned.

        The GpgSigningAdapter produces input_path.asc and renames it to output_path.
        To ensure the final .asc file contains clearsigned content:
        1. Run detach_sign first -> produces temp.asc, renames to temp.detached
        2. Run clearsign second -> produces fresh temp.asc, renames to temp.asc

        If clearsign runs first, its output gets overwritten by detach_sign.
        """
        mock_signing = MagicMock()
        mock_signing.get_key_info.return_value = {"keys": ["test-key"]}

        release = release_module.Release(
            codename="stable",
            files={"main/binary-amd64/Packages": {"sha256": "abc123", "size": 100}},
        )

        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_file = MagicMock()
            mock_file.name = "/tmp/test.Release"
            mock_file.close = MagicMock()
            mock_temp.return_value.__enter__.return_value = mock_file

            with patch("os.unlink"):
                release.sign(mock_s3_adapter, mock_signing)

        # Get call order
        calls = mock_signing.method_calls

        # Find indices of each call
        detach_idx = next(i for i, c in enumerate(calls) if c[0] == "detach_sign")
        clearsign_idx = next(i for i, c in enumerate(calls) if c[0] == "clearsign")

        # detach_sign must run BEFORE clearsign
        assert detach_idx < clearsign_idx, (
            f"detach_sign must be called before clearsign to ensure .asc "
            f"contains clearsigned content. Got order: {calls}"
        )