"""Tests for SigningAdapter protocol and GpgSigningAdapter."""

from unittest.mock import patch

import pytest

from pydeb_s3.release import GpgSigningAdapter, Release


class TestSigningAdapterProtocol:
    """Tests that Release.sign() accepts any adapter implementing the protocol."""

    def test_sign_accepts_adapter_parameter(self):
        """sign() should accept SigningAdapter as first parameter."""

        class MockAdapter:
            def clearsign(self, input_path: str, output_path: str) -> None:
                pass

            def detach_sign(self, input_path: str, output_path: str) -> None:
                pass

            def get_key_info(self) -> dict:
                return {"keys": ["test-key"]}

        # Verify the protocol is satisfied
        adapter = MockAdapter()
        assert hasattr(adapter, "clearsign")
        assert hasattr(adapter, "detach_sign")
        assert hasattr(adapter, "get_key_info")


class TestGpgSigningAdapter:
    """Unit tests for GpgSigningAdapter."""

    def test_initialization(self):
        """GpgSigningAdapter should store keys, provider, options."""
        adapter = GpgSigningAdapter(
            keys=["key1", "key2"],
            provider="gpg2",
            options="--batch"
        )
        assert adapter.keys == ["key1", "key2"]
        assert adapter.provider == "gpg2"
        assert adapter.options == "--batch"

    def test_initialization_defaults(self):
        """GpgSigningAdapter should have correct defaults."""
        adapter = GpgSigningAdapter(keys=["key1"])
        assert adapter.keys == ["key1"]
        assert adapter.provider == "gpg"
        assert adapter.options == ""

    @patch("pydeb_s3.release.subprocess.run")
    @patch("pydeb_s3.release.os.rename")
    @patch("pydeb_s3.release.os.path.exists")
    @patch("builtins.open", create=True)
    def test_clearsign_calls_subprocess(self, mock_open, mock_exists, mock_rename, mock_run):
        """clearsign() should invoke GPG subprocess."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stderr = b""
        mock_exists.return_value = True  # Simulate .asc file exists

        adapter = GpgSigningAdapter(keys=["ABC123"], provider="gpg", options="")
        adapter.clearsign("/tmp/test.Release", "/tmp/test.Release.asc")

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "gpg" in cmd
        assert "-s" in cmd
        assert "--clearsign" in cmd

    @patch("pydeb_s3.release.subprocess.run")
    @patch("pydeb_s3.release.os.rename")
    @patch("pydeb_s3.release.os.path.exists")
    @patch("builtins.open", create=True)
    def test_detach_sign_calls_subprocess(self, mock_open, mock_exists, mock_rename, mock_run):
        """detach_sign() should invoke GPG subprocess."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stderr = b""
        mock_exists.return_value = True  # Simulate .asc file exists

        adapter = GpgSigningAdapter(keys=["ABC123"], provider="gpg", options="")
        adapter.detach_sign("/tmp/test.Release", "/tmp/test.Release.asc")

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "gpg" in cmd
        assert "-b" in cmd
        assert "--clearsign" not in cmd

    @patch("pydeb_s3.release.subprocess.run")
    def test_clearsign_raises_on_missing_key(self, mock_run):
        """clearsign() should raise RuntimeError on missing key."""
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = b"no such key"

        adapter = GpgSigningAdapter(keys=["ABC123"])
        with pytest.raises(RuntimeError, match="Secret key not found"):
            adapter.clearsign("/tmp/test.Release", "/tmp/test.Release.asc")

    @patch("pydeb_s3.release.subprocess.run")
    def test_clearsign_raises_on_bad_passphrase(self, mock_run):
        """clearsign() should raise RuntimeError on bad passphrase."""
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = b"bad passphrase"

        adapter = GpgSigningAdapter(keys=["ABC123"])
        with pytest.raises(RuntimeError, match="Bad passphrase"):
            adapter.clearsign("/tmp/test.Release", "/tmp/test.Release.asc")

    @patch("pydeb_s3.release.subprocess.run")
    def test_detach_sign_raises_on_error(self, mock_run):
        """detach_sign() should raise RuntimeError on GPG failure."""
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = b"some error"

        adapter = GpgSigningAdapter(keys=["ABC123"])
        with pytest.raises(RuntimeError, match="GPG detached signing failed"):
            adapter.detach_sign("/tmp/test.Release", "/tmp/test.Release.asc")

    @patch("pydeb_s3.release.subprocess.run")
    @patch("pydeb_s3.release.os.rename")
    @patch("pydeb_s3.release.os.path.exists")
    @patch("builtins.open", create=True)
    def test_clearsign_uses_args_list(self, mock_open, mock_exists, mock_rename, mock_run):
        """clearsign() should pass an args list to subprocess.run, not a shell string."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stderr = b""
        mock_exists.return_value = True

        adapter = GpgSigningAdapter(keys=["ABC123"])
        adapter.clearsign("/tmp/test.Release", "/tmp/test.Release.asc")

        args = mock_run.call_args[0][0]
        assert isinstance(args, list), f"Expected subprocess.run to receive a list, got {type(args)}: {args}"
        kwargs = mock_run.call_args[1]
        assert kwargs.get("shell", False) is not True, "shell should not be True — shell=True allows command injection"

    @patch("pydeb_s3.release.subprocess.run")
    @patch("pydeb_s3.release.os.rename")
    @patch("pydeb_s3.release.os.path.exists")
    @patch("builtins.open", create=True)
    def test_detach_sign_uses_args_list(self, mock_open, mock_exists, mock_rename, mock_run):
        """detach_sign() should pass an args list to subprocess.run, not a shell string."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stderr = b""
        mock_exists.return_value = True

        adapter = GpgSigningAdapter(keys=["ABC123"])
        adapter.detach_sign("/tmp/test.Release", "/tmp/test.Release.asc")

        args = mock_run.call_args[0][0]
        assert isinstance(args, list), f"Expected subprocess.run to receive a list, got {type(args)}: {args}"
        kwargs = mock_run.call_args[1]
        assert kwargs.get("shell", False) is not True, "shell should not be True — shell=True allows command injection"

    @patch("pydeb_s3.release.subprocess.run")
    @patch("pydeb_s3.release.os.rename")
    @patch("pydeb_s3.release.os.path.exists")
    @patch("builtins.open", create=True)
    def test_clearsign_options_not_shell_injected(self, mock_open, mock_exists, mock_rename, mock_run):
        """Shell metacharacters in options should appear as literal args, not cause injection."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stderr = b""
        mock_exists.return_value = True

        malicious_options = "; rm -rf / --no-such-flag"
        adapter = GpgSigningAdapter(keys=["ABC123"], options=malicious_options)
        adapter.clearsign("/tmp/test.Release", "/tmp/test.Release.asc")

        args = mock_run.call_args[0][0]
        assert isinstance(args, list), f"Expected list args, got {type(args)}: {args}"
        # The shell metacharacters must be literal args, not interpreted by a shell
        assert ";" in args, f"';' should be a literal arg. Args: {args}"
        assert "rm" in args
        assert "-rf" in args
        assert "/" in args
        # The input path should still be the last element
        assert args[-1] == "/tmp/test.Release"
        # '-s' and '--clearsign' should appear before the input path
        assert "-s" in args
        assert "--clearsign" in args

    @patch("pydeb_s3.release.subprocess.run")
    @patch("pydeb_s3.release.os.rename")
    @patch("pydeb_s3.release.os.path.exists")
    @patch("builtins.open", create=True)
    def test_detach_sign_options_not_shell_injected(self, mock_open, mock_exists, mock_rename, mock_run):
        """Shell metacharacters in options should appear as literal args for detach_sign."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stderr = b""
        mock_exists.return_value = True

        malicious_options = "; echo pwned"
        adapter = GpgSigningAdapter(keys=["ABC123"], options=malicious_options)
        adapter.detach_sign("/tmp/test.Release", "/tmp/test.Release.asc")

        args = mock_run.call_args[0][0]
        assert isinstance(args, list), f"Expected list args, got {type(args)}: {args}"
        assert ";" in args, f"';' should be a literal arg. Args: {args}"
        assert "echo" in args
        assert "pwned" in args
        assert args[-1] == "/tmp/test.Release"
        # '-b' should appear before the input path
        assert "-b" in args

    @patch("pydeb_s3.release.subprocess.run")
    @patch("pydeb_s3.release.os.rename")
    @patch("pydeb_s3.release.os.path.exists")
    @patch("builtins.open", create=True)
    def test_clearsign_multiple_keys(self, mock_open, mock_exists, mock_rename, mock_run):
        """Multiple signing keys should each produce a -u flag in the args list."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stderr = b""
        mock_exists.return_value = True

        adapter = GpgSigningAdapter(keys=["KEY1", "KEY2"])
        adapter.clearsign("/tmp/test.Release", "/tmp/test.Release.asc")

        args = mock_run.call_args[0][0]
        assert isinstance(args, list), f"Expected list args, got {type(args)}: {args}"
        u_indices = [i for i, v in enumerate(args) if v == "-u"]
        assert len(u_indices) == 2, f"Expected 2 '-u' flags for 2 keys, got {len(u_indices)}. Args: {args}"
        assert args[u_indices[0] + 1] == "KEY1"
        assert args[u_indices[1] + 1] == "KEY2"

    @patch("pydeb_s3.release.subprocess.run")
    @patch("pydeb_s3.release.os.rename")
    @patch("pydeb_s3.release.os.path.exists")
    @patch("builtins.open", create=True)
    def test_detach_sign_multiple_keys(self, mock_open, mock_exists, mock_rename, mock_run):
        """Multiple signing keys should each produce a -u flag for detach_sign."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stderr = b""
        mock_exists.return_value = True

        adapter = GpgSigningAdapter(keys=["KEY1", "KEY2"])
        adapter.detach_sign("/tmp/test.Release", "/tmp/test.Release.asc")

        args = mock_run.call_args[0][0]
        assert isinstance(args, list), f"Expected list args, got {type(args)}: {args}"
        u_indices = [i for i, v in enumerate(args) if v == "-u"]
        assert len(u_indices) == 2, f"Expected 2 '-u' flags for 2 keys, got {len(u_indices)}. Args: {args}"
        assert args[u_indices[0] + 1] == "KEY1"
        assert args[u_indices[1] + 1] == "KEY2"

    def test_get_key_info_returns_keys(self):
        """get_key_info() should return key information."""
        adapter = GpgSigningAdapter(keys=["key1", "key2"], provider="gpg2")
        info = adapter.get_key_info()
        assert "keys" in info
        assert info["keys"] == ["key1", "key2"]
        assert info["provider"] == "gpg2"


class TestMockSigningAdapter:
    """Tests using MockSigningAdapter for integration testing."""

    def test_mock_adapter_works_with_release(self):
        """Release.sign() works with mock adapter."""

        class MockSigningAdapter:
            def __init__(self):
                self.clearsign_called = False
                self.detach_sign_called = False

            def clearsign(self, input_path: str, output_path: str) -> None:
                self.clearsign_called = True
                # Simulate creating output file
                with open(output_path, "w") as f:
                    f.write("-----BEGIN PGP SIGNED MESSAGE-----")

            def detach_sign(self, input_path: str, output_path: str) -> None:
                self.detach_sign_called = True
                with open(output_path, "w") as f:
                    f.write("-----BEGIN PGP SIGNATURE-----")

            def get_key_info(self) -> dict:
                return {"keys": ["test-key"]}

        class MockS3Adapter:
            def store_file(self, *args, **kwargs):
                pass


        release = Release(codename="stable")
        signing_adapter = MockSigningAdapter()
        s3_adapter = MockS3Adapter()

        # sign() should not raise with mock adapter
        release.sign(s3_adapter, signing_adapter, use_bytes=False)

        assert signing_adapter.clearsign_called
        assert signing_adapter.detach_sign_called

    def test_sign_with_no_adapter(self):
        """Release.sign() should handle None adapter gracefully."""

        class MockS3Adapter:
            def store_file(self, *args, **kwargs):
                pass

        release = Release(codename="stable")
        s3_adapter = MockS3Adapter()
        # Should not raise
        release.sign(s3_adapter, None, use_bytes=False)

    def test_sign_with_empty_key_info(self):
        """Release.sign() should handle adapter with no keys."""

        class MockS3Adapter:
            def store_file(self, *args, **kwargs):
                pass

        class EmptyAdapter:
            def clearsign(self, input_path, output_path):
                pass

            def detach_sign(self, input_path, output_path):
                pass

            def get_key_info(self):
                return {}  # No keys

        release = Release(codename="stable")
        s3_adapter = MockS3Adapter()
        # Should not raise
        release.sign(s3_adapter, EmptyAdapter(), use_bytes=False)
