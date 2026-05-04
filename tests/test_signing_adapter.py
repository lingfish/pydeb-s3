"""Tests for SigningAdapter protocol and GpgSigningAdapter."""

import os
import pytest
from unittest.mock import MagicMock, patch

from pydeb_s3.release import SigningAdapter, GpgSigningAdapter


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

    def test_get_key_info_returns_keys(self):
        """get_key_info() should return key information."""
        adapter = GpgSigningAdapter(keys=["key1", "key2"], provider="gpg2")
        info = adapter.get_key_info()
        assert "keys" in info
        assert info["keys"] == ["key1", "key2"]
        assert info["provider"] == "gpg2"


class TestMockSigningAdapter:
    """Tests using MockSigningAdapter for integration testing."""

    @patch("pydeb_s3.release.s3_store")
    def test_mock_adapter_works_with_release(self, mock_s3_store):
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

        from pydeb_s3.release import Release

        release = Release(codename="stable")
        adapter = MockSigningAdapter()

        # sign() should not raise with mock adapter
        release.sign(adapter, visibility="public", use_bytes=False)

        assert adapter.clearsign_called
        assert adapter.detach_sign_called

    def test_sign_with_no_adapter(self):
        """Release.sign() should handle None adapter gracefully."""
        from pydeb_s3.release import Release

        release = Release(codename="stable")
        # Should not raise
        release.sign(None, visibility="public", use_bytes=False)

    def test_sign_with_empty_key_info(self):
        """Release.sign() should handle adapter with no keys."""
        from pydeb_s3.release import Release

        class EmptyAdapter:
            def clearsign(self, input_path, output_path):
                pass

            def detach_sign(self, input_path, output_path):
                pass

            def get_key_info(self):
                return {}  # No keys

        release = Release(codename="stable")
        # Should not raise
        release.sign(EmptyAdapter(), visibility="public", use_bytes=False)
