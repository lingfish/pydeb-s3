"""Unit tests for GpgSigningAdapter.extract_signing_key().

Tests the method that extracts a GPG key ID from an existing InRelease file
by running gpg --verify and parsing the output.
"""

import subprocess
from unittest.mock import patch

import pytest

from pydeb_s3.release import GpgSigningAdapter


class TestExtractSigningKey:
    """Tests for GpgSigningAdapter.extract_signing_key()."""

    def _make_adapter(self, provider="gpg", options=""):
        return GpgSigningAdapter(keys=["placeholder"], provider=provider, options=options)

    def test_extracts_full_key_id_from_gpg_output(self):
        adapter = self._make_adapter()
        gpg_stderr = b"gpg: Signature made Tue 01 Jan 2024 00:00:00 AM UTC\n"
        gpg_stderr += b"gpg:                using RSA key ABCDEF1234567890\n"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=b"", stderr=gpg_stderr,
            )
            result = adapter.extract_signing_key("fake inrelease content")

        assert result == "ABCDEF1234567890"

    def test_extracts_short_key_id(self):
        adapter = self._make_adapter()
        gpg_stderr = b"gpg: using RSA key ABCD1234\n"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=b"", stderr=gpg_stderr,
            )
            result = adapter.extract_signing_key("content")

        assert result == "ABCD1234"

    def test_writes_inrelease_to_temp_file_for_gpg(self):
        adapter = self._make_adapter()
        inrelease_content = "-----BEGIN PGP SIGNED MESSAGE-----\nHash: SHA256\n\ntest\n"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=b"", stderr=b"using RSA key AAAA\n",
            )
            adapter.extract_signing_key(inrelease_content)

            # Verify gpg was called with a temp file path
            call_args = mock_run.call_args[0][0]
            assert call_args[0] == "gpg"
            assert "--verify" in call_args
            # Last arg should be a temp file path
            temp_path = call_args[-1]
            assert temp_path.endswith(".Release")

    def test_extracts_key_from_stdout(self):
        adapter = self._make_adapter()
        gpg_stdout = b"gpg: using RSA key DEADBEEF12345678\n"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=gpg_stdout, stderr=b"",
            )
            result = adapter.extract_signing_key("content")

        assert result == "DEADBEEF12345678"

    def test_prefers_stderr_key_id_when_both_present(self):
        adapter = self._make_adapter()
        gpg_stdout = b"using RSA key 1111111111111111\n"
        gpg_stderr = b"using RSA key 2222222222222222\n"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=gpg_stdout, stderr=gpg_stderr,
            )
            result = adapter.extract_signing_key("content")

        assert result == "2222222222222222"

    def test_raises_on_gpg_nonzero_exit(self):
        adapter = self._make_adapter()

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=2, stdout=b"", stderr=b"gpg: no data\n",
            )
            with pytest.raises(RuntimeError, match="GPG verification failed"):
                adapter.extract_signing_key("content")

    def test_raises_on_bad_signature(self):
        adapter = self._make_adapter()
        gpg_stderr = b"gpg: BAD signature from \"Test Key\"\n"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=b"", stderr=gpg_stderr,
            )
            with pytest.raises(RuntimeError, match="BAD signature"):
                adapter.extract_signing_key("content")

    def test_raises_when_no_key_id_in_output(self):
        adapter = self._make_adapter()

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=b"some output", stderr=b"no key here",
            )
            with pytest.raises(RuntimeError, match="Could not determine signing key"):
                adapter.extract_signing_key("content")

    def test_raises_when_gpg_output_is_empty(self):
        adapter = self._make_adapter()

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=b"", stderr=b"",
            )
            with pytest.raises(RuntimeError, match="Could not determine signing key"):
                adapter.extract_signing_key("content")

    def test_handles_key_id_with_0x_prefix(self):
        adapter = self._make_adapter()
        gpg_stderr = b"gpg: using RSA key 0xABCDEF1234567890\n"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=b"", stderr=gpg_stderr,
            )
            result = adapter.extract_signing_key("content")

        assert result == "ABCDEF1234567890"

    def test_handles_ecdsa_key(self):
        adapter = self._make_adapter()
        gpg_stderr = b"gpg: using ECDSA key ABCDEF1234567890\n"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=b"", stderr=gpg_stderr,
            )
            result = adapter.extract_signing_key("content")

        assert result == "ABCDEF1234567890"

    def test_passes_options_to_gpg_command(self):
        adapter = self._make_adapter(options="--pinentry-mode loopback")

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=b"", stderr=b"using RSA key AAAA\n",
            )
            adapter.extract_signing_key("content")

            call_args = mock_run.call_args[0][0]
            assert "--pinentry-mode" in call_args
            assert "loopback" in call_args
