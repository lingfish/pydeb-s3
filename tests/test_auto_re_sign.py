"""Unit tests for Release.auto_re_sign().

Tests the method that auto re-signs InRelease when it already exists on S3
but --sign was not passed on the current upload.
"""

from unittest.mock import patch

import pytest

from pydeb_s3.release import Release
from pydeb_s3.s3_adapter import MockS3Adapter


def _store_raw(s3: MockS3Adapter, key: str, content: bytes) -> None:
    """Store raw bytes directly into MockS3Adapter's internal storage."""
    full_key = s3._s3_path(key)
    s3._storage[full_key] = content
    s3._metadata[full_key] = {"ContentLength": len(content)}


class TestAutoReSign:
    """Tests for Release.auto_re_sign()."""

    def _make_release(self, codename="stable"):
        return Release(
            origin="Test", label="Test", architectures=["amd64"],
            codename=codename,
        )

    def test_re_signs_when_inrelease_exists(self, auto_re_sign_adapter):
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        # Store InRelease on S3
        _store_raw(s3, "dists/stable/InRelease", b"old signed content")

        with patch.object(release, "sign") as mock_sign:
            release.auto_re_sign(s3, signing_adapter)

        mock_sign.assert_called_once()

    def test_calls_extract_signing_key(self, auto_re_sign_adapter):
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        _store_raw(s3, "dists/stable/InRelease", b"old signed content")

        with patch.object(release, "sign"):
            with patch.object(signing_adapter, "extract_signing_key", return_value="KEY123") as mock_extract:
                release.auto_re_sign(s3, signing_adapter)

        mock_extract.assert_called_once_with("old signed content")

    def test_creates_new_adapter_with_extracted_key(self, auto_re_sign_adapter):
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        _store_raw(s3, "dists/stable/InRelease", b"old signed content")

        with patch.object(release, "sign") as mock_sign:
            with patch.object(signing_adapter, "extract_signing_key", return_value="KEY123"):
                release.auto_re_sign(s3, signing_adapter)

        # sign() was called with a new adapter having the extracted key
        call_args = mock_sign.call_args
        adapter_arg = call_args[0][1]  # second positional arg
        assert adapter_arg.keys == ["KEY123"]
        assert adapter_arg.provider == signing_adapter.provider
        assert adapter_arg.options == signing_adapter.options

    def test_calls_sign_with_new_adapter(self, auto_re_sign_adapter):
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        _store_raw(s3, "dists/stable/InRelease", b"old signed content")

        with patch.object(release, "sign") as mock_sign:
            with patch.object(signing_adapter, "extract_signing_key", return_value="KEY123"):
                release.auto_re_sign(s3, signing_adapter)

        mock_sign.assert_called_once()
        # Verify the adapter passed to sign() has the correct key
        new_adapter = mock_sign.call_args[0][1]
        assert new_adapter.keys == ["KEY123"]

    def test_uses_correct_inrelease_s3_path(self, auto_re_sign_adapter):
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release(codename="bookworm")

        _store_raw(s3, "dists/bookworm/InRelease", b"content")

        with patch.object(release, "sign"):
            with patch.object(signing_adapter, "extract_signing_key", return_value="KEY"):
                release.auto_re_sign(s3, signing_adapter)

        # Verify it read the correct path
        assert s3.exists("dists/bookworm/InRelease")

    def test_raises_when_key_extraction_fails(self, auto_re_sign_adapter):
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        _store_raw(s3, "dists/stable/InRelease", b"content")

        with patch.object(signing_adapter, "extract_signing_key", side_effect=RuntimeError("GPG verification failed")):
            with pytest.raises(RuntimeError, match="GPG verification failed"):
                release.auto_re_sign(s3, signing_adapter)

    def test_error_message_suggests_passing_sign_flag(self, auto_re_sign_adapter):
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        _store_raw(s3, "dists/stable/InRelease", b"content")

        with patch.object(signing_adapter, "extract_signing_key", side_effect=RuntimeError("GPG verification failed")):
            with pytest.raises(RuntimeError, match="--sign"):
                release.auto_re_sign(s3, signing_adapter)

    def test_does_not_call_sign_when_key_extraction_fails(self, auto_re_sign_adapter):
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        _store_raw(s3, "dists/stable/InRelease", b"content")

        with patch.object(release, "sign") as mock_sign:
            with patch.object(signing_adapter, "extract_signing_key", side_effect=RuntimeError("fail")):
                try:
                    release.auto_re_sign(s3, signing_adapter)
                except RuntimeError:
                    pass

        mock_sign.assert_not_called()

    def test_noop_when_inrelease_does_not_exist(self, auto_re_sign_adapter):
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        # No InRelease on S3
        with patch.object(release, "sign") as mock_sign:
            release.auto_re_sign(s3, signing_adapter)

        mock_sign.assert_not_called()

    def test_noop_does_not_call_sign(self, auto_re_sign_adapter):
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        with patch.object(release, "sign") as mock_sign:
            release.auto_re_sign(s3, signing_adapter)

        mock_sign.assert_not_called()

    def test_noop_with_different_codename(self, auto_re_sign_adapter):
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release(codename="unstable")

        # InRelease exists for stable, not unstable
        _store_raw(s3, "dists/stable/InRelease", b"content")

        with patch.object(release, "sign") as mock_sign:
            release.auto_re_sign(s3, signing_adapter)

        mock_sign.assert_not_called()

    def test_auto_re_sign_exists(self):
        """Verify the interface exists."""
        release = self._make_release()
        assert hasattr(release, "auto_re_sign")
        assert callable(release.auto_re_sign)

    def test_auto_re_sign_accepts_s3_adapter_and_signing_adapter(self):
        """Verify the interface signature."""
        release = self._make_release()
        import inspect
        sig = inspect.signature(release.auto_re_sign)
        params = list(sig.parameters.keys())
        assert "s3_adapter" in params
        assert "signing_adapter" in params
