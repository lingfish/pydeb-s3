"""Integration tests for auto re-sign InRelease.

Tests the flow where InRelease already exists on S3 and a subsequent upload
without --sign triggers automatic re-signing using the existing key.
"""

from unittest.mock import patch

import pytest

from pydeb_s3.release import Release
from pydeb_s3.s3_adapter import MockS3Adapter, S3NotFoundError

pytestmark = pytest.mark.integration


def _store_raw(s3: MockS3Adapter, key: str, content: bytes) -> None:
    """Store raw bytes directly into MockS3Adapter's internal storage."""
    full_key = s3._s3_path(key)
    s3._storage[full_key] = content
    s3._metadata[full_key] = {"ContentLength": len(content)}


class TestAutoReSignIntegration:
    """End-to-end tests for auto re-sign behavior."""

    def _make_release(self, codename="stable"):
        return Release(
            origin="Test", label="Test", architectures=["amd64"],
            codename=codename,
        )

    def test_upload_with_sign_creates_inrelease(self, moto_s3_adapter):
        """First upload with --sign creates InRelease."""

        # Initially no InRelease
        with pytest.raises(S3NotFoundError):
            moto_s3_adapter.read("dists/stable/InRelease")

    def test_upload_without_sign_auto_resigns_if_inrelease_exists(self, auto_re_sign_adapter):
        """Second upload without --sign triggers auto re-sign with extracted key."""
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        # Simulate InRelease already exists from a previous signed upload
        _store_raw(s3, "dists/stable/InRelease", b"old signed content")

        # Track what key the new adapter was created with
        captured_keys = []

        def mock_sign(s3_adapter, signing_adapter, use_bytes=False):
            captured_keys.extend(signing_adapter.keys)

        with patch.object(release, "sign", side_effect=mock_sign):
            with patch.object(signing_adapter, "extract_signing_key", return_value="EXTRACTED_KEY_123"):
                release.auto_re_sign(s3, signing_adapter)

        assert captured_keys == ["EXTRACTED_KEY_123"]

    def test_auto_resign_preserves_inrelease_path(self, auto_re_sign_adapter):
        """InRelease stays at correct S3 path after auto re-sign."""
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release(codename="bookworm")

        _store_raw(s3, "dists/bookworm/InRelease", b"content")

        with patch.object(release, "sign"):
            with patch.object(signing_adapter, "extract_signing_key", return_value="KEY"):
                release.auto_re_sign(s3, signing_adapter)

        # InRelease should still exist at the correct path
        assert s3.exists("dists/bookworm/InRelease")

    def test_no_auto_resign_when_inrelease_absent(self, auto_re_sign_adapter):
        """No auto re-sign when InRelease never existed."""
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        with patch.object(release, "sign") as mock_sign:
            release.auto_re_sign(s3, signing_adapter)

        mock_sign.assert_not_called()

    def test_cli_sign_then_auto_resign_simulated(self, auto_re_sign_adapter):
        """Full CLI-level simulation of the complete flow."""
        signing_adapter, s3 = auto_re_sign_adapter
        release = self._make_release()

        # Step 1: Simulate first upload with --sign (creates InRelease)
        _store_raw(s3, "dists/stable/InRelease", b"initial signed content")

        # Step 2: Simulate second upload without --sign (auto re-sign)
        with patch.object(release, "sign") as mock_sign:
            with patch.object(signing_adapter, "extract_signing_key", return_value="REAL_KEY_ABCD"):
                release.auto_re_sign(s3, signing_adapter)

        # Verify sign was called with the extracted key
        mock_sign.assert_called_once()
        new_adapter = mock_sign.call_args[0][1]
        assert new_adapter.keys == ["REAL_KEY_ABCD"]
        assert new_adapter.provider == signing_adapter.provider
