"""Tests for Boto3S3Adapter store_file() with progress callbacks."""

import os
import tempfile
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from pydeb_s3.s3_adapter import Boto3S3Adapter


class TestS3StoreWithProgress:
    """Tests for store_file() with progress callbacks."""

    @pytest.fixture
    def temp_file(self):
        """Create a temporary file for testing."""
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            f.write("test content for upload")
            temp_path = f.name

        yield temp_path

        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.fixture
    def mock_adapter(self):
        """Create a mocked S3 adapter using moto."""
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            adapter = Boto3S3Adapter(client=client, bucket="test-bucket")
            yield adapter

    def test_store_file_uses_upload_file(self, temp_file, mock_adapter):
        """store_file() uses upload_file instead of put_object."""
        with patch.object(mock_adapter._client, "upload_file") as mock_upload:
            mock_adapter.store_file(temp_file, "test/key")

            mock_upload.assert_called_once()
            call_kwargs = mock_upload.call_args[1]
            assert "Callback" in call_kwargs

    def test_store_file_with_progress_callback(self, temp_file, mock_adapter):
        """store_file() invokes progress callback during upload."""
        with patch.object(
            mock_adapter._client,
            "upload_file",
            side_effect=lambda *args, **kwargs: kwargs.get("Callback", lambda x: None)(100)
        ):
            mock_adapter.store_file(temp_file, "test/key")

    def test_store_file_stores_file_correctly(self, temp_file, mock_adapter):
        """store_file() actually stores the file in S3."""
        mock_adapter.store_file(temp_file, "test/key")

        response = mock_adapter._client.get_object(Bucket="test-bucket", Key="test/key")
        content = response["Body"].read()
        assert b"test content for upload" in content

    def test_store_file_with_custom_content_type(self, temp_file, mock_adapter):
        """store_file() respects custom content type."""
        mock_adapter.store_file(temp_file, "test/key", content_type="application/x-deb")

        response = mock_adapter._client.head_object(Bucket="test-bucket", Key="test/key")
        assert response["ContentType"] == "application/x-deb"

    def test_store_file_with_cache_control(self, temp_file, mock_adapter):
        """store_file() respects cache control."""
        mock_adapter.store_file(temp_file, "test/key", cache_control="max-age=3600")

        response = mock_adapter._client.head_object(Bucket="test-bucket", Key="test/key")
        assert response["CacheControl"] == "max-age=3600"

    def test_store_file_with_prefix(self, temp_file, mock_adapter):
        """store_file() applies prefix to key."""
        prefix_adapter = Boto3S3Adapter(
            client=mock_adapter._client,
            bucket="test-bucket",
            prefix="myrepo"
        )
        prefix_adapter.store_file(temp_file, "pool/test.deb")

        response = mock_adapter._client.get_object(Bucket="test-bucket", Key="myrepo/pool/test.deb")
        assert response is not None

    def test_store_file_with_public_acl(self, temp_file, mock_adapter):
        """store_file() applies public-read ACL when configured."""
        acl_adapter = Boto3S3Adapter(
            client=mock_adapter._client,
            bucket="test-bucket",
            access_policy="public-read"
        )

        with patch.object(mock_adapter._client, "upload_file") as mock_upload:
            acl_adapter.store_file(temp_file, "test/key")
            call_kwargs = mock_upload.call_args[1]
            assert call_kwargs["ExtraArgs"]["ACL"] == "public-read"

    def test_store_file_with_server_side_encryption(self, temp_file, mock_adapter):
        """store_file() applies server-side encryption when configured."""
        enc_adapter = Boto3S3Adapter(
            client=mock_adapter._client,
            bucket="test-bucket",
            encryption=True
        )
        enc_adapter.store_file(temp_file, "test/key")

        response = mock_adapter._client.head_object(Bucket="test-bucket", Key="test/key")
        assert response["ServerSideEncryption"] == "AES256"

    def test_store_file_calculates_md5_streaming(self, temp_file, mock_adapter):
        """store_file() calculates MD5 using streaming."""
        mock_adapter.store_file(temp_file, "test/key")

        response = mock_adapter._client.head_object(Bucket="test-bucket", Key="test/key")
        metadata = response.get("Metadata", {})
        assert "md5" in metadata
        assert len(metadata["md5"]) == 32

    def test_store_file_fail_if_exists_same_content(self, temp_file, mock_adapter):
        """store_file() with fail_if_exists skips upload if content is same."""
        mock_adapter.store_file(temp_file, "test/key")
        mock_adapter.store_file(temp_file, "test/key", fail_if_exists=True)

        response = mock_adapter._client.get_object(Bucket="test-bucket", Key="test/key")
        assert response is not None


class TestS3StoreProgressIntegration:
    """Integration tests for store_file with progress callbacks."""

    @pytest.fixture
    def large_temp_file(self):
        """Create a larger temporary file for testing progress."""
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            f.write("x" * (100 * 1024))
            temp_path = f.name

        yield temp_path

        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.fixture
    def mock_adapter(self):
        """Create a mocked S3 adapter using moto."""
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            adapter = Boto3S3Adapter(client=client, bucket="test-bucket")
            yield adapter

    def test_progress_callback_receives_bytes(self, large_temp_file, mock_adapter):
        """Progress callback receives byte count updates."""
        original_upload_file = mock_adapter._client.upload_file

        def mock_upload(*args, **kwargs):
            callback = kwargs.get("Callback")
            if callback:
                callback(10240)
                callback(20480)
                callback(51200)
            return original_upload_file(*args, **kwargs)

        with patch.object(mock_adapter._client, "upload_file", side_effect=mock_upload):
            mock_adapter.store_file(large_temp_file, "test/key")

        response = mock_adapter._client.get_object(Bucket="test-bucket", Key="test/key")
        assert response is not None
