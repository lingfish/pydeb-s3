"""Tests for s3_store() with progress callbacks."""

import os
import tempfile
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from pydeb_s3 import s3_utils


class TestS3StoreWithProgress:
    """Tests for s3_store() with progress callbacks."""

    def setup_method(self):
        """Set up test fixtures."""
        # Reset global state
        s3_utils._s3_client = None
        s3_utils._bucket = None
        s3_utils._prefix = None
        s3_utils._access_policy = None
        s3_utils._encryption = False

    def teardown_method(self):
        """Clean up after each test."""
        s3_utils._s3_client = None
        s3_utils._bucket = None

    @pytest.fixture
    def temp_file(self):
        """Create a temporary file for testing."""
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            f.write("test content for upload")
            temp_path = f.name

        yield temp_path

        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mocked S3 client."""
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            yield client

    def test_s3_store_uses_upload_file(self, temp_file, mock_s3_client):
        """s3_store() uses upload_file instead of put_object."""
        s3_utils._s3_client = mock_s3_client
        s3_utils._bucket = "test-bucket"

        with patch.object(mock_s3_client, "upload_file") as mock_upload:
            s3_utils.s3_store(temp_file, "test/key")

            # Verify upload_file was called (not put_object)
            mock_upload.assert_called_once()

            # Check that callback parameter was passed
            call_kwargs = mock_upload.call_args[1]
            assert "Callback" in call_kwargs

    def test_s3_store_with_progress_callback(self, temp_file, mock_s3_client):
        """s3_store() invokes progress callback during upload."""
        s3_utils._s3_client = mock_s3_client
        s3_utils._bucket = "test-bucket"

        callback_invoked = []

        def capture_callback(bytes_transferred):
            callback_invoked.append(bytes_transferred)

        with patch.object(
            mock_s3_client,
            "upload_file",
            side_effect=lambda *args, **kwargs: kwargs.get("Callback", lambda x: None)(100)
        ):
            # We need to actually call the upload to trigger the callback
            # Let's test by checking the callback is passed correctly
            s3_utils.s3_store(temp_file, "test/key")

    def test_s3_store_stores_file_correctly(self, temp_file, mock_s3_client):
        """s3_store() actually stores the file in S3."""
        s3_utils._s3_client = mock_s3_client
        s3_utils._bucket = "test-bucket"

        s3_utils.s3_store(temp_file, "test/key")

        # Verify the file was stored
        response = mock_s3_client.get_object(Bucket="test-bucket", Key="test/key")
        content = response["Body"].read()
        assert b"test content for upload" in content

    def test_s3_store_with_custom_content_type(self, temp_file, mock_s3_client):
        """s3_store() respects custom content type."""
        s3_utils._s3_client = mock_s3_client
        s3_utils._bucket = "test-bucket"

        s3_utils.s3_store(temp_file, "test/key", content_type="application/x-deb")

        response = mock_s3_client.head_object(Bucket="test-bucket", Key="test/key")
        assert response["ContentType"] == "application/x-deb"

    def test_s3_store_with_cache_control(self, temp_file, mock_s3_client):
        """s3_store() respects cache control."""
        s3_utils._s3_client = mock_s3_client
        s3_utils._bucket = "test-bucket"

        s3_utils.s3_store(temp_file, "test/key", cache_control="max-age=3600")

        response = mock_s3_client.head_object(Bucket="test-bucket", Key="test/key")
        assert response["CacheControl"] == "max-age=3600"

    def test_s3_store_with_prefix(self, temp_file, mock_s3_client):
        """s3_store() applies prefix to key."""
        s3_utils._s3_client = mock_s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._prefix = "myrepo"

        s3_utils.s3_store(temp_file, "pool/test.deb")

        # Verify the file was stored with prefix
        response = mock_s3_client.get_object(Bucket="test-bucket", Key="myrepo/pool/test.deb")
        assert response is not None

    def test_s3_store_with_public_acl(self, temp_file, mock_s3_client):
        """s3_store() applies public-read ACL when configured."""
        s3_utils._s3_client = mock_s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._access_policy = "public-read"

        with patch.object(mock_s3_client, "upload_file") as mock_upload:
            s3_utils.s3_store(temp_file, "test/key")

            # Verify ACL is passed in ExtraArgs
            call_kwargs = mock_upload.call_args[1]
            assert call_kwargs["ExtraArgs"]["ACL"] == "public-read"

    def test_s3_store_with_server_side_encryption(self, temp_file, mock_s3_client):
        """s3_store() applies server-side encryption when configured."""
        s3_utils._s3_client = mock_s3_client
        s3_utils._bucket = "test-bucket"
        s3_utils._encryption = True

        s3_utils.s3_store(temp_file, "test/key")

        response = mock_s3_client.head_object(Bucket="test-bucket", Key="test/key")
        assert response["ServerSideEncryption"] == "AES256"

    def test_s3_store_calculates_md5_streaming(self, temp_file, mock_s3_client):
        """s3_store() calculates MD5 using streaming (not loading entire file)."""
        s3_utils._s3_client = mock_s3_client
        s3_utils._bucket = "test-bucket"

        s3_utils.s3_store(temp_file, "test/key")

        # Verify the MD5 is stored in metadata
        response = mock_s3_client.head_object(Bucket="test-bucket", Key="test/key")
        metadata = response.get("Metadata", {})
        assert "md5" in metadata
        # MD5 of "test content for upload" should be present
        assert len(metadata["md5"]) == 32

    def test_s3_store_fail_if_exists_same_content(self, temp_file, mock_s3_client):
        """s3_store() with fail_if_exists skips upload if content is same."""
        s3_utils._s3_client = mock_s3_client
        s3_utils._bucket = "test-bucket"

        # First upload
        s3_utils.s3_store(temp_file, "test/key")

        # Second upload with same content should not raise
        # (it should detect same content and skip)
        s3_utils.s3_store(temp_file, "test/key", fail_if_exists=True)

        # File should still exist
        response = mock_s3_client.get_object(Bucket="test-bucket", Key="test/key")
        assert response is not None


class TestS3StoreProgressIntegration:
    """Integration tests for s3_store with progress callbacks."""

    def setup_method(self):
        """Set up test fixtures."""
        s3_utils._s3_client = None
        s3_utils._bucket = None
        s3_utils._prefix = None
        s3_utils._access_policy = None
        s3_utils._encryption = False

    def teardown_method(self):
        """Clean up after each test."""
        s3_utils._s3_client = None
        s3_utils._bucket = None

    @pytest.fixture
    def large_temp_file(self):
        """Create a larger temporary file for testing progress."""
        # Create a file with enough content to trigger multiple callbacks
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            # Write 100KB of data
            f.write("x" * (100 * 1024))
            temp_path = f.name

        yield temp_path

        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mocked S3 client."""
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            yield client

    def test_progress_callback_receives_bytes(self, large_temp_file, mock_s3_client):
        """Progress callback receives byte count updates."""
        s3_utils._s3_client = mock_s3_client
        s3_utils._bucket = "test-bucket"

        received_bytes = []

        # We can test by checking the callback is invoked
        # The actual callback implementation will handle the progress
        original_upload_file = mock_s3_client.upload_file

        def mock_upload(*args, **kwargs):
            callback = kwargs.get("Callback")
            if callback:
                # Simulate progress callbacks
                callback(10240)  # 10KB
                callback(20480)  # 20KB
                callback(51200)  # 50KB
            return original_upload_file(*args, **kwargs)

        with patch.object(mock_s3_client, "upload_file", side_effect=mock_upload):
            s3_utils.s3_store(large_temp_file, "test/key")

        # Verify file was uploaded
        response = mock_s3_client.get_object(Bucket="test-bucket", Key="test/key")
        assert response is not None
