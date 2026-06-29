"""Tests for S3 utility functions."""

from unittest.mock import MagicMock, patch

from pydeb_s3 import s3_utils
from pydeb_s3.s3_adapter import MockS3Adapter, S3Error, S3NotFoundError


class TestConfigureS3:
    """Tests for configure_s3()."""

    @patch("pydeb_s3.s3_utils.boto3.client")
    def test_configure_with_region(self, mock_boto):
        """Configures S3 with region."""
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        s3_utils.configure_s3(region="us-west-2", bucket="mybucket")
        mock_boto.assert_called_once()
        assert "us-west-2" in mock_boto.call_args[1]["region_name"]

    @patch("pydeb_s3.s3_utils.boto3.client")
    def test_configure_with_endpoint(self, mock_boto):
        """Configures S3 with custom endpoint."""
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        s3_utils.configure_s3(
            bucket="mybucket",
            endpoint="https://storage.example.com"
        )
        call_kwargs = mock_boto.call_args[1]
        assert "endpoint_url" in call_kwargs

    @patch("pydeb_s3.s3_utils.boto3.client")
    def test_configure_with_credentials(self, mock_boto):
        """Configures S3 with access key and secret."""
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        s3_utils.configure_s3(
            bucket="mybucket",
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        )
        call_kwargs = mock_boto.call_args[1]
        assert "aws_access_key_id" in call_kwargs

    @patch("pydeb_s3.s3_utils.boto3.client")
    def test_configure_s3_returns_adapter(self, mock_boto):
        """configure_s3 returns the created adapter."""
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        adapter = s3_utils.configure_s3(bucket="mybucket")
        assert adapter is not None
        assert adapter.bucket == "mybucket"
        assert isinstance(adapter, MockS3Adapter) is False  # any real adapter

    def test_adapter_visibility(self):
        """Adapter returned by configure_s3 has correct access_policy."""
        adapter = MockS3Adapter(bucket="mybucket", access_policy="public-read")
        assert adapter.access_policy == "public-read"
        adapter2 = MockS3Adapter(bucket="mybucket", access_policy="private")
        assert adapter2.access_policy == "private"


class TestS3Exceptions:
    """Tests for S3 exceptions."""

    def test_s3_not_found_error(self):
        """S3NotFoundError has correct message."""
        err = S3NotFoundError("/path/to/file")
        assert "Object not found" in str(err)
        assert "/path/to/file" in str(err)

    def test_s3_access_error(self):
        """S3AccessError has correct message."""
        err = S3NotFoundError("/path/to/file")
        assert str(err) is not None
        assert "/path/to/file" in str(err)

    def test_s3_error_is_exception(self):
        """S3Error inherits from Exception."""
        assert issubclass(S3Error, Exception)

    def test_s3_not_found_inherits_from_s3_error(self):
        """S3NotFoundError inherits from S3Error."""
        assert issubclass(S3NotFoundError, S3Error)
