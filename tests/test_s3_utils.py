"""Tests for S3 utility functions."""


from unittest.mock import MagicMock, patch

import pytest

from pydeb_s3 import s3_utils
from pydeb_s3.s3_utils import S3AccessError, S3Error, S3NotFoundError


class TestS3Path:
    """Tests for s3_path() path construction."""

    def teardown_method(self):
        """Reset prefix after each test."""
        s3_utils._prefix = None

    def test_without_prefix_returns_path(self):
        """Returns path as-is when no prefix."""
        s3_utils._prefix = None
        assert s3_utils.s3_path("pool/foo.deb") == "pool/foo.deb"

    def test_with_prefix_joins_path(self):
        """Joins prefix with path."""
        s3_utils._prefix = "myrepo"
        result = s3_utils.s3_path("pool/foo.deb")
        assert "myrepo" in result
        assert "pool" in result

    def test_with_trailing_slash_prefix(self):
        """Handles prefix with trailing slash."""
        s3_utils._prefix = "myrepo/"
        result = s3_utils.s3_path("pool/foo.deb")
        assert result.startswith("myrepo/")


class TestConfigureS3:
    """Tests for configure_s3()."""

    def teardown_method(self):
        """Reset globals after each test."""
        s3_utils._s3_client = None
        s3_utils._bucket = None
        s3_utils._access_policy = None

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
    def test_public_visibility_sets_acl(self, mock_boto):
        """Public visibility sets public-read ACL."""
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        s3_utils.configure_s3(bucket="mybucket", visibility="public")
        assert s3_utils._access_policy == "public-read"

    @patch("pydeb_s3.s3_utils.boto3.client")
    def test_private_visibility_sets_acl(self, mock_boto):
        """Private visibility sets private ACL."""
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        s3_utils.configure_s3(bucket="mybucket", visibility="private")
        assert s3_utils._access_policy == "private"

    @patch("pydeb_s3.s3_utils.boto3.client")
    def test_authenticated_visibility_sets_acl(self, mock_boto):
        """Authenticated visibility sets authenticated-read ACL."""
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        s3_utils.configure_s3(bucket="mybucket", visibility="authenticated")
        assert s3_utils._access_policy == "authenticated-read"


class TestS3Exists:
    """Tests for s3_exists()."""

    def teardown_method(self):
        """Reset globals after each test."""
        s3_utils._s3_client = None
        s3_utils._bucket = None

    def test_raises_when_no_client(self):
        """Raises S3Error when S3 client not configured."""
        s3_utils._s3_client = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_exists("any/path")

    def test_raises_when_no_bucket(self):
        """Raises S3Error when bucket not configured."""
        s3_utils._s3_client = MagicMock()
        s3_utils._bucket = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_exists("any/path")

    @patch("pydeb_s3.s3_utils.boto3.client")
    def test_returns_true_when_exists(self, mock_boto):
        """Returns True when object exists."""
        mock_client = MagicMock()
        mock_client.head_object.return_value = {}
        mock_boto.return_value = mock_client

        s3_utils._s3_client = mock_client
        s3_utils._bucket = "mybucket"

        assert s3_utils.s3_exists("path/to/object") is True

    @patch("pydeb_s3.s3_utils.boto3.client")
    def test_returns_false_when_not_found(self, mock_boto):
        """Returns False when object not found."""
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        error_response = {"Error": {"Code": "404"}}
        mock_client.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_boto.return_value = mock_client

        s3_utils._s3_client = mock_client
        s3_utils._bucket = "mybucket"

        assert s3_utils.s3_exists("missing/object") is False


class TestS3Read:
    """Tests for s3_read()."""

    def teardown_method(self):
        """Reset globals after each test."""
        s3_utils._s3_client = None
        s3_utils._bucket = None

    def test_raises_when_no_client(self):
        """Raises S3Error when S3 client not configured."""
        s3_utils._s3_client = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_read("any/path")

    def test_raises_when_no_bucket(self):
        """Raises S3Error when bucket not configured."""
        s3_utils._s3_client = MagicMock()
        s3_utils._bucket = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_read("any/path")

    @patch("pydeb_s3.s3_utils.boto3.client")
    def test_returns_content(self, mock_boto):
        """Returns content when object exists."""
        mock_client = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = b"file content"
        mock_client.get_object.return_value = {"Body": mock_body}
        mock_boto.return_value = mock_client

        s3_utils._s3_client = mock_client
        s3_utils._bucket = "mybucket"

        result = s3_utils.s3_read("path/to/file")
        assert result == "file content"

    @patch("pydeb_s3.s3_utils.boto3.client")
    def test_raises_not_found(self, mock_boto):
        """Raises S3NotFoundError when object not found."""
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_client.get_object.side_effect = ClientError(error_response, "GetObject")
        mock_boto.return_value = mock_client

        s3_utils._s3_client = mock_client
        s3_utils._bucket = "mybucket"

        with pytest.raises(S3NotFoundError):
            s3_utils.s3_read("missing/object")


class TestS3Store:
    """Tests for s3_store()."""

    def teardown_method(self):
        """Reset globals after each test."""
        s3_utils._s3_client = None
        s3_utils._bucket = None

    def test_raises_when_no_client(self):
        """Raises S3Error when S3 client not configured."""
        s3_utils._s3_client = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_store("/tmp/foo", "key")

    def test_raises_when_no_bucket(self):
        """Raises S3Error when bucket not configured."""
        s3_utils._s3_client = MagicMock()
        s3_utils._bucket = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_store("/tmp/foo", "key")


class TestS3Remove:
    """Tests for s3_remove()."""

    def teardown_method(self):
        """Reset globals after each test."""
        s3_utils._s3_client = None
        s3_utils._bucket = None

    def test_raises_when_no_client(self):
        """Raises S3Error when S3 client not configured."""
        s3_utils._s3_client = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_remove("key")

    def test_raises_when_no_bucket(self):
        """Raises S3Error when bucket not configured."""
        s3_utils._s3_client = MagicMock()
        s3_utils._bucket = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_remove("key")


class TestS3ListObjects:
    """Tests for s3_list_objects()."""

    def teardown_method(self):
        """Reset globals after each test."""
        s3_utils._s3_client = None
        s3_utils._bucket = None

    def test_raises_when_no_client(self):
        """Raises S3Error when S3 client not configured."""
        s3_utils._s3_client = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_list_objects("prefix")

    def test_raises_when_no_bucket(self):
        """Raises S3Error when bucket not configured."""
        s3_utils._s3_client = MagicMock()
        s3_utils._bucket = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_list_objects("prefix")

    @patch("pydeb_s3.s3_utils.boto3.client")
    def test_returns_list_and_token(self, mock_boto):
        """Returns list of objects and continuation token."""
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "obj1"}],
            "NextContinuationToken": "token123"
        }
        mock_boto.return_value = mock_client

        s3_utils._s3_client = mock_client
        s3_utils._bucket = "mybucket"

        contents, token = s3_utils.s3_list_objects("prefix")
        assert len(contents) == 1
        assert token == "token123"


class TestS3Exceptions:
    """Tests for S3 exceptions."""

    def test_s3_not_found_error(self):
        """S3NotFoundError has correct message."""
        err = S3NotFoundError("/path/to/file")
        assert "Object not found" in str(err)
        assert "/path/to/file" in str(err)

    def test_s3_access_error(self):
        """S3AccessError has correct message."""
        err = S3AccessError("/path/to/file", "read")
        assert "Access denied" in str(err)
        assert "/path/to/file" in str(err)

    def test_s3_error_is_exception(self):
        """S3Error inherits from Exception."""
        assert issubclass(S3Error, Exception)

    def test_s3_not_found_inherits_from_s3_error(self):
        """S3NotFoundError inherits from S3Error."""
        assert issubclass(S3NotFoundError, S3Error)

    def test_s3_access_inherits_from_s3_error(self):
        """S3AccessError inherits from S3Error."""
        assert issubclass(S3AccessError, S3Error)
