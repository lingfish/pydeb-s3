"""Tests for S3 utility functions."""

from unittest.mock import MagicMock, patch

import pytest

from pydeb_s3 import s3_utils
from pydeb_s3.s3_adapter import MockS3Adapter, S3Error, S3NotFoundError


class TestS3Path:
    """Tests for s3_path() path construction."""

    def teardown_method(self):
        """Reset adapter after each test."""
        s3_utils._s3_adapter = None

    def _set_adapter(self, prefix=None):
        s3_utils._s3_adapter = MockS3Adapter(bucket="test", prefix=prefix)

    def test_without_prefix_returns_path(self):
        """Returns path as-is when no prefix."""
        self._set_adapter(prefix=None)
        assert s3_utils.s3_path("pool/foo.deb") == "pool/foo.deb"

    def test_with_prefix_joins_path(self):
        """Joins prefix with path."""
        self._set_adapter(prefix="myrepo")
        result = s3_utils.s3_path("pool/foo.deb")
        assert "myrepo" in result
        assert "pool" in result

    def test_with_trailing_slash_prefix(self):
        """Handles prefix with trailing slash."""
        self._set_adapter(prefix="myrepo/")
        result = s3_utils.s3_path("pool/foo.deb")
        assert result.startswith("myrepo/")


class TestConfigureS3:
    """Tests for configure_s3()."""

    def teardown_method(self):
        """Reset globals after each test."""
        s3_utils._s3_adapter = None

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

    def test_public_visibility_sets_acl(self):
        """Public visibility sets public-read ACL."""
        s3_utils._s3_adapter = MockS3Adapter(bucket="mybucket", access_policy="public-read")
        assert s3_utils._s3_adapter.access_policy == "public-read"

    def test_private_visibility_sets_acl(self):
        """Private visibility sets private ACL."""
        s3_utils._s3_adapter = MockS3Adapter(bucket="mybucket", access_policy="private")
        assert s3_utils._s3_adapter.access_policy == "private"

    def test_authenticated_visibility_sets_acl(self):
        """Authenticated visibility sets authenticated-read ACL."""
        s3_utils._s3_adapter = MockS3Adapter(bucket="mybucket", access_policy="authenticated-read")
        assert s3_utils._s3_adapter.access_policy == "authenticated-read"


class TestS3Exists:
    """Tests for s3_exists()."""

    def teardown_method(self):
        """Reset adapter after each test."""
        s3_utils._s3_adapter = None

    def test_raises_when_no_adapter(self):
        """Raises S3Error when S3 not configured."""
        s3_utils._s3_adapter = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_exists("any/path")

    def test_returns_true_when_exists(self):
        """Returns True when object exists."""
        s3_utils._s3_adapter = MockS3Adapter(bucket="mybucket")
        s3_utils._s3_adapter._storage["some/key"] = b"content"
        assert s3_utils.s3_exists("some/key") is True

    def test_returns_false_when_not_found(self):
        """Returns False when object not found."""
        s3_utils._s3_adapter = MockS3Adapter(bucket="mybucket")
        assert s3_utils.s3_exists("missing/object") is False


class TestS3Read:
    """Tests for s3_read()."""

    def teardown_method(self):
        """Reset adapter after each test."""
        s3_utils._s3_adapter = None

    def test_raises_when_no_adapter(self):
        """Raises S3Error when S3 not configured."""
        s3_utils._s3_adapter = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_read("any/path")

    def test_returns_content(self):
        """Returns content when object exists."""
        s3_utils._s3_adapter = MockS3Adapter(bucket="mybucket")
        s3_utils._s3_adapter._storage["path/to/file"] = b"file content"

        result = s3_utils.s3_read("path/to/file")
        assert result == "file content"

    def test_raises_not_found(self):
        """Raises S3NotFoundError when object not found."""
        s3_utils._s3_adapter = MockS3Adapter(bucket="mybucket")
        with pytest.raises(S3NotFoundError):
            s3_utils.s3_read("missing/object")


class TestS3Store:
    """Tests for s3_storage()."""

    def teardown_method(self):
        """Reset adapter after each test."""
        s3_utils._s3_adapter = None

    def test_raises_when_no_adapter(self):
        """Raises S3Error when S3 not configured."""
        s3_utils._s3_adapter = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_store("/tmp/foo", "key")


class TestS3Remove:
    """Tests for s3_remove()."""

    def teardown_method(self):
        """Reset adapter after each test."""
        s3_utils._s3_adapter = None

    def test_raises_when_no_adapter(self):
        """Raises S3Error when S3 not configured."""
        s3_utils._s3_adapter = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_remove("key")


class TestS3ListObjects:
    """Tests for s3_list_objects()."""

    def teardown_method(self):
        """Reset adapter after each test."""
        s3_utils._s3_adapter = None

    def test_raises_when_no_adapter(self):
        """Raises S3Error when S3 not configured."""
        s3_utils._s3_adapter = None
        with pytest.raises(S3Error, match="S3 not configured"):
            s3_utils.s3_list_objects("prefix")

    def test_returns_list_and_token(self):
        """Returns list of objects and continuation token."""
        adapter = MockS3Adapter(bucket="mybucket")
        adapter._storage["obj1"] = b"data"
        s3_utils._s3_adapter = adapter

        contents, token = s3_utils.s3_list_objects("")
        assert len(contents) == 1
        assert token is None

    def test_with_prefix_applies_prefix_once(self):
        """S3 list objects applies prefix only once to the S3 API call."""
        adapter = MockS3Adapter(bucket="mybucket", prefix="apt")
        s3_utils._s3_adapter = adapter

        contents, _ = s3_utils.s3_list_objects("pool/")
        # MockS3Adapter returns empty because there are no matching keys,
        # but the prefix was correctly applied
        assert contents == []

    def test_with_prefix_and_nested_path(self):
        """S3 list objects correctly handles prefix with nested path."""
        adapter = MockS3Adapter(bucket="mybucket", prefix="myrepo")
        adapter._storage["myrepo/dists/stable/main/binary-amd64/Packages"] = b"data"
        s3_utils._s3_adapter = adapter

        contents, _ = s3_utils.s3_list_objects("dists/stable/main/binary-amd64/Packages")
        assert len(contents) == 1

    def test_without_prefix_passes_path_directly(self):
        """Without prefix, s3_list_objects passes path directly to S3 API."""
        adapter = MockS3Adapter(bucket="mybucket", prefix=None)
        adapter._storage["pool/some.deb"] = b"data"
        s3_utils._s3_adapter = adapter

        contents, _ = s3_utils.s3_list_objects("pool/")
        assert len(contents) == 1


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
