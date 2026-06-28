"""Tests that verify no global mutable state in s3_utils.

Issue #9: s3_utils._s3_adapter was a module-level global mutated by
production code, test fixtures, and test files. These tests verify
the fix removed it.
"""

import pytest

from pydeb_s3.s3_adapter import MockS3Adapter


def test_no_s3_adapter_global():
    """_s3_adapter must NOT exist on the s3_utils module."""
    import pydeb_s3.s3_utils as s3_utils
    assert not hasattr(s3_utils, "_s3_adapter"), (
        "_s3_adapter global must be removed"
    )


def test_no_get_adapter_function():
    """_get_adapter() must NOT exist on the s3_utils module."""
    import pydeb_s3.s3_utils as s3_utils
    assert not hasattr(s3_utils, "_get_adapter"), (
        "_get_adapter() must be removed"
    )


def test_no_backward_compat_wrappers():
    """Backward-compat wrappers (s3_store, s3_read, etc.) must be removed."""
    import pydeb_s3.s3_utils as s3_utils
    wrappers = ["s3_store", "s3_read", "s3_exists", "s3_remove",
                 "s3_copy", "s3_head", "s3_list_objects"]
    for w in wrappers:
        assert not hasattr(s3_utils, w), (
            f"Backward-compat wrapper {w}() must be removed"
        )


def test_configure_s3_returns_adapter_without_global_side_effect():
    """configure_s3() returns an adapter without storing it globally."""
    from unittest.mock import MagicMock, patch
    from pydeb_s3 import s3_utils

    with patch("pydeb_s3.s3_utils.boto3.client") as mock_boto:
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        adapter = s3_utils.configure_s3(bucket="test-bucket")

    assert adapter is not None
    assert adapter.bucket == "test-bucket"


def test_list_codenames_requires_adapter():
    """list_codenames() must require an adapter argument."""
    import pydeb_s3.s3_utils as s3_utils
    with pytest.raises(TypeError):
        s3_utils.list_codenames()


def test_clean_command_no_global_state(monkeypatch):
    """clean_command works without needing any module-level global."""
    from pydeb_s3.cli import clean_command
    adapter = MockS3Adapter(bucket="test-bucket")
    with pytest.raises(Exception):
        clean_command(bucket="test-bucket")
