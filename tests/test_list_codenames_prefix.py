"""Tests for list_codenames() - verifies S3 prefix handling.

These tests verify that list_codenames() correctly handles S3 prefixes
when listing codenames from the dists/ directory.

The bug: when S3 prefix is set (e.g., "apt/"), s3_list_objects("dists/")
returns keys with the prefix prepended (e.g., "apt/dists/stable/Release").
The code checks if key.startswith("dists/") which fails because the actual
key is "apt/dists/stable/Release".
"""

from unittest.mock import patch

import pytest

from pydeb_s3 import s3_utils


class TestListCodenamesWithPrefix:
    """Tests for list_codenames() with S3 prefix configured."""

    @pytest.fixture(autouse=True)
    def setup(self, mock_s3_adapter):
        """Set up S3 adapter for each test."""
        s3_utils._s3_adapter = mock_s3_adapter

    def test_list_codenames_with_prefix_strips_prefix(self, mock_s3_adapter):
        """list_codenames() should strip S3 prefix from keys before parsing.

        When prefix is "apt", s3_list_objects returns keys like:
        - apt/dists/stable/Release
        - apt/dists/rc/Release

        The function should strip "apt/" to find "dists/stable/Release"
        and extract codenames ["stable", "rc"].
        """
        mock_s3_adapter.prefix = "apt"

        with patch.object(mock_s3_adapter, "list_objects") as mock_list:
            mock_list.return_value = (
                [
                    {"Key": "apt/dists/stable/Release"},
                    {"Key": "apt/dists/rc/Release"},
                ],
                None,
            )
            result = s3_utils.list_codenames()

        assert result == ["stable", "rc"]

    def test_list_codenames_without_prefix(self, mock_s3_adapter):
        """list_codenames() works without prefix configured.

        Without prefix, keys are returned as:
        - dists/stable/Release
        - dists/testing/Release

        Should extract codenames ["stable", "testing"].
        """
        mock_s3_adapter.prefix = None

        with patch.object(mock_s3_adapter, "list_objects") as mock_list:
            mock_list.return_value = (
                [
                    {"Key": "dists/stable/Release"},
                    {"Key": "dists/testing/Release"},
                ],
                None,
            )
            result = s3_utils.list_codenames()

        assert result == ["stable", "testing"]

    def test_list_codenames_with_nested_paths(self, mock_s3_adapter):
        """list_codenames() handles nested paths like Packages files.

        With prefix "myrepo", returns keys like:
        - myrepo/dists/rc/main/binary-amd64/Packages
        - myrepo/dists/rc/main/binary-i386/Packages

        Should still extract codename "rc".
        """
        mock_s3_adapter.prefix = "myrepo"

        with patch.object(mock_s3_adapter, "list_objects") as mock_list:
            mock_list.return_value = (
                [
                    {"Key": "myrepo/dists/rc/main/binary-amd64/Packages"},
                    {"Key": "myrepo/dists/rc/main/binary-i386/Packages"},
                ],
                None,
            )
            result = s3_utils.list_codenames()

        assert result == ["rc"]

    def test_list_codenames_prefix_with_trailing_slash(self, mock_s3_adapter):
        """list_codenames() handles prefix with trailing slash.

        When prefix is "apt/" (with trailing slash), returns keys like:
        - apt/dists/stable/Release

        Should correctly strip prefix and extract "stable".
        """
        mock_s3_adapter.prefix = "apt/"

        with patch.object(mock_s3_adapter, "list_objects") as mock_list:
            mock_list.return_value = (
                [
                    {"Key": "apt/dists/stable/Release"},
                ],
                None,
            )
            result = s3_utils.list_codenames()

        assert result == ["stable"]

    def test_list_codenames_empty_prefix(self, mock_s3_adapter):
        """list_codenames() handles empty prefix gracefully.

        When prefix is "", should treat as no prefix.
        Keys are returned as:
        - dists/stable/Release
        """
        mock_s3_adapter.prefix = ""

        with patch.object(mock_s3_adapter, "list_objects") as mock_list:
            mock_list.return_value = (
                [
                    {"Key": "dists/stable/Release"},
                ],
                None,
            )
            result = s3_utils.list_codenames()

        assert result == ["stable"]

    def test_list_codenames_multiple_codename_paths(self, mock_s3_adapter):
        """list_codenames() finds multiple codenames from mixed paths.

        Returns various Release and Packages files:
        - apt/dists/stable/Release
        - apt/dists/stable/main/binary-amd64/Packages
        - apt/dists/testing/Release
        - apt/dists/rc/Release
        """
        mock_s3_adapter.prefix = "apt"

        with patch.object(mock_s3_adapter, "list_objects") as mock_list:
            mock_list.return_value = (
                [
                    {"Key": "apt/dists/stable/Release"},
                    {"Key": "apt/dists/stable/main/binary-amd64/Packages"},
                    {"Key": "apt/dists/testing/Release"},
                    {"Key": "apt/dists/rc/Release"},
                ],
                None,
            )
            result = s3_utils.list_codenames()

        # Should contain all unique codenames
        assert "stable" in result
        assert "testing" in result
        assert "rc" in result
        assert len(result) == 3

    def test_list_codenames_ignores_non_dists_keys(self, mock_s3_adapter):
        """list_codenames() ignores keys not starting with dists/ after stripping."""
        mock_s3_adapter.prefix = "apt"

        with patch.object(mock_s3_adapter, "list_objects") as mock_list:
            mock_list.return_value = (
                [
                    {"Key": "apt/pool/main/foo.deb"},
                    {"Key": "apt/dists/stable/Release"},
                    {"Key": "apt/InRelease"},
                ],
                None,
            )
            result = s3_utils.list_codenames()

        # Should only include stable, ignore pool and InRelease
        assert result == ["stable"]


class TestListCodenamesPagination:
    """Tests for list_codenames() with S3 pagination."""

    @pytest.fixture(autouse=True)
    def setup(self, mock_s3_adapter):
        """Set up S3 adapter for each test."""
        s3_utils._s3_adapter = mock_s3_adapter

    def test_list_codenames_with_pagination(self, mock_s3_adapter):
        """list_codenames() handles pagination correctly.

        First call returns partial results with continuation token.
        Second call returns remaining results without token.
        """
        mock_s3_adapter.prefix = "apt"

        # First page: stable
        with patch.object(mock_s3_adapter, "list_objects") as mock_list:
            mock_list.side_effect = [
                (
                    [
                        {"Key": "apt/dists/stable/Release"},
                    ],
                    "continuation_token_123",
                ),
                # Second page: rc
                (
                    [
                        {"Key": "apt/dists/rc/Release"},
                    ],
                    None,
                ),
            ]

            result = s3_utils.list_codenames()

        # Both codenames should be found across pages
        assert "stable" in result
        assert "rc" in result
        assert len(result) == 2


class TestListCodenamesEdgeCases:
    """Edge case tests for list_codenames()."""

    @pytest.fixture(autouse=True)
    def setup(self, mock_s3_adapter):
        """Set up S3 adapter for each test."""
        s3_utils._s3_adapter = mock_s3_adapter

    def test_list_codenames_empty_results(self, mock_s3_adapter):
        """list_codenames() returns empty list when no objects."""
        mock_s3_adapter.prefix = None

        with patch.object(mock_s3_adapter, "list_objects") as mock_list:
            mock_list.return_value = ([], None)

            result = s3_utils.list_codenames()

        assert result == []

    def test_list_codenames_no_dists_keys(self, mock_s3_adapter):
        """list_codenames() returns empty when no dists/ keys found."""
        mock_s3_adapter.prefix = "apt"

        with patch.object(mock_s3_adapter, "list_objects") as mock_list:
            mock_list.return_value = (
                [
                    {"Key": "apt/pool/main/package.deb"},
                    {"Key": "apt/README"},
                ],
                None,
            )
            result = s3_utils.list_codenames()

        assert result == []

    def test_list_codenames_duplicates_not_added(self, mock_s3_adapter):
        """list_codenames() doesn't add duplicate codenames."""
        mock_s3_adapter.prefix = "apt"

        with patch.object(mock_s3_adapter, "list_objects") as mock_list:
            mock_list.return_value = (
                [
                    {"Key": "apt/dists/stable/Release"},
                    {"Key": "apt/dists/stable/main/binary-amd64/Packages"},
                    {"Key": "apt/dists/stable/InRelease"},
                    {"Key": "apt/dists/testing/Release"},
                ],
                None,
            )
            result = s3_utils.list_codenames()

        # Should only have unique codenames (no duplicates)
        assert result.count("stable") == 1
        assert "testing" in result