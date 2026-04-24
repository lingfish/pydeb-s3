"""Pytest configuration and fixtures for pydeb-s3 tests."""

import pytest


@pytest.fixture
def sample_package_data():
    """Sample package data for testing."""
    return {
        "name": "test-package",
        "version": "1.0.0",
        "architecture": "amd64",
        "maintainer": "Test Maintainer <test@example.com>",
        "description": "A test package",
    }


@pytest.fixture
def sample_packages_content():
    """Sample Packages file content."""
    return """Package: test-package
Version: 1.0.0
Architecture: amd64
Maintainer: Test Maintainer <test@example.com>
Description: A test package
 This is a test package description.
 It has multiple lines.
"""
