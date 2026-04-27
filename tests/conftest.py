"""Pytest configuration and fixtures for pydeb-s3 tests."""

import boto3
import pytest
from moto import mock_aws

from pydeb_s3 import s3_utils


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    """Patch environment variables for moto AWS mocking."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_client():
    """Create a mocked S3 client."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client
        s3_utils._s3_client = None
        s3_utils._bucket = None


@pytest.fixture
def sample_deb_file():
    """Return path to sample test .deb file."""
    return "tests/fixtures/test-pkg_1.0.0_amd64.deb"


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
