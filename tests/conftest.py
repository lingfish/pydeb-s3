"""Pytest configuration and fixtures for pydeb-s3 tests."""


import sys

import boto3
import pytest
from moto import mock_aws

from pydeb_s3.s3_adapter import Boto3S3Adapter, MockS3Adapter


class MockSigningAdapter:
    """Mock SigningAdapter for testing without GPG."""

    def __init__(self):
        self.clearsign_called = False
        self.detach_sign_called = False
        self.keys = ["mock-key-12345"]

    def clearsign(self, input_path: str, output_path: str) -> None:
        """Simulate clearsigning by writing a mock signed file."""
        self.clearsign_called = True
        with open(output_path, "w") as f:
            f.write("-----BEGIN PGP SIGNED MESSAGE-----\n")
            f.write("Test signed content\n")
            f.write("-----END PGP SIGNATURE-----\n")

    def detach_sign(self, input_path: str, output_path: str) -> None:
        """Simulate detached signing by writing a mock signature file."""
        self.detach_sign_called = True
        with open(output_path, "w") as f:
            f.write("-----BEGIN PGP SIGNATURE-----\n")
            f.write("Mock signature\n")
            f.write("-----END PGP SIGNATURE-----\n")

    def get_key_info(self) -> dict:
        """Return mock key info."""
        return {"keys": self.keys, "provider": "mock"}


class MotoS3AdapterFixture:
    """Context manager for moto-backed S3 testing.

    Creates a real Boto3S3Adapter backed by moto mock.
    Use this for tests that call CLI commands (which create their own adapters)
    or tests that use s3_utils module-level functions.
    """

    def __init__(self, bucket="test-bucket", prefix="", access_policy="public-read"):
        self.bucket = bucket
        self.prefix = prefix
        self.access_policy = access_policy
        self._mock = None
        self._adapter = None

    def __enter__(self):
        self._mock = mock_aws()
        self._mock.start()
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=self.bucket)
        self._adapter = Boto3S3Adapter(
            client=client,
            bucket=self.bucket,
            prefix=self.prefix,
            access_policy=self.access_policy,
        )
        return self

    def __exit__(self, *args):
        self._mock.stop()

    @property
    def adapter(self):
        return self._adapter


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    """Patch environment variables for moto AWS mocking."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(autouse=True)
def setup_loguru():
    """Configure loguru to output to captured stderr."""
    from loguru import logger
    logger.remove()
    logger.add(lambda msg: sys.stderr.write(msg), format="{message}")


@pytest.fixture
def s3_client():
    """Create a mocked S3 client with moto."""
    from moto import mock_aws
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client


@pytest.fixture
def moto_s3_adapter():
    """Moto-backed Boto3S3Adapter for tests that need real boto3 mocking.

    Use this for tests that:
    - Call CLI commands (delete_command, list_command, etc.)
    - Need real boto3 mocking behavior
    """
    with MotoS3AdapterFixture() as f:
        yield f.adapter


@pytest.fixture
def moto_s3_adapter_with_prefix():
    """Moto-backed adapter with prefix for testing prefix handling."""
    with MotoS3AdapterFixture(bucket="test-bucket", prefix="apt") as f:
        yield f.adapter


@pytest.fixture
def mock_s3_adapter():
    """Provide a MockS3Adapter for tests that need S3 without real S3.

    This is a fast in-memory adapter. Use for tests that directly call
    module methods with S3Adapter parameters (e.g., release.write_to_s3(adapter)).
    Do NOT use for tests that call CLI commands - those need moto_s3_adapter.
    """
    return MockS3Adapter(bucket="test-bucket", prefix="repo")


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


@pytest.fixture
def mock_signing_adapter():
    """Provide a MockSigningAdapter for tests that need signing without GPG."""
    return MockSigningAdapter()
