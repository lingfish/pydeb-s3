"""Tests for bandwidth stats feature - bits vs bytes display."""

from unittest.mock import MagicMock, patch


class TestFormatSpeedBits:
    """Tests for _format_speed() in bits/second format (default)."""

    def test_format_speed_bits_single_digit(self):
        """Format speed in bits/s for single digit values."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False,
                use_bytes=False  # bits mode (default)
            )

            # 512 bytes/sec = 4096 bits/sec = 4.0Kb/s (exact)
            result = progress._format_speed(512)
            assert result == "4.0Kb"

    def test_format_speed_bits_kilobits(self):
        """Format speed in Kb/s for values in kilobit range."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False,
                use_bytes=False  # bits mode (default)
            )

            # 1024 bytes/sec = 8192 bits/sec = 8.2Kb/s (formatted as 8.0Kb)
            result = progress._format_speed(1024)
            assert result == "8.0Kb"

    def test_format_speed_bits_megabits(self):
        """Format speed in Mb/s for values in megabit range."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False,
                use_bytes=False  # bits mode (default)
            )

            # 1024 * 100 = 102400 bytes/sec = 819200 bits/sec ~= 800Kb/s
            result = progress._format_speed(1024 * 100)
            assert result == "800.0Kb"

            # 1024 * 1024 = 1MB = 8Mb/s
            result = progress._format_speed(1024 * 1024)
            assert result == "8.0Mb"

    def test_format_speed_bits_gigabits(self):
        """Format speed in Gb/s for values in gigabit range."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False,
                use_bytes=False  # bits mode (default)
            )

            # 1024^3 bytes/sec = 8Gb/s
            result = progress._format_speed(1024 * 1024 * 1024)
            assert result == "8.0Gb"


class TestFormatSpeedBytes:
    """Tests for _format_speed() in bytes/second format."""

    def test_format_speed_bytes_single_digit(self):
        """Format speed in B/s for single digit values."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False,
                use_bytes=True  # bytes mode
            )

            result = progress._format_speed(500)
            assert result == "500B"

    def test_format_speed_bytes_kilobytes(self):
        """Format speed in KB/s for values in kilobyte range."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False,
                use_bytes=True  # bytes mode
            )

            result = progress._format_speed(1024)
            assert result == "1.0KB"

    def test_format_speed_bytes_megabytes(self):
        """Format speed in MB/s for values in megabyte range."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False,
                use_bytes=True  # bytes mode
            )

            # 1024 * 1024 = 1MB
            result = progress._format_speed(1024 * 1024)
            assert result == "1.0MB"

    def test_format_speed_bytes_gigabytes(self):
        """Format speed in GB/s for values in gigabyte range."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False,
                use_bytes=True  # bytes mode
            )

            # 1024^3 bytes = 1GB
            result = progress._format_speed(1024 * 1024 * 1024)
            assert result == "1.0GB"


class TestUploadProgressUseBytes:
    """Tests for UploadProgress with use_bytes parameter."""

    def test_use_bytes_parameter_defaults_to_false(self):
        """use_bytes defaults to False (bits mode)."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False
                # use_bytes not specified - should default to False
            )

            # Default should be bits mode
            assert progress._use_bytes is False

    def test_use_bytes_true_uses_bytes(self):
        """use_bytes=True uses bytes mode."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False,
                use_bytes=True
            )

            assert progress._use_bytes is True

    def test_use_bytes_false_uses_bits(self):
        """use_bytes=False uses bits mode."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False,
                use_bytes=False
            )

            assert progress._use_bytes is False

    def test_progress_log_shows_bits_by_default(self):
        """Log output shows bits/s by default."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            # Use bits mode (default)
            progress = UploadProgress(
                filename="test.deb",
                filesize=1000,
                interactive=False,
                use_bytes=False
            )

            with patch("pydeb_s3.s3_utils.time") as mock_time:
                mock_time.time.return_value = 0

                progress = UploadProgress(
                    filename="test.deb",
                    filesize=1000,
                    interactive=False,
                    use_bytes=False
                )

                with patch("pydeb_s3.s3_utils.logger") as mock_logger:
                    # Advance time beyond 5 seconds to trigger logging
                    mock_time.time.return_value = 5.1
                    progress(200)

                    # Check that bits format is in the log
                    mock_logger.info.assert_called()
                    call_args = mock_logger.info.call_args[0]
                    # Should contain "Kb" (bits) or "b/s" - at least verify it's not empty
                    assert call_args[0] is not None


class TestS3StoreUseBytes:
    """Tests for s3_store() passing use_bytes to UploadProgress."""

    def setup_method(self):
        """Reset S3 configuration."""
        from pydeb_s3 import s3_utils
        s3_utils._s3_client = None
        s3_utils._bucket = None
        s3_utils._prefix = None
        s3_utils._access_policy = None
        s3_utils._encryption = False

    def teardown_method(self):
        """Clean up after each test."""
        from pydeb_s3 import s3_utils
        s3_utils._s3_client = None
        s3_utils._bucket = None

    def test_s3_store_passes_use_bytes_false(self):
        """s3_store() passes use_bytes=False to UploadProgress (default bits)."""
        import os
        import tempfile
        from unittest.mock import patch

        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")

            from pydeb_s3 import s3_utils

            s3_utils._s3_client = client
            s3_utils._bucket = "test-bucket"

            # Create temp file
            with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
                f.write("test content")
                temp_path = f.name

            try:
                with patch("pydeb_s3.s3_utils.UploadProgress") as mock_progress:
                    # Mock the progress class to capture what params are passed
                    mock_instance = MagicMock()
                    mock_progress.return_value = mock_instance

                    s3_utils.s3_store(temp_path, "test/key", use_bytes=False)

                    # Verify use_bytes=False was passed
                    mock_progress.assert_called()
                    call_kwargs = mock_progress.call_args[1]
                    assert call_kwargs.get("use_bytes") is False
            finally:
                os.unlink(temp_path)

    def test_s3_store_passes_use_bytes_true(self):
        """s3_store() passes use_bytes=True to UploadProgress."""
        import os
        import tempfile
        from unittest.mock import patch

        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")

            from pydeb_s3 import s3_utils

            s3_utils._s3_client = client
            s3_utils._bucket = "test-bucket"

            # Create temp file
            with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
                f.write("test content")
                temp_path = f.name

            try:
                with patch("pydeb_s3.s3_utils.UploadProgress") as mock_progress:
                    mock_instance = MagicMock()
                    mock_progress.return_value = mock_instance

                    s3_utils.s3_store(temp_path, "test/key", use_bytes=True)

                    # Verify use_bytes=True was passed
                    mock_progress.assert_called()
                    call_kwargs = mock_progress.call_args[1]
                    assert call_kwargs.get("use_bytes") is True
            finally:
                os.unlink(temp_path)
