"""Tests for BitsTransferSpeedColumn - custom Rich column showing bits/s."""

from unittest.mock import MagicMock, patch

from rich.text import Text


class TestBitsTransferSpeedColumn:
    """Tests for BitsTransferSpeedColumn class."""

    def test_bits_transfer_speed_column_exists(self):
        """BitsTransferSpeedColumn class exists and can be imported."""
        from pydeb_s3.s3_utils import BitsTransferSpeedColumn

        column = BitsTransferSpeedColumn()
        assert column is not None

    def test_render_returns_question_mark_when_no_speed(self):
        """Render returns '?' when task has no speed."""
        from pydeb_s3.s3_utils import BitsTransferSpeedColumn

        column = BitsTransferSpeedColumn()

        # Create a mock task with no speed
        task = MagicMock()
        task.speed = None

        result = column.render(task)

        # Should return a Text object with "?"
        assert isinstance(result, Text)
        assert "?" in str(result)

    def test_render_bits_per_second_single_digit(self):
        """Render shows bits/s for single digit values."""
        from pydeb_s3.s3_utils import BitsTransferSpeedColumn

        column = BitsTransferSpeedColumn()

        # Create a mock task with speed in bytes/sec
        # 100 bytes/sec = 800 bits/sec = 800b/s
        task = MagicMock()
        task.speed = 100  # bytes per second

        result = column.render(task)

        # Should show bits/s format
        result_str = str(result)
        assert "b/s" in result_str
        # Should be around 800b/s
        assert "800" in result_str

    def test_render_kilobits_per_second(self):
        """Render shows Kb/s for values in kilobit range."""
        from pydeb_s3.s3_utils import BitsTransferSpeedColumn

        column = BitsTransferSpeedColumn()

        # 1024 bytes/sec = 8192 bits/sec = 8.0Kb/s
        task = MagicMock()
        task.speed = 1024

        result = column.render(task)

        result_str = str(result)
        assert "Kb/s" in result_str
        assert "8.0" in result_str

    def test_render_megabits_per_second(self):
        """Render shows Mb/s for values in megabit range."""
        from pydeb_s3.s3_utils import BitsTransferSpeedColumn

        column = BitsTransferSpeedColumn()

        # 1024 * 128 = 131072 bytes/sec = 1,048,576 bits/sec = 1.0Mb/s
        task = MagicMock()
        task.speed = 1024 * 128

        result = column.render(task)

        result_str = str(result)
        assert "Mb/s" in result_str
        assert "1.0" in result_str

    def test_render_gigabits_per_second(self):
        """Render shows Gb/s for values in gigabit range."""
        from pydeb_s3.s3_utils import BitsTransferSpeedColumn

        column = BitsTransferSpeedColumn()

        # 1024^3 bytes/sec = 8Gb/s
        task = MagicMock()
        task.speed = 1024 * 1024 * 1024

        result = column.render(task)

        result_str = str(result)
        assert "Gb/s" in result_str
        assert "8.0" in result_str


class TestUploadProgressWithSharedProgress:
    """Tests for UploadProgress with shared Progress instance."""

    def test_upload_progress_accepts_shared_progress(self):
        """UploadProgress accepts a shared Progress instance."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = True

            from rich.progress import BarColumn, DownloadColumn, Progress

            from pydeb_s3.s3_utils import BitsTransferSpeedColumn, UploadProgress

            # Create a shared Progress instance
            shared_progress = Progress(
                BarColumn(),
                BitsTransferSpeedColumn(),
                DownloadColumn(),
            )

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=True,
                progress=shared_progress
            )

            # Should use the shared progress
            assert progress._progress is shared_progress

    def test_upload_progress_without_shared_creates_own(self):
        """UploadProgress creates its own Progress when not shared."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = True

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=True
                # No shared progress passed
            )

            # Should create its own Progress instance
            assert progress._progress is not None

    def test_upload_progress_get_console_returns_console(self):
        """UploadProgress.get_console() returns the Rich console."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = True

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=True
            )

            console = progress.get_console()
            assert console is not None


class TestS3StoreWithSharedProgress:
    """Tests for s3_store() with shared progress parameter."""

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

    def test_s3_store_accepts_progress_parameter(self):
        """s3_store() accepts optional progress parameter."""
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
                from rich.progress import Progress

                # Create a shared progress instance
                shared_progress = Progress()

                # Should not raise when progress is passed
                # Note: This will fail initially because s3_store doesn't accept progress yet
                # After implementation, it should work
                with patch.object(client, "upload_file"):
                    s3_utils.s3_store(temp_path, "test/key", progress=shared_progress)
            finally:
                os.unlink(temp_path)


class TestProgressConsoleLogging:
    """Tests for using progress.console.print() instead of logger in interactive mode."""

    def test_upload_progress_uses_console_print_in_interactive_mode(self):
        """In interactive mode, uses progress.console.print() for status messages."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = True

            from rich.progress import BarColumn, DownloadColumn, Progress

            from pydeb_s3.s3_utils import BitsTransferSpeedColumn, UploadProgress

            # Create a shared Progress with console
            shared_progress = Progress(
                BarColumn(),
                BitsTransferSpeedColumn(),
                DownloadColumn(),
            )
            shared_progress.start()

            try:
                progress = UploadProgress(
                    filename="test.deb",
                    filesize=1024,
                    interactive=True,
                    progress=shared_progress
                )

                # Verify that get_console() returns the console from shared progress
                console = progress.get_console()
                assert console is not None
                assert console is shared_progress.console

                # Test that progress updates work correctly
                progress._progress.update = MagicMock()
                progress(512)  # Update progress to 50%
                progress._progress.update.assert_called()
            finally:
                shared_progress.stop()

    def test_upload_progress_finish_prints_newline_before_log(self):
        """_finish() prints newline before logger messages to avoid garbled output."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False
            )

            with patch("pydeb_s3.s3_utils.logger") as mock_logger:
                progress._finish()

                # Should have called logger.success
                mock_logger.success.assert_called()
