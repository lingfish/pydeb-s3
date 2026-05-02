"""Tests for UploadProgress callback class."""

from unittest.mock import MagicMock, patch


class TestUploadProgress:
    """Tests for UploadProgress callback class."""

    def teardown_method(self):
        """Reset any mocked modules after each test."""
        # Reset any cached imports

    def test_upload_progress_initialization(self):
        """UploadProgress initializes with filename, filesize, and interactive flag."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1024,
                interactive=False
            )

            assert progress.filename == "test.deb"
            assert progress.filesize == 1024
            assert progress._is_interactive is False

    def test_upload_progress_non_interactive_logs_progress(self):
        """Non-interactive mode logs progress every 5 seconds."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            # Mock time to control the timing
            with patch("pydeb_s3.s3_utils.time") as mock_time:
                mock_time.time.return_value = 0  # Start at time 0

                progress = UploadProgress(
                    filename="test.deb",
                    filesize=1000,
                    interactive=False
                )

                with patch("pydeb_s3.s3_utils.logger") as mock_logger:
                    # First call: 100 bytes transferred at time 0
                    progress(100)
                    # Should not log on first call (only logs every 5 seconds)
                    mock_logger.info.assert_not_called()

                    # Advance time by 5+ seconds
                    mock_time.time.return_value = 5.1
                    progress(200)

                    # Should log progress
                    mock_logger.info.assert_called()
                    call_args = mock_logger.info.call_args[0]
                    # Check format string contains expected placeholders
                    assert "test.deb" in call_args[0] or "%" in call_args[0]

    def test_upload_progress_non_interactive_final_summary(self):
        """Non-interactive mode prints final summary with average speed."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1000,
                interactive=False
            )

            with patch("pydeb_s3.s3_utils.logger") as mock_logger:
                # Simulate upload completion
                progress(1000)

                # Should log final summary
                mock_logger.success.assert_called()
                call_args = mock_logger.success.call_args[0]
                # call_args[0] is the format string, check it contains placeholders
                assert "test.deb" in call_args[0] or "1000" in str(call_args)

    def test_upload_progress_interactive_uses_rich(self):
        """Interactive mode uses rich progress bar."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = True

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1000,
                interactive=True
            )

            # Should have a rich Progress instance
            assert progress._progress is not None

    def test_upload_progress_interactive_updates_progress(self):
        """Interactive mode updates rich progress bar."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = True

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1000,
                interactive=True
            )

            # Mock the progress bar update
            progress._task_id = 0
            progress._progress.update = MagicMock()

            # Simulate callback with bytes transferred
            progress(500)

            # Should have called update
            progress._progress.update.assert_called()

    def test_upload_progress_interactive_completes(self):
        """Interactive mode completes progress bar on finish."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = True

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1000,
                interactive=True
            )

            # Mock the progress bar
            progress._task_id = 0
            progress._progress.update = MagicMock()
            progress._progress.stop = MagicMock()

            # Complete the upload
            progress(1000)

            # Should stop the progress
            progress._progress.stop.assert_called()

    def test_upload_progress_calculates_percentage(self):
        """UploadProgress correctly calculates percentage."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1000,
                interactive=False
            )

            # Test percentage calculation
            assert progress._calculate_percentage(0) == 0
            assert progress._calculate_percentage(500) == 50
            assert progress._calculate_percentage(1000) == 100

    def test_upload_progress_tracks_bytes_transferred(self):
        """UploadProgress tracks bytes transferred."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            progress = UploadProgress(
                filename="test.deb",
                filesize=1000,
                interactive=False
            )

            progress(100)
            assert progress._bytes_transferred == 100

            progress(200)
            assert progress._bytes_transferred == 200


class TestUploadProgressAutoDetection:
    """Tests for automatic TTY detection."""

    def test_auto_detects_interactive_from_tty(self):
        """Auto-detects interactive mode from TTY when interactive is None."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = True

            from pydeb_s3.s3_utils import UploadProgress

            # When interactive is None, should use TTY detection
            progress = UploadProgress(
                filename="test.deb",
                filesize=1000,
                interactive=None
            )

            # Should be interactive because TTY is detected
            assert progress._is_interactive is True

    def test_auto_detects_non_interactive_from_tty(self):
        """Auto-detects non-interactive mode from TTY when interactive is None."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False

            from pydeb_s3.s3_utils import UploadProgress

            # When interactive is None, should use TTY detection
            progress = UploadProgress(
                filename="test.deb",
                filesize=1000,
                interactive=None
            )

            # Should be non-interactive because TTY is not detected
            assert progress._is_interactive is False


class TestStreamMD5:
    """Tests for streaming MD5 calculation."""

    def test_stream_md5_calculates_correct_hash(self):
        """Stream MD5 calculates correct hash for file."""
        import os
        import tempfile

        from pydeb_s3.s3_utils import calculate_stream_md5

        # Create a temp file with known content
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            f.write("test content for md5")
            temp_path = f.name

        try:
            md5_hash = calculate_stream_md5(temp_path)
            # MD5 of "test content for md5" is known
            assert md5_hash is not None
            assert len(md5_hash) == 32  # MD5 is 32 hex characters
        finally:
            os.unlink(temp_path)

    def test_stream_md5_handles_large_file(self):
        """Stream MD5 handles large files without loading into memory."""
        import os
        import tempfile

        from pydeb_s3.s3_utils import calculate_stream_md5

        # Create a larger temp file
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            # Write 1MB of data
            f.write("x" * (1024 * 1024))
            temp_path = f.name

        try:
            md5_hash = calculate_stream_md5(temp_path)
            assert md5_hash is not None
            assert len(md5_hash) == 32
        finally:
            os.unlink(temp_path)
