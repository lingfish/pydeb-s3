"""Progress tracking utilities for S3 uploads."""

import hashlib
import os
import sys
import time
from typing import Optional

from loguru import logger

try:
    from rich.progress import (
        BarColumn,
        DownloadColumn,
        Progress,
        ProgressColumn,
        TransferSpeedColumn,
    )
    from rich.text import Text
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class BitsTransferSpeedColumn(ProgressColumn):
    """Custom Rich column showing transfer speed in bits/second."""

    def render(self, task):
        """Render the transfer speed in bits/second."""
        speed = task.speed
        if speed is None:
            return Text("?", style="progress.data.speed")
        bits_per_second = speed * 8
        if bits_per_second < 1024:
            return Text(f"{bits_per_second:.0f}b/s", style="progress.data.speed")
        if bits_per_second < 1024 * 1024:
            return Text(f"{bits_per_second / 1024:.1f}Kb/s", style="progress.data.speed")
        if bits_per_second < 1024 * 1024 * 1024:
            return Text(f"{bits_per_second / 1024**2:.1f}Mb/s", style="progress.data.speed")
        return Text(f"{bits_per_second / 1024**3:.1f}Gb/s", style="progress.data.speed")


def calculate_stream_md5(filepath: str, chunk_size: int = 8192) -> str:
    """Calculate MD5 hash of a file using streaming (memory efficient)."""
    md5_hash = hashlib.md5()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


class UploadProgress:
    """Callback class for tracking upload progress."""

    def __init__(
        self,
        filename: str,
        filesize: int,
        interactive: Optional[bool] = None,
        use_bytes: bool = False,
        progress: Optional[Progress] = None,
    ):
        self.filename = filename
        self.filesize = filesize
        self._use_bytes = use_bytes

        if interactive is None:
            self._is_interactive = sys.stderr.isatty()
        else:
            self._is_interactive = interactive

        self._bytes_transferred = 0
        self._start_time = time.time()
        self._last_log_time = self._start_time
        self._progress = None
        self._task_id = None
        self._shared_progress = progress is not None

        if self._is_interactive and RICH_AVAILABLE:
            if progress is not None:
                self._progress = progress
                if not hasattr(self._progress, '_started') or not self._progress._started:
                    try:
                        if not self._progress.task_ids:
                            self._progress.start()
                    except Exception:
                        self._progress.start()
            else:
                if use_bytes:
                    self._progress = Progress(
                        BarColumn(),
                        TransferSpeedColumn(),
                        DownloadColumn(),
                    )
                else:
                    self._progress = Progress(
                        BarColumn(),
                        BitsTransferSpeedColumn(),
                        DownloadColumn(),
                    )
                self._progress.start()

            self._task_id = self._progress.add_task(
                f"Uploading {filename}",
                total=filesize
            )

    def __call__(self, bytes_transferred: int) -> None:
        """Called by boto3 upload_file with current bytes transferred."""
        self._bytes_transferred = bytes_transferred
        current_time = time.time()

        if self._is_interactive and self._progress:
            self._progress.update(self._task_id, completed=bytes_transferred)
        elif current_time - self._last_log_time >= 5:
            percentage = self._calculate_percentage(bytes_transferred)
            elapsed = current_time - self._start_time
            speed = bytes_transferred / elapsed if elapsed > 0 else 0

            logger.info(
                "Uploading {}: {}% ({} / {} bytes) at {}/s",
                self.filename,
                percentage,
                bytes_transferred,
                self.filesize,
                self._format_speed(speed)
            )
            self._last_log_time = current_time

        if bytes_transferred >= self.filesize:
            self._finish()

    def _calculate_percentage(self, bytes_transferred: int) -> int:
        if self.filesize == 0:
            return 100
        return min(100, int((bytes_transferred / self.filesize) * 100))

    def _format_speed(self, bytes_per_second: float) -> str:
        if self._use_bytes:
            if bytes_per_second < 1024:
                return f"{bytes_per_second:.0f}B"
            if bytes_per_second < 1024 * 1024:
                return f"{bytes_per_second / 1024:.1f}KB"
            if bytes_per_second < 1024 * 1024 * 1024:
                return f"{bytes_per_second / (1024 * 1024):.1f}MB"
            return f"{bytes_per_second / (1024 * 1024 * 1024):.1f}GB"
        bits_per_second = bytes_per_second * 8
        if bits_per_second < 1024:
            return f"{bits_per_second:.0f}b"
        if bits_per_second < 1024 * 1024:
            return f"{bits_per_second / 1024:.1f}Kb"
        if bits_per_second < 1024 * 1024 * 1024:
            return f"{bits_per_second / (1024 * 1024):.1f}Mb"
        return f"{bits_per_second / (1024 * 1024 * 1024):.1f}Gb"

    def get_console(self):
        if self._progress is not None:
            return self._progress.console
        return None

    def _finish(self) -> None:
        elapsed = time.time() - self._start_time
        avg_speed = self.filesize / elapsed if elapsed > 0 else 0

        if self._is_interactive and self._progress:
            self._progress.update(self._task_id, completed=self.filesize)
            if not self._shared_progress:
                self._progress.stop()
        else:
            print()
            logger.success(
                "Uploaded {} ({} bytes) in {:.1f}s (avg {}/s)",
                self.filename,
                self.filesize,
                elapsed,
                self._format_speed(avg_speed)
            )