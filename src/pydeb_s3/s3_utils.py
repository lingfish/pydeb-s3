"""S3 utility functions for interacting with AWS S3."""

import hashlib
import os
import sys
import time
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from loguru import logger

# Rich is available via typer dependency
try:
    from rich.progress import BarColumn, DownloadColumn, Progress, ProgressColumn, TransferSpeedColumn
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
        bits_per_second = speed * 8  # Convert bytes to bits
        if bits_per_second < 1024:
            return Text(f"{bits_per_second:.0f}b/s", style="progress.data.speed")
        if bits_per_second < 1024 * 1024:
            return Text(f"{bits_per_second / 1024:.1f}Kb/s", style="progress.data.speed")
        if bits_per_second < 1024 * 1024 * 1024:
            return Text(f"{bits_per_second / 1024**2:.1f}Mb/s", style="progress.data.speed")
        return Text(f"{bits_per_second / 1024**3:.1f}Gb/s", style="progress.data.speed")


def calculate_stream_md5(filepath: str, chunk_size: int = 8192) -> str:
    """Calculate MD5 hash of a file using streaming (memory efficient).

    Args:
        filepath: Path to the file
        chunk_size: Size of chunks to read (default 8KB)

    Returns:
        Hexadecimal MD5 hash string
    """
    md5_hash = hashlib.md5()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


class UploadProgress:
    """Callback class for tracking upload progress.

    Supports both interactive (rich progress bar) and non-interactive (loguru logging) modes.
    """

    def __init__(
        self,
        filename: str,
        filesize: int,
        interactive: Optional[bool] = None,
        use_bytes: bool = False,
        progress: Optional[Progress] = None,
    ):
        """Initialize the upload progress tracker.

        Args:
            filename: Name of the file being uploaded
            filesize: Total size of the file in bytes
            interactive: Force interactive (True) or non-interactive (False) mode.
                        If None, auto-detects from TTY.
            use_bytes: If True, display speed in bytes/s (B/KB/MB/GB).
                      If False (default), display speed in bits/s (b/Kb/Mb/Gb).
            progress: Optional shared Progress instance. If provided, uses this instead of
                      creating a new one. This allows multiple uploads to share a single
                      progress display.
        """
        self.filename = filename
        self.filesize = filesize
        self._use_bytes = use_bytes

        # Auto-detect interactive mode from TTY if not specified
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

        # Initialize rich progress for interactive mode
        if self._is_interactive and RICH_AVAILABLE:
            if progress is not None:
                # Use shared progress instance
                self._progress = progress
                # Don't start if already started (shared) - check if any tasks exist
                # or check the internal _started state if available
                if not hasattr(self._progress, '_started') or not self._progress._started:
                    try:
                        # Check if progress has been started by looking at task state
                        # If no tasks have been added yet, we should start it
                        if not self._progress.task_ids:
                            self._progress.start()
                    except Exception:
                        # If we can't determine, try to start it
                        self._progress.start()
            else:
                # Create our own progress instance with bits/s column
                if use_bytes:
                    # Use standard TransferSpeedColumn for bytes/s
                    self._progress = Progress(
                        BarColumn(),
                        TransferSpeedColumn(),
                        DownloadColumn(),
                    )
                else:
                    # Use custom BitsTransferSpeedColumn for bits/s
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
        """Called by boto3 upload_file with current bytes transferred.

        Args:
            bytes_transferred: Total bytes transferred so far
        """
        self._bytes_transferred = bytes_transferred
        current_time = time.time()

        if self._is_interactive and self._progress:
            # Interactive mode: update rich progress bar
            self._progress.update(self._task_id, completed=bytes_transferred)
        # Non-interactive mode: log progress every 5 seconds
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

        # Check if upload is complete
        if bytes_transferred >= self.filesize:
            self._finish()

    def _calculate_percentage(self, bytes_transferred: int) -> int:
        """Calculate percentage of upload complete."""
        if self.filesize == 0:
            return 100
        return min(100, int((bytes_transferred / self.filesize) * 100))

    def _format_speed(self, bytes_per_second: float) -> str:
        """Format speed in human-readable format.

        Args:
            bytes_per_second: Speed in bytes per second

        Returns:
            Formatted speed string. If use_bytes is False (default), formats as bits/s
            (b/Kb/Mb/Gb). If use_bytes is True, formats as bytes/s (B/KB/MB/GB).
        """
        if self._use_bytes:
            # bytes/s format: B/KB/MB/GB
            if bytes_per_second < 1024:
                return f"{bytes_per_second:.0f}B"
            if bytes_per_second < 1024 * 1024:
                return f"{bytes_per_second / 1024:.1f}KB"
            if bytes_per_second < 1024 * 1024 * 1024:
                return f"{bytes_per_second / (1024 * 1024):.1f}MB"
            return f"{bytes_per_second / (1024 * 1024 * 1024):.1f}GB"
        # bits/s format: b/Kb/Mb/Gb (convert bytes to bits by multiplying by 8)
        bits_per_second = bytes_per_second * 8
        if bits_per_second < 1024:
            return f"{bits_per_second:.0f}b"
        if bits_per_second < 1024 * 1024:
            return f"{bits_per_second / 1024:.1f}Kb"
        if bits_per_second < 1024 * 1024 * 1024:
            return f"{bits_per_second / (1024 * 1024):.1f}Mb"
        return f"{bits_per_second / (1024 * 1024 * 1024):.1f}Gb"

    def get_console(self):
        """Get the Rich console for printing above the progress bar.

        Returns:
            The Rich console from the progress instance, or None if not in interactive mode.
        """
        if self._progress is not None:
            return self._progress.console
        return None

    def _finish(self) -> None:
        """Handle upload completion."""
        elapsed = time.time() - self._start_time
        avg_speed = self.filesize / elapsed if elapsed > 0 else 0

        if self._is_interactive and self._progress:
            # Complete the progress bar
            self._progress.update(self._task_id, completed=self.filesize)
            # Only stop if we created our own progress (not shared)
            if not self._shared_progress:
                self._progress.stop()
        else:
            # Print newline before logger message to avoid garbled output
            print()
            # Log final summary
            logger.success(
                "Uploaded {} ({} bytes) in {:.1f}s (avg {}/s)",
                self.filename,
                self.filesize,
                elapsed,
                self._format_speed(avg_speed)
            )


class S3Error(Exception):
    """Base S3 exception."""


class S3NotFoundError(S3Error):
    """Object not found."""

    def __init__(self, path: str):
        super().__init__(f"Object not found: {path}")
        self.path = path


class S3AccessError(S3Error):
    """Access denied."""

    def __init__(self, path: str, operation: str):
        super().__init__(f"Access denied to {path}: {operation}")
        self.path = path
        self.operation = operation


_s3_client: Optional[boto3.client] = None
_bucket: Optional[str] = None
_access_policy: Optional[str] = None
_signing_key: Optional[list[str]] = None
_gpg_provider: str = "gpg"
_gpg_options: str = ""
_prefix: Optional[str] = None
_encryption: bool = False


def configure_s3(
    region: str = "us-east-1",
    endpoint: Optional[str] = None,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    bucket: Optional[str] = None,
    prefix: Optional[str] = None,
    visibility: str = "public",
    signing_key: Optional[list[str]] = None,
    gpg_provider: str = "gpg",
    gpg_options: str = "",
    encryption: bool = False,
    proxy_uri: Optional[str] = None,
    force_path_style: bool = False,
    checksum_when_required: bool = False,
) -> None:
    """Configure the S3 client."""
    global _s3_client, _bucket, _access_policy, _signing_key
    global _gpg_provider, _gpg_options, _prefix, _encryption

    logger.info("Configuring S3: region={}, bucket={}", region, bucket)

    settings = {"region_name": region}

    if endpoint:
        settings["endpoint_url"] = endpoint
    if proxy_uri:
        settings["proxy"] = {"http": proxy_uri, "https": proxy_uri}
    if force_path_style:
        settings["use_accelerate_endpoint"] = False
    if checksum_when_required:
        settings["request_checksum_calculation"] = "when_required"

    if access_key_id and secret_access_key:
        settings["aws_access_key_id"] = access_key_id
        settings["aws_secret_access_key"] = secret_access_key
        if session_token:
            settings["aws_session_token"] = session_token

    _s3_client = boto3.client("s3", **settings)
    _bucket = bucket
    _prefix = prefix
    _signing_key = signing_key
    _gpg_provider = gpg_provider
    _gpg_options = gpg_options
    _encryption = encryption

    if visibility == "public":
        _access_policy = "public-read"
    elif visibility == "private":
        _access_policy = "private"
    elif visibility == "authenticated":
        _access_policy = "authenticated-read"
    elif visibility == "bucket_owner":
        _access_policy = "bucket-owner-full-control"
    else:
        _access_policy = None

    logger.success("S3 configured: bucket={}, prefix={}", bucket, prefix)


def s3_path(path: str) -> str:
    """Get the full S3 path with prefix."""
    if _prefix:
        return os.path.join(_prefix, path)
    return path


def s3_exists(path: str) -> bool:
    """Check if an object exists in S3."""
    if not _s3_client or not _bucket:
        logger.error("S3 not configured")
        raise S3Error("S3 not configured")

    try:
        _s3_client.head_object(Bucket=_bucket, Key=s3_path(path))
        logger.debug("Object exists: {}", path)
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            logger.debug("Object not found: {}", path)
            return False
        logger.error("S3 error checking {}: {}", path, e)
        raise S3Error(f"Failed to check {path}: {e}")


def s3_read(path: str) -> str:
    """Read an object from S3."""
    if not _s3_client or not _bucket:
        logger.error("S3 not configured")
        raise S3Error("S3 not configured")

    try:
        logger.info("Reading s3://{}/{}", _bucket, path)
        response = _s3_client.get_object(Bucket=_bucket, Key=s3_path(path))
        content = response["Body"].read().decode("utf-8")
        logger.success("Read {} bytes from {}", len(content), path)
        return content
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "NoSuchKey":
            logger.warning("Object not found: {}", path)
            raise S3NotFoundError(path)
        logger.error("S3 error reading {}: {}", path, e)
        raise S3Error(f"Failed to read {path}: {e}")


def s3_store(
    filepath: str,
    key: str,
    content_type: str = "application/octet-stream; charset=binary",
    cache_control: Optional[str] = None,
    fail_if_exists: bool = False,
    show_progress: Optional[bool] = None,
    use_bytes: bool = False,
    progress: Optional[Progress] = None,
) -> None:
    """Store a file in S3.

    Args:
        filepath: Path to the local file to upload
        key: S3 key (path) for the file
        content_type: Content type for the S3 object
        cache_control: Cache control header
        fail_if_exists: If True, raise error if file with different content exists
        show_progress: Force progress display on/off. If None, auto-detect from TTY.
        use_bytes: If True, display speed in bytes/s. If False (default), display in bits/s.
        progress: Optional shared Progress instance for multiple uploads.
    """
    if not _s3_client or not _bucket:
        logger.error("S3 not configured")
        raise S3Error("S3 not configured")

    key = s3_path(key)

    # Determine if we should use interactive mode with progress console
    progress_console = None
    if progress is not None and RICH_AVAILABLE:
        progress_console = progress.console

    # Use progress console for status messages in interactive mode, otherwise use logger
    if progress_console is not None:
        progress_console.print(f"Storing {os.path.basename(filepath)} to s3://{_bucket}/{key}")
    else:
        logger.info("Storing {} to s3://{}/{}", filepath, _bucket, key)

    # Get file size for progress tracking
    filesize = os.path.getsize(filepath)

    # Calculate MD5 using streaming (memory efficient)
    md5_hash = calculate_stream_md5(filepath)

    if fail_if_exists and s3_exists(key):
        existing = s3_head(key)
        if existing:
            etag = existing.get("ETag", "").strip('"')
            meta_md5 = existing.get("Metadata", {}).get("md5", "")
            if md5_hash == etag or md5_hash == meta_md5:
                if progress_console:
                    progress_console.print(f"File already exists with same content: {key}")
                else:
                    logger.info("File already exists with same content: {}", key)
                return
            logger.error("File exists with different content: {}", key)
            raise S3Error(f"file {key} already exists with different contents")

    extra_args = {
        "ContentType": content_type.split(";")[0].strip(),
        "Metadata": {"md5": md5_hash}
    }

    if _access_policy:
        extra_args["ACL"] = _access_policy

    if cache_control:
        extra_args["CacheControl"] = cache_control

    if _encryption:
        extra_args["ServerSideEncryption"] = "AES256"

    # Create progress callback, passing shared progress if provided
    progress_callback = UploadProgress(
        filename=os.path.basename(filepath),
        filesize=filesize,
        interactive=show_progress,
        use_bytes=use_bytes,
        progress=progress
    )

    # Use upload_file for progress callback support and automatic multipart
    _s3_client.upload_file(
        filepath,
        _bucket,
        key,
        ExtraArgs=extra_args,
        Callback=progress_callback
    )

    if progress_console:
        progress_console.print(f"Stored {filesize} bytes to s3://{_bucket}/{key}")
    else:
        logger.success("Stored {} bytes to s3://{}/{}", filesize, _bucket, key)


def s3_head(path: str) -> dict:
    """Get head information for an object."""
    if not _s3_client or not _bucket:
        logger.error("S3 not configured")
        raise S3Error("S3 not configured")

    try:
        logger.debug("Head s3://{}/{}", _bucket, path)
        return _s3_client.head_object(Bucket=_bucket, Key=s3_path(path))
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            logger.warning("Object not found: {}", path)
            raise S3NotFoundError(path)
        logger.error("S3 error head {}: {}", path, e)
        raise S3Error(f"Failed to head {path}: {e}")


def s3_remove(path: str) -> None:
    """Remove an object from S3."""
    if not _s3_client or not _bucket:
        logger.error("S3 not configured")
        raise S3Error("S3 not configured")

    try:
        logger.info("Removing s3://{}/{}", _bucket, path)
        _s3_client.delete_object(Bucket=_bucket, Key=s3_path(path))
        logger.success("Removed s3://{}/{}", _bucket, path)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            logger.warning("Object not found (already removed?): {}", path)
            raise S3NotFoundError(path)
        logger.error("S3 error removing {}: {}", path, e)
        raise S3Error(f"Failed to remove {path}: {e}")


def s3_copy(source: str, destination: str) -> None:
    """Copy an object in S3."""
    if not _s3_client or not _bucket:
        logger.error("S3 not configured")
        raise S3Error("S3 not configured")

    try:
        logger.info("Copying s3://{}/{} to s3://{}/{}", _bucket, source, _bucket, destination)
        _s3_client.copy_object(
            Bucket=_bucket,
            CopySource=s3_path(source),
            Key=s3_path(destination),
        )
        logger.success("Copied s3://{}/{} to s3://{}/{}", _bucket, source, _bucket, destination)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            logger.warning("Source object not found: {}", source)
            raise S3NotFoundError(source)
        if code in ("403", "403"):
            logger.error("Access denied copying {}: {}", source, e)
            raise S3AccessError(source, "copy")
        logger.error("S3 error copying {} to {}: {}", source, destination, e)
        raise S3Error(f"Failed to copy {source} to {destination}: {e}")


def s3_list_objects(prefix: str, continuation_token: Optional[str] = None) -> tuple[list, Optional[str]]:
    """List objects with a given prefix."""

    if not _s3_client or not _bucket:
        logger.error("S3 not configured")
        raise S3Error("S3 not configured")

    try:
        resolved_prefix = s3_path(prefix)
        logger.debug("Listing s3://{}/{} (S3 Prefix: {})", _bucket, prefix, resolved_prefix)
        params = {"Bucket": _bucket, "Prefix": resolved_prefix}
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = _s3_client.list_objects_v2(**params)
        contents = response.get("Contents", [])
        next_token = response.get("NextContinuationToken")

        logger.debug("Listed {} objects", len(contents))
        return contents, next_token
    except ClientError as e:
        logger.error("S3 error listing {}: {}", prefix, e)
        raise S3Error(f"Failed to list {prefix}: {e}")


def list_codenames() -> list:
    """List all codenames by scanning the dists/ directory in S3.

    Returns a list of codename names found in the dists/ directory.
    Handles S3 pagination to ensure all codenames are found.
    """
    codenames = []
    continuation_token = None

    while True:
        result = s3_list_objects("dists/", continuation_token=continuation_token)
        objects, continuation_token = result

        for obj in objects:
            key = obj.get("Key", "")
            # Strip S3 prefix from key for comparison
            if _prefix and key.startswith(_prefix):
                key = key[len(_prefix):].lstrip("/")

            # Parse codename from path like "dists/stable/Release" or "dists/rc/main/binary-amd64/Packages"
            # Extract the codename (first directory after "dists/")
            if key.startswith("dists/"):
                # Remove "dists/" prefix and split
                path_after_dists = key[len("dists/"):]
                parts = path_after_dists.split("/")
                if len(parts) >= 2:
                    codename = parts[0]
                    if codename and codename not in codenames:
                        codenames.append(codename)

        if not continuation_token:
            break

    logger.debug("Found codenames: {}", codenames)
    return codenames
