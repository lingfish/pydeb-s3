"""S3 Adapter module - provides a seam for S3 operations.

Defines the S3Adapter protocol and Boto3S3Adapter implementation,
replacing the global state pattern in s3_utils.py.
"""

import hashlib
import os
from typing import Optional, Protocol, Tuple

from botocore.exceptions import ClientError
from loguru import logger

from pydeb_s3.progress import UploadProgress, calculate_stream_md5


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


class S3Adapter(Protocol):
    """Interface for S3 operations - a seam at the storage boundary.

    This protocol defines the interface that all S3 adapters must satisfy.
    Implementations can be real (Boto3S3Adapter) or mock (for testing).

    Interface facts for callers:
    - All methods raise S3Error (or subclass) on failure
    - exists() returns False (not raise) on 404
    - store_file() handles ContentType, ACL, CacheControl, encryption
    - list_objects() handles pagination via continuation_token
    """

    def store_file(
        self,
        filepath: str,
        key: str,
        content_type: str = "application/octet-stream",
        cache_control: Optional[str] = None,
        fail_if_exists: bool = False,
        show_progress: Optional[bool] = None,
        use_bytes: bool = False,
        progress: Optional[object] = None,
    ) -> None:
        """Store a local file to S3.

        Args:
            filepath: Path to the local file to upload
            key: S3 key (path) for the file
            content_type: Content type for the S3 object
            cache_control: Cache control header
            fail_if_exists: If True, raise error if file with different content exists
            show_progress: Force progress display on/off. If None, auto-detect from TTY.
            use_bytes: If True, display speed in bytes/s. If False, display in bits/s.
            progress: Optional shared Progress instance for multiple uploads.
        """
        ...

    def read(self, path: str) -> str:
        """Read an object from S3, return as string.

        Raises:
            S3NotFoundError: If object doesn't exist
            S3Error: On other S3 failures
        """
        ...

    def exists(self, path: str) -> bool:
        """Check if an object exists in S3.

        Returns:
            True if exists, False otherwise
        """
        ...

    def remove(self, path: str) -> None:
        """Remove an object from S3.

        Raises:
            S3NotFoundError: If object doesn't exist
            S3Error: On other S3 failures
        """
        ...

    def copy(self, source: str, destination: str) -> None:
        """Copy an object within S3.

        Raises:
            S3NotFoundError: If source doesn't exist
            S3AccessError: If access denied
            S3Error: On other S3 failures
        """
        ...

    def head(self, path: str) -> dict:
        """Get head/metadata for an object.

        Returns:
            dict with headers/metadata

        Raises:
            S3NotFoundError: If object doesn't exist
            S3Error: On other S3 failures
        """
        ...

    def list_objects(
        self, prefix: str, continuation_token: Optional[str] = None
    ) -> Tuple[list, Optional[str]]:
        """List objects with a given prefix.

        Args:
            prefix: S3 prefix to list
            continuation_token: Token for pagination

        Returns:
            Tuple of (contents list, next continuation token)
        """
        ...

    def store_content(
        self,
        content: str,
        key: str,
        content_type: str = "text/plain",
        md5: Optional[str] = None,
    ) -> None:
        """Store string content directly to S3 (for lock files, etc).

        Args:
            content: String content to store
            key: S3 key (path) for the object
            content_type: Content type for the S3 object
            md5: Optional MD5 hash for conditional operations
        """
        ...

    def copy_with_if_match(
        self, source: str, destination: str, etag: str
    ) -> None:
        """Copy an object with If-Match condition (for lock mechanism).

        Args:
            source: Source S3 key
            destination: Destination S3 key
            etag: ETag value for condition

        Raises:
            Exception: With "PreconditionFailed" in message if condition fails
        """
        ...


class Boto3S3Adapter:
    """Concrete adapter wrapping boto3 S3 client.

    This is the production implementation that replaces the global state
    pattern from s3_utils.py.

    Attributes:
        bucket: S3 bucket name
        prefix: Path prefix for all S3 operations
        access_policy: ACL policy (public-read, private, etc.)
        encryption: Whether to use AES256 server-side encryption
        _client: boto3 S3 client
    """

    def __init__(
        self,
        client,
        bucket: str,
        prefix: Optional[str] = None,
        access_policy: Optional[str] = None,
        encryption: bool = False,
    ):
        """Initialize the adapter.

        Args:
            client: boto3 S3 client
            bucket: S3 bucket name
            prefix: Path prefix for all S3 operations
            access_policy: ACL policy (public-read, private, etc.)
            encryption: Whether to use AES256 server-side encryption
        """
        self._client = client
        self.bucket = bucket
        self.prefix = prefix
        self.access_policy = access_policy
        self.encryption = encryption

    def _s3_path(self, path: str) -> str:
        """Get the full S3 path with prefix."""
        if self.prefix:
            return os.path.join(self.prefix, path)
        return path

    def store_file(
        self,
        filepath: str,
        key: str,
        content_type: str = "application/octet-stream",
        cache_control: Optional[str] = None,
        fail_if_exists: bool = False,
        show_progress: Optional[bool] = None,
        use_bytes: bool = False,
        progress: Optional[object] = None,
    ) -> None:
        """Store a local file to S3."""
        if not self._client or not self.bucket:
            logger.error("S3 not configured")
            raise S3Error("S3 not configured")

        key = self._s3_path(key)

        # Get file size for progress tracking
        filesize = os.path.getsize(filepath)

        # Calculate MD5 using streaming (memory efficient)
        md5_hash = calculate_stream_md5(filepath)

        if fail_if_exists and self.exists(key):
            existing = self.head(key)
            if existing:
                etag = existing.get("ETag", "").strip('"')
                meta_md5 = existing.get("Metadata", {}).get("md5", "")
                if md5_hash == etag or md5_hash == meta_md5:
                    logger.info("File already exists with same content: {}", key)
                    return
                logger.error("File exists with different content: {}", key)
                raise S3Error(f"file {key} already exists with different contents")

        extra_args = {
            "ContentType": content_type.split(";")[0].strip(),
            "Metadata": {"md5": md5_hash}
        }

        if self.access_policy:
            extra_args["ACL"] = self.access_policy

        if cache_control:
            extra_args["CacheControl"] = cache_control

        if self.encryption:
            extra_args["ServerSideEncryption"] = "AES256"

        # Create progress callback if needed
        progress_console = None
        if progress is not None:
            try:
                from rich.progress import Progress
                if isinstance(progress, Progress):
                    progress_console = progress.console
            except ImportError:
                pass

        if progress_console is not None:
            progress_console.print(f"Storing {os.path.basename(filepath)} to s3://{self.bucket}/{key}")
        else:
            logger.info("Storing {} to s3://{}/{}", filepath, self.bucket, key)

        # Create progress callback
        progress_callback = UploadProgress(
            filename=os.path.basename(filepath),
            filesize=filesize,
            interactive=show_progress,
            use_bytes=use_bytes,
            progress=progress
        )

        # Use upload_file for progress callback support and automatic multipart
        try:
            self._client.upload_file(
                filepath,
                self.bucket,
                key,
                ExtraArgs=extra_args,
                Callback=progress_callback
            )
        finally:
            progress_callback.cleanup()

        if progress_console:
            progress_console.print(f"Stored {filesize} bytes to s3://{self.bucket}/{key}")
        else:
            logger.success("Stored {} bytes to s3://{}/{}", filesize, self.bucket, key)

    def read(self, path: str) -> str:
        """Read an object from S3."""
        if not self._client or not self.bucket:
            logger.error("S3 not configured")
            raise S3Error("S3 not configured")

        try:
            logger.info("Reading s3://{}/{}", self.bucket, path)
            response = self._client.get_object(Bucket=self.bucket, Key=self._s3_path(path))
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

    def exists(self, path: str) -> bool:
        """Check if an object exists in S3."""
        if not self._client or not self.bucket:
            logger.error("S3 not configured")
            raise S3Error("S3 not configured")

        try:
            self._client.head_object(Bucket=self.bucket, Key=self._s3_path(path))
            logger.debug("Object exists: {}", path)
            return True
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "404":
                logger.debug("Object not found: {}", path)
                return False
            logger.error("S3 error checking {}: {}", path, e)
            raise S3Error(f"Failed to check {path}: {e}")

    def remove(self, path: str) -> None:
        """Remove an object from S3."""
        if not self._client or not self.bucket:
            logger.error("S3 not configured")
            raise S3Error("S3 not configured")

        try:
            logger.info("Removing s3://{}/{}", self.bucket, path)
            self._client.delete_object(Bucket=self.bucket, Key=self._s3_path(path))
            logger.success("Removed s3://{}/{}", self.bucket, path)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "404":
                logger.warning("Object not found (already removed?): {}", path)
                raise S3NotFoundError(path)
            logger.error("S3 error removing {}: {}", path, e)
            raise S3Error(f"Failed to remove {path}: {e}")

    def copy(self, source: str, destination: str) -> None:
        """Copy an object within S3."""
        if not self._client or not self.bucket:
            logger.error("S3 not configured")
            raise S3Error("S3 not configured")

        try:
            logger.info("Copying s3://{}/{} to s3://{}/{}", self.bucket, source, self.bucket, destination)
            self._client.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": self._s3_path(source)},
                Key=self._s3_path(destination),
            )
            logger.success("Copied s3://{}/{} to s3://{}/{}", self.bucket, source, self.bucket, destination)
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

    def head(self, path: str) -> dict:
        """Get head/metadata for an object."""
        if not self._client or not self.bucket:
            logger.error("S3 not configured")
            raise S3Error("S3 not configured")

        try:
            logger.debug("Head s3://{}/{}", self.bucket, path)
            return self._client.head_object(Bucket=self.bucket, Key=self._s3_path(path))
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "404":
                logger.warning("Object not found: {}", path)
                raise S3NotFoundError(path)
            logger.error("S3 error head {}: {}", path, e)
            raise S3Error(f"Failed to head {path}: {e}")

    def list_objects(
        self, prefix: str, continuation_token: Optional[str] = None
    ) -> Tuple[list, Optional[str]]:
        """List objects with a given prefix."""
        if not self._client or not self.bucket:
            logger.error("S3 not configured")
            raise S3Error("S3 not configured")

        try:
            resolved_prefix = self._s3_path(prefix)
            logger.debug("Listing s3://{}/{} (S3 Prefix: {})", self.bucket, prefix, resolved_prefix)
            params = {"Bucket": self.bucket, "Prefix": resolved_prefix}
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = self._client.list_objects_v2(**params)
            contents = response.get("Contents", [])
            next_token = response.get("NextContinuationToken")

            logger.debug("Listed {} objects", len(contents))
            return contents, next_token
        except ClientError as e:
            logger.error("S3 error listing {}: {}", prefix, e)
            raise S3Error(f"Failed to list {prefix}: {e}")

    def store_content(
        self,
        content: str,
        key: str,
        content_type: str = "text/plain",
        md5: Optional[str] = None,
    ) -> None:
        """Store string content directly to S3."""
        if not self._client or not self.bucket:
            return

        key = self._s3_path(key)
        extra_args = {"ContentType": content_type}

        if self.access_policy:
            extra_args["ACL"] = self.access_policy

        extra_args["Metadata"] = {}
        if md5:
            extra_args["Metadata"]["md5"] = md5

        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content.encode(),
            **extra_args,
        )

    def copy_with_if_match(
        self, source: str, destination: str, etag: str
    ) -> None:
        """Copy an object with If-Match condition."""
        if not self._client or not self.bucket:
            return

        source_path = self._s3_path(source)
        dest_path = self._s3_path(destination)

        try:
            self._client.copy_object(
                Bucket=self.bucket,
                Key=dest_path,
                CopySource=f"/{self.bucket}/{source_path}",
                CopySourceIfMatch=etag,
            )
        except Exception as e:
            if "PreconditionFailed" in str(e):
                raise Exception("PreconditionFailed")
            raise


class MockS3Adapter:
    """In-memory mock S3 adapter for testing.

    This adapter stores objects in a simple dictionary, providing
    a fast in-memory implementation for unit tests without requiring
    moto or real S3.

    Attributes:
        bucket: S3 bucket name (for interface compatibility)
        prefix: Path prefix for all S3 operations
        _storage: Internal dictionary storing objects
    """

    def __init__(
        self,
        bucket: str = "test-bucket",
        prefix: Optional[str] = None,
        access_policy: Optional[str] = None,
        encryption: bool = False,
    ):
        """Initialize the mock adapter.

        Args:
            bucket: S3 bucket name (stored but not used in memory)
            prefix: Path prefix for all S3 operations
            access_policy: ACL policy (stored but not used)
            encryption: Whether encryption would be used (stored but not used)
        """
        self.bucket = bucket
        self.prefix = prefix
        self.access_policy = access_policy
        self.encryption = encryption
        self._storage: dict[str, bytes] = {}
        self._metadata: dict[str, dict] = {}

    def _s3_path(self, path: str) -> str:
        """Get the full S3 path with prefix."""
        if self.prefix:
            return os.path.join(self.prefix, path)
        return path

    def store_file(
        self,
        filepath: str,
        key: str,
        content_type: str = "application/octet-stream",
        cache_control: Optional[str] = None,
        fail_if_exists: bool = False,
        show_progress: Optional[bool] = None,
        use_bytes: bool = False,
        progress: Optional[object] = None,
    ) -> None:
        """Store a local file to the mock S3."""
        full_key = self._s3_path(key)

        if fail_if_exists and full_key in self._storage:
            existing_meta = self._metadata.get(full_key, {})
            with open(filepath, "rb") as f:
                new_content = f.read()
            existing_content = self._storage.get(full_key, b"")
            if new_content == existing_content:
                return  # Same content, don't error
            raise S3Error(f"file {key} already exists with different contents")

        with open(filepath, "rb") as f:
            content = f.read()

        self._storage[full_key] = content
        self._metadata[full_key] = {
            "ContentLength": len(content),
            "ContentType": content_type,
            "Metadata": {"md5": hashlib.md5(content).hexdigest()},
        }

    def read(self, path: str) -> str:
        """Read an object from the mock S3."""
        full_key = self._s3_path(path)
        if full_key not in self._storage:
            raise S3NotFoundError(path)
        return self._storage[full_key].decode("utf-8")

    def exists(self, path: str) -> bool:
        """Check if an object exists in the mock S3."""
        full_key = self._s3_path(path)
        return full_key in self._storage

    def remove(self, path: str) -> None:
        """Remove an object from the mock S3."""
        full_key = self._s3_path(path)
        if full_key not in self._storage:
            raise S3NotFoundError(path)
        del self._storage[full_key]
        if full_key in self._metadata:
            del self._metadata[full_key]

    def copy(self, source: str, destination: str) -> None:
        """Copy an object within the mock S3."""
        source_key = self._s3_path(source)
        dest_key = self._s3_path(destination)

        if source_key not in self._storage:
            raise S3NotFoundError(source)

        self._storage[dest_key] = self._storage[source_key]
        self._metadata[dest_key] = self._metadata.get(source_key, {}).copy()

    def head(self, path: str) -> dict:
        """Get head/metadata for an object."""
        full_key = self._s3_path(path)
        if full_key not in self._storage:
            raise S3NotFoundError(path)
        return self._metadata.get(full_key, {})

    def list_objects(
        self, prefix: str, continuation_token: Optional[str] = None
    ) -> tuple[list, Optional[str]]:
        """List objects with a given prefix."""
        resolved_prefix = self._s3_path(prefix)
        contents = []
        for key in self._storage:
            if key.startswith(resolved_prefix):
                contents.append({
                    "Key": key,
                    "Size": len(self._storage[key]),
                })
        return contents, None

    def store_content(
        self,
        content: str,
        key: str,
        content_type: str = "text/plain",
        md5: Optional[str] = None,
    ) -> None:
        """Store string content directly to the mock S3."""
        full_key = self._s3_path(key)
        self._storage[full_key] = content.encode("utf-8")
        self._metadata[full_key] = {
            "ContentLength": len(content),
            "ContentType": content_type,
            "Metadata": {"md5": md5} if md5 else {},
        }

    def copy_with_if_match(
        self, source: str, destination: str, etag: str
    ) -> None:
        """Copy an object with If-Match condition."""
        source_key = self._s3_path(source)
        dest_key = self._s3_path(destination)

        if source_key not in self._storage:
            raise S3NotFoundError(source)

        # Check etag match (simplified - just compare with stored md5)
        source_meta = self._metadata.get(source_key, {})
        stored_etag = source_meta.get("Metadata", {}).get("md5", "")

        if etag != stored_etag:
            raise Exception("PreconditionFailed")

        self._storage[dest_key] = self._storage[source_key]
        self._metadata[dest_key] = source_meta.copy()
