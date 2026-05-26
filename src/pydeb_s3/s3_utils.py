"""S3 utility functions for interacting with AWS S3.

Deprecated: Use S3Adapter from s3_adapter module instead.
This module is maintained for backward compatibility with existing tests.
"""

from typing import Optional

import boto3
from loguru import logger

# Re-export progress utilities for backward compatibility
from pydeb_s3.s3_adapter import Boto3S3Adapter, S3Adapter, S3Error
from pydeb_s3.progress import BitsTransferSpeedColumn, UploadProgress, calculate_stream_md5  # noqa: F401

# Global S3Adapter instance (deprecated - for backward compatibility only)
_s3_adapter: Optional[S3Adapter] = None


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
) -> S3Adapter:
    """Configure S3 and return an S3Adapter instance."""
    global _s3_adapter

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

    client = boto3.client("s3", **settings)

    # Map visibility to access policy
    access_policy = None
    if visibility == "public":
        access_policy = "public-read"
    elif visibility == "private":
        access_policy = "private"
    elif visibility == "authenticated":
        access_policy = "authenticated-read"
    elif visibility == "bucket_owner":
        access_policy = "bucket-owner-full-control"

    _s3_adapter = Boto3S3Adapter(
        client=client,
        bucket=bucket,
        prefix=prefix,
        access_policy=access_policy,
        encryption=encryption,
    )

    logger.success("S3 configured: bucket={}, prefix={}", bucket, prefix)
    return _s3_adapter


def _get_adapter() -> S3Adapter:
    """Get the global S3Adapter instance, raising S3Error if not configured."""
    if _s3_adapter is None:
        logger.error("S3 not configured")
        raise S3Error("S3 not configured")
    return _s3_adapter


# Backward compatibility wrapper functions
def s3_store(
    filepath: str,
    key: str,
    content_type: str = "application/octet-stream",
    cache_control: Optional[str] = None,
    fail_if_exists: bool = False,
    use_bytes: bool = False,
    progress: Optional[object] = None,
) -> None:
    """Backward compatibility wrapper - use S3Adapter.store_file() instead."""
    adapter = _get_adapter()
    adapter.store_file(
        filepath=filepath,
        key=key,
        content_type=content_type,
        cache_control=cache_control,
        fail_if_exists=fail_if_exists,
        use_bytes=use_bytes,
        progress=progress,
    )


def s3_read(path: str) -> str:
    """Backward compatibility wrapper - use S3Adapter.read() instead."""
    adapter = _get_adapter()
    return adapter.read(path)


def s3_exists(path: str) -> bool:
    """Backward compatibility wrapper - use S3Adapter.exists() instead."""
    adapter = _get_adapter()
    return adapter.exists(path)


def s3_remove(path: str) -> None:
    """Backward compatibility wrapper - use S3Adapter.remove() instead."""
    adapter = _get_adapter()
    return adapter.remove(path)


def s3_copy(source: str, destination: str) -> None:
    """Backward compatibility wrapper - use S3Adapter.copy() instead."""
    adapter = _get_adapter()
    return adapter.copy(source, destination)


def s3_head(path: str) -> dict:
    """Backward compatibility wrapper - use S3Adapter.head() instead."""
    adapter = _get_adapter()
    return adapter.head(path)


def s3_list_objects(prefix: str, continuation_token: Optional[str] = None) -> tuple[list, Optional[str]]:
    """Backward compatibility wrapper - use S3Adapter.list_objects() instead."""
    adapter = _get_adapter()
    return adapter.list_objects(prefix, continuation_token)


def s3_path(path: str) -> str:
    """Backward compatibility wrapper - use S3Adapter._s3_path() instead."""
    adapter = _get_adapter()
    return adapter._s3_path(path)








def list_codenames(adapter: Optional["S3Adapter"] = None) -> list:
    """List all codenames by scanning the dists/ directory in S3.

    Args:
        adapter: Optional S3Adapter to use. If None, uses the global adapter.

    Returns a list of codename names found in the dists/ directory.
    Handles S3 pagination to ensure all codenames are found.
    """
    # Use the provided adapter or get the global adapter
    if adapter is None:
        adapter = _get_adapter()

    codenames = []
    continuation_token = None

    while True:
        contents, continuation_token = adapter.list_objects("dists/", continuation_token=continuation_token)

        for obj in contents:
            key = obj.get("Key", "")
            # Strip S3 prefix from key for comparison
            if adapter.prefix:
                prefix_to_use = adapter.prefix.rstrip("/") + "/"
                if key.startswith(prefix_to_use):
                    key = key[len(prefix_to_use):]

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
