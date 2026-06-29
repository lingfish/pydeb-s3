"""S3 utility functions for interacting with AWS S3.

Use S3Adapter from s3_adapter module instead of these utilities.
"""

from typing import Optional

import boto3
from loguru import logger

from pydeb_s3.progress import (  # noqa: F401
    BitsTransferSpeedColumn,
    UploadProgress,
    calculate_stream_md5,
)
from pydeb_s3.s3_adapter import Boto3S3Adapter, S3Adapter


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

    adapter = Boto3S3Adapter(
        client=client,
        bucket=bucket,
        prefix=prefix,
        access_policy=access_policy,
        encryption=encryption,
    )

    logger.success("S3 configured: bucket={}, prefix={}", bucket, prefix)
    return adapter



def list_codenames(adapter: "S3Adapter") -> list:
    """List all codenames by scanning the dists/ directory in S3.

    Args:
        adapter: S3Adapter to use.

    Returns a list of codename names found in the dists/ directory.
    Handles S3 pagination to ensure all codenames are found.
    """
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
