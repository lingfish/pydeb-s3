"""S3 utility functions for interacting with AWS S3."""

import hashlib
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from loguru import logger


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
) -> None:
    """Store a file in S3."""
    if not _s3_client or not _bucket:
        logger.error("S3 not configured")
        raise S3Error("S3 not configured")

    key = s3_path(key)
    logger.info("Storing {} to s3://{}/{}", filepath, _bucket, key)

    if fail_if_exists and s3_exists(key):
        existing = s3_head(key)
        if existing:
            file_md5 = hashlib.md5(open(filepath, "rb").read()).hexdigest()
            etag = existing.get("ETag", "").strip('"')
            meta_md5 = existing.get("Metadata", {}).get("md5", "")
            if file_md5 == etag or file_md5 == meta_md5:
                logger.info("File already exists with same content: {}", key)
                return
            logger.error("File exists with different content: {}", key)
            raise S3Error(f"file {key} already exists with different contents")

    extra_args = {"ContentType": content_type.split(";")[0].strip()}

    if _access_policy:
        extra_args["ACL"] = _access_policy

    if cache_control:
        extra_args["CacheControl"] = cache_control

    if _encryption:
        extra_args["ServerSideEncryption"] = "AES256"

    with open(filepath, "rb") as f:
        data = f.read()
        md5_hash = hashlib.md5(data).hexdigest()
        extra_args["Metadata"] = {"md5": md5_hash}

        _s3_client.put_object(
            Bucket=_bucket,
            Key=key,
            Body=data,
            **extra_args,
        )

    logger.success("Stored {} bytes to s3://{}/{}", len(data), _bucket, key)


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
        logger.debug("Listing s3://{}/{} with prefix {}", _bucket, prefix, prefix)
        params = {"Bucket": _bucket, "Prefix": s3_path(prefix)}
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
