"""S3 utility functions for interacting with AWS S3."""

import hashlib
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError

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


def s3_path(path: str) -> str:
    """Get the full S3 path with prefix."""
    if _prefix:
        return os.path.join(_prefix, path)
    return path


def s3_exists(path: str) -> bool:
    """Check if an object exists in S3."""
    if not _s3_client or not _bucket:
        return False
    try:
        _s3_client.head_object(Bucket=_bucket, Key=s3_path(path))
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def s3_read(path: str) -> Optional[str]:
    """Read an object from S3."""
    if not _s3_client or not _bucket:
        return None
    try:
        response = _s3_client.get_object(Bucket=_bucket, Key=s3_path(path))
        return response["Body"].read().decode("utf-8")
    except ClientError:
        return None


def s3_store(
    filepath: str,
    key: str,
    content_type: str = "application/octet-stream; charset=binary",
    cache_control: Optional[str] = None,
    fail_if_exists: bool = False,
) -> None:
    """Store a file in S3."""
    if not _s3_client or not _bucket:
        return

    key = s3_path(key)

    if fail_if_exists and s3_exists(key):
        existing = s3_head(key)
        if existing:
            file_md5 = hashlib.md5(open(filepath, "rb").read()).hexdigest()
            etag = existing.get("ETag", "").strip('"')
            meta_md5 = existing.get("Metadata", {}).get("md5", "")
            if file_md5 == etag or file_md5 == meta_md5:
                return
            raise Exception(f"file {key} already exists with different contents")

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


def s3_head(path: str) -> Optional[dict]:
    """Get head information for an object."""
    if not _s3_client or not _bucket:
        return None
    try:
        return _s3_client.head_object(Bucket=_bucket, Key=s3_path(path))
    except ClientError:
        return None


def s3_remove(path: str) -> None:
    """Remove an object from S3."""
    if not _s3_client or not _bucket:
        return
    try:
        _s3_client.delete_object(Bucket=_bucket, Key=s3_path(path))
    except ClientError:
        pass


def s3_copy(source: str, destination: str) -> None:
    """Copy an object in S3."""
    if not _s3_client or not _bucket:
        return
    _s3_client.copy_object(
        Bucket=_bucket,
        CopySource=s3_path(source),
        Key=s3_path(destination),
    )


def s3_list_objects(prefix: str, continuation_token: Optional[str] = None) -> tuple[list, Optional[str]]:
    """List objects with a given prefix."""
    if not _s3_client or not _bucket:
        return [], None

    params = {"Bucket": _bucket, "Prefix": s3_path(prefix)}
    if continuation_token:
        params["ContinuationToken"] = continuation_token

    response = _s3_client.list_objects_v2(**params)
    contents = response.get("Contents", [])
    next_token = response.get("NextContinuationToken")

    return contents, next_token


def log(message: str) -> None:
    """Print a log message."""
    print(f">> {message}")


def sublog(message: str) -> None:
    """Print a sub log message."""
    print(f"   -- {message}")


def error(message: str) -> None:
    """Print an error message and exit."""
    print(f"!! {message}", file=__import__("sys").stderr)
    __import__("sys").exit(1)
