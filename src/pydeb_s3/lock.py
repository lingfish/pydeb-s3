"""Lock module for S3-based repository locking."""

import hashlib
import socket
import time
from dataclasses import dataclass

from pydeb_s3.s3_utils import (
    s3_exists,
    s3_read,
    s3_remove,
)


@dataclass
class Lock:
    """Represents a lock on the repository."""

    user: str
    host: str


class LockError(Exception):
    """Error acquiring lock."""


def _initial_lock_path(
    codename: str,
    component: str = None,
    architecture: str = None,
    cache_control: str = None,
) -> str:
    """Get the initial lock file path."""
    return f"dists/{codename}/lockfile.lock"


def _lock_path(
    codename: str,
    component: str = None,
    architecture: str = None,
    cache_control: str = None,
) -> str:
    """Get the final lock file path."""
    return f"dists/{codename}/lockfile"


def lock(
    codename: str,
    component: str = None,
    architecture: str = None,
    cache_control: str = None,
    max_attempts: int = 60,
    max_wait_interval: int = 10,
) -> None:
    """Acquire a lock on the repository."""
    import getpass

    lockbody = f"{getpass.getuser()}@{socket.gethostname()}"
    initial_lockfile = _initial_lock_path(codename, component, architecture, cache_control)
    final_lockfile = _lock_path(codename, component, architecture, cache_control)

    md5_hex = hashlib.md5(lockbody.encode()).hexdigest()

    for i in range(max_attempts):
        wait_interval = min((1 << i) / 10, max_wait_interval)

        if s3_exists(final_lockfile):
            current_lock = _current(codename, component, architecture, cache_control)
            print(
                f"Repository is locked by another user: {current_lock.user} at host {current_lock.host} (phase-1)"
            )
            print(f"Attempting to obtain a lock after {wait_interval} second(s).")
            time.sleep(wait_interval)
        else:
            try:
                s3_store_by_content(
                    lockbody,
                    initial_lockfile,
                    "text/plain",
                    md5=md5_hex,
                )
            except Exception:
                pass

            try:
                s3_copy_with_if_match(
                    initial_lockfile,
                    final_lockfile,
                    md5_hex,
                )
                return
            except Exception:
                current_lock = _current(codename, component, architecture, cache_control)
                print(
                    f"Repository is locked by another user: {current_lock.user} at host {current_lock.host} (phase-2)"
                )
                print(f"Attempting to obtain a lock after {wait_interval} second(s).")
                time.sleep(wait_interval)

    raise LockError(f"Unable to obtain a lock after {max_attempts} attempts, giving up.")


def s3_store_by_content(
    content: str,
    key: str,
    content_type: str = "text/plain",
    md5: str = None,
) -> None:
    """Store content directly to S3."""
    from pydeb_s3 import s3_utils

    if not s3_utils._s3_client or not s3_utils._bucket:
        return

    key = s3_utils.s3_path(key)
    extra_args = {"ContentType": content_type}

    if s3_utils._access_policy:
        extra_args["ACL"] = s3_utils._access_policy

    extra_args["Metadata"] = {}
    if md5:
        extra_args["Metadata"]["md5"] = md5

    s3_utils._s3_client.put_object(
        Bucket=s3_utils._bucket,
        Key=key,
        Body=content.encode(),
        **extra_args,
    )


def s3_copy_with_if_match(source: str, destination: str, etag: str) -> None:
    """Copy an object with If-Match condition."""
    from pydeb_s3 import s3_utils

    if not s3_utils._s3_client or not s3_utils._bucket:
        return

    source_path = s3_utils.s3_path(source)
    dest_path = s3_utils.s3_path(destination)

    try:
        s3_utils._s3_client.copy_object(
            Bucket=s3_utils._bucket,
            Key=dest_path,
            CopySource=f"/{s3_utils._bucket}/{source_path}",
            CopySourceIfMatch=etag,
        )
    except Exception as e:
        if "PreconditionFailed" in str(e):
            raise Exception("PreconditionFailed")
        raise


def unlock(
    codename: str,
    component: str = None,
    architecture: str = None,
    cache_control: str = None,
) -> None:
    """Release a lock on the repository."""
    initial = _initial_lock_path(codename, component, architecture, cache_control)
    final = _lock_path(codename, component, architecture, cache_control)

    if s3_exists(initial):
        s3_remove(initial)
    if s3_exists(final):
        s3_remove(final)


def _current(
    codename: str,
    component: str = None,
    architecture: str = None,
    cache_control: str = None,
) -> Lock:
    """Get the current lock holder."""
    lock_path = _lock_path(codename, component, architecture, cache_control)
    lockbody = s3_read(lock_path)

    if lockbody:
        user_host = lockbody.split("@", 1)
        if len(user_host) == 2:
            return Lock(user_host[0], user_host[1])

    return Lock("unknown", "unknown")
