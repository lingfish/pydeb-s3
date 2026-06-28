"""Lock module for S3-based repository locking."""

import os
import socket
import time
from dataclasses import dataclass

from pydeb_s3.s3_adapter import S3Adapter


@dataclass
class Lock:
    """Represents a lock on the repository."""

    user: str
    host: str


class LockError(Exception):
    """Error acquiring lock."""


def _locks_prefix(codename: str) -> str:
    """Get the locks prefix path for a codename."""
    return f"dists/{codename}/locks/"


def lock(
    s3_adapter: S3Adapter,
    codename: str,
    component: str = None,
    architecture: str = None,
    cache_control: str = None,
    max_attempts: int = 60,
    max_wait_interval: int = 10,
) -> None:
    """Acquire a lock on the repository using claim-file-based distributed lock."""
    import getpass

    hostname = socket.gethostname()
    pid = os.getpid()
    claim_id = f"{hostname}-{pid}"
    lockbody = f"{getpass.getuser()}@{hostname}"
    prefix = _locks_prefix(codename)
    claim_path = prefix + claim_id

    s3_adapter.store_content(lockbody, claim_path, content_type="text/plain")

    acquired = False
    try:
        for i in range(max_attempts):
            wait_interval = min((1 << i) / 10, max_wait_interval)

            objects, _ = s3_adapter.list_objects(prefix)
            claim_keys = sorted(obj["Key"] for obj in objects)

            if not claim_keys:
                s3_adapter.store_content(lockbody, claim_path, content_type="text/plain")
                continue

            first_key = claim_keys[0]
            if first_key.endswith(claim_path):
                acquired = True
                return

            current_lock = _current(s3_adapter, codename)
            print(
                f"Repository is locked by another user: {current_lock.user} at host {current_lock.host}"
            )
            print(f"Attempting to obtain a lock after {wait_interval} second(s).")
            time.sleep(wait_interval)

        raise LockError(f"Unable to obtain a lock after {max_attempts} attempts, giving up.")
    finally:
        if not acquired:
            try:
                s3_adapter.remove(claim_path)
            except Exception:
                pass


def unlock(
    s3_adapter: S3Adapter,
    codename: str,
    component: str = None,
    architecture: str = None,
    cache_control: str = None,
) -> None:
    """Release a lock on the repository. Removes all claim files."""
    prefix = _locks_prefix(codename)
    objects, _ = s3_adapter.list_objects(prefix)
    for obj in objects:
        key = obj["Key"]
        idx = key.find(prefix)
        if idx >= 0:
            relative_path = key[idx:]
            try:
                s3_adapter.remove(relative_path)
            except Exception:
                pass


def _current(
    s3_adapter: S3Adapter,
    codename: str,
    component: str = None,
    architecture: str = None,
    cache_control: str = None,
) -> Lock:
    """Get the current lock holder by reading the first claim file."""
    prefix = _locks_prefix(codename)
    objects, _ = s3_adapter.list_objects(prefix)
    claim_keys = sorted(obj["Key"] for obj in objects)

    if not claim_keys:
        return Lock("unknown", "unknown")

    first_key = claim_keys[0]
    idx = first_key.find(prefix)
    if idx < 0:
        return Lock("unknown", "unknown")

    relative_path = first_key[idx:]
    try:
        body = s3_adapter.read(relative_path)
    except Exception:
        return Lock("unknown", "unknown")

    parts = body.split("@", 1)
    if len(parts) == 2:
        return Lock(parts[0], parts[1])
    return Lock("unknown", "unknown")
