"""Tests for the Lock module - claim-file-based distributed lock."""

import getpass as getpass_module
import os
import socket
import threading
import time

import pytest

from pydeb_s3 import lock as lock_module
from pydeb_s3.s3_adapter import MockS3Adapter


@pytest.fixture
def adapter():
    return MockS3Adapter()


class TestLockFunctions:
    """Tests for lock() and unlock() functions."""

    def test_lock_writes_unique_claim(self, adapter, monkeypatch):
        """Verify lock creates a unique claim file."""
        monkeypatch.setattr(socket, "gethostname", lambda: "testhost")
        monkeypatch.setattr(os, "getpid", lambda: 12345)
        monkeypatch.setattr(getpass_module, "getuser", lambda: "testuser")
        lock_module.lock(adapter, "stable", max_attempts=5, max_wait_interval=0.1)

        objects, _ = adapter.list_objects("dists/stable/locks/")
        assert len(objects) == 1
        key = objects[0]["Key"]
        assert key.endswith("dists/stable/locks/testhost-12345")

        body = adapter.read("dists/stable/locks/testhost-12345")
        assert body == "testuser@testhost"

    def test_lock_acquires_when_first(self, adapter, monkeypatch):
        """With no other claims, lock should succeed without error."""
        monkeypatch.setattr(socket, "gethostname", lambda: "testhost")
        monkeypatch.setattr(os, "getpid", lambda: 12345)
        monkeypatch.setattr(getpass_module, "getuser", lambda: "testuser")
        lock_module.lock(adapter, "stable", max_attempts=5, max_wait_interval=0.1)

    def test_unlock_removes_claim(self, adapter, monkeypatch):
        """Verify unlock removes the claim file."""
        monkeypatch.setattr(socket, "gethostname", lambda: "testhost")
        monkeypatch.setattr(os, "getpid", lambda: 12345)
        monkeypatch.setattr(getpass_module, "getuser", lambda: "testuser")
        lock_module.lock(adapter, "stable", max_attempts=5, max_wait_interval=0.1)
        lock_module.unlock(adapter, "stable")

        objects, _ = adapter.list_objects("dists/stable/locks/")
        assert len(objects) == 0

    def test_concurrent_locks_have_mutual_exclusion(self, adapter, monkeypatch):
        """Two processes: only one acquires the lock."""
        monkeypatch.setattr(getpass_module, "getuser", lambda: "testuser")

        # Process A acquires lock
        monkeypatch.setattr(socket, "gethostname", lambda: "hostA")
        monkeypatch.setattr(os, "getpid", lambda: 1000)
        lock_module.lock(adapter, "stable", max_attempts=5, max_wait_interval=0.1)

        # Process B fails to acquire
        monkeypatch.setattr(socket, "gethostname", lambda: "hostB")
        monkeypatch.setattr(os, "getpid", lambda: 2000)
        with pytest.raises(lock_module.LockError):
            lock_module.lock(adapter, "stable", max_attempts=2, max_wait_interval=0.01)

    def test_lock_retries_on_contention(self, adapter, monkeypatch):
        """Verify lock retries when another claim exists, then acquires."""
        monkeypatch.setattr(getpass_module, "getuser", lambda: "testuser")
        monkeypatch.setattr(socket, "gethostname", lambda: "testhost")
        monkeypatch.setattr(os, "getpid", lambda: 54321)

        prefix = "dists/stable/locks/"
        adapter.store_content("other@aaa", prefix + "aaa-1")

        def remove_competing():
            time.sleep(0.05)
            adapter.remove(prefix + "aaa-1")

        thread = threading.Thread(target=remove_competing)
        thread.start()
        time.sleep(0.01)

        lock_module.lock(adapter, "stable", max_attempts=30, max_wait_interval=0.1)
        thread.join()

        objects, _ = adapter.list_objects(prefix)
        assert len(objects) == 1

    def test_lock_times_out(self, adapter, monkeypatch):
        """Verify LockError after max retries when another claim exists."""
        monkeypatch.setattr(getpass_module, "getuser", lambda: "testuser")
        monkeypatch.setattr(socket, "gethostname", lambda: "testhost")
        monkeypatch.setattr(os, "getpid", lambda: 99999)

        prefix = "dists/stable/locks/"
        adapter.store_content("other@aaa", prefix + "aaa-1")

        with pytest.raises(lock_module.LockError):
            lock_module.lock(adapter, "stable", max_attempts=2, max_wait_interval=0.01)

    def test_current_returns_lock_holder(self, adapter, monkeypatch):
        """Verify _current() returns the correct holder."""
        monkeypatch.setattr(socket, "gethostname", lambda: "testhost")
        monkeypatch.setattr(os, "getpid", lambda: 12345)
        monkeypatch.setattr(getpass_module, "getuser", lambda: "testuser")
        lock_module.lock(adapter, "stable", max_attempts=5, max_wait_interval=0.1)

        current = lock_module._current(adapter, "stable")
        assert current.user == "testuser"
        assert current.host == "testhost"

    def test_no_race_between_sequential_locks(self, adapter, monkeypatch):
        """Lock, unlock, lock again should work."""
        monkeypatch.setattr(socket, "gethostname", lambda: "testhost")
        monkeypatch.setattr(os, "getpid", lambda: 12345)
        monkeypatch.setattr(getpass_module, "getuser", lambda: "testuser")

        lock_module.lock(adapter, "stable", max_attempts=5, max_wait_interval=0.1)
        lock_module.unlock(adapter, "stable")
        lock_module.lock(adapter, "stable", max_attempts=5, max_wait_interval=0.1)

    def test_initial_lock_path_not_available(self):
        """Old _initial_lock_path and _lock_path should not exist."""
        assert not hasattr(lock_module, "_initial_lock_path")
        assert not hasattr(lock_module, "_lock_path")

    def test_locks_prefix(self):
        """_locks_prefix returns correct path."""
        assert lock_module._locks_prefix("stable") == "dists/stable/locks/"
        assert lock_module._locks_prefix("testing") == "dists/testing/locks/"
