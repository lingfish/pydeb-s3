"""Tests for the Lock module."""


import pytest

from pydeb_s3 import lock as lock_module


class TestLockPath:
    """Tests for lock path generation."""

    def test_initial_lock_path(self):
        """Returns correct initial lock path."""
        result = lock_module._initial_lock_path("stable")
        assert result == "dists/stable/lockfile.lock"

    def test_final_lock_path(self):
        """Returns correct final lock path."""
        result = lock_module._lock_path("stable")
        assert result == "dists/stable/lockfile"

    def test_initial_lock_path_with_codename(self):
        """Includes codename in initial lock path."""
        result = lock_module._initial_lock_path("testing")
        assert "testing" in result

    def test_lock_path_with_codename(self):
        """Includes codename in final lock path."""
        result = lock_module._lock_path("testing")
        assert "testing" in result


class TestLockDataclass:
    """Tests for Lock dataclass."""

    def test_creates_lock_with_user_host(self):
        """Creates Lock with user and host."""
        lock = lock_module.Lock(user="alice", host="host1.example.com")
        assert lock.user == "alice"
        assert lock.host == "host1.example.com"

    def test_lock_is_dataclass(self):
        """Verifies Lock is a dataclass."""
        assert hasattr(lock_module.Lock, "__dataclass_fields__")


class TestLockError:
    """Tests for LockError exception."""

    def test_lock_error_exists(self):
        """LockError is defined."""
        assert issubclass(lock_module.LockError, Exception)

    def test_can_raise_lock_error(self):
        """Can raise LockError."""
        with pytest.raises(lock_module.LockError):
            raise lock_module.LockError("test message")
