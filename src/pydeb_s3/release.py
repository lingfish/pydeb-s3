"""Release file module for APT repository."""

import os
import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import Optional

from pydeb_s3 import manifest as man_module
from pydeb_s3.s3_utils import (
    s3_read,
    s3_remove,
    s3_store,
)


@dataclass
class Release:
    """Represents a Release file for APT repository."""

    codename: str = "stable"
    origin: Optional[str] = None
    suite: Optional[str] = None
    architectures: list[str] = field(default_factory=list)
    components: list[str] = field(default_factory=list)
    cache_control: str = ""
    files: dict = field(default_factory=dict)
    policy: str = "public_read"

    @classmethod
    def retrieve(
        cls,
        codename: str,
        origin: Optional[str] = None,
        suite: Optional[str] = None,
        cache_control: str = "",
    ) -> "Release":
        """Retrieve or create a Release object."""
        path = f"dists/{codename}/Release"
        s = s3_read(path)

        r = cls()
        if s:
            r._parse(s)

        r.codename = codename
        if origin is not None:
            r.origin = origin
        if suite is not None:
            r.suite = suite
        r.cache_control = cache_control
        return r

    def _parse(self, content: str) -> None:
        """Parse Release file content."""
        self.codename = self._get_field(content, "Codename")
        self.origin = self._get_field(content, "Origin")
        self.suite = self._get_field(content, "Suite")

        archs = self._get_field(content, "Architectures")
        if archs:
            self.architectures = archs.split()

        comps = self._get_field(content, "Components")
        if comps:
            self.components = comps.split()

        for line in content.split("\n"):
            match = re.match(r"^\s+(\S+)\s+(\d+)\s+(.+)$", line)
            if match:
                hash_str, size, name = match.groups()
                self.files[name] = {"size": int(size)}
                if len(hash_str) == 32:
                    self.files[name]["md5"] = hash_str
                elif len(hash_str) == 40:
                    self.files[name]["sha1"] = hash_str
                elif len(hash_str) == 64:
                    self.files[name]["sha256"] = hash_str

    def _get_field(self, content: str, field: str) -> Optional[str]:
        """Get a field from Release content."""
        pattern = f"^{field}: (.+)$"
        match = re.search(pattern, content, re.MULTILINE)
        if match:
            return match.group(1)
        return None

    @property
    def filename(self) -> str:
        """Get the Release filename."""
        return f"dists/{self.codename}/Release"

    def generate(self) -> str:
        """Generate Release file content."""
        lines = []

        if self.origin:
            lines.append(f"Origin: {self.origin}")
        if self.suite:
            lines.append(f"Suite: {self.suite}")
        lines.append(f"Codename: {self.codename}")

        if self.components:
            lines.append(f"Components: {' '.join(self.components)}")
        if self.architectures:
            lines.append(f"Architectures: {' '.join(self.architectures)}")

        lines.append("")
        for name, hashes in sorted(self.files.items()):
            for hash_type in ["md5", "sha1", "sha256"]:
                if hash_type in hashes:
                    lines.append(f" {hashes[hash_type]} {hashes.get('size', 0)} {name}")
                    break

        return "\n".join(lines)

    def write_to_s3(self, callback: Optional[callable] = None) -> None:
        """Write the Release file to S3."""
        self._validate_others(callback)

        release_content = self.generate()
        release_temp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".Release", delete=False
        )
        try:
            release_temp.write(release_content)
            release_temp.close()

            if callback:
                callback(self.filename)
            s3_store(
                release_temp.name,
                self.filename,
                "text/plain; charset=utf-8",
                self.cache_control,
            )

            self._sign_release(release_temp.name, callback)
        finally:
            os.unlink(release_temp.name)

    def _sign_release(
        self, release_path: str, callback: Optional[callable] = None
    ) -> None:
        """Sign the Release file with GPG."""
        from pydeb_s3.s3_utils import _gpg_options, _gpg_provider, _signing_key

        signing_key = _signing_key or []
        if not signing_key:
            return

        key_param = " ".join(f"-u {k}" for k in signing_key) if signing_key else ""

        cmd = f"{_gpg_provider} -a {key_param} --digest-algo SHA256 {_gpg_options} -s --clearsign {release_path}"
        result = subprocess.run(cmd, check=False, shell=True, capture_output=True)

        if result.returncode == 0:
            asc_path = release_path + ".asc"
            if os.path.exists(asc_path):
                inrelease_path = f"dists/{self.codename}/InRelease"
                if callback:
                    callback(inrelease_path)
                s3_store(
                    asc_path,
                    inrelease_path,
                    "application/pgp-signature; charset=us-ascii",
                    self.cache_control,
                )
                os.unlink(asc_path)

        cmd = f"{_gpg_provider} -a {key_param} --digest-algo SHA256 {_gpg_options} -b {release_path}"
        result = subprocess.run(cmd, check=False, shell=True, capture_output=True)

        if result.returncode == 0:
            asc_path = release_path + ".asc"
            if os.path.exists(asc_path):
                gpg_path = self.filename + ".gpg"
                if callback:
                    callback(gpg_path)
                s3_store(
                    asc_path,
                    gpg_path,
                    "application/pgp-signature; charset=us-ascii",
                    self.cache_control,
                )
                os.unlink(asc_path)
        else:
            s3_remove(self.filename + ".gpg")

    def _validate_others(self, callback: Optional[callable] = None) -> None:
        """Validate other architectures are present."""
        for comp in self.components:
            for arch in ["amd64", "i386", "armhf", "arm64"]:
                key = f"{comp}/binary-{arch}/Packages"
                if key in self.files or arch in self.architectures:
                    m = man_module.Manifest()
                    m.codename = self.codename
                    m.component = comp
                    m.architecture = arch
                    m.write_to_s3(callback)
                    self.update_manifest(m)

    def update_manifest(self, manifest: man_module.Manifest) -> None:
        """Update Release with manifest information."""
        if manifest.component not in self.components:
            self.components.append(manifest.component)
        if manifest.architecture not in self.architectures:
            self.architectures.append(manifest.architecture)
        self.files.update(manifest.files)


def parse_release(content: str) -> Release:
    """Parse Release content into a Release object."""
    r = Release()
    r._parse(content)
    return r
