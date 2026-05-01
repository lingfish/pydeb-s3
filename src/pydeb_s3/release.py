"""Release file module for APT repository."""

import os
import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import Optional

from loguru import logger

from pydeb_s3 import manifest as man_module
from pydeb_s3.s3_utils import (
    S3NotFoundError,
    s3_read,
    s3_store,
)


@dataclass
class Release:
    """Represents a Release file for APT repository."""

    codename: str = "stable"
    origin: Optional[str] = None
    label: Optional[str] = None
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
        try:
            s = s3_read(path)
        except S3NotFoundError:
            s = None

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
        self.label = self._get_field(content, "Label")
        self.suite = self._get_field(content, "Suite")

        archs = self._get_field(content, "Architectures")
        if archs:
            self.architectures = archs.split()

        comps = self._get_field(content, "Components")
        if comps:
            self.components = comps.split()

        current_section = None
        for line in content.split("\n"):
            stripped = line.strip()
            if stripped == "MD5Sum:":
                current_section = "md5"
            elif stripped == "SHA1:":
                current_section = "sha1"
            elif stripped == "SHA256:":
                current_section = "sha256"
            elif stripped == "SHA512:":
                current_section = "sha512"
            elif current_section and line and line[0:1] == " " and len(stripped) > 10:
                parts = stripped.split()
                if len(parts) >= 3:
                    hash_val = parts[0]
                    size = int(parts[1])
                    filename = " ".join(parts[2:])
                    if filename not in self.files:
                        self.files[filename] = {}
                    self.files[filename][current_section] = hash_val
                    self.files[filename]["size"] = size
            elif stripped == "":
                current_section = None

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

    def _get_signature_files(self) -> set:
        """Get the set of signature file paths that should be excluded fromRelease hash sections."""
        return {f"dists/{self.codename}/InRelease", f"dists/{self.codename}/Release.gpg"}

    def generate(self) -> str:
        """Generate Release file content."""
        import datetime

        lines = []

        if self.origin:
            lines.append(f"Origin: {self.origin}")
        else:
            lines.append(f"Origin: {self.codename}")
        if self.label:
            lines.append(f"Label: {self.label}")
        if self.suite:
            lines.append(f"Suite: {self.suite}")
        lines.append(f"Codename: {self.codename}")

        if self.components:
            lines.append(f"Components: {' '.join(self.components)}")
        if self.architectures:
            lines.append(f"Architectures: {' '.join(self.architectures)}")

        date_str = datetime.datetime.now(datetime.timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
        lines.append(f"Date: {date_str}")
        lines.append("Acquire-By-Hash: yes")

        logger.debug(f"Release.generate(): files={self.files}")

        signature_files = self._get_signature_files()
        regular_files = {k: v for k, v in self.files.items() if k not in signature_files}

        for name, hashes in sorted(regular_files.items()):
            logger.debug(f"Release.generate(): hash entry name={name} hashes={hashes}")

        if any("sha256" in h for h in regular_files.values()):
            lines.append("SHA256:")
            for name, hashes in sorted(regular_files.items()):
                if "sha256" in hashes:
                    lines.append(f"  {hashes['sha256']} {hashes.get('size', 0)} {name}")

        if any("sha512" in h for h in regular_files.values()):
            lines.append("SHA512:")
            for name, hashes in sorted(regular_files.items()):
                if "sha512" in hashes:
                    lines.append(f"  {hashes['sha512']} {hashes.get('size', 0)} {name}")

        if any("md5" in h for h in regular_files.values()):
            lines.append("MD5Sum:")
            for name, hashes in sorted(regular_files.items()):
                if "md5" in hashes:
                    lines.append(f"  {hashes['md5']} {hashes.get('size', 0)} {name}")

        return "\n".join(lines)

    def sign(
        self,
        keys: list[str],
        provider: str = "gpg",
        options: str = "",
        visibility: str = "public",
        use_bytes: bool = False,
    ) -> None:
        """Sign the Release file with GPG and upload it to S3."""
        import tempfile

        from pydeb_s3 import s3_utils

        s3_utils._signing_key = keys
        s3_utils._gpg_provider = provider
        s3_utils._gpg_options = options
        s3_utils._access_policy = self._get_policy(visibility)

        release_content = self.generate()
        logger.debug(f"Release.sign(): content=\n{release_content}")
        release_temp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".Release", delete=False
        )
        try:
            release_temp.write(release_content)
            release_temp.close()
            s3_store(
                release_temp.name,
                self.filename,
                "text/plain; charset=utf-8",
                self.cache_control,
                use_bytes=use_bytes,
            )
            self._sign_release(release_temp.name, visibility, use_bytes=use_bytes)
        finally:
            os.unlink(release_temp.name)

    def upload(self, visibility: str = "public", use_bytes: bool = False) -> None:
        """Upload the Release file to S3."""
        from pydeb_s3 import s3_utils

        release_content = self.generate()
        s3_utils._access_policy = self._get_policy(visibility)

        release_temp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".Release", delete=False
        )
        try:
            release_temp.write(release_content)
            release_temp.close()
            s3_store(
                release_temp.name,
                self.filename,
                "text/plain; charset=utf-8",
                self.cache_control,
                use_bytes=use_bytes,
            )
        finally:
            os.unlink(release_temp.name)

    def _get_policy(self, visibility: str) -> str:
        """Get the access policy from visibility string."""
        if visibility == "public":
            return "public-read"
        if visibility == "private":
            return "private"
        if visibility == "authenticated":
            return "authenticated-read"
        if visibility == "bucket_owner":
            return "bucket-owner-full-control"
        return "public-read"

    def write_to_s3(
        self,
        callback: Optional[callable] = None,
        use_bytes: bool = False,
        progress: Optional["Progress"] = None,
    ) -> None:
        """Write the Release file to S3.

        Args:
            callback: Optional callback function for progress updates.
            use_bytes: If True, display speed in bytes/s. If False, display in bits/s.
            progress: Optional shared Progress instance for multiple uploads.
        """
        # Import Progress type for type hint (avoid circular import at runtime)
        from rich.progress import Progress

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
                use_bytes=use_bytes,
                progress=progress,
            )
        finally:
            os.unlink(release_temp.name)

    def _sign_release(
        self, release_path: str, visibility: str = "public", callback: Optional[callable] = None, use_bytes: bool = False
    ) -> None:
        """Sign the Release file with GPG - both clearsign (InRelease) and detached (Release.gpg)."""
        from pydeb_s3 import s3_utils

        signing_key = s3_utils._signing_key or []
        if not signing_key:
            return

        s3_utils._access_policy = self._get_policy(visibility)
        key_param = " ".join(f"-u {k}" for k in signing_key) if signing_key else ""

        cmd = f"{s3_utils._gpg_provider} -a {key_param} --digest-algo SHA256 {s3_utils._gpg_options} -s --clearsign {release_path}"
        result = subprocess.run(cmd, check=False, shell=True, capture_output=True)

        if result.returncode != 0:
            stderr = result.stderr.decode() if result.stderr else ""
            if "no such key" in stderr.lower() or "secret key not found" in stderr.lower():
                raise RuntimeError(
                    "GPG signing failed: Secret key not found. "
                    "Make sure the key ID is correct and the secret key is available."
                )
            if "bad passphrase" in stderr.lower() or "passphrase" in stderr.lower():
                raise RuntimeError(
                    "GPG signing failed: Bad passphrase or passphrase required. "
                    "Make sure GPG agent is running or use --gpg-options to provide passphrase."
                )
            raise RuntimeError(f"GPG signing failed with return code {result.returncode}: {stderr}")

        asc_path = release_path + ".asc"
        if not os.path.exists(asc_path):
            raise RuntimeError(
                f"GPG clearsign did not produce expected output file: {asc_path}. "
                "Check GPG configuration and try again."
            )

        inrelease_path = f"dists/{self.codename}/InRelease"
        if callback:
            callback(inrelease_path)
        s3_store(
            asc_path,
            inrelease_path,
            "application/pgp-signature; charset=us-ascii",
            self.cache_control,
            use_bytes=use_bytes,
        )
        os.unlink(asc_path)

        cmd = f"{s3_utils._gpg_provider} -a {key_param} --digest-algo SHA256 {s3_utils._gpg_options} -b {release_path}"
        result = subprocess.run(cmd, check=False, shell=True, capture_output=True)

        if result.returncode != 0:
            stderr = result.stderr.decode() if result.stderr else ""
            if "no such key" in stderr.lower() or "secret key not found" in stderr.lower():
                raise RuntimeError(
                    "GPG detached signing failed: Secret key not found. "
                    "Make sure the key ID is correct and the secret key is available."
                )
            if "bad passphrase" in stderr.lower() or "passphrase" in stderr.lower():
                raise RuntimeError(
                    "GPG detached signing failed: Bad passphrase or passphrase required. "
                    "Make sure GPG agent is running or use --gpg-options to provide passphrase."
                )
            raise RuntimeError(f"GPG detached signing failed with return code {result.returncode}: {stderr}")

        asc_path = release_path + ".asc"
        if not os.path.exists(asc_path):
            raise RuntimeError(
                f"GPG detached signing did not produce expected output file: {asc_path}. "
                "Check GPG configuration and try again."
            )

        gpg_path = self.filename + ".gpg"
        if callback:
            callback(gpg_path)
        s3_store(
            asc_path,
            gpg_path,
            "application/pgp-signature; charset=us-ascii",
            self.cache_control,
            use_bytes=use_bytes,
        )
        os.unlink(asc_path)

    def _validate_others(self, callback: Optional[callable] = None, use_bytes: bool = False) -> None:
        """Validate other architectures are present."""
        for comp in self.components:
            for arch in ["amd64", "i386", "armhf", "arm64"]:
                key = f"{comp}/binary-{arch}/Packages"
                if key in self.files or arch in self.architectures:
                    m = man_module.Manifest()
                    m.codename = self.codename
                    m.component = comp
                    m.architecture = arch
                    m.write_to_s3(callback, use_bytes=use_bytes)
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
