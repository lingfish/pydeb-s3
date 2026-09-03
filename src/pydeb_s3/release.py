"""Release file module for APT repository."""

import os
import re
import shlex
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import Optional, Protocol

from loguru import logger

from pydeb_s3 import manifest as man_module
from pydeb_s3.s3_adapter import S3Adapter, S3NotFoundError


class SigningAdapter(Protocol):
    """Interface for GPG signing operations.

    A SigningAdapter provides a seam between the Release module
    and the signing implementation (currently GPG subprocess).
    """

    def clearsign(self, input_path: str, output_path: str) -> None:
        """Create clearsigned file (InRelease).

        Args:
            input_path: Path to file to sign
            output_path: Path where clearsigned output should be written
        """
        ...

    def detach_sign(self, input_path: str, output_path: str) -> None:
        """Create detached signature (Release.gpg).

        Args:
            input_path: Path to file to sign
            output_path: Path where detached signature should be written
        """
        ...

    def get_key_info(self) -> dict:
        """Return info about signing keys (for error messages)."""
        ...


class GpgSigningAdapter:
    """Concrete adapter wrapping gpg subprocess.

    Attributes:
        keys: List of GPG key IDs to use for signing
        provider: GPG provider command (default: "gpg")
        options: Additional GPG command line options
    """

    def __init__(self, keys: list[str], provider: str = "gpg", options: str = ""):
        self.keys = keys
        self.provider = provider
        self.options = options

    def clearsign(self, input_path: str, output_path: str) -> None:
        """Create clearsigned file (InRelease)."""
        args = [self.provider, "-a"]
        for k in self.keys:
            args.extend(["-u", k])
        args.extend(["--digest-algo", "SHA256", "--batch", "--no-tty", "--yes"])
        if self.options:
            args.extend(shlex.split(self.options))
        args.extend(["-s", "--clearsign", input_path])
        result = subprocess.run(args, check=False, capture_output=True)

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

        asc_path = input_path + ".asc"
        if not os.path.exists(asc_path):
            raise RuntimeError(
                f"GPG clearsign did not produce expected output file: {asc_path}. "
                "Check GPG configuration and try again."
            )

        # Rename to output_path
        os.rename(asc_path, output_path)

    def detach_sign(self, input_path: str, output_path: str) -> None:
        """Create detached signature (Release.gpg)."""
        args = [self.provider, "-a"]
        for k in self.keys:
            args.extend(["-u", k])
        args.extend(["--digest-algo", "SHA256", "--batch", "--no-tty", "--yes"])
        if self.options:
            args.extend(shlex.split(self.options))
        args.extend(["-b", input_path])
        result = subprocess.run(args, check=False, capture_output=True)

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

        asc_path = input_path + ".asc"
        if not os.path.exists(asc_path):
            raise RuntimeError(
                f"GPG detached signing did not produce expected output file: {asc_path}. "
                "Check GPG configuration and try again."
            )

        # Rename to output_path
        os.rename(asc_path, output_path)

    def get_key_info(self) -> dict:
        """Return info about signing keys."""
        return {"keys": self.keys, "provider": self.provider}

    def extract_signing_key(self, inrelease_content: str) -> str:
        """Extract the signing key ID from an existing InRelease file.

        Runs gpg --verify on the content and parses the output for the key ID.

        Args:
            inrelease_content: The raw content of the InRelease file.

        Returns:
            The hex key ID string.

        Raises:
            RuntimeError: If gpg fails or key ID cannot be determined.
        """
        temp_path = None
        try:
            # Write InRelease content to a temp file for gpg
            with tempfile.NamedTemporaryFile(suffix=".Release", delete=False, mode="wb") as f:
                f.write(inrelease_content.encode() if isinstance(inrelease_content, str) else inrelease_content)
                temp_path = f.name

            args = [self.provider, "--verify", temp_path]
            if self.options:
                args.extend(shlex.split(self.options))

            result = subprocess.run(args, check=False, capture_output=True)

            stderr_text = result.stderr.decode() if result.stderr else ""
            stdout_text = result.stdout.decode() if result.stdout else ""

            # Check for BAD signature first
            if "BAD signature" in stderr_text or "BAD signature" in stdout_text:
                raise RuntimeError(
                    "Cannot determine signing key: BAD signature in existing InRelease. "
                    "Use --sign <key-id> to re-sign, or delete the InRelease file from S3."
                )

            # Extract key ID from gpg output — prefer stderr (gpg's typical output channel)
            for text in (stderr_text, stdout_text):
                match = re.search(r"using\s+\w+\s+key\s+(?:0x)?([0-9A-Fa-f]+)", text)
                if match:
                    return match.group(1)

            # Check for non-zero exit code
            combined = f"{stdout_text}\n{stderr_text}"
            if result.returncode != 0:
                raise RuntimeError(
                    f"GPG verification failed (exit code {result.returncode}): {combined}"
                )

            raise RuntimeError(
                f"Could not determine signing key from existing InRelease. "
                f"GPG output: {combined}"
            )
        finally:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)


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
        s3_adapter: S3Adapter,
        codename: str,
        origin: Optional[str] = None,
        suite: Optional[str] = None,
    ) -> "Release":
        """Retrieve an existing Release file from S3 or create a new one."""
        path = f"dists/{codename}/Release"
        try:
            s = s3_adapter.read(path)
        except S3NotFoundError:
            s = None

        r = cls()
        if s:
            r._parse(s)

        r.codename = codename
        r.origin = origin
        r.suite = suite
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
        s3_adapter: S3Adapter,
        signing_adapter: SigningAdapter,
        use_bytes: bool = False,
    ) -> None:
        """Sign the Release file with GPG and upload it to S3.

        Args:
            s3_adapter: Adapter for S3 storage operations
            signing_adapter: Adapter handling GPG signing operations
            use_bytes: If True, display speed in bytes/s
        """
        if not signing_adapter or not signing_adapter.get_key_info():
            return  # No keys = no signing

        release_content = self.generate()
        logger.debug(f"Release.sign(): content=\n{release_content}")
        release_temp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".Release", delete=False
        )
        try:
            release_temp.write(release_content)
            release_temp.close()

            # Upload unsigned Release to S3
            s3_adapter.store_file(
                release_temp.name,
                self.filename,
                content_type="text/plain; charset=utf-8",
                cache_control=self.cache_control,
                use_bytes=use_bytes,
            )

            # Adapter handles ONLY GPG operations
            # IMPORTANT: Use distinct paths and run detach_sign BEFORE clearsign.
            # GpgSigningAdapter produces input_path + ".asc" then renames it to output_path.
            # If both use the same path, detach_sign overwrites clearsign output.
            # If clearsign runs last, its output gets overwritten.
            # Solution: detach_sign first to temp.detached, then clearsign to temp.asc
            detached_path = release_temp.name + ".detached"
            clearsigned_path = release_temp.name + ".asc"
            signing_adapter.detach_sign(release_temp.name, detached_path)
            signing_adapter.clearsign(release_temp.name, clearsigned_path)

            # Upload signed files to S3
            inrelease_path = f"dists/{self.codename}/InRelease"
            s3_adapter.store_file(
                clearsigned_path,
                inrelease_path,
                content_type="application/pgp-signature; charset=us-ascii",
                cache_control=self.cache_control,
                use_bytes=use_bytes,
            )

            gpg_path = self.filename + ".gpg"
            s3_adapter.store_file(
                detached_path,
                gpg_path,
                content_type="application/pgp-signature; charset=us-ascii",
                cache_control=self.cache_control,
                use_bytes=use_bytes,
            )
        finally:
            os.unlink(release_temp.name)

    def auto_re_sign(
        self,
        s3_adapter: S3Adapter,
        signing_adapter: "GpgSigningAdapter",
    ) -> None:
        """Re-sign InRelease if it already exists on S3.

        Called when --sign was NOT passed but InRelease already exists.
        Extracts the key ID from the existing InRelease and re-signs.

        Args:
            s3_adapter: Adapter for S3 storage operations
            signing_adapter: The original signing adapter (used to get provider/options)

        Raises:
            RuntimeError: If key extraction fails (hard fail).
        """
        inrelease_path = f"dists/{self.codename}/InRelease"

        if not s3_adapter.exists(inrelease_path):
            return  # No InRelease to re-sign

        logger.info(f"InRelease already exists for {self.codename}, auto re-signing...")

        content = s3_adapter.read(inrelease_path)
        try:
            key_id = signing_adapter.extract_signing_key(content)
        except RuntimeError as e:
            raise RuntimeError(
                f"Cannot auto re-sign InRelease for {self.codename}: {e}. "
                f"Use --sign <key-id> to re-sign, or delete {inrelease_path} from S3."
            ) from e

        new_adapter = GpgSigningAdapter(
            keys=[key_id],
            provider=signing_adapter.provider,
            options=signing_adapter.options,
        )
        logger.info(f"Auto re-signing InRelease with key {key_id}")
        self.sign(s3_adapter, new_adapter)

    def upload(
        self,
        s3_adapter: S3Adapter,
        visibility: str = "public",
        use_bytes: bool = False,
    ) -> None:
        """Upload the Release file to S3."""
        release_content = self.generate()

        release_temp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".Release", delete=False
        )
        try:
            release_temp.write(release_content)
            release_temp.close()
            s3_adapter.store_file(
                release_temp.name,
                self.filename,
                content_type="text/plain; charset=utf-8",
                cache_control=self.cache_control,
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
        s3_adapter: S3Adapter,
        callback: Optional[callable] = None,
        use_bytes: bool = False,
        progress: Optional["Progress"] = None,
    ) -> None:
        """Write the Release file to S3.

        Args:
            s3_adapter: Adapter for S3 storage operations
            callback: Optional callback function for progress updates.
            use_bytes: If True, display speed in bytes/s. If False, display in bits/s.
            progress: Optional shared Progress instance for multiple uploads.
        """
        # Import Progress type for type hint (avoid circular import at runtime)

        release_content = self.generate()
        release_temp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".Release", delete=False
        )
        try:
            release_temp.write(release_content)
            release_temp.close()

            if callback:
                callback(self.filename)
            s3_adapter.store_file(
                release_temp.name,
                self.filename,
                content_type="text/plain; charset=utf-8",
                cache_control=self.cache_control,
                use_bytes=use_bytes,
                progress=progress,
            )
        finally:
            os.unlink(release_temp.name)

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
