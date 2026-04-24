"""Package model for parsing and representing Debian packages."""

import hashlib
import os
import subprocess
from dataclasses import dataclass, field
from typing import Optional

from debian import debfile


@dataclass
class Package:
    """Represents a Debian package."""

    name: Optional[str] = None
    version: Optional[str] = None
    epoch: Optional[str] = None
    iteration: Optional[str] = None
    maintainer: Optional[str] = None
    vendor: Optional[str] = None
    url: Optional[str] = None
    category: Optional[str] = None
    license: Optional[str] = None
    architecture: Optional[str] = None
    description: Optional[str] = None
    dependencies: list[str] = field(default_factory=list)
    attributes: dict = field(default_factory=dict)
    sha1: Optional[str] = None
    sha256: Optional[str] = None
    md5: Optional[str] = None
    size: Optional[int] = None
    filename: Optional[str] = None

    url_filename: Optional[str] = None

    @property
    def full_version(self) -> Optional[str]:
        """Return the full version string (epoch:version-iteration)."""
        if all(v is None for v in [self.epoch, self.version, self.iteration]):
            return None
        parts = []
        if self.epoch:
            parts.append(self.epoch)
        if self.version:
            parts.append(self.version)
        full = ":".join(parts)
        if self.iteration:
            full = f"{full}-{self.iteration}"
        return full

    def url_filename_for(self, component: str) -> str:
        """Return the URL filename for this package in the pool."""
        if self.url_filename:
            return self.url_filename
        if not self.filename:
            return ""
        return f"pool/{component}/{self.name[0]}/{self.name[0:2]}/{os.path.basename(self.filename)}"

    @classmethod
    def parse_file(cls, filepath: str) -> "Package":
        """Parse a .deb file and return a Package object."""
        pkg = cls()
        pkg.filename = filepath

        try:
            df = debfile.DebFile(filepath)
            pkg._extract_from_debfile(df)
        except Exception:
            pkg._extract_control_manually(filepath)

        pkg._apply_file_info(filepath)
        return pkg

    def _extract_from_debfile(self, df: debfile.DebFile) -> None:
        """Extract info from a debfile.DebFile object."""
        self.name = df.get("Package")
        self.version = df.get("Version")
        self.architecture = df.get("Architecture")
        self.maintainer = df.get("Maintainer")
        self.description = df.get("Description")
        self.url = df.get("Homepage")
        self.category = df.get("Section")
        self.license = df.get("License")

        full_version = self.version or ""
        if ":" in full_version:
            self.epoch, self.version = full_version.split(":", 1)
        if "-" in self.version:
            parts = self.version.rsplit("-", 1)
            if parts[0]:
                self.version = parts[0]
                self.iteration = parts[1]

        self.attributes["deb_priority"] = df.get("Priority")
        self.attributes["deb_origin"] = df.get("Origin")
        self.attributes["deb_installed_size"] = df.get("Installed-Size")

        self._parse_depends(df.get("Depends"))
        self.attributes["deb_recommends"] = df.get("Recommends")
        self.attributes["deb_suggests"] = df.get("Suggests")
        self.attributes["deb_enhances"] = df.get("Enhances")
        self.attributes["deb_pre_depends"] = df.get("Pre-Depends")
        self.attributes["deb_breaks"] = df.get("Breaks")
        self.attributes["deb_conflicts"] = df.get("Conflicts")
        self.attributes["deb_provides"] = df.get("Provides")
        self.attributes["deb_replaces"] = df.get("Replaces")

    def _extract_control_manually(self, filepath: str) -> None:
        """Extract control info using dpkg or manual ar/tar parsing."""
        try:
            result = subprocess.run(
                ["dpkg", "-f", filepath],
                capture_output=True,
                text=True,
                check=True,
            )
            self._parse_control(result.stdout)
        except (FileNotFoundError, subprocess.CalledProcessError):
            self._extract_control_with_ar(filepath)

    def _extract_control_with_ar(self, filepath: str) -> None:
        """Extract control info using ar and tar."""
        import subprocess

        try:
            result = subprocess.run(
                ["ar", "t", filepath],
                capture_output=True,
                text=True,
                check=True,
            )
            files = result.stdout.strip().split("\n")
            control_file = next((f for f in files if f.startswith("control.")), None)

            if not control_file:
                return

            if control_file.endswith(".zst"):
                compression = "zstd -d"
            elif control_file.endswith(".gz"):
                compression = "-z"
            elif control_file.endswith(".xz"):
                compression = "-J"
            else:
                compression = ""

            control_cmd = f"ar p {filepath} {control_file} | tar {compression} -xf - -O control"
            result = subprocess.run(
                control_cmd,
                check=False, shell=True,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                self._parse_control(result.stdout)
        except Exception:
            pass

    def _parse_control(self, control: str) -> None:
        """Parse control file content."""
        current_field = None
        value = []

        for line in control.split("\n"):
            if not line:
                continue

            if line.startswith(" ") and current_field:
                value.append(line[1:])
            elif ":" in line:
                if current_field:
                    self._set_field(current_field, "\n".join(value))
                current_field, val = line.split(":", 1)
                current_field = current_field.strip()
                value = [val.strip()]
            elif value:
                value.append(line)

        if current_field:
            self._set_field(current_field, "\n".join(value))

    def _set_field(self, field: str, value: str) -> None:
        """Set a field on this package."""
        field_lower = field.lower()
        if field_lower == "package":
            self.name = value
        elif field_lower == "version":
            self.version = value
            if ":" in value:
                self.epoch, self.version = value.split(":", 1)
            if "-" in self.version:
                parts = self.version.rsplit("-", 1)
                self.version = parts[0]
                self.iteration = parts[1] if len(parts) > 1 else None
        elif field_lower == "architecture":
            self.architecture = value
        elif field_lower == "maintainer":
            self.maintainer = value
        elif field_lower == "description":
            self.description = value
        elif field_lower == "homepage":
            self.url = value
        elif field_lower == "section":
            self.category = value
        elif field_lower == "license":
            self.license = value
        elif field_lower == "priority":
            self.attributes["deb_priority"] = value
        elif field_lower == "origin":
            self.attributes["deb_origin"] = value
        elif field_lower == "installed-size":
            self.attributes["deb_installed_size"] = value
        elif field_lower == "depends":
            self._parse_depends(value)

    def _parse_depends(self, depends_str: Optional[str]) -> None:
        """Parse Depends field into a list of dependencies."""
        if not depends_str:
            return

        import re

        dep_re = re.compile(r"^([^ ]+)(?: \(([>=<]+) ([^)]+)\))?$")
        for dep in depends_str.split(", "):
            m = dep_re.match(dep)
            if m:
                name, op, version = m.groups()
                if op and version:
                    self.dependencies.append(f"{name} ({op} {version})")
                else:
                    self.dependencies.append(name.strip())

    def _apply_file_info(self, filepath: str) -> None:
        """Apply file-specific information (hashes, size)."""
        stat = os.stat(filepath)
        self.size = stat.st_size

        with open(filepath, "rb") as f:
            data = f.read()
            self.sha1 = hashlib.sha1(data).hexdigest()
            self.sha256 = hashlib.sha256(data).hexdigest()
            self.md5 = hashlib.md5(data).hexdigest()

    def generate(self, component: str) -> str:
        """Generate the package entry for the Packages file."""
        lines = []
        if self.name:
            lines.append(f"Package: {self.name}")
        if self.version:
            full_ver = self.full_version or self.version
            lines.append(f"Version: {full_ver}")
        if self.architecture:
            lines.append(f"Architecture: {self.architecture}")
        if self.maintainer:
            lines.append(f"Maintainer: {self.maintainer}")
        if self.category:
            lines.append(f"Section: {self.category}")
        if self.license:
            lines.append(f"License: {self.license}")
        if self.url:
            lines.append(f"Homepage: {self.url}")
        if self.description:
            desc = self.description.replace("\n", "\n ")
            lines.append(f"Description: {desc}")

        if self.dependencies:
            lines.append(f"Depends: {', '.join(self.dependencies)}")

        for key, value in self.attributes.items():
            if value:
                field = key.replace("_", "-").title().replace("-", "")
                lines.append(f"{field}: {value}")

        lines.append(f"Filename: {self.url_filename_for(component)}")
        if self.sha256:
            lines.append(f"SHA256: {self.sha256}")
        if self.sha1:
            lines.append(f"SHA1: {self.sha1}")
        if self.md5:
            lines.append(f"MD5sum: {self.md5}")
        if self.size:
            lines.append(f"Size: {self.size}")

        return "\n".join(lines)


def parse_string(s: str) -> Package:
    """Parse a package from a string (Packages file entry)."""
    pkg = Package()
    pkg._parse_control(s)
    return pkg
