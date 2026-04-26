"""Manifest module for managing APT Packages files."""

import gzip
import hashlib
import os
import tempfile
from dataclasses import dataclass, field
from typing import Optional

from debian.deb822 import Packages

from pydeb_s3 import package as pkg_module
from pydeb_s3.s3_utils import S3NotFoundError, s3_exists, s3_read, s3_store


class AlreadyExistsError(Exception):
    """Raised when a package already exists."""


@dataclass
class Manifest:
    """Represents a Packages manifest for APT repository."""

    codename: str = "stable"
    component: str = "main"
    cache_control: str = ""
    architecture: str = "amd64"
    fail_if_exists: bool = False
    skip_package_upload: bool = False

    packages: list[pkg_module.Package] = field(default_factory=list)
    packages_to_be_upload: list[pkg_module.Package] = field(default_factory=list)
    files: dict = field(default_factory=dict)

    @classmethod
    def retrieve(
        cls,
        codename: str,
        component: str,
        architecture: str,
        cache_control: str = "",
        fail_if_exists: bool = False,
        skip_package_upload: bool = False,
    ) -> "Manifest":
        """Retrieve an existing manifest from S3 or create a new one."""
        path = f"dists/{codename}/{component}/binary-{architecture}/Packages"
        try:
            s = s3_read(path)
        except S3NotFoundError:
            s = None

        m = cls()
        if s:
            m._parse_packages(s)

        m.codename = codename
        m.component = component
        m.architecture = architecture
        m.cache_control = cache_control
        m.fail_if_exists = fail_if_exists
        m.skip_package_upload = skip_package_upload
        return m

    def _parse_packages(self, content: str) -> None:
        """Parse Packages content into Package objects."""
        for para in Packages.iter_paragraphs(content.splitlines()):
            pkg = self._package_from_paragraph(para)
            if pkg:
                self.packages.append(pkg)

    def _package_from_paragraph(self, para: dict) -> Optional[pkg_module.Package]:
        """Convert a debian.deb822 paragraph to a Package."""
        pkg = pkg_module.Package()
        pkg.name = para.get("Package")
        pkg.version = para.get("Version")
        pkg.architecture = para.get("Architecture")
        pkg.maintainer = para.get("Maintainer")
        pkg.category = para.get("Section")
        pkg.license = para.get("License")
        pkg.url = para.get("Homepage")
        pkg.description = para.get("Description")
        pkg.url_filename = para.get("Filename")
        pkg.sha1 = para.get("SHA1")
        pkg.sha256 = para.get("SHA256")
        pkg.md5 = para.get("MD5sum")

        size_str = para.get("Size")
        if size_str:
            try:
                pkg.size = int(size_str)
            except ValueError:
                pass

        deps = para.get("Depends")
        if deps:
            pkg._parse_depends(deps)

        return pkg

    def add(
        self,
        pkg: pkg_module.Package,
        preserve_versions: bool = True,
        needs_uploading: bool = True,
    ) -> pkg_module.Package:
        """Add a package to the manifest."""
        if self.fail_if_exists:
            for p in self.packages:
                if (
                    p.name == pkg.name
                    and p.full_version == pkg.full_version
                    and os.path.basename(p.url_filename_for(self.component))
                    != os.path.basename(pkg.url_filename_for(self.component))
                ):
                    raise AlreadyExistsError(
                        f"package {pkg.name}_{pkg.full_version} already exists "
                        f"with different filename"
                    )

        if preserve_versions:
            self.packages = [
                p
                for p in self.packages
                if not (p.name == pkg.name and p.full_version == pkg.full_version)
            ]
        else:
            self.packages = [p for p in self.packages if p.name != pkg.name]

        self.packages.append(pkg)
        if needs_uploading:
            self.packages_to_be_upload.append(pkg)
        return pkg

    def delete_package(
        self,
        name: str,
        versions: Optional[list[str]] = None
    ) -> list[pkg_module.Package]:
        """Delete packages matching name and optionally versions."""
        deleted = []
        new_packages = []

        for p in self.packages:
            if p.name != name or (versions and p.full_version not in versions):
                new_packages.append(p)
            else:
                deleted.append(p)

        self.packages = new_packages
        return deleted

    def generate(self) -> str:
        """Generate the Packages file content."""
        lines = []
        for pkg in self.packages:
            lines.append(pkg.generate(self.component))
            lines.append("")
        return "\n".join(lines)

    def write_to_s3(
        self,
        callback: Optional[callable] = None
    ) -> None:
        """Write the manifest to S3."""
        manifest = self.generate()
        from loguru import logger
        logger.debug(f"write_to_s3: generated manifest length: {len(manifest)}")
        logger.debug(f"write_to_s3: generated manifest preview: {manifest[:200]}")

        if not self.skip_package_upload:
            for pkg in self.packages_to_be_upload:
                new_path = pkg.url_filename_for(self.component)

                if not s3_exists(new_path):
                    if callback:
                        callback(new_path)
                    s3_store(
                        pkg.filename,
                        new_path,
                        "application/octet-stream; charset=binary",
                        self.cache_control,
                        self.fail_if_exists,
                    )

        packages_temp = tempfile.NamedTemporaryFile(
            mode="wb", suffix=".Packages", delete=False
        )
        try:
            packages_temp.write(manifest.encode("utf-8"))
            packages_temp.flush()
            packages_temp.close()
            from loguru import logger
            logger.debug(f"write_to_s3: Packages temp file size: {os.path.getsize(packages_temp.name)}")
            logger.debug(f"write_to_s3: Packages manifest length: {len(manifest)}")
            path = f"dists/{self.codename}/{self.component}/binary-{self.architecture}/Packages"
            if callback:
                callback(path)
            s3_store(
                packages_temp.name,
                path,
                "text/plain; charset=utf-8",
                self.cache_control,
            )
            self.files[f"{self.component}/binary-{self.architecture}/Packages"] = self._hashfile(
                packages_temp.name
            )
            logger.debug(f"write_to_s3: Packages hash result: {self.files[f'{self.component}/binary-{self.architecture}/Packages']}")
        finally:
            os.unlink(packages_temp.name)

        gztemp = tempfile.NamedTemporaryFile(
            mode="wb", suffix=".Packages.gz", delete=False
        )
        try:
            with gzip.open(gztemp.name, "wt") as gz:
                gz.write(manifest)
            path = f"dists/{self.codename}/{self.component}/binary-{self.architecture}/Packages.gz"
            if callback:
                callback(path)
            s3_store(
                gztemp.name,
                path,
                "application/x-gzip; charset=binary",
                self.cache_control,
            )
            self.files[f"{self.component}/binary-{self.architecture}/Packages.gz"] = self._hashfile(
                gztemp.name
            )
        finally:
            os.unlink(gztemp.name)

    def _hashfile(self, path: str) -> dict:
        """Calculate hashes for a file."""
        with open(path, "rb") as f:
            data = f.read()
        return {
            "size": os.path.getsize(path),
            "sha1": hashlib.sha1(data).hexdigest(),
            "sha256": hashlib.sha256(data).hexdigest(),
            "sha512": hashlib.sha512(data).hexdigest(),
            "md5": hashlib.md5(data).hexdigest(),
        }


def parse_packages(content: str) -> Manifest:
    """Parse Packages content into a Manifest."""
    m = Manifest()
    m._parse_packages(content)
    return m
