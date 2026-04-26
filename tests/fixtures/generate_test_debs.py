#!/usr/bin/env python3
"""Generate test .deb files for package parsing tests."""

import io
import os
import tarfile

AR_MAGIC = b'!<arch>\n'

def create_deb(output_path: str, name: str, version: str, arch: str) -> None:
    """Create a minimal .deb file."""
    control_content = f"""Package: {name}
Version: {version}
Section: test
Priority: low
Architecture: {arch}
Maintainer: Test <test@example.com>
Description: Test package
 Test package for pydeb-s3.
"""

    data_tar = io.BytesIO()
    with tarfile.open(fileobj=data_tar, mode='w:gz') as tar:
        pass
    data_tar.seek(0)

    control_tar = io.BytesIO()
    with tarfile.open(fileobj=control_tar, mode='w:gz') as tar:
        info = tarfile.TarInfo(name='control')
        info.size = len(control_content.encode())
        tar.addfile(info, io.BytesIO(control_content.encode()))

        info = tarfile.TarInfo(name='md5sums')
        info.size = 0
        tar.addfile(info, io.BytesIO(b''))
    control_tar.seek(0)

    with open(output_path, 'wb') as f:
        f.write(AR_MAGIC)

        _add_ar_member(f, b'debian-binary', b'2.0\n')
        _add_ar_member(f, b'control.tar.gz', control_tar.getvalue())
        _add_ar_member(f, b'data.tar.gz', data_tar.getvalue())

def _add_ar_member(f, name: bytes, data: bytes) -> None:
    """Add a member to an ar archive."""
    now = b'0'
    mode = b'100664'
    uid = b'0'
    gid = b'0'

    header = name.ljust(16)
    header += now + b' '
    header += uid + b' '
    header += gid + b' '
    header += mode + b' '
    header += str(len(data)).encode().ljust(10)
    header += b' `\n'

    f.write(header)
    f.write(data)
    if len(data) % 2:
        f.write(b'\n')

if __name__ == '__main__':
    fixtures_dir = os.path.dirname(os.path.abspath(__file__))
    for arch in ['amd64', 'arm64', 'all']:
        output = os.path.join(fixtures_dir, f'test-pkg_1.0.0_{arch}.deb')
        create_deb(output, 'test-pkg', '1.0.0', arch)
        print(f"Created: {output}")
