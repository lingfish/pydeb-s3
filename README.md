# pydeb-s3

**pydeb-s3** is a Python port of [deb-s3](https://github.com/deb-s3/deb-s3), a simple utility to make creating and managing APT repositories on S3.

Most existing guides on using S3 to host an APT repository have you using something like [reprepro](http://mirrorer.alioth.debian.org/) to generate the repository file structure, and then [s3cmd](http://s3tools.org/s3cmd) to sync the files to S3.

The annoying thing about this process is it requires you to maintain a local copy of the file tree for regenerating and syncing the next time. Personally, my process is to use one-off virtual machines with [Vagrant](http://vagrantup.com), script out the build process, and then would prefer to just upload the final `.deb` from my Mac.

With **pydeb-s3**, there is no need for this. pydeb-s3 features:

- Downloads the existing package manifest and parses it.
- Updates it with the new package, replacing the existing entry if already there or adding a new one if not.
- Uploads the package itself, the Packages manifest, and the Packages.gz manifest. It will skip the uploading if the package is already there.
- Updates the Release file with the new hashes and file sizes.

## Getting Started

Install the package via pip:

```console
$ pip install pydeb-s3
```

Now to upload a package, simply use:

```console
$ pydeb-s3 upload --bucket my-bucket my-deb-package-1.0.0_amd64.deb
>> Examining package file my-deb-package-1.0.0_amd64.deb
>> Retrieving existing package manifest
>> Uploading package and new manifests to S3
   -- Transferring pool/m/my/my-deb-package-1.0.0_amd64.deb
   -- Transferring dists/stable/main/binary-amd64/Packages
   -- Transferring dists/stable/main/binary-amd64/Packages.gz
   -- Transferring dists/stable/Release
>> Update complete.
```

For Google Cloud Storage (or other S3-compatible endpoints) you need to disable SDK checksum negotiation headers and set visibility settings to nil:

```console
$ pydeb-s3 upload --bucket my-bucket --endpoint https://storage.googleapis.com --checksum-when-required --visibility nil my-deb-package-1.0.0_amd64.deb
```

## Usage

```
Usage:
  pydeb-s3 upload FILES
  pydeb-s3 list
  pydeb-s3 show PACKAGE VERSION ARCH
  pydeb-s3 exists PACKAGE VERSION ARCH [PACKAGE VERSION ARCH ...]
  pydeb-s3 copy PACKAGE TO_CODENAME TO_COMPONENT
  pydeb-s3 delete PACKAGE
  pydeb-s3 verify
  pydeb-s3 clean
```

## Commands

### upload

Uploads the given files to a S3 bucket as an APT repository.

```
pydeb-s3 upload [--arch=ARCH] [--preserve-versions] [--lock] [--fail-if-exists]
              [--skip-package-upload] [--bucket=BUCKET] [--prefix=PREFIX]
              [--origin=ORIGIN] [--suite=SUITE] [--codename=CODENAME]
              [--component=COMPONENT] [--access-key-id=KEY] [--secret-access-key=KEY]
              [--s3-region=REGION] [--force-path-style] [--proxy-uri=URI]
              [--visibility=VISIBILITY] [--sign=KEY] [--gpg-options=OPTIONS]
              [--encryption] [--quiet] [--cache-control=CONTROL]
              FILES
```

### list

Lists packages in given codename, component, and optionally architecture.

```
pydeb-s3 list [--long] [--arch=ARCH] [--bucket=BUCKET] [--prefix=PREFIX]
             [--codename=CODENAME] [--component=COMPONENT]
             [--s3-region=REGION] [--quiet]
```

### show

Shows information about a package.

```
pydeb-s3 show PACKAGE VERSION ARCH [--bucket=BUCKET] [--prefix=PREFIX]
                          [--codename=CODENAME] [--component=COMPONENT]
                          [--s3-region=REGION] [--quiet]
```

### exists

Check if packages exist in the repository.

```
pydeb-s3 exists PACKAGE VERSION ARCH [PACKAGE VERSION ARCH ...]
                 [--bucket=BUCKET] [--prefix=PREFIX]
                 [--codename=CODENAME] [--component=COMPONENT]
                 [--s3-region=REGION] [--quiet]
```

### copy

Copy the package named PACKAGE to given codename and component.

```
pydeb-s3 copy PACKAGE TO_CODENAME TO_COMPONENT
              [--arch=ARCH] [--lock] [--versions=VERSIONS]
              [--preserve-versions] [--fail-if-exists]
              [--bucket=BUCKET] [--prefix=PREFIX]
              [--codename=CODENAME] [--component=COMPONENT]
              [--s3-region=REGION] [--quiet]
```

### delete

Remove the package named PACKAGE.

```
pydeb-s3 delete PACKAGE [--arch=ARCH] [--lock] [--versions=VERSIONS]
                  [--bucket=BUCKET] [--prefix=PREFIX]
                  [--origin=ORIGIN] [--suite=SUITE]
                  [--codename=CODENAME] [--component=COMPONENT]
                  [--s3-region=REGION] [--visibility=VISIBILITY]
                  [--sign=KEY] [--gpg-options=OPTIONS]
                  [--encryption] [--quiet] [--cache-control=CONTROL]
```

### verify

Verifies that the files in the package manifests exist.

```
pydeb-s3 verify [--fix-manifests] [--bucket=BUCKET] [--prefix=PREFIX]
               [--origin=ORIGIN] [--suite=SUITE]
               [--codename=CODENAME] [--component=COMPONENT]
               [--s3-region=REGION] [--visibility=VISIBILITY]
               [--sign=KEY] [--gpg-options=OPTIONS]
               [--encryption] [--quiet] [--cache-control=CONTROL]
```

### clean

Delete packages from the pool which are no longer referenced.

```
pydeb-s3 clean [--lock] [--bucket=BUCKET] [--prefix=PREFIX]
              [--origin=ORIGIN] [--suite=SUITE]
              [--codename=CODENAME] [--component=COMPONENT]
              [--s3-region=REGION] [--visibility=VISIBILITY]
              [--sign=KEY] [--gpg-options=OPTIONS]
              [--encryption] [--quiet] [--cache-control=CONTROL]
```

## Configuration

### AWS Credentials

pydeb-s3 supports multiple methods for AWS credentials:

1. **Command-line options**:
   - `--access-key-id` and `--secret-access-key`
   - `--session-token` (for temporary credentials)

2. **Environment variables**:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_DEFAULT_REGION`

3. **AWS Config file**: Uses standard boto3 credential resolution

### S3 Bucket

The `--bucket` option is required for all commands.

### Visibility

The `--visibility` option controls ACL on uploaded files:
- `public` (default) - public-read
- `private` - private
- `authenticated` - authenticated-read
- `nil` - do not set ACL (for buckets without ACL support)

## Example S3 IAM Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": ["arn:aws:s3:::BUCKETNAME"]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion",
                "s3:GetObjectAcl",
                "s3:GetObjectVersionAcl",
                "s3:PutObjectAcl",
                "s3:PutObjectVersionAcl"
            ],
            "Resource": ["arn:aws:s3:::BUCKETNAME/*"]
        }
    ]
}
```

## License

MIT License - see LICENSE file for details.

## Credits

- Original [deb-s3](https://github.com/deb-s3/deb-s3) by [Ken Robertson](https://github.com/krobertson)
- Python port by pydeb-s3 team