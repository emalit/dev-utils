"""
Tiny S3 helper CLI for testing S3 permissions:
- list: list objects under a prefix (tests s3:ListBucket*)
- put: create (upload) a text object in the bucket (tests s3:PutObject*)
- get: download/read an object (tests s3:GetObject)
- delete: delete an object (tests s3:DeleteObject*)
- get-location: get bucket location (tests s3:GetBucketLocation)
- get-version: get a specific version of an object (tests s3:GetObjectVersion)
- abort-multipart: abort a multipart upload (tests s3:AbortMultipartUpload)

Credentials:
  Uses the standard AWS credential chain (env vars, ~/.aws, IAM role, etc).

Examples:
  python s3_bucket_client.py --bucket {bucket_name} list --prefix {directory_name}/
  python s3_bucket_client.py --bucket {bucket_name} put --prefix {directory_name}/ --name hello.txt --content "hi"
  python s3_bucket_client.py --bucket {bucket_name} get --key {directory_name}/hello.txt
  python s3_bucket_client.py --bucket {bucket_name} delete --key {directory_name}/hello.txt
  python s3_bucket_client.py --bucket {bucket_name} get-location
  python s3_bucket_client.py --bucket {bucket_name} get-version --key {directory_name}/hello.txt --version-id abc123
  python s3_bucket_client.py --bucket {bucket_name} abort-multipart --key {directory_name}/upload.txt --upload-id xyz789
"""

from __future__ import annotations

import argparse
from typing import Iterator, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def _s3_client(profile: Optional[str]):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return session.client("s3")


def _normalize_prefix(prefix: str) -> str:
    if not prefix:
        return ""
    return prefix if prefix.endswith("/") else f"{prefix}/"


def iter_s3_keys(*, s3, bucket: str, prefix: str, max_keys: int = 1000) -> Iterator[str]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": max_keys}):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def cmd_list(args) -> int:
    s3 = _s3_client(args.profile)
    prefix = _normalize_prefix(args.prefix)
    try:
        keys = list(iter_s3_keys(s3=s3, bucket=args.bucket, prefix=prefix, max_keys=args.max_keys))
        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                for key in keys:
                    f.write(f"{key}\n")

        if not keys:
            print(f"(no objects found under s3://{args.bucket}/{prefix})")
            if args.out:
                print(f"Wrote empty file: {args.out}")
            return 0

        for key in keys:
            print(key)
        if args.out:
            print(f"Wrote {len(keys)} keys to: {args.out}")
        return 0
    except (ClientError, BotoCoreError) as e:
        print(f"ERROR: failed to list s3://{args.bucket}/{prefix}: {e}")
        return 1


def _object_exists(*, s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def cmd_put(args) -> int:
    s3 = _s3_client(args.profile)
    prefix = _normalize_prefix(args.prefix)
    key = args.key if args.key else f"{prefix}{args.name}"

    try:
        if not args.overwrite and _object_exists(s3=s3, bucket=args.bucket, key=key):
            print(f"ERROR: s3://{args.bucket}/{key} already exists (pass --overwrite to replace it)")
            return 2

        if args.content_file:
            with open(args.content_file, "rb") as f:
                body = f.read()
        else:
            body = (args.content or "").encode("utf-8")

        resp = s3.put_object(
            Bucket=args.bucket,
            Key=key,
            Body=body,
            ContentType="text/plain; charset=utf-8",
        )
        etag = resp.get("ETag")
        print(f"Created: s3://{args.bucket}/{key}" + (f" (ETag: {etag})" if etag else ""))
        return 0
    except (ClientError, BotoCoreError, OSError) as e:
        print(f"ERROR: failed to create s3://{args.bucket}/{key}: {e}")
        return 1


def cmd_get(args) -> int:
    s3 = _s3_client(args.profile)
    try:
        resp = s3.get_object(Bucket=args.bucket, Key=args.key)
        body = resp["Body"].read()

        if args.out:
            with open(args.out, "wb") as f:
                f.write(body)
            print(f"Downloaded s3://{args.bucket}/{args.key} to {args.out} ({len(body)} bytes)")
        else:
            print(f"Content of s3://{args.bucket}/{args.key} ({len(body)} bytes):")
            try:
                print(body.decode("utf-8"))
            except UnicodeDecodeError:
                print(f"(binary content, use --out to save to file)")
        return 0
    except (ClientError, BotoCoreError, OSError) as e:
        print(f"ERROR: failed to get s3://{args.bucket}/{args.key}: {e}")
        return 1


def cmd_delete(args) -> int:
    s3 = _s3_client(args.profile)
    try:
        if not args.force and not _object_exists(s3=s3, bucket=args.bucket, key=args.key):
            print(f"ERROR: s3://{args.bucket}/{args.key} does not exist (pass --force to skip check)")
            return 2

        s3.delete_object(Bucket=args.bucket, Key=args.key)
        print(f"Deleted: s3://{args.bucket}/{args.key}")
        return 0
    except (ClientError, BotoCoreError) as e:
        print(f"ERROR: failed to delete s3://{args.bucket}/{args.key}: {e}")
        return 1


def cmd_get_location(args) -> int:
    s3 = _s3_client(args.profile)
    try:
        resp = s3.get_bucket_location(Bucket=args.bucket)
        location = resp.get("LocationConstraint") or "us-east-1"
        print(f"Bucket location: {location}")
        return 0
    except (ClientError, BotoCoreError) as e:
        print(f"ERROR: failed to get bucket location for {args.bucket}: {e}")
        return 1


def cmd_get_version(args) -> int:
    s3 = _s3_client(args.profile)
    try:
        resp = s3.get_object(Bucket=args.bucket, Key=args.key, VersionId=args.version_id)
        body = resp["Body"].read()
        version_id = resp.get("VersionId", "unknown")

        if args.out:
            with open(args.out, "wb") as f:
                f.write(body)
            print(f"Downloaded s3://{args.bucket}/{args.key} (version: {version_id}) to {args.out} ({len(body)} bytes)")
        else:
            print(f"Content of s3://{args.bucket}/{args.key} (version: {version_id}) ({len(body)} bytes):")
            try:
                print(body.decode("utf-8"))
            except UnicodeDecodeError:
                print(f"(binary content, use --out to save to file)")
        return 0
    except (ClientError, BotoCoreError, OSError) as e:
        print(f"ERROR: failed to get version {args.version_id} of s3://{args.bucket}/{args.key}: {e}")
        return 1


def cmd_abort_multipart(args) -> int:
    s3 = _s3_client(args.profile)
    try:
        s3.abort_multipart_upload(
            Bucket=args.bucket,
            Key=args.key,
            UploadId=args.upload_id
        )
        print(f"Aborted multipart upload: s3://{args.bucket}/{args.key} (upload ID: {args.upload_id})")
        return 0
    except (ClientError, BotoCoreError) as e:
        print(f"ERROR: failed to abort multipart upload for s3://{args.bucket}/{args.key}: {e}")
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="S3 permission testing CLI.")
    parser.add_argument("--bucket", default="risk-streaming-staging-lakehouse")
    parser.add_argument("--profile", default=None, help="AWS profile name (optional).")

    subparsers = parser.add_subparsers(dest="command", required=True)

    p_list = subparsers.add_parser("list", help="List objects under a prefix.")
    p_list.add_argument("--prefix", default="paimon/")
    p_list.add_argument("--max-keys", type=int, default=1000, help="Page size for listing (default: 1000).")
    p_list.add_argument("--out", default=None, help="Optional local file to write keys to (one per line).")
    p_list.set_defaults(func=cmd_list)

    p_put = subparsers.add_parser("put", help="Create (upload) a text object in S3.")
    p_put.add_argument("--prefix", default="paimon/", help="Prefix used when you pass --name (ignored if using --key).")
    name_or_key = p_put.add_mutually_exclusive_group(required=True)
    name_or_key.add_argument("--name", help="File name to create under --prefix (e.g. hello.txt).")
    name_or_key.add_argument("--key", help="Full S3 key to create (e.g. paimon/hello.txt).")

    content_src = p_put.add_mutually_exclusive_group()
    content_src.add_argument("--content", default="", help="Text content to write (default: empty).")
    content_src.add_argument("--content-file", help="Path to a local file to upload as the object body.")

    p_put.add_argument("--overwrite", action="store_true", help="Overwrite if the object already exists.")
    p_put.set_defaults(func=cmd_put)

    p_get = subparsers.add_parser("get", help="Download/read an object from S3.")
    p_get.add_argument("--key", required=True, help="S3 key of the object to get.")
    p_get.add_argument("--out", default=None, help="Optional local file to save the object to.")
    p_get.set_defaults(func=cmd_get)

    p_delete = subparsers.add_parser("delete", help="Delete an object from S3.")
    p_delete.add_argument("--key", required=True, help="S3 key of the object to delete.")
    p_delete.add_argument("--force", action="store_true", help="Skip existence check before deletion.")
    p_delete.set_defaults(func=cmd_delete)

    p_get_location = subparsers.add_parser("get-location", help="Get the bucket's location/region.")
    p_get_location.set_defaults(func=cmd_get_location)

    p_get_version = subparsers.add_parser("get-version", help="Get a specific version of an object.")
    p_get_version.add_argument("--key", required=True, help="S3 key of the object.")
    p_get_version.add_argument("--version-id", required=True, help="Version ID of the object to retrieve.")
    p_get_version.add_argument("--out", default=None, help="Optional local file to save the object to.")
    p_get_version.set_defaults(func=cmd_get_version)

    p_abort_multipart = subparsers.add_parser("abort-multipart", help="Abort a multipart upload.")
    p_abort_multipart.add_argument("--key", required=True, help="S3 key of the multipart upload.")
    p_abort_multipart.add_argument("--upload-id", required=True, help="Upload ID of the multipart upload to abort.")
    p_abort_multipart.set_defaults(func=cmd_abort_multipart)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
