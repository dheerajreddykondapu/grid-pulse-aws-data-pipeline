import os
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3")

BUCKET = os.environ["BUCKET_NAME"]

SOURCE_PREFIXES = [
    "raw/games/",
    "raw/plays/",
    "curated/games/",
    "curated/plays/"
]


def list_objects(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/") or key.endswith("$folder$"):
                continue
            keys.append(key)
    return keys


def move_object(bucket, old_key, new_key):
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": old_key},
        Key=new_key
    )
    s3.delete_object(Bucket=bucket, Key=old_key)


def lambda_handler(event, context):
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_root = f"archive/{ts}/"

    moved = []

    for source_prefix in SOURCE_PREFIXES:
        keys = list_objects(BUCKET, source_prefix)

        for key in keys:
            new_key = archive_root + key
            move_object(BUCKET, key, new_key)
            moved.append({"from": key, "to": new_key})

    return {
        "statusCode": 200,
        "message": "Archive complete",
        "archive_root": archive_root,
        "files_moved": len(moved)
    }
