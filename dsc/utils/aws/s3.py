from __future__ import annotations

import logging
import subprocess
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterator

logger = logging.getLogger(__name__)


class S3Client:
    """A class to perform common S3 operations for this application."""

    def __init__(self) -> None:
        self.client = boto3.client("s3")

    def move_file(
        self,
        source_file: str,
        destination_file: str,
    ) -> None:
        """Move an S3 object to another location.

        Like the AWS CLI 'mv' command, this method copies the source object or file
        to the specified destination and then deletes the source object or file.

        Args:
            source_file: S3 URI to source object to copy.
            destination_file: S3 URI to destination object.
        """
        parsed_source_uri = urlparse(source_file, allow_fragments=False)
        parsed_destination_uri = urlparse(destination_file, allow_fragments=False)

        source_bucket, source_key = (
            parsed_source_uri.netloc,
            parsed_source_uri.path.lstrip("/"),
        )
        destination_bucket, destination_key = (
            parsed_destination_uri.netloc,
            parsed_destination_uri.path.lstrip("/"),
        )

        self.client.copy_object(
            Bucket=destination_bucket,
            CopySource=f"{source_bucket}/{source_key}",
            Key=destination_key,
        )
        self.client.delete_object(
            Bucket=source_bucket,
            Key=source_key,
        )
        logger.debug(f"Moved file from {source_file} to {destination_file}")

    def put_file(
        self,
        bucket: str,
        key: str,
        file_content: str | bytes,
    ) -> None:
        """Create an object in a specified S3 bucket.

        Args:
            bucket: The S3 bucket where the file will be uploaded.
            key: The key to be used for the uploaded file.
            file_content: The content of the file to be uploaded.
        """
        self.client.put_object(
            Body=file_content,
            Bucket=bucket,
            Key=key,
        )
        logger.debug(f"File uploaded to S3: {bucket}/{key}")

    def files_iter(
        self,
        bucket: str,
        prefix: str = "",
        item_identifier: str = "",
        file_type: str = "",
        exclude_prefixes: list[str] | None = None,
    ) -> Iterator[str]:
        """Yield object keys for files stored on S3.

        Results can be filtered by prefix, file identifier, and file type.
        This method can also exclude files containing certain prefixes
        (i.e., to skip files in 'archived/' folder).

        Args:
            bucket: S3 bucket name.
            prefix: Filter file list by prefix (i.e., subfolder in S3 bucket).
            item_identifier: Filter file list by unique identifier
                in filename. Defaults to an empty string ("").
            file_type: Filter file list by file type (extension).
                Defaults to an empty string ("").
            exclude_prefixes: Filter file list by excluding
                file keys containing certain prefixes (e.g., 'archived').

        Yields:
            Files matching filters.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        if exclude_prefixes is None:
            exclude_prefixes = []

        for page in page_iterator:
            for content in page.get("Contents", []):
                if content["Key"] == prefix:
                    # skip base folder
                    continue

                if (
                    content["Key"].endswith(file_type)
                    and item_identifier in content["Key"]
                ):
                    if any(
                        exclude_prefix in content["Key"]
                        for exclude_prefix in exclude_prefixes
                    ):
                        continue

                    yield f"s3://{bucket}/{content['Key']}"


def run_aws_cli_sync(
    source: str,
    destination: str,
    *,
    exclude_patterns: list[str] | None = None,
    dry_run: bool = False,
) -> int:
    logger.info(f"Syncing data from {source} to {destination}")

    args = ["aws", "s3", "sync", source, destination, "--delete"]

    # add optional args
    # exclude all files or objects from the command that matches the specified patterns
    if exclude_patterns:
        for pattern in exclude_patterns:
            args.extend(["--exclude", pattern])
    # only display operations that would be performed without execution
    if dry_run:
        args.append("--dryrun")

    process = subprocess.Popen(  # noqa: S603
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    # log process output (stdout and stderr) in real-time
    if process.stdout:
        for line in process.stdout:
            if line:
                logger.info(line)
    else:
        logger.info("No changes detected in source, no sync required")

    if process.stderr:
        for line in process.stderr:
            if line:
                logger.error(line)

    # wait for the process to complete
    process.wait()
    return_code = process.returncode

    if return_code != 0:
        logger.error(f"Failed to sync (exit code: {return_code})")
    else:
        logger.info("Sync completed successfully")

    return return_code
