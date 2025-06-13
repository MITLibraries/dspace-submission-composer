from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterator

logger = logging.getLogger(__name__)


class S3Client:
    """A class to perform common S3 operations for this application."""

    def __init__(self) -> None:
        self.client = boto3.client("s3")

    def archive_file_with_new_key(
        self, bucket: str, key: str, archived_key_prefix: str
    ) -> None:
        """Update the key of the specified file to archive it from processing.

        Args:
            bucket: The S3 bucket containing the files to be archived.
            key: The key of the file to archive.
            archived_key_prefix: The prefix to be applied to the archived file.
        """
        self.client.copy_object(
            Bucket=bucket,
            CopySource=f"{bucket}/{key}",
            Key=f"{archived_key_prefix}/{key}",
        )
        self.client.delete_object(
            Bucket=bucket,
            Key=key,
        )

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

                    yield f"s3://{bucket}/{content["Key"]}"
