from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from boto3 import client

if TYPE_CHECKING:
    from collections.abc import Iterator

    from mypy_boto3_s3.type_defs import PutObjectOutputTypeDef

logger = logging.getLogger(__name__)


class S3Client:
    """An S3 class that provides a generic boto3 s3 client."""

    def __init__(self) -> None:
        self.client = client("s3")

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
        self, file_content: str | bytes, bucket: str, key: str
    ) -> PutObjectOutputTypeDef:
        """Put a file in a specified S3 bucket with a specified key.

        Args:
            file_content: The content of the file to be uploaded.
            bucket: The S3 bucket where the file will be uploaded.
            key: The key to be used for the uploaded file.
        """
        response = self.client.put_object(
            Body=file_content,
            Bucket=bucket,
            Key=key,
        )
        logger.debug("%s uploaded to S3", key)
        return response

    def retrieve_file_type_from_bucket(
        self, bucket: str, file_type: str, excluded_key_prefix: str
    ) -> Iterator[str]:
        """Retrieve file based on file type, bucket, and without excluded prefix.

        Args:
            bucket: The S3 bucket to search.
            file_type: The file type to retrieve.
            excluded_key_prefix: Files with this key prefix will not be retrieved.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket)
        for s3_object in [
            s3_object
            for page in pages
            for s3_object in page["Contents"]
            if s3_object["Key"].endswith(file_type)
            and excluded_key_prefix not in s3_object["Key"]
        ]:
            yield s3_object["Key"]
