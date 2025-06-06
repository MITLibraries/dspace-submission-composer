from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dsc.config import Config
from dsc.utilities.aws.s3 import S3Client
from dsc.utilities.aws.sqs import SQSClient

if TYPE_CHECKING:  # pragma: no cover

    from mypy_boto3_sqs.type_defs import SendMessageResultTypeDef

logger = logging.getLogger(__name__)
CONFIG = Config()


@dataclass
class ItemSubmission:
    """A class to store the required values for a DSpace submission."""

    dspace_metadata: dict[str, Any]
    bitstream_s3_uris: list[str]
    item_identifier: str
    metadata_s3_uri: str = ""

    def upload_dspace_metadata(self, bucket: str, prefix: str) -> None:
        """Upload DSpace metadata to S3 using the specified bucket and keyname.

        Args:
            bucket: The S3 bucket for uploading the item metadata file.
            prefix: The S3 prefix used for objects in this workflow. Does NOT include
            the item identifier.
        """
        s3_client = S3Client()
        metadata_s3_key = f"{prefix}{self.item_identifier}_metadata.json"
        s3_client.put_file(
            bucket=bucket,
            key=metadata_s3_key,
            file_content=json.dumps(self.dspace_metadata),
        )
        metadata_s3_uri = f"s3://{bucket}/{metadata_s3_key}"
        logger.info(f"Metadata uploaded to S3: {metadata_s3_uri}")
        self.metadata_s3_uri = metadata_s3_uri

    def send_submission_message(
        self,
        submission_source: str,
        output_queue: str,
        submission_system: str,
        collection_handle: str,
    ) -> SendMessageResultTypeDef:
        """Send a submission message to the DSS input queue.

        Args:
            submission_source: The source for the submission.
            output_queue: The SQS output queue used for retrieving result messages.
            submission_system: The system where the submission is uploaded
            (e.g. DSpace@MIT).
            collection_handle: The handle of collection where the submission is uploaded.
        """
        sqs_client = SQSClient(
            region=CONFIG.aws_region_name, queue_name=CONFIG.sqs_queue_dss_input
        )
        message_attributes = sqs_client.create_dss_message_attributes(
            self.item_identifier, submission_source, output_queue
        )
        message_body = sqs_client.create_dss_message_body(
            submission_system,
            collection_handle,
            self.metadata_s3_uri,
            self.bitstream_s3_uris,
        )
        return sqs_client.send(message_attributes, message_body)
