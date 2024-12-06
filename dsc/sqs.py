from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from boto3 import client

from dsc.exceptions import InvalidSQSMessageError

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from mypy_boto3_sqs.type_defs import (
        EmptyResponseMetadataTypeDef,
        MessageAttributeValueTypeDef,
        MessageTypeDef,
        SendMessageResultTypeDef,
    )

logger = logging.getLogger(__name__)


class SQSClient:
    """A class to perform common SQS operations for this application."""

    def __init__(self, region: str, base_url: str, queue_name: str) -> None:
        self.client = client("sqs", region_name=region)
        self.base_url: str = base_url
        self.queue_name: str = queue_name

    @staticmethod
    def create_dss_message_attributes(
        package_id: str, submission_source: str, output_queue: str
    ) -> dict[str, Any]:
        """Create attributes for a DSpace Submission Service message.

        Args:
            package_id: The PackageID field which is populated by the submission's
            identifier.
            submission_source: The source for the submission.
            output_queue: The SQS output queue used for retrieving result messages.
        """
        return {
            "PackageID": {"DataType": "String", "StringValue": package_id},
            "SubmissionSource": {"DataType": "String", "StringValue": submission_source},
            "OutputQueue": {"DataType": "String", "StringValue": output_queue},
        }

    @staticmethod
    def create_dss_message_body(
        submission_system: str,
        collection_handle: str,
        metadata_s3_uri: str,
        bitstream_file_name: str,
        bitstream_s3_uri: str,
    ) -> str:
        """Create body for a DSpace Submission Service message.

        Args:
        submission_system: The system where the article is uploaded.
        collection_handle: The handle of collection where the article is uploaded.
        metadata_s3_uri: The S3 URI for the metadata JSON file.
        bitstream_file_name: The file name for the article content which is uploaded as a
        bitstream.
        bitstream_s3_uri: The S3 URI for the article content file.
        """
        return json.dumps(
            {
                "SubmissionSystem": submission_system,
                "CollectionHandle": collection_handle,
                "MetadataLocation": metadata_s3_uri,
                "Files": [
                    {
                        "BitstreamName": bitstream_file_name,
                        "FileLocation": bitstream_s3_uri,
                        "BitstreamDescription": None,
                    }
                ],
            }
        )

    def delete(self, receipt_handle: str) -> EmptyResponseMetadataTypeDef:
        """Delete message from SQS queue.

        Args:
            receipt_handle: The receipt handle of the message to be deleted.
        """
        logger.debug("Deleting %s from SQS queue: %s", receipt_handle, self.queue_name)
        response = self.client.delete_message(
            QueueUrl=f"{self.base_url}{self.queue_name}",
            ReceiptHandle=receipt_handle,
        )
        logger.debug("Message deleted from SQS queue: %s", response)
        return response

    def process_result_message(self, sqs_message: MessageTypeDef) -> tuple[str, str]:
        """Validate, extract data, and delete an SQS result message.

        Args:
            sqs_message: An SQS result message to be processed.
        """
        if not self.valid_sqs_message(sqs_message):
            raise InvalidSQSMessageError
        identifier = sqs_message["MessageAttributes"]["PackageID"]["StringValue"]
        message_body = json.loads(str(sqs_message["Body"]))
        self.delete(sqs_message["ReceiptHandle"])
        return identifier, message_body

    def receive(self) -> Iterator[MessageTypeDef]:
        """Receive messages from SQS queue."""
        logger.debug("Receiving messages from SQS queue: %s", self.queue_name)
        while True:
            response = self.client.receive_message(
                QueueUrl=f"{self.base_url}{self.queue_name}",
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
            )
            if "Messages" in response:
                for message in response["Messages"]:
                    logger.debug(
                        "Message retrieved from SQS queue %s: %s",
                        self.queue_name,
                        message,
                    )
                    yield message
            else:
                logger.debug("No more messages from SQS queue: %s", self.queue_name)
                break

    def send(
        self,
        message_attributes: Mapping[str, MessageAttributeValueTypeDef],
        message_body: str,
    ) -> SendMessageResultTypeDef:
        """Send message via SQS.

        Args:
            message_attributes: The attributes of the message to send.
            message_body: The body of the message to send.
        """
        logger.debug("Sending message to SQS queue: %s", self.queue_name)
        response = self.client.send_message(
            QueueUrl=f"{self.base_url}{self.queue_name}",
            MessageAttributes=message_attributes,
            MessageBody=str(message_body),
        )
        logger.debug("Response from SQS queue: %s", response)
        return response

    @staticmethod
    def valid_result_message_attributes(sqs_message: MessageTypeDef) -> bool:
        """Validate that "MessageAttributes" field is formatted as expected.

        Args:
            sqs_message: An SQS message to be evaluated.
        """
        valid = False
        if (
            "MessageAttributes" in sqs_message
            and any(
                field
                for field in sqs_message["MessageAttributes"]
                if "PackageID" in field
            )
            and sqs_message["MessageAttributes"]["PackageID"].get("StringValue")
        ):
            valid = True
        else:
            logger.exception("Failed to parse SQS message attributes: %s", sqs_message)
        return valid

    @staticmethod
    def valid_result_message_body(sqs_message: MessageTypeDef) -> bool:
        """Validate that "Body" field is formatted as expected.

        Args:
            sqs_message: An SQS message to be evaluated.
        """
        valid = False
        if "Body" in sqs_message and json.loads(str(sqs_message["Body"])):
            valid = True
        else:
            logger.exception("Failed to parse SQS message body: %s", sqs_message)
        return valid

    def valid_sqs_message(self, sqs_message: MessageTypeDef) -> bool:
        """Validate that an SQS message is formatted as expected.

        Args:
            sqs_message:  An SQS message to be evaluated.

        """
        valid = False
        if (
            self.valid_result_message_attributes(sqs_message=sqs_message)
            and self.valid_result_message_body(sqs_message=sqs_message)
            and sqs_message.get("ReceiptHandle")
        ):
            valid = True
        else:
            logger.exception("Failed to parse SQS message: %s", sqs_message)
        return valid
