from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from boto3 import client

from dsc.exceptions import InvalidSQSMessageError

if TYPE_CHECKING:  # pragma: no cover
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

    def __init__(
        self, region: str, queue_name: str, queue_url: str | None = None
    ) -> None:
        self.client = client("sqs", region_name=region)
        self.queue_name = queue_name
        self._queue_url: str | None = queue_url

    @property
    def queue_url(self) -> str:
        """Property to provide QueueUrl, caching it for reuse."""
        if not self._queue_url:
            self._queue_url = self.get_queue_url()
        return self._queue_url

    def get_queue_url(self) -> str:
        """Get SQS queue URL from name."""
        return self.client.get_queue_url(QueueName=self.queue_name)["QueueUrl"]

    @staticmethod
    def create_dss_message_attributes(
        item_identifier: str, submission_source: str, output_queue: str
    ) -> dict[str, Any]:
        """Create attributes for a DSpace Submission Service message.

        Link to the DSS input message specification:

        https://github.com/MITLibraries/dspace-submission-service/blob/main/docs/specifications/submission-message-specification.md#messageattributes

        Args:
            item_identifier: The submission's identifier which is populates the PackageID
            field.
            submission_source: The source for the submission.
            output_queue: The SQS output queue used for retrieving result messages.
        """
        return {
            "PackageID": {"DataType": "String", "StringValue": item_identifier},
            "SubmissionSource": {"DataType": "String", "StringValue": submission_source},
            "OutputQueue": {"DataType": "String", "StringValue": output_queue},
        }

    @staticmethod
    def create_dss_message_body(
        submission_system: str,
        collection_handle: str,
        metadata_s3_uri: str,
        bitstream_s3_uris: list[str],
    ) -> str:
        """Create body for a DSpace Submission Service message.

        Link to the DSS input message specification:

        https://github.com/MITLibraries/dspace-submission-service/blob/main/docs/specifications/submission-message-specification.md#messagebody

        Args:
            submission_system: The system where the submission is uploaded
            (e.g. DSpace@MIT).
            collection_handle: The handle of collection where the submission is uploaded.
            metadata_s3_uri: The S3 URI for the metadata JSON file.
            bitstream_s3_uris: The S3 URIs for the submission's bitstreams.
        """
        files = []
        for bitstream_s3_uri in bitstream_s3_uris:
            bitstream_file_name = bitstream_s3_uri.split("/")[-1]
            files.append(
                {
                    "BitstreamName": bitstream_file_name,
                    "FileLocation": bitstream_s3_uri,
                    "BitstreamDescription": None,
                }
            )
        return json.dumps(
            {
                "SubmissionSystem": submission_system,
                "CollectionHandle": collection_handle,
                "MetadataLocation": metadata_s3_uri,
                "Files": files,
            }
        )

    def delete(self, receipt_handle: str) -> EmptyResponseMetadataTypeDef:
        """Delete message from SQS queue.

        Args:
            receipt_handle: The receipt handle of the message to be deleted.
        """
        logger.debug("Deleting '{receipt_handle}' from SQS queue: {self.queue_name}")
        response = self.client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
        )
        logger.debug(f"Message deleted from SQS queue: {response}")

        return response

    def process_result_message(self, sqs_message: MessageTypeDef) -> tuple[str, str]:
        """Validate, extract data, and delete an SQS result message.

        Args:
            sqs_message: An SQS result message to be processed.
        """
        self.validate_result_message(sqs_message)
        identifier = sqs_message["MessageAttributes"]["PackageID"]["StringValue"]
        message_body = json.loads(str(sqs_message["Body"]))
        self.delete(sqs_message["ReceiptHandle"])
        return identifier, message_body

    def receive(self) -> Iterator[MessageTypeDef]:
        """Receive messages from SQS queue."""
        logger.debug(f"Receiving messages from SQS queue: {self.queue_name}")
        while True:
            response = self.client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
            )
            if "Messages" in response:
                for message in response["Messages"]:
                    logger.debug(
                        f"Message retrieved from SQS queue {self.queue_name}: {message}"
                    )
                    yield message
            else:
                logger.debug(f"No more messages from SQS queue: {self.queue_name}")
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
        logger.debug(f"Sending message to SQS queue: {self.queue_name}")
        response = self.client.send_message(
            QueueUrl=self.queue_url,
            MessageAttributes=message_attributes,
            MessageBody=str(message_body),
        )
        logger.debug(f"Response from SQS queue: {response}")
        return response

    def validate_result_message(self, sqs_message: MessageTypeDef) -> None:
        """Validate that an SQS result message is formatted as expected.

        Args:
            sqs_message:  An SQS message to be evaluated.

        """
        if not sqs_message.get("ReceiptHandle"):
            raise InvalidSQSMessageError(
                f"Failed to retrieve 'ReceiptHandle' from message: {sqs_message}"
            )
        self.validate_message_attributes(sqs_message=sqs_message)
        self.validate_message_body(sqs_message=sqs_message)

    @staticmethod
    def validate_message_attributes(sqs_message: MessageTypeDef) -> None:
        """Validate that "MessageAttributes" field is formatted as expected.

        Args:
            sqs_message: An SQS message to be evaluated.
        """
        if (
            "MessageAttributes" not in sqs_message
            or "PackageID" not in sqs_message["MessageAttributes"]
            or not sqs_message["MessageAttributes"]["PackageID"].get("StringValue")
        ):
            raise InvalidSQSMessageError(
                f"Failed to parse SQS message attributes: {sqs_message}"
            )

    @staticmethod
    def validate_message_body(sqs_message: MessageTypeDef) -> None:
        """Validate that "Body" field is formatted as expected.

        Args:
            sqs_message: An SQS message to be evaluated.
        """
        if "Body" not in sqs_message or not json.loads(str(sqs_message["Body"])):
            raise InvalidSQSMessageError(
                f"Failed to parse SQS message body: {sqs_message}"
            )
