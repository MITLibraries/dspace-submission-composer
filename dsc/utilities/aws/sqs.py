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

    def delete(
        self, receipt_handle: str, message_id: str
    ) -> EmptyResponseMetadataTypeDef:
        """Delete message from SQS queue."""
        response = self.client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
        )
        logger.debug(f"Deleted message: {message_id}")
        return response

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
        response = self.client.send_message(
            QueueUrl=self.queue_url,
            MessageAttributes=message_attributes,
            MessageBody=str(message_body),
        )
        logger.debug(f"Sent message: {response["MessageId"]}")
        return response

    def receive(self) -> Iterator[MessageTypeDef]:
        """Receive messages from SQS queue."""
        message_count = 0
        logger.debug(f"Receiving messages from the queue '{self.queue_name}'")

        while True:
            response = self.client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
            )
            if "Messages" in response:
                for message in response["Messages"]:
                    logger.debug(f"Retrieved message: {message["MessageId"]}")
                    message_count += 1
                    yield message
            else:
                if message_count == 0:
                    logger.info(f"No messages found in the queue '{self.queue_name}'")
                else:
                    logger.info(f"No messages remain in the queue '{self.queue_name}'")
                break

        logger.info(
            f"Retrieved {message_count} message(s) from the queue '{self.queue_name}'"
        )

    def parse_dss_result_message(self, sqs_message: MessageTypeDef) -> tuple[str, dict]:
        message_id = sqs_message["MessageId"]
        self.validate_dss_result_message(sqs_message)
        item_identifier = sqs_message["MessageAttributes"]["PackageID"]["StringValue"]
        message_body = json.loads(sqs_message["Body"])
        logger.debug(f"Parsed message: {message_id}")
        return item_identifier, message_body

    def validate_dss_result_message(self, sqs_message: MessageTypeDef) -> None:
        """Validate format of DSS result message.

        Args:
            sqs_message: An SQS message to be evaluated.
        """
        if not sqs_message.get("ReceiptHandle"):
            raise InvalidSQSMessageError(
                "Failed to retrieve 'ReceiptHandle' from message: "
                f"{sqs_message["MessageId"]}"
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
                f"Failed to parse message attributes: {sqs_message["MessageId"]}"
            )

    @staticmethod
    def validate_message_body(sqs_message: MessageTypeDef) -> None:
        """Validate that "Body" field is formatted as expected.

        Args:
            sqs_message: An SQS message to be evaluated.
        """
        if "Body" not in sqs_message or not json.loads(str(sqs_message["Body"])):
            raise InvalidSQSMessageError(
                f"Failed to parse message body: {sqs_message["MessageId"]}"
            )
