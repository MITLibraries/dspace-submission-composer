from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError
from pynamodb.exceptions import DoesNotExist

from dsc.config import Config
from dsc.db.models import ITEM_SUBMISSION_LOG_STR, ItemSubmissionDB, ItemSubmissionStatus
from dsc.exceptions import (
    DSpaceMetadataUploadError,
    InvalidDSpaceMetadataError,
    ItemMetadatMissingRequiredFieldError,
    SQSMessageSendError,
)
from dsc.utilities.aws.s3 import S3Client
from dsc.utilities.aws.sqs import SQSClient

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterator
    from datetime import datetime

    from mypy_boto3_sqs.type_defs import SendMessageResultTypeDef

logger = logging.getLogger(__name__)
CONFIG = Config()


@dataclass
class ItemSubmission:
    """Domain class that stores both persistence and business logic for item submissions.

    This class maintains two types of attributes:

    1. Persisted attributes: Mapped directly to ItemSubmissionDB columns
       and persisted in the database
    2. Processing attributes: Temporary data used during submission processing
       but not persisted

    This class provides high-level methods for managing item submissions while abstracting
    the underlying database operations. It wraps the dsc.db.models.ItemSubmissionDB model,
    providing intuitive CRUD-like methods (get, create, upsert) for data persistence.
    """

    # persisted attributes
    batch_id: str
    item_identifier: str
    workflow_name: str
    collection_handle: str | None = None
    last_run_date: datetime | None = None
    submit_attempts: int = 0
    ingest_attempts: int = 0
    ingest_date: str | None = None
    last_result_message: str | None = None
    dspace_handle: str | None = None
    status: str | None = None
    status_details: str | None = None

    # processing attributes
    source_metadata: dict[str, Any] | None = None
    dspace_metadata: dict[str, Any] | None = None
    bitstream_s3_uris: list[str] | None = None
    metadata_s3_uri: str = ""

    @classmethod
    def get_or_create(
        cls, batch_id: str, item_identifier: str, workflow_name: str
    ) -> ItemSubmission:
        """Get or create an ItemSubmission.

        The method hydrates the ItemSubmission with data from DynamoDB if it
        finds a corresponding record.
        """
        return cls.get(batch_id, item_identifier) or cls.create(
            batch_id, item_identifier, workflow_name
        )

    @classmethod
    def get(
        cls, batch_id: str | None, item_identifier: str | None
    ) -> ItemSubmission | None:
        """Get an ItemSubmission with data from its associated record in DynamoDB.

        If there is no corresponding record in DynamoDB, the method returns None.
        """
        try:
            item_submission_db = ItemSubmissionDB.get(batch_id, item_identifier)
        except DoesNotExist:
            return None

        return cls._from_db(item_submission_db)

    @classmethod
    def get_batch(cls, batch_id: str) -> Iterator[ItemSubmission]:
        """Yield instances of ItemSubmission for a given batch.

        This method will first query the item submissions DynamoDB table for all
        records with matching 'batch_id'. The yielded ItemSubmissionDB records
        are then used to create instances of the ItemSubmission domain class,
        hydrated with data from the corresponding record in DynamoDB.
        """
        for item_submission_db in ItemSubmissionDB.query(batch_id):
            yield cls._from_db(item_submission_db)

    @classmethod
    def create(
        cls, batch_id: str, item_identifier: str, workflow_name: str
    ) -> ItemSubmission:
        """Create an ItemSubmission."""
        return cls(batch_id, item_identifier, workflow_name)

    @classmethod
    def _from_db(cls, item_submission_db: ItemSubmissionDB) -> ItemSubmission:
        """Hydrate the ItemSubmission with data from DynamoDB.

        This is intended as a 'private' method, in an effort to encapsulate any
        database interactions as part of the ItemSubmission domain class and abstract
        away any PynamoDB logic.
        """
        item_submission = cls(
            batch_id=item_submission_db.batch_id,
            item_identifier=item_submission_db.item_identifier,
            workflow_name=item_submission_db.workflow_name,
        )
        for attr in item_submission_db.get_attributes():
            if hasattr(item_submission_db, attr):
                value = getattr(item_submission_db, attr)
                setattr(item_submission, attr, value)
        logger.debug(
            f"Populated record {ITEM_SUBMISSION_LOG_STR.format(
                batch_id=item_submission_db.batch_id,
                item_identifier=item_submission_db.item_identifier
            )}"
        )
        return item_submission

    def upsert_db(self) -> None:
        """Upsert a record in DynamoDB from ItemSubmission.

        This will either update the associated record in the item submissions DynamoDB
        table using the content of ItemSubmission if it already exists, or
        insert a new record to the table using the content of the ItemSubmission
        """
        item_submission_record = ItemSubmissionDB(
            **{attr: getattr(self, attr) for attr in ItemSubmissionDB.get_attributes()}
        )
        item_submission_record.save()

        logger.info(
            f"Saved record "
            f"{ITEM_SUBMISSION_LOG_STR.format(
                batch_id=self.batch_id, item_identifier=self.item_identifier
                )}"
        )

    def ready_to_submit(self) -> bool:
        """Check if the item submission is ready to be submitted."""
        ready_to_submit = False

        self._exceeded_retry_threshold()

        match self.status:
            case ItemSubmissionStatus.INGEST_SUCCESS:
                logger.info(
                    "Record "
                    f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=self.item_identifier)
                    } "
                    "already ingested, skipping submission"
                )
            case ItemSubmissionStatus.SUBMIT_SUCCESS:
                logger.info(
                    f"Record "
                    f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=self.item_identifier)
                    } "
                    " already submitted, skipping submission"
                )
            case ItemSubmissionStatus.MAX_RETRIES_REACHED:
                logger.info(
                    f"Record "
                    f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=self.item_identifier)
                    } "
                    "max retries reached, skipping submission"
                )
            case None | ItemSubmissionStatus.RECONCILE_FAILED:
                logger.info(
                    f"Record "
                    f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=self.item_identifier)
                    } "
                    " not reconciled, skipping submission"
                )
            case _:
                logger.debug(
                    f"Record "
                    f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=self.item_identifier)
                    } "
                    "allowed for submission"
                )
                ready_to_submit = True

        return ready_to_submit

    def _exceeded_retry_threshold(self) -> None:
        """Check whether ingest attempts have exceeded retry threshold.

        If exceeded, set status to MAX_RETRIES_REACHED.
        """
        if self.ingest_attempts > CONFIG.retry_threshold:
            self.status = ItemSubmissionStatus.MAX_RETRIES_REACHED

    def prepare_dspace_metadata(
        self, metadata_mapping: dict, item_metadata: dict, s3_bucket: str, batch_path: str
    ) -> None:
        """Prepare DSpace metadata for the item submission."""
        self.create_dspace_metadata(
            item_metadata=item_metadata,
            metadata_mapping=metadata_mapping,
        )
        self.validate_dspace_metadata()
        self.upload_dspace_metadata(bucket=s3_bucket, prefix=batch_path)

    def create_dspace_metadata(
        self, item_metadata: dict[str, Any], metadata_mapping: dict
    ) -> None:
        """Create DSpace metadata from the item's source metadata.

        A metadata mapping is a dict with the format seen below:

        {
            "dc.contributor": {
                "source_field_name": "contributor",
                "language": "<language>",
                "delimiter": "<delimiting character>",
                "required": true | false
            }
        }

        When setting up the metadata mapping JSON file, "language" and "delimiter"
        can be omitted from the file if not applicable. Required fields ("item_identifier"
        and "title") must be set as required (true); if "required" is not listed as a
        a config, the field defaults as not required (false).

        Args:
            item_metadata: Item metadata from which the DSpace metadata will be derived.
            metadata_mapping: A mapping of DSpace metadata fields to source metadata
            fields.
        """
        metadata_entries = []
        for field_name, field_mapping in metadata_mapping.items():
            if field_name not in ["item_identifier"]:

                field_value = item_metadata.get(field_mapping["source_field_name"])
                if not field_value and field_mapping.get("required", False):
                    raise ItemMetadatMissingRequiredFieldError(
                        "Item metadata missing required field: '"
                        f"{field_mapping["source_field_name"]}'"
                    )

                if field_value:
                    if isinstance(field_value, list):
                        field_values = field_value
                    elif delimiter := field_mapping.get("delimiter"):
                        field_values = field_value.split(delimiter)
                    else:
                        field_values = [field_value]

                    metadata_entries.extend(
                        [
                            {
                                "key": field_name,
                                "value": value,
                                "language": field_mapping.get("language"),
                            }
                            for value in field_values
                        ]
                    )
        self.dspace_metadata = {"metadata": metadata_entries}

    def validate_dspace_metadata(self) -> bool:
        """Validate that DSpace metadata follows the expected format for DSpace 6.x.

        Args:
            dspace_metadata: DSpace metadata to be validated.
        """
        valid = False
        if self.dspace_metadata and self.dspace_metadata.get("metadata") is not None:
            for element in self.dspace_metadata["metadata"]:
                if element.get("key") is not None and element.get("value") is not None:
                    valid = True
            logger.debug("Valid DSpace metadata created")
        else:
            raise InvalidDSpaceMetadataError(
                f"Invalid DSpace metadata created: {self.dspace_metadata} ",
            )
        return valid

    def upload_dspace_metadata(self, bucket: str, prefix: str) -> None:
        """Upload DSpace metadata to S3 using the specified bucket and keyname.

        Args:
            bucket: The S3 bucket for uploading the item metadata file.
            prefix: The S3 prefix (or 'folder') in which the 'subfolder' for
                DSpace metadata is created. In practice, this corresponds with
                Workflow.batch_path.

        """
        s3_client = S3Client()
        metadata_s3_key = f"{prefix}dspace_metadata/{self.item_identifier}_metadata.json"
        try:
            s3_client.put_file(
                bucket=bucket,
                key=metadata_s3_key,
                file_content=json.dumps(self.dspace_metadata),
            )
        except Exception as exception:
            message = (
                f"Failed to upload DSpace metadata for item '{self.item_identifier}'"
            )
            logger.exception(message)
            raise DSpaceMetadataUploadError(f"{message}': {exception}") from exception

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
        if not self.metadata_s3_uri or not self.bitstream_s3_uris:
            message = (
                "Metadata S3 URI or bitstream S3 URIs not set "
                f"for item: {self.item_identifier}"
            )
            logger.error(message)
            raise ValueError(message)

        message_body = sqs_client.create_dss_message_body(
            submission_system,
            collection_handle,
            self.metadata_s3_uri,
            self.bitstream_s3_uris,
        )
        try:
            response = sqs_client.send(message_attributes, message_body)
        except ClientError as exception:
            message = (
                f"Failed to send submission message for item: {self.item_identifier}. "
            )
            logger.exception(message)
            raise SQSMessageSendError(f"{message} {exception}") from exception
        return response
