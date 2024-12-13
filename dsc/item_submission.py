import json
import logging
from dataclasses import dataclass
from typing import Any

from dsc.utilities.aws.s3 import S3Client

logger = logging.getLogger(__name__)


@dataclass
class ItemSubmission:
    """A class to store the required values for a DSpace submission."""

    source_metadata: dict[str, Any]
    metadata_mapping: dict[str, Any]
    s3_client: S3Client
    bitstream_uris: list[str]
    metadata_keyname: str
    metadata_uri: str = ""

    def generate_and_upload_dspace_metadata(self, bucket: str) -> None:
        """Generate DSpace metadata from the item's source metadata and upload it to S3.

        Args:
            bucket: The S3 bucket for uploading the item metadata file.
        """
        dspace_metadata = self.create_dspace_metadata()
        self.s3_client.put_file(
            json.dumps(dspace_metadata),
            bucket,
            self.metadata_keyname,
        )
        metadata_uri = f"s3://{bucket}/{self.metadata_keyname}"
        logger.info(f"Metadata uploaded to S3: {metadata_uri}")
        self.metadata_uri = metadata_uri

    def create_dspace_metadata(self) -> dict[str, Any]:
        """Create DSpace metadata from the item's source metadata."""
        metadata_entries = []
        for field_name, field_mapping in self.metadata_mapping.items():
            if field_name not in ["item_identifier", "source_system_identifier"]:

                field_value = self.source_metadata.get(field_mapping["source_field_name"])
                if field_value:
                    delimiter = field_mapping["delimiter"]
                    language = field_mapping["language"]
                    if delimiter:
                        metadata_entries.extend(
                            [
                                {
                                    "key": field_name,
                                    "value": value,
                                    "language": language,
                                }
                                for value in field_value.split(delimiter)
                            ]
                        )
                    else:
                        metadata_entries.append(
                            {
                                "key": field_name,
                                "value": field_value,
                                "language": language,
                            }
                        )

        return {"metadata": metadata_entries}
