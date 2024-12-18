import json
import logging
from dataclasses import dataclass
from typing import Any

from dsc.utilities.aws.s3 import S3Client

logger = logging.getLogger(__name__)


@dataclass
class ItemSubmission:
    """A class to store the required values for a DSpace submission."""

    dspace_metadata: dict[str, Any]
    bitstream_uris: list[str]
    metadata_s3_key: str
    metadata_uri: str = ""

    def upload_dspace_metadata(self, bucket: str) -> None:
        """Upload DSpace metadata to S3 using the specified bucket and keyname.

        Args:
            bucket: The S3 bucket for uploading the item metadata file.
        """
        s3_client = S3Client()
        s3_client.put_file(json.dumps(self.dspace_metadata), bucket, self.metadata_s3_key)
        metadata_uri = f"s3://{bucket}/{self.metadata_s3_key}"
        logger.info(f"Metadata uploaded to S3: {metadata_uri}")
        self.metadata_uri = metadata_uri
