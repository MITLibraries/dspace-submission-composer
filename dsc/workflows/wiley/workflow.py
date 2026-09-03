import concurrent.futures
import json
import logging
import os
import tempfile
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import smart_open

from dsc import exceptions
from dsc.config import Config
from dsc.db.models import ItemSubmissionStatus
from dsc.item_submission import ItemSubmission
from dsc.utils.aws import S3Client, run_aws_cli_sync
from dsc.workflows.base import Workflow
from dsc.workflows.wiley import WileyTransformer

CONFIG = Config()
logger = logging.getLogger(__name__)

WILEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
}


class Wiley(Workflow):
    workflow_name: str = "wiley"
    metadata_transformer = WileyTransformer

    @property
    def metadata_mapping_path(self) -> str:
        raise NotImplementedError

    def get_batch_bitstream_uris(self) -> list[str]:
        raise NotImplementedError

    def item_metadata_iter(self) -> Iterator[dict[str, Any]]:
        raise NotImplementedError

    def prepare_batch(self, *, synced: bool = False) -> tuple[list, ...]:  # noqa: ARG002
        """Prepare a batch folder in the DSC S3 bucket.

        This method will first prepare the batch in a local temp directory
        before uploading the batch to S3. Each time the method is called,
        it mints a new batch ID using the run date.

        NOTE: Item creation failures are recorded in DynamoDB, so the method
        will always return an empty 'errors' list.
        """
        create_summary: dict[str, int] = {
            "total": 0,
            "created": 0,
            "skipped": 0,
            "errors": 0,
        }
        item_submissions = []
        errors: list[tuple] = []  # set but not used

        # get original batch id and path
        original_batch_id = self.batch_id
        original_batch_path = self.batch_path

        # create versioned batch id
        self.batch_id = self._update_batch_id(original_batch_id)

        # create temporary directory
        tmp_batch_path = self._create_tmp_batch_dir()

        # copy csv of DOIs into temp batch folder
        s3_client = S3Client()
        s3_client.download_file(
            s3_uri=f"s3://{CONFIG.s3_bucket_submission_assets}/{original_batch_path}MIT_Authored_Articles_Wiley.csv",
            destination_file=str(
                Path(tmp_batch_path) / "MIT_Authored_Articles_Wiley.csv"
            ),
        )

        # get list of DOIs
        dois = pd.read_csv(
            Path(tmp_batch_path) / "MIT_Authored_Articles_Wiley.csv",
            dtype="str",
            header=None,
        )[0].to_list()
        logger.info(f"Retrieved {len(dois)} DOIs from input file")

        # get list of DOIs for completed item submissions
        skip_list = self._get_completed_item_submission_ids()

        # prepare an ItemSubmission for each DOI
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(
                    self._prepare_item_submission, doi, tmp_batch_path, skip_list
                )
                for doi in dois
            ]

            for future in concurrent.futures.as_completed(futures):
                create_summary["total"] += 1
                item_submission = future.result()
                if item_submission:
                    item_submissions.append(item_submission)
                    if item_submission.status == ItemSubmissionStatus.CREATE_FAILED:
                        create_summary["errors"] += 1
                    elif item_submission.status == ItemSubmissionStatus.CREATE_SUCCESS:
                        create_summary["created"] += 1
                else:
                    create_summary["skipped"] += 1

        # sync batch folder in temporary directory to batch folder in DSC S3 bucket
        run_aws_cli_sync(
            source=tmp_batch_path,
            destination=f"s3://{CONFIG.s3_bucket_submission_assets}/{self.batch_path}",
        )
        logger.info(
            f"Created items for batch '{self.batch_id}': {json.dumps(create_summary)}"
        )

        return item_submissions, errors

    def _prepare_item_submission(
        self, doi: str, output_dir: str, skip_list: list[str] | None = None
    ) -> ItemSubmission | None:
        """Prepare an item submission associated with the DOI.

        The method will attempt to retrieve a PDF from Wiley and fetch
        metadata from Crossref (in that order).
            - If the method fails to retrieve a PDF, the method will
              skip fetching metadata.
            - If the method fails to retrieve the metadata, the downloaded
              PDF will be stored in the batch folder but effectively
              ignored.
        The method returns an ItemSubmission with a status indicating success
        or failure.

        Args:
            doi: The Digital Object Identifier (DOI) for the item submission,
                also used as the item identifier.
            output_dir: Where to save the downloaded submissions assets.
            skip_list: A list of DOIs to skip (i.e., item submissions that
                were already submitted to the submission queue). Defaults to None.
        """
        if skip_list is None:
            skip_list = []

        if doi in skip_list:
            logger.info(f"Item with item_identifier={doi} already submitted to DSpace")
            return None

        item_submission = ItemSubmission(
            batch_id=self.batch_id,
            item_identifier=doi,
            workflow_name=self.workflow_name,
        )

        try:
            self._download_bitstream(
                item_identifier=item_submission.item_identifier,
                output_dir=output_dir,
            )
            self._get_crossref_metadata(
                item_identifier=item_submission.item_identifier,
                output_dir=output_dir,
            )
        except (
            exceptions.ItemBitstreamsNotFoundError,
            exceptions.ItemMetadataNotFoundError,
        ) as exception:
            item_submission.status = ItemSubmissionStatus.CREATE_FAILED
            item_submission.status_details = str(exception)
        else:
            item_submission.status = ItemSubmissionStatus.CREATE_SUCCESS

        return item_submission

    def _get_completed_item_submission_ids(self) -> list[str]:
        """Get completed Wiley item submissions.

        Completed Wiley item submissions refer to items
        sent to the submission queue, represented with a status
        of "ingest_success".
        """
        item_submissions = list(
            ItemSubmission.get_workflow_submissions(
                workflow_name=self.workflow_name,
                status=ItemSubmissionStatus.INGEST_SUCCESS,
                attributes_to_get=["item_identifier"],
            )
        )
        return [item_submission.item_identifier for item_submission in item_submissions]

    def _create_tmp_batch_dir(self) -> str:
        """Create temporary directory for batch preparation."""
        tmp_dir = tempfile.TemporaryDirectory(delete=False)
        tmp_batch_path = Path(tmp_dir.name) / self.batch_id
        os.makedirs(tmp_batch_path)
        logger.info(f"Created batch folder in temporary directory: {tmp_dir.name}")
        return str(tmp_batch_path)

    def _download_bitstream(self, item_identifier: str, output_dir: str) -> None:
        """Download PDF from Wiley.

        PDFs are saved to a folder named with the item identifier,
        using the filename: <item_identifier>.pdf.
        """
        logger.info("Downloading content from Wiley")
        url = f"https://{CONFIG.wiley_bitstream_api_url}{item_identifier}"

        try:
            response = requests.get(url, headers=WILEY_HEADERS, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exception:
            logger.exception(f"Failed to retrieve content from {url}")
            raise exceptions.ItemBitstreamsNotFoundError from exception

        if not response.headers["content-type"].startswith("application/pdf"):
            logger.error(
                f"Expected PDF but retrieved {response.headers['content-type']} instead"
            )
            raise exceptions.ItemBitstreamsNotFoundError

        # set filepath for bitstream PDF file, creating intermediate directories
        filepath = (
            Path(output_dir)
            / item_identifier.replace("/", "-")
            / f"{item_identifier.replace('/', '-')}.pdf"
        )
        filepath.parent.mkdir(parents=True, exist_ok=True)

        with open(filepath, "wb") as file:
            file.write(response.content)
            logger.info(f"Saved PDF to {file.name}")

    def _get_crossref_metadata(self, item_identifier: str, output_dir: str) -> None:
        """Fetch metadata from Crossref.

        PDFs are saved to a folder named with the item identifier,
        using the filename: <item_identifier>.pdf.
        """
        logger.info("Fetching metadata from Crossref")
        url = f"https://{CONFIG.wiley_metadata_api_url}{item_identifier}"
        try:
            response = requests.get(
                url, params={"mailto": "dspace-lib@mit.edu"}, timeout=30
            )
            metadata = response.json()
        except requests.exceptions.JSONDecodeError as exception:
            logger.exception("Failed to parse JSON from response")
            raise exceptions.ItemMetadataNotFoundError from exception
        except Exception as exception:
            logger.exception(f"Failed to retrieve metadata from {url}")
            raise exceptions.ItemMetadataNotFoundError from exception

        # set filepath for metadata JSON file, creating intermediate directories
        filepath = (
            Path(output_dir)
            / item_identifier.replace("/", "-")
            / f"{item_identifier.replace('/', '-')}.json"
        )
        filepath.parent.mkdir(parents=True, exist_ok=True)

        with open(filepath, "w") as file:
            json.dump(metadata, file)
            logger.info(f"Saved metadata to {file.name}")

    def _update_batch_id(self, batch_id: str) -> str:
        """Create a new batch ID with a date timestamp.

        This method is only used when creating a batch *without syncing*.
        The updated batch ID is used to distinguish different runs of
        Workflow.create_batch, which can be run as many times as needed
        until a batch is ready for submission.
        """
        return f"{batch_id}-{self.run_date.strftime('%Y%m%dT%H%M%SZ')}"

    def submit_items(self, collection_handle: str | None = None) -> list:
        logger.info(
            f"Submitting messages to the DSS input queue '{CONFIG.sqs_queue_dss_input}' "
            f"for batch '{self.batch_id}'"
        )

        manifest = self._load_batch_manifest()

        items = []
        for item_submission in ItemSubmission.get_batch(self.batch_id):
            self.submission_summary["total"] += 1
            item_submission.last_run_date = self.run_date
            logger.debug(
                f"Preparing submission for item: {item_submission.item_identifier}"
            )

            # validate whether a message should be sent for this item submission
            if not item_submission.ready_to_submit():
                self.submission_summary["skipped"] += 1
                continue
            try:
                # get item metadata
                item_metadata = self._get_transformed_metadata(
                    source_metadata_file=manifest[item_submission.item_identifier][
                        "metadata_file"
                    ],
                )

                # prepare submission assets
                item_submission.prepare_dspace_metadata(
                    item_metadata=item_metadata,
                    s3_bucket=self.s3_bucket,
                    batch_path=self.batch_path,
                )

                # attach bitstreams from manifest
                item_submission.bitstream_s3_uris = manifest[
                    item_submission.item_identifier
                ]["bitstream_files"]

                # attach collection handle
                item_submission.collection_handle = collection_handle

                # send submission message
                response = item_submission.send_submission_message(
                    submission_source=self.workflow_name,
                    output_queue=self.output_queue,
                    submission_system=self.submission_system,
                    collection_handle=item_submission.collection_handle,
                )

                # Record details of the item submission message
                item_data = {
                    "item_identifier": item_submission.item_identifier,
                    "message_id": response["MessageId"],
                }
                items.append(item_data)
                self.submission_summary["submitted"] += 1

                logger.info(f"Sent item submission message: {item_data['message_id']}")

                # Set status in DynamoDB
                item_submission.status = ItemSubmissionStatus.SUBMIT_SUCCESS
                item_submission.status_details = None
                item_submission.submit_attempts += 1
                item_submission.upsert_db()
                self._publish_count_metric(
                    "item_submitted", f"item {item_submission.item_identifier}"
                )

            except Exception as exception:  # noqa: BLE001
                self.submission_summary["errors"] += 1
                item_submission.status = ItemSubmissionStatus.SUBMIT_FAILED
                item_submission.status_details = str(exception)
                item_submission.submit_attempts += 1
                item_submission.upsert_db()
                self._publish_count_metric(
                    "submission_error", f"item {item_submission.item_identifier}"
                )

        logger.info(
            f"Submitted messages to the DSS input queue '{CONFIG.sqs_queue_dss_input}' "
            f"for batch '{self.batch_id}': {json.dumps(self.submission_summary)}"
        )

        return items

    def _load_batch_manifest(self) -> dict:
        """Create a manifest for a batch of item submissions.

        This method "walks" the contents of the batch in S3, examining contents
        of the new- and replacement-theses subfolders. The method returns
        a dictionary where the keys = item identifiers and the value
        is a dict with important meta about the item submission: thesis type and
        the S3 URIs for the metadata file and the associated bitstream.
        """
        manifest: defaultdict = defaultdict(dict)

        s3_client = S3Client()

        for file in s3_client.files_iter(
            bucket=self.s3_bucket, prefix=f"{self.batch_path}"
        ):
            # in S3, the slash in the item identifier (the DOI) is replaced with a dash
            item_identifier = file.rsplit("/", maxsplit=2)[1].replace("-", "/")

            # add item submission ID to manifest
            if item_identifier not in manifest:
                manifest[item_identifier]

            if file.endswith(".json"):
                manifest[item_identifier]["metadata_file"] = file
            if file.endswith(".pdf"):
                manifest[item_identifier].setdefault("bitstream_files", []).append(file)

        return manifest

    def _get_transformed_metadata(self, source_metadata_file: str) -> dict:
        """Get transformed metadata for an item submission.

        This method expects a filepath to an Alma MARC XML file.
        The contents of the XML file are passed to DigitizedTheses.metadata_transformer
        as bytes. The transformer returns a dictionary with key-value
        pairs of Qualified Dublin Core (QDC) metadata, where the value is a
        list of values for each field entry.

        The method returns a dictionary with the QDC metadata, and
        additional entries for the dc.description.provenance
        and dspace.imported metadata fields.
        """
        with smart_open.open(source_metadata_file, "rb") as file:
            source_metadata = file.read()

        return self.metadata_transformer.transform(source_metadata)
