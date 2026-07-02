# ruff: noqa: FIX002, TD002, TD003
import glob
import json
import logging
import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Iterator
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import requests
import smart_open
from dspace_rest_client.client import DSpaceClient
from dspace_rest_client.models import Item as DSpaceItem
from lxml import etree

from dsc import exceptions
from dsc.config import Config
from dsc.db.models import ItemSubmissionStatus
from dsc.item_submission import ItemSubmission
from dsc.reports import CreateReport, DigitizedThesesFinalizeReport, Report, SubmitReport
from dsc.utils.aws.s3 import S3Client, run_aws_cli_sync
from dsc.workflows.base import Workflow
from dsc.workflows.digitized_theses import NSMAP, DigitizedThesesTransformer

CONFIG = Config()
logger = logging.getLogger(__name__)

MIT_THESES_COLLECTION_HANDLES = {
    "Bachelor": {"dev": "1721.1/131024", "prod": "1721.1/131024"},
    "Engineer": {"dev": "1721.1/131023", "prod": "1721.1/131023"},
    "Master": {"dev": "1721.1/131023", "prod": "1721.1/131023"},
    "Doctoral": {"dev": "1721.1/131022", "prod": "1721.1/131022"},
}


class MITThesesCommunityUUID(StrEnum):
    DEV = "6fc02cc2-0d14-4023-8a6f-d9900d0c4302"
    PROD = "6fc02cc2-0d14-4023-8a6f-d9900d0c4302"


class DigitizedTheses(Workflow):
    """Workflow for digitized theses from the Imaging Lab.

    This workflow is unique to other workflows in that it relies on an
    additional S3 bucket that serves as a "workspace" for digitized theses.
    Users create a batch folder in this workspace S3 bucket and upload
    theses (PDF files) named with the OCLC number: <oclc-number>.pdf.

    When this workflow creates a batch *without* syncing, it will first
    copy the contents of the original batch folder in the workspace S3 bucket
    to a dated batch folder in a temporary directory on local disk.
    The batch folder will organize item submission assets into subfolders
    based on whether it is a new or replacement thesis or whether it should
    be skipped. Once the contents of the local batch are ready, it is
    synced to a similarly named batch folder in the DSC S3 bucket and
    ItemSubmission's are recorded in DynamoDB.

    When this workflow creates a batch *with* syncing, this means an organized
    batch folder already exists in the DSC S3 bucket in Prod, which is the
    result of syncing data from Stage. This method will instead walk
    the contents of the synced batch folder to create ItemSubmission's
    that are recorded in DynamoDB.

    This workflow provides its own implementation for queueing items for
    ingest, sending submission messages to DSS based on the "thesis type"
    (i.e., whether the item submission is a 'New thesis' or 'Replacement thesis').
    The submission message informs DSS on whether to create or update
    an item.

    TODO: Add description `finalize` method.
    """

    workflow_name: str = "digitized-theses"
    metadata_transformer = DigitizedThesesTransformer
    reporting_modules: ClassVar[dict[str, type[Report]]] = {
        "create": CreateReport,
        "submit": SubmitReport,
        "finalize": DigitizedThesesFinalizeReport,
    }

    def __init__(self, batch_id: str):
        self._dspace_client = None
        self.community_uuid = (
            MITThesesCommunityUUID.PROD
            if CONFIG.workspace == "prod"
            else MITThesesCommunityUUID.DEV
        )
        super().__init__(batch_id)

    @property
    def metadata_mapping_path(self) -> str:
        raise NotImplementedError

    @property
    def dspace_client(self) -> DSpaceClient:
        if not self._dspace_client:
            logger.debug(
                f"Creating DSpace client for destination {self.submission_system}"
            )

            try:
                credentials = CONFIG.dspace_credentials[self.submission_system]
            except KeyError as exception:
                raise exceptions.DSpaceClientCredentialsNotFoundError(
                    f"No credentials for {self.submission_system}"
                ) from exception

            client = DSpaceClient(
                api_endpoint=credentials["url"],
                username=credentials["user"],
                password=credentials["password"],
                fake_user_agent=True,
            )
            authenticated = client.authenticate()
            if not authenticated:
                raise exceptions.DSpaceClientAuthenticationError(
                    credentials["url"], credentials["user"]
                )
            self._dspace_client = client
            logger.info(
                f"Successfully authenticated to {credentials['url']} "
                f"as {credentials['url']}"
            )

        return self._dspace_client

    def get_batch_bitstream_uris(self) -> list[str]:
        raise NotImplementedError

    def item_metadata_iter(self) -> Iterator[dict[str, Any]]:
        raise NotImplementedError

    def prepare_batch(self, *, synced: bool = False) -> tuple[list, ...]:
        """Prepare a batch folder in the DSC S3 bucket.

        Unlike other workflows, successes, failures, and skips are recorded in the
        item submissions table in DynamoDB, as a means to collate all results for a batch
        in the batch creation report. This means, the method will always return an empty
        'errors' list.
        """
        item_submissions = []
        errors: list[tuple] = []  # set but not used

        if synced:
            logger.info(f"Batch folder already created in WORKSPACE={CONFIG.workspace}")
            item_submissions = self._get_item_submissions_from_synced_batch()
        else:
            item_submissions = self._create_batch_in_s3()

        return item_submissions, errors

    def _get_item_submissions_from_synced_batch(self) -> list[ItemSubmission]:
        """Create ItemSubmission's from a synced batch folder in the DSC S3 bucket.

        This method loops through the uris for objects in the synced batch folder.
        From each uri, the method extracts the theses subfolder and the item identifier
        for an item submission. The theses subfolder determines the ItemSubmission.status
        to set in DynamoDB:
            - Item submissions in replacement-theses or new-theses -> CREATE_SUCCESS
            - Item submissions in skipped-theses -> CREATE_SKIPPED

        As the method loops through the uris, it checks against a list of seen
        item identifiers to avoid duplicates.
        """
        item_submissions = []
        s3_client = S3Client()

        # pattern to extract meta about item submission from uri
        pattern = r"/(?:[^/]+/)*?([a-zA-Z0-9-]*-theses)/(\d+)(?:/.*)?$"

        seen_item_identifiers = []
        for uri in s3_client.files_iter(bucket=self.s3_bucket, prefix=self.batch_path):
            match = re.search(pattern, uri)
            if not match:
                logger.warning(f"Cannot create item submission for uri: {uri}")
                continue

            # retrieve theses subfolder and item identifier from uri
            theses_subfolder, item_identifier = match.groups()
            if item_identifier in seen_item_identifiers:
                logger.debug(
                    f"Already created an item submission for item_identifier={item_identifier}"  # noqa: E501
                )
                continue

            # track identifier as 'seen'
            seen_item_identifiers.append(item_identifier)

            # create an instance of ItemSubmission
            item_submission = ItemSubmission(
                batch_id=self.batch_id,
                item_identifier=item_identifier,
                workflow_name=self.workflow_name,
            )

            if theses_subfolder == "replacement-theses":
                try:
                    dspace_item = self._get_item_from_dspace(
                        item_submission.item_identifier
                    )
                    item_submission.dspace_handle = dspace_item.handle
                    item_submission.operation = "update"
                    item_submission.status = ItemSubmissionStatus.CREATE_SUCCESS
                    item_submission.status_details = "Replacement thesis"
                except exceptions.DSpaceClientSearchError as exception:
                    item_submission.status = ItemSubmissionStatus.CREATE_SKIPPED
                    item_submission.status_details = str(exception)
            elif theses_subfolder == "new-theses":
                item_submission.operation = "create"
                item_submission.status = ItemSubmissionStatus.CREATE_SUCCESS
                item_submission.status_details = "New thesis"
            else:
                item_submission.status = ItemSubmissionStatus.CREATE_SKIPPED
                item_submission.status_details = "Skipped thesis"
            item_submissions.append(item_submission)

        return item_submissions

    def _create_batch_in_s3(self) -> list[ItemSubmission]:
        """Create a batch of item submissions in the DSC S3 bucket.

        This method begins with syncing the contents of the original batch folder
        in the digitized-theses-workspace S3 bucket to a dated batch folder in
        a temporary directory on local disk. From there, it performs the following
        steps for each thesis:

        1. Download the MARC XML record from Alma and save to an XML file
        2. Check if an item in DSpace already exists:
            - If it exists and it is a student-submitted electronic thesis, skip
            - If it exists and is not, mark as 'Replacement thesis'
            - If it does not exist, mark as 'New thesis'
        3. Organize the contents of the dated batch folder by thesis type
        4. Sync the dated batch folder to the workflow folder in the DSC S3 bucket
        5. Clean up the temporary directory
        """
        item_submissions = []

        # update self.batch_id to include current date timestamp
        original_batch_id = self.batch_id
        self.batch_id = self._update_batch_id(original_batch_id)

        # create temp directory
        tmp_dir = tempfile.TemporaryDirectory()
        logger.info(f"Created temporary directory: {tmp_dir.name}")

        # sync batch folder from digitized-theses-workspace S3 bucket
        # to dated batch folder in temp directory
        tmp_batch_path = f"{tmp_dir.name}/{self.batch_id}"
        run_aws_cli_sync(
            source=f"s3://{CONFIG.s3_bucket_digitized_theses}/{original_batch_id}",
            destination=tmp_batch_path,
        )

        for file in os.listdir(tmp_batch_path):
            item_submission = ItemSubmission(
                batch_id=self.batch_id,
                item_identifier=self.parse_item_identifier(file),
                workflow_name=self.workflow_name,
            )

            # get MARC XML metadata from Alma
            try:
                self._download_metadata_from_alma(
                    item_submission, batch_location=tmp_batch_path
                )
            except (
                requests.exceptions.HTTPError,
                exceptions.ItemMetadataNotFoundError,
            ) as exception:
                item_submission.status = ItemSubmissionStatus.CREATE_FAILED
                item_submission.status_details = str(exception)
                item_submissions.append(item_submission)
                continue

            # check if item with the OCLC number exists
            try:
                dspace_item = self._get_item_from_dspace(item_submission.item_identifier)
            except exceptions.DSpaceClientSearchError as exception:
                item_submission.status = ItemSubmissionStatus.CREATE_FAILED
                item_submission.status_details = str(exception)
                item_submissions.append(item_submission)
                continue

            # check if item submission is a 'Replacement thesis'
            if dspace_item and not self._is_replacement_thesis(dspace_item):
                item_submission.dspace_handle = dspace_item.handle
                item_submission.status = ItemSubmissionStatus.CREATE_SKIPPED
                item_submission.status_details = "Cannot replace the electronic version submitted by the student author."  # noqa: E501
                item_submissions.append(item_submission)
                continue

            if dspace_item and self._is_replacement_thesis(dspace_item):
                item_submission.dspace_handle = dspace_item.handle
                item_submission.operation = "update"
                item_submission.status = ItemSubmissionStatus.CREATE_SUCCESS
                item_submission.status_details = "Replacement thesis"
            else:
                item_submission.operation = "create"
                item_submission.status = ItemSubmissionStatus.CREATE_SUCCESS
                item_submission.status_details = "New thesis"
            item_submissions.append(item_submission)

        self._move_batch_files_to_theses_subfolders(
            item_submissions, batch_location=tmp_batch_path
        )

        # sync batch folder from digitized-theses-workspace S3 bucket
        # to dated batch folder in temporary directory
        run_aws_cli_sync(
            source=tmp_batch_path,
            destination=f"s3://{CONFIG.s3_bucket_submission_assets}/{self.batch_path}",
        )

        # clean up temp directory
        tmp_dir.cleanup()

        return item_submissions

    def _update_batch_id(self, batch_id: str) -> str:
        """Create a new batch ID with a date timestamp.

        This method is only used when creating a batch *without syncing*.
        The updated batch ID is used to distinguish different runs of
        Workflow.create_batch, which can be run as many times as needed
        until a batch is ready for submission.
        """
        return f"{batch_id}-{self.run_date.strftime('%Y%m%dT%H%M%SZ')}"

    def _download_metadata_from_alma(
        self, item_submission: ItemSubmission, batch_location: str
    ) -> bytes:
        """Download MARC XML metadata for an item submission from Alma.

        This method writes an XML file with MARC metadata to the
        batch folder in S3 named: '<oclc-number>-MIT.xml'

        For more information, see:
        https://developers.exlibrisgroup.com/alma/integrations/sru/.
        """
        logger.debug(
            f"Retrieving metadata from Alma for an item with alma.oclc_control_number_035_a={item_submission.item_identifier}"  # noqa: E501
        )

        query_url = f"https://{CONFIG.metadata_api_url}"
        response = requests.get(
            query_url,
            params={
                "operation": "searchRetrieve",
                "recordSchema": "marcxml",
                "query": (
                    f"alma.oclc_control_number_035_a={item_submission.item_identifier}"
                ),
            },
            timeout=180,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as exception:
            # TODO: Custom error includes HTTPError message
            raise exceptions.ItemMetadataNotFoundError from exception

        # get nested `<marc:record>` element from SRU response
        record_element = self._parse_record_from_sru_response(response.content)

        # write an XML file to batch folder
        with open(
            Path(batch_location) / f"{item_submission.item_identifier}.xml", "wb"
        ) as file:
            tree = etree.ElementTree(record_element)
            tree.write(file, xml_declaration=True, encoding="UTF-8")

        return response.content

    def _get_item_from_dspace(self, item_identifier: str) -> DSpaceItem:
        """Get Item object from DSpace given an item identifier.

        This method expects only a single Item object for a given item identifier
        (if any). If DSpaceClient.search_objects returns None or a list of more than
        one Item objects, this method raises exceptions.DSpaceClientSearchError.
        """
        logger.debug(
            f"Searching DSpace for an item with dc.identifier.oclc={item_identifier}"
        )

        dspace_objects = self.dspace_client.search_objects(
            query=f"dc.identifier.oclc:{item_identifier}",
            scope=self.community_uuid,
            dso_type="item",
        )

        if dspace_objects is None:
            raise exceptions.DSpaceClientSearchError(
                f"Failed search for item with dc.identifier.oclc={item_identifier} "
                f"in community {'<community>'}"
            )
        if len(dspace_objects) > 1:
            raise exceptions.DSpaceClientSearchError(
                f"Expecting one item with dc.identifier.oclc={item_identifier} "
                f"in community {'<community>'}; found {len(dspace_objects)} items"
            )
        if len(dspace_objects) == 0:
            return None

        return dspace_objects[0]

    def _move_batch_files_to_theses_subfolders(
        self, item_submissions: list[ItemSubmission], batch_location: str
    ) -> None:
        """Move item submission assets in batch folder into theses subfolders.

        This method will inspect the `status_details` for each item submission to
        determine which theses subfolder (prefix) its content should be moved to.
        At this point, `status_details` will contain "Replacement thesis",
        "New thesis", or an exception message. If `status_details` contains an
        exception message, contents are moved to a skipped-theses/ subfolder.

        When contents for an item submission are moved to theses subfolders,
        they are nested under a prefix named with the item identifier:

            batch-0/replacement-theses/001/001.xml <-- Alma MARC metadata
            batch-0/replacement-theses/001/001.pdf <-- digitized theses
        """
        for item_submission in item_submissions:
            if item_submission.status_details == "Replacement thesis":
                theses_prefix = "replacement-theses"
            elif item_submission.status_details == "New thesis":
                theses_prefix = "new-theses"
            else:
                theses_prefix = "skipped-theses"

            # create item submission prefix under theses prefix
            item_submission_location = (
                Path(batch_location) / theses_prefix / item_submission.item_identifier
            )
            if not os.path.exists(item_submission_location):
                os.makedirs(item_submission_location)

            logger.debug(
                "Moving files associated with item_identifier="
                f"{item_submission.item_identifier} to '{item_submission_location}'"
            )

            # move item submission assets to their assigned subfolder
            for file in glob.glob(f"{batch_location}/{item_submission.item_identifier}*"):
                filename = file.rsplit("/", maxsplit=1)[-1]
                shutil.move(file, f"{item_submission_location}/{filename}")

    @staticmethod
    def _is_replacement_thesis(item: DSpaceItem) -> bool:
        """Determine if a DSpace item is a replacement thesis.

        If an item is already present in DSpace, its thesis file (bitstream)
        should be replaced only when it is not a student-submitted electronic PDF.
        This determination is made by checking for a specific target string in
        the dc.description field.

        This method returns True if the target string is absent
        from all dc.description entries (if any exist), indicating that it is
        safe to proceed with the replacement.
        """
        # skip the record if any dc_description value matches this string
        target_string = "This electronic version was submitted by the student author."

        if item.metadata.get("dc.description"):
            dc_description_values = [
                entry.get("value", "") for entry in item.metadata["dc.description"]
            ]

            if target_string in "|".join(dc_description_values):
                return False

        return True

    def submit_items(self, collection_handle: str | None = None) -> list:
        """Submit items to the DSpace Submission Service according to the workflow class.

        This method begins by creating a manifest for the batch of item submissions. The
        purpose of this step is to "walk" the contents of the batch in S3 in one go
        instead  of retrieving assets from S3 per-item. The method retrieves batch
        item submissions from DynamoDB and performs the following steps for each:

        1. Check if the item submission is "ready to submit"
        2. Transform the item metadata to Qualified Dublin Core (QDC)
        3. Get the item's collection handle based on the mit.thesis.degree field
        4. Send a submission message based on the thesis type ("New thesis" vs.
           "Replacement thesis")
            - New theses must provide the "CollectionHandle"
            - Replacement theses must provide the "ItemHandle"
              and set "Operation" to "update"
        """
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
                    item_identifier=item_submission.item_identifier,
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
                item_submission.bitstream_s3_uris = manifest[
                    item_submission.item_identifier
                ]["bitstream_files"]

                # send submission message based on thesis type
                if item_submission.operation == "update":
                    response = item_submission.send_submission_message(
                        submission_source=self.workflow_name,
                        output_queue=self.output_queue,
                        submission_system=self.submission_system,
                        operation=item_submission.operation,
                        item_handle=item_submission.dspace_handle,
                    )
                else:
                    # get collection handle based on mit.thesis.degree
                    item_submission.collection_handle = (
                        collection_handle
                        or self._get_item_collection_handle(item_metadata)
                    )

                    response = item_submission.send_submission_message(
                        submission_source=self.workflow_name,
                        output_queue=self.output_queue,
                        submission_system=self.submission_system,
                        collection_handle=item_submission.collection_handle,
                    )

                # record message id for item submission
                items.append(
                    {
                        "item_identifier": item_submission.item_identifier,
                        "message_id": response["MessageId"],
                    }
                )
                self.submission_summary["submitted"] += 1

                logger.info(f"Sent item submission message: {response['MessageId']}")

                # set status in DynamoDB
                item_submission.status = ItemSubmissionStatus.SUBMIT_SUCCESS
                item_submission.status_details = None
                item_submission.submit_attempts += 1
                item_submission.upsert_db()
            except NotImplementedError:
                raise
            except Exception as exception:  # noqa: BLE001
                self.submission_summary["errors"] += 1
                item_submission.status = ItemSubmissionStatus.SUBMIT_FAILED
                item_submission.status_details = str(exception)
                item_submission.submit_attempts += 1
                item_submission.upsert_db()

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
        type_prefixes = (
            ("New thesis", "new-theses"),
            ("Replacement thesis", "replacement-theses"),
        )
        for thesis_type, thesis_prefix in type_prefixes:
            for file in s3_client.files_iter(
                bucket=self.s3_bucket, prefix=f"{self.batch_path}{thesis_prefix}"
            ):
                item_identifier = file.rsplit("/", maxsplit=2)[1]

                if item_identifier not in manifest:
                    manifest[item_identifier]["thesis_type"] = thesis_type

                if file.endswith(".xml"):
                    manifest[item_identifier]["metadata_file"] = file

                if file.endswith(".pdf"):
                    manifest[item_identifier].setdefault("bitstream_files", []).append(
                        file
                    )

        return manifest

    def _get_transformed_metadata(
        self, item_identifier: str, source_metadata_file: str
    ) -> dict:
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

        transformed_metadata = self.metadata_transformer.transform(source_metadata)

        # set dc.identifier.oclc to item identifier
        transformed_metadata["dc.identifier.oclc"] = item_identifier

        # if replacement thesis, include additional dc.description.provenance entry
        if "replacement-theses" in source_metadata_file:
            replacement_message = f"The thesis import has been updated on {self.run_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"  # noqa: E501
            if transformed_metadata.get("dc.description.provenance"):
                transformed_metadata["dc.description.provenance"].append(
                    replacement_message
                )
            else:
                transformed_metadata["dc.description.provenance"] = [replacement_message]

        # add/update 'dspace.imported' date timestamp
        transformed_metadata["dspace.imported"] = self.run_date.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        return transformed_metadata

    def _get_item_collection_handle(self, item_metadata: dict) -> str:
        """Get collection handle for item based on mit.thesis.degree.

        There are three collection handles for the MIT Theses Community:
            1. Doctoral Theses
            2. Graduate Theses
            3. Undergraduate Theses

        Each item submission belongs to one of the three collection handles based
        on the derived mit.thesis.degree value, where this field is constrained to
        the set: ['Bachelor', 'Engineer', 'Master', 'Doctoral'].

        The global variables MIT_THESES_COLLECTION_HANDLES includes mappings of
        mit.thesis.degree value to collection handles in the 'dev' and 'prod'
        environments.
        """
        mit_thesis_degrees = item_metadata.get("mit.thesis.degree")

        if not mit_thesis_degrees:
            raise TypeError(
                f"Cannot determine collection handle when mit.thesis.degree={mit_thesis_degrees}"  # noqa: E501
            )

        if len(mit_thesis_degrees) > 1:
            logger.warning(
                f"Found multiple values for mit.thesis.degree={mit_thesis_degrees}; "
                "retrieving collection handle for first match"
            )

        for value in mit_thesis_degrees:
            if value in MIT_THESES_COLLECTION_HANDLES:
                if CONFIG.workspace == "prod":
                    return MIT_THESES_COLLECTION_HANDLES[value]["prod"]
                return MIT_THESES_COLLECTION_HANDLES[value]["dev"]

        raise ValueError(
            f"No collection handles found for mit.thesis.degree={mit_thesis_degrees}"
        )

    @staticmethod
    def _parse_record_from_sru_response(content: bytes) -> etree._Element:
        root = etree.fromstring(content)
        number_of_records = root.find("sru:numberOfRecords", namespaces=NSMAP)

        if number_of_records is None:
            # TODO: Custom error includes message "Unexpected response from Alma SRU"
            raise exceptions.ItemMetadataNotFoundError

        if int(number_of_records.text) == 0:
            raise exceptions.ItemMetadataNotFoundError

        if int(number_of_records.text) > 1:
            # TODO: Custom error includes message
            # "Unexpected response from Alma SRU, multiple records with OCLC"
            raise exceptions.ItemMetadataNotFoundError

        return root.xpath("//marc:record", namespaces=NSMAP)[0]

    @staticmethod
    def parse_item_identifier(filename: str) -> str:
        return filename.rsplit("/", maxsplit=1)[-1].removesuffix(".pdf")
