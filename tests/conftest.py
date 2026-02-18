import csv
import json
import time
import uuid
import zipfile
from io import StringIO

import boto3
import pytest
from click.testing import CliRunner
from freezegun import freeze_time
from moto import mock_aws
from moto.server import ThreadedMotoServer

from dsc.config import Config
from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.item_submission import ItemSubmission
from dsc.utilities.aws.s3 import S3Client
from dsc.utilities.aws.ses import SESClient
from dsc.utilities.aws.sqs import SQSClient
from dsc.workflows import ArchivesSpace, OpenCourseWare, SimpleCSV, Workflow


# Test Workflow classes ######################
class TestWorkflow(Workflow):

    workflow_name: str = "test"
    submission_system: str = "Test@MIT"

    @property
    def metadata_mapping_path(self) -> str:
        return "tests/fixtures/test_metadata_mapping.json"

    @property
    def output_queue(self) -> str:
        return "mock-output-queue"

    def get_batch_bitstream_uris(self) -> list[str]:
        return [
            "s3://dsc/test/batch-aaa/123_01.pdf",
            "s3://dsc/test/batch-aaa/123_02.pdf",
            "s3://dsc/test/batch-aaa/789_01.pdf",
        ]

    def item_metadata_iter(self):
        yield from [
            {
                "title": "Title",
                "contributor": "Author 1|Author 2",
                "item_identifier": "123",
            },
            {
                "title": "2nd Title",
                "contributor": "Author 3|Author 4",
                "item_identifier": "789",
            },
        ]

    def _prepare_batch(self, *, synced: bool = False):  # noqa: ARG002
        return (
            [
                ItemSubmission(
                    batch_id="batch-aaa",
                    item_identifier="123",
                    workflow_name="test",
                ),
                ItemSubmission(
                    batch_id="batch-aaa",
                    item_identifier="789",
                    workflow_name="test",
                ),
            ],
            [],
        )


class TestOpenCourseWare(OpenCourseWare):

    @property
    def output_queue(self) -> str:
        return "mock-output-queue"


class TestSimpleCSV(SimpleCSV):

    workflow_name = "simple-csv"
    submission_system: str = "Test@MIT"

    @property
    def metadata_mapping_path(self) -> str:
        return "tests/fixtures/test_metadata_mapping.json"

    @property
    def item_identifier_column_names(self) -> list[str]:
        return ["item_identifier", "filename"]

    @property
    def output_queue(self) -> str:
        return "mock-output-queue"


# Test Workflow instances ####################
@pytest.fixture
@freeze_time("2025-01-01 09:00:00")
def base_workflow_instance(item_metadata, metadata_mapping, mocked_s3):
    return TestWorkflow(batch_id="batch-aaa")


@pytest.fixture
def simple_csv_workflow_instance(metadata_mapping):
    return TestSimpleCSV(batch_id="batch-aaa")


@pytest.fixture
@freeze_time("2025-01-01 09:00:00")
def archivesspace_workflow_instance(tmp_path):
    return ArchivesSpace(batch_id="batch-aaa")


@pytest.fixture
def opencourseware_workflow_instance():
    return TestOpenCourseWare(batch_id="batch-aaa")


# Test fixtures ##############################
@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "None")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("WARNING_ONLY_LOGGERS", "botocore,smart_open,urllib3")
    monkeypatch.setenv("AWS_REGION_NAME", "us-east-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("ITEM_SUBMISSIONS_TABLE_NAME", "dsc-test")
    monkeypatch.setenv("RETRY_THRESHOLD", "20")
    monkeypatch.setenv("S3_BUCKET_SUBMISSION_ASSETS", "dsc")
    monkeypatch.setenv("SOURCE_EMAIL", "noreply@example.com")
    monkeypatch.setenv("SQS_QUEUE_DSS_INPUT", "mock-input-queue")


@pytest.fixture
def moto_server():
    """Fixture to run a mocked AWS server for testing."""
    # Note: pass `port=0` to get a random free port.
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()
    time.sleep(0.5)


@pytest.fixture
def config_instance():
    return Config()


@pytest.fixture
def dspace_metadata():
    return {
        "metadata": [
            {
                "key": "dc.title",
                "language": "en_US",
                "value": "Title",
            },
            {
                "key": "dc.contributor",
                "language": None,
                "value": "Author 1",
            },
            {
                "key": "dc.contributor",
                "language": None,
                "value": "Author 2",
            },
        ]
    }


@pytest.fixture
def item_metadata():
    return {
        "title": "Title",
        "contributor": "Author 1|Author 2",
        "item_identifier": "123",
    }


@pytest.fixture
def item_submission_instance(dspace_metadata):
    return ItemSubmission(
        batch_id="batch-aaa",
        item_identifier="123",
        workflow_name="test",
        dspace_metadata=dspace_metadata,
        bitstream_s3_uris=[
            "s3://dsc/workflow/folder/123_01.pdf",
            "s3://dsc/workflow/folder/123_02.pdf",
        ],
    )


@pytest.fixture
def metadata_mapping():
    with open("tests/fixtures/test_metadata_mapping.json") as mapping_file:
        return json.load(mapping_file)


@pytest.fixture
def mocked_item_submission_db(config_instance):
    with mock_aws():
        if not ItemSubmissionDB.exists():
            ItemSubmissionDB.set_table_name(config_instance.item_submissions_table_name)
            ItemSubmissionDB.create_table()
        yield


@pytest.fixture
def mock_item_submission_db_with_records(mocked_item_submission_db):
    # create two records for 'batch-aaa'
    ItemSubmissionDB(
        batch_id="aaa",
        item_identifier="123",
        workflow_name="test",
        status=ItemSubmissionStatus.BATCH_CREATED,
    ).create()
    ItemSubmissionDB(
        batch_id="aaa",
        item_identifier="456",
        workflow_name="test",
        status=ItemSubmissionStatus.BATCH_CREATED,
    ).create()


@pytest.fixture
def mocked_s3(config_instance):
    with mock_aws():
        s3 = boto3.client("s3", region_name=config_instance.aws_region_name)
        s3.create_bucket(Bucket="dsc")
        yield s3


@pytest.fixture
def mocked_s3_simple_csv(mocked_s3, item_metadata):
    # write in-memory metadata CSV file
    csv_buffer = StringIO()
    fieldnames = item_metadata.keys()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows([item_metadata])

    # seek to the beginning of the in-memory file before uploading
    csv_buffer.seek(0)

    mocked_s3.put_object(
        Bucket="dsc",
        Key="simple-csv/batch-aaa/metadata.csv",
        Body=csv_buffer.getvalue(),
    )
    return mocked_s3


@pytest.fixture
def mocked_ses(config_instance):
    with mock_aws():
        ses = boto3.client("ses", region_name=config_instance.aws_region_name)
        ses.verify_email_identity(EmailAddress="noreply@example.com")
        yield ses


@pytest.fixture
def mocked_sqs_input(config_instance):
    with mock_aws():
        sqs = boto3.client("sqs", region_name=config_instance.aws_region_name)
        sqs.create_queue(QueueName="mock-input-queue")
        yield sqs


@pytest.fixture
def mocked_sqs_output():
    with mock_aws():
        sqs = boto3.client("sqs", region_name="us-east-1")
        sqs.create_queue(QueueName="mock-output-queue")
        yield sqs


@pytest.fixture
def opencourseware_source_metadata():
    with (
        zipfile.ZipFile("tests/fixtures/opencourseware/123.zip", "r") as zip_file,
        zip_file.open("data.json") as file,
    ):
        return json.load(file)


@pytest.fixture
def result_message_attributes():
    return {
        "PackageID": {"DataType": "String", "StringValue": "10.1002/term.3131"},
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
    }


@pytest.fixture
def result_message_body_success():
    return json.dumps(
        {
            "ResultType": "success",
            "ItemHandle": "1721.1/131022",
            "lastModified": "Thu Sep 09 17:56:39 UTC 2021",
            "Bitstreams": [
                {
                    "BitstreamName": "10.1002-term.3131.pdf",
                    "BitstreamUUID": "a1b2c3d4e5",
                    "BitstreamChecksum": {
                        "value": "a4e0f4930dfaff904fa3c6c85b0b8ecc",
                        "checkSumAlgorithm": "MD5",
                    },
                }
            ],
        }
    )


@pytest.fixture
def result_message_body_error():
    return json.dumps(
        {
            "DSpaceResponse": "none",
            "ErrorInfo": "Failure during ingest",
            "ErrorTimestamp": "Thu Sep 09 17:56:39 UTC 2021",
            "ExceptionTraceback": [
                "Traceback...",
            ],
            "ItemHandle": None,
            "ResultType": "error",
        }
    )


@pytest.fixture
def result_message_valid(result_message_attributes, result_message_body_success):
    return {
        "ReceiptHandle": "lvpqxcxlmyaowrhbvxadosldaghhidsdralddmejhdrnrfeyfuphzs",
        "Body": result_message_body_success,
        "MessageAttributes": result_message_attributes,
        "MessageId": uuid.uuid4(),
    }


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def s3_client():
    return S3Client()


@pytest.fixture
def ses_client(config_instance):
    return SESClient(region=config_instance.aws_region_name)


@pytest.fixture
def sqs_client(config_instance):
    return SQSClient(
        region=config_instance.aws_region_name,
        queue_name="mock-output-queue",
    )


@pytest.fixture
def submission_message_attributes():
    return {
        "PackageID": {"DataType": "String", "StringValue": "123"},
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
        "OutputQueue": {"DataType": "String", "StringValue": "DSS queue"},
    }


@pytest.fixture
def submission_message_body():
    return json.dumps(
        {
            "SubmissionSystem": "DSpace@MIT",
            "CollectionHandle": "123.4/5678",
            "MetadataLocation": "s3://dsc/10.1002-term.3131.json",
            "Files": [
                {
                    "BitstreamName": "10.1002-term.3131.pdf",
                    "FileLocation": "s3://dsc/10.1002-term.3131.pdf",
                    "BitstreamDescription": None,
                }
            ],
        }
    )
