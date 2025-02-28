import csv
import json
import uuid
from io import StringIO

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_aws

from dsc.config import Config
from dsc.item_submission import ItemSubmission
from dsc.utilities.aws.s3 import S3Client
from dsc.utilities.aws.ses import SESClient
from dsc.utilities.aws.sqs import SQSClient
from dsc.workflows import OpenCourseWare
from dsc.workflows.base import Workflow, WorkflowEvents
from dsc.workflows.base.simple_csv import SimpleCSV


class TestWorkflow(Workflow):

    workflow_name: str = "test"
    submission_system: str = "Test@MIT"

    @property
    def metadata_mapping_path(self) -> str:
        return "tests/fixtures/test_metadata_mapping.json"

    @property
    def s3_bucket(self) -> str:
        return "dsc"

    @property
    def output_queue(self) -> str:
        return "mock-output-queue"

    def reconcile_bitstreams_and_metadata(self):
        raise TypeError(
            f"Method '{self.reconcile_bitstreams_and_metadata.__name__}' "
            f"not used by workflow '{self.__class__.__name__}'"
        )

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

    def get_item_identifier(self, item_metadata):
        return item_metadata["item_identifier"]

    def get_bitstream_s3_uris(self, item_identifier):
        bitstreams = [
            "s3://dsc/test/batch-aaa/123_01.pdf",
            "s3://dsc/test/batch-aaa/123_02.pdf",
            "s3://dsc/test/batch-aaa/456_01.pdf",
        ]
        return [bitstream for bitstream in bitstreams if item_identifier in bitstream]


class TestSimpleCSV(SimpleCSV):

    workflow_name = "simple_csv"
    submission_system: str = "Test@MIT"

    @property
    def metadata_mapping_path(self) -> str:
        return "tests/fixtures/test_metadata_mapping.json"

    @property
    def s3_bucket(self) -> str:
        return "dsc"

    @property
    def output_queue(self) -> str:
        return "mock-output-queue"


class TestOpenCourseWare(OpenCourseWare):

    @property
    def s3_bucket(self) -> str:
        return "dsc"

    @property
    def output_queue(self) -> str:
        return "mock-output-queue"


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "None")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("AWS_REGION_NAME", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("DSS_INPUT_QUEUE", "mock-input-queue")
    monkeypatch.setenv("DSC_SOURCE_EMAIL", "noreply@example.com")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)


@pytest.fixture
def base_workflow_instance(item_metadata, metadata_mapping, mocked_s3):
    return TestWorkflow(batch_id="batch-aaa")


@pytest.fixture
def simple_csv_workflow_instance(metadata_mapping):
    return TestSimpleCSV(batch_id="batch-aaa")


@pytest.fixture
def opencourseware_workflow_instance():
    return TestOpenCourseWare(batch_id="batch-aaa")


@pytest.fixture
def opencourseware_source_metadata():
    return {
        "course_title": "Matrix Calculus for Machine Learning and Beyond",
        "course_description": "We all know that calculus courses.",
        "site_uid": "2318fd9f-1b5c-4a48-8a04-9c56d902a1f8",
        "instructors": [
            {
                "first_name": "Alan",
                "last_name": "Edelman",
                "middle_initial": "",
                "salutation": "Prof.",
                "title": "Prof. Alan Edelman",
            },
            {
                "first_name": "Steven",
                "last_name": "Johnson",
                "middle_initial": "G.",
                "salutation": "Prof.",
                "title": "Prof. Steven G. Johnson",
            },
        ],
    }


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
        dspace_metadata=dspace_metadata,
        bitstream_s3_uris=[
            "s3://dsc/workflow/folder/123_01.pdf",
            "s3://dsc/workflow/folder/123_02.pdf",
        ],
        item_identifier="123",
    )


@pytest.fixture
def metadata_mapping():
    with open("tests/fixtures/test_metadata_mapping.json") as mapping_file:
        return json.load(mapping_file)


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
        Key="simple_csv/batch-aaa/metadata.csv",
        Body=csv_buffer.getvalue(),
    )


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
def result_message_attributes():
    return {
        "PackageID": {"DataType": "String", "StringValue": "10.1002/term.3131"},
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
    }


@pytest.fixture
def result_message_body():
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
def result_message_valid(result_message_attributes, result_message_body):
    return {
        "ReceiptHandle": "lvpqxcxlmyaowrhbvxadosldaghhidsdralddmejhdrnrfeyfuphzs",
        "Body": result_message_body,
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


@pytest.fixture
def workflow_events_reconcile():
    return WorkflowEvents(
        reconciled_items={
            "123": ["123.pdf", "123.tiff"],
        },
        reconcile_errors={
            "bitstreams_without_metadata": ["124.pdf"],
            "metadata_without_bitstreams": ["125"],
        },
    )


@pytest.fixture
def workflow_events_finalize(result_message_body):
    return WorkflowEvents(
        processed_items=[
            {
                "item_identifier": "123",
                "result_message_body": json.loads(result_message_body),
                "ingested": "success",
            }
        ],
        errors=["Failed to retrieve 'ReceiptHandle' from message: abc"],
    )
