import json
from io import StringIO

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_aws

from dsc.config import Config
from dsc.utilities.aws.s3 import S3Client
from dsc.utilities.aws.ses import SESClient
from dsc.utilities.aws.sqs import SQSClient


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "None")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("AWS_REGION_NAME", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")


@pytest.fixture
def config_instance() -> Config:
    return Config()


@pytest.fixture
def mocked_s3(config_instance):
    with mock_aws():
        s3 = boto3.client("s3", region_name=config_instance.AWS_REGION_NAME)
        s3.create_bucket(Bucket="dsc")
        yield s3


@pytest.fixture
def mocked_ses(config_instance):
    with mock_aws():
        ses = boto3.client("ses", region_name=config_instance.AWS_REGION_NAME)
        ses.verify_email_identity(EmailAddress="noreply@example.com")
        yield ses


@pytest.fixture
def mocked_sqs_input(sqs_client, config_instance):
    with mock_aws():
        sqs = boto3.resource("sqs", region_name=config_instance.AWS_REGION_NAME)
        sqs.create_queue(QueueName="mock-input-queue")
        yield sqs


@pytest.fixture
def mocked_sqs_output():
    with mock_aws():
        sqs = boto3.resource("sqs", region_name="us-east-1")
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
def runner():
    return CliRunner()


@pytest.fixture
def s3_client():
    return S3Client()


@pytest.fixture
def ses_client(config_instance):
    return SESClient(region=config_instance.AWS_REGION_NAME)


@pytest.fixture
def sqs_client(config_instance):
    return SQSClient(
        region=config_instance.AWS_REGION_NAME,
        queue_name="mock-output-queue",
    )


@pytest.fixture
def stream():
    return StringIO()


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
def result_message_valid(result_message_attributes, result_message_body):
    return {
        "ReceiptHandle": "lvpqxcxlmyaowrhbvxadosldaghhidsdralddmejhdrnrfeyfuphzs",
        "Body": result_message_body,
        "MessageAttributes": result_message_attributes,
    }
