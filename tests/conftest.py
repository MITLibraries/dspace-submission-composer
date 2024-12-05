from io import StringIO

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_aws

from dsc.config import Config
from dsc.s3 import S3Client


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "None")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("AWS_REGION_NAME", "us-east-1")


@pytest.fixture
def config_instance() -> Config:
    return Config()


@pytest.fixture
def mocked_s3(config_instance):
    with mock_aws():
        s3 = boto3.client("s3", region_name=config_instance.AWS_REGION_NAME)
        s3.create_bucket(Bucket="awd")
        yield s3


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def s3_client():
    return S3Client()


@pytest.fixture
def stream():
    return StringIO()
