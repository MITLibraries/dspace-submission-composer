import json
import re
from http import HTTPStatus

import pytest
from botocore.exceptions import ClientError


def test_s3_client_archive_file_with_new_key_success(mocked_s3, s3_client):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="test.csv",
    )
    s3_client.archive_file_with_new_key(
        bucket="dsc",
        key="test.csv",
        archived_key_prefix="archived",
    )
    with pytest.raises(
        ClientError,
        match=re.escape(
            "An error occurred (NoSuchKey) when calling the GetObject operation: "
            "The specified key does not exist."
        ),
    ):
        s3_client.client.get_object(Bucket="dsc", Key="test.csv")
    response = s3_client.client.get_object(Bucket="dsc", Key="archived/test.csv")

    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_s3_client_put_file_success(mocked_s3, s3_client):
    assert "Contents" not in s3_client.client.list_objects(Bucket="dsc")

    s3_client.put_file(
        file_content=json.dumps({"metadata": {"key": "dc.title", "value": "A Title"}}),
        bucket="dsc",
        key="test.json",
    )

    assert len(s3_client.client.list_objects(Bucket="dsc")["Contents"]) == 1
    assert (
        s3_client.client.list_objects(Bucket="dsc")["Contents"][0]["Key"] == "test.json"
    )


def test_s3_client_files_iter_success(mocked_s3, s3_client):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="workflow/batch-aaa/metadata.csv",
    )

    assert list(s3_client.files_iter(bucket="dsc")) == [
        "s3://dsc/workflow/batch-aaa/metadata.csv"
    ]


def test_s3_client_files_iter_with_prefix_success(mocked_s3, s3_client):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="workflow/batch-aaa/metadata.csv",
    )

    assert list(s3_client.files_iter(bucket="dsc", prefix="workflow/batch-aaa")) == [
        "s3://dsc/workflow/batch-aaa/metadata.csv"
    ]


def test_s3_client_files_iter_with_item_identifier_success(mocked_s3, s3_client):
    s3_client.put_file(file_content="", bucket="dsc", key="workflow/batch-aaa/123.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="workflow/batch-aaa/124.pdf")

    assert list(
        s3_client.files_iter(
            bucket="dsc", prefix="workflow/batch-aaa/", item_identifier="123"
        )
    ) == ["s3://dsc/workflow/batch-aaa/123.pdf"]


def test_s3_client_files_iter_with_file_type_success(mocked_s3, s3_client):
    s3_client.put_file(file_content="", bucket="dsc", key="workflow/batch-aaa/123.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="workflow/batch-aaa/123.tiff")
    s3_client.put_file(file_content="", bucket="dsc", key="workflow/batch-aaa/124.pdf")

    assert list(
        s3_client.files_iter(
            bucket="dsc",
            prefix="workflow/batch-aaa/",
            item_identifier="123",
            file_type="pdf",
        )
    ) == ["s3://dsc/workflow/batch-aaa/123.pdf"]


def test_s3_client_files_iter_with_exclude_prefixes_success(mocked_s3, s3_client):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="workflow/batch-aaa/archived/metadata.csv",
    )
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="workflow/batch-aaa/archived/123.pdf",
    )
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="workflow/batch-aaa/456.pdf",
    )
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="workflow/batch-aaa/metadata.csv",
    )

    assert list(
        s3_client.files_iter(
            bucket="dsc",
            prefix="workflow/batch-aaa",
            exclude_prefixes=["archived", "workflow/batch-aaa/metadata.csv"],
        )
    ) == ["s3://dsc/workflow/batch-aaa/456.pdf"]
