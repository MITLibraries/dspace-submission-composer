from http import HTTPStatus

import pytest
from botocore.exceptions import ClientError


def test_s3_archive_file_in_bucket(mocked_s3, s3_client):
    s3_client.put_file(
        file_content="test1,test2,test3,test4",
        bucket="dsc",
        key="test.csv",
    )
    s3_client.archive_file_with_new_key(
        bucket="dsc",
        key="test.csv",
        archived_key_prefix="archived",
    )
    with pytest.raises(ClientError) as e:
        response = s3_client.client.get_object(Bucket="dsc", Key="test.csv")
    assert (
        "An error occurred (NoSuchKey) when calling the GetObject operation: The"
        " specified key does not exist." in str(e.value)
    )
    response = s3_client.client.get_object(Bucket="dsc", Key="archived/test.csv")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_s3_put_file(mocked_s3, s3_client):
    assert "Contents" not in s3_client.client.list_objects(Bucket="dsc")
    s3_client.put_file(
        file_content=str({"metadata": {"key": "dc.title", "value": "A Title"}}),
        bucket="dsc",
        key="test.json",
    )
    assert len(s3_client.client.list_objects(Bucket="dsc")["Contents"]) == 1
    assert (
        s3_client.client.list_objects(Bucket="dsc")["Contents"][0]["Key"] == "test.json"
    )


def test_s3_get_files_iter_with_matching_csv(mocked_s3, s3_client):
    s3_client.put_file(
        file_content="test1,test2,test3,test4",
        bucket="dsc",
        key="test.csv",
    )
    assert list(
        s3_client.get_files_iter(
            bucket="dsc", file_type="csv", excluded_key_prefix="archived"
        )
    ) == ["test.csv"]


def test_s3_get_files_iter_without_matching_csv(mocked_s3, s3_client):
    s3_client.put_file(
        file_content="test1,test2,test3,test4",
        bucket="dsc",
        key="archived/test.csv",
    )
    assert (
        list(
            s3_client.get_files_iter(
                bucket="dsc", file_type="csv", excluded_key_prefix="archived"
            )
        )
        == []
    )
