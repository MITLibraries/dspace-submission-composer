from http import HTTPStatus


def test_itemsubmission_init_success(item_submission_instance, dspace_metadata):
    assert item_submission_instance.dspace_metadata == dspace_metadata
    assert item_submission_instance.bitstream_s3_uris == [
        "s3://dsc/workflow/folder/123_01.pdf",
        "s3://dsc/workflow/folder/123_02.pdf",
    ]
    assert item_submission_instance.item_identifier == "123"


def test_upload_dspace_metadata(mocked_s3, item_submission_instance, s3_client):
    item_submission_instance.upload_dspace_metadata("dsc", "workflow/folder/")
    assert (
        item_submission_instance.metadata_s3_uri
        == "s3://dsc/workflow/folder/dspace_metadata/123_metadata.json"
    )
    response = s3_client.client.get_object(
        Bucket="dsc", Key="workflow/folder/dspace_metadata/123_metadata.json"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_send_submission_message(
    mocked_sqs_input, mocked_sqs_output, item_submission_instance
):
    response = item_submission_instance.send_submission_message(
        "workflow",
        "mock-output-queue",
        "DSpace@MIT",
        "1234/5678",
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
