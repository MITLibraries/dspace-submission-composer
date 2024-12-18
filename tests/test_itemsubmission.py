from http import HTTPStatus


def test_itemsubmission_init_success(item_submission_instance, dspace_metadata):
    assert item_submission_instance.dspace_metadata == dspace_metadata
    assert item_submission_instance.bitstream_uris == [
        "s3://dsc/workflow/folder/123_01.pdf",
        "s3://dsc/workflow/folder/123_02.pdf",
    ]
    assert item_submission_instance.metadata_s3_key == "workflow/folder/123_metadata.json"


def test_upload_dspace_metadata_success(mocked_s3, item_submission_instance, s3_client):
    item_submission_instance.upload_dspace_metadata("dsc")
    assert (
        item_submission_instance.metadata_uri
        == "s3://dsc/workflow/folder/123_metadata.json"
    )
    response = s3_client.client.get_object(
        Bucket="dsc", Key="workflow/folder/123_metadata.json"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
