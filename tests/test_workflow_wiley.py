from dsc.utilities.aws import S3Client
from dsc.workflows import Wiley


def test_workflow_wiley_init(mocked_s3):
    # upload DOI CSV files to mock S3 bucket
    csv_filepaths = [
        "tests/fixtures/wiley/mit_authored_articles_001.csv",
        "tests/fixtures/wiley/mit_authored_articles_002.csv",
    ]
    for filepath in csv_filepaths:
        with open(filepath, "rb") as file:
            filename = filepath.split("/")[-1]
            mocked_s3.put_object(Bucket="dsc", Key=f"wiley/{filename}", Body=file)

    workflow_instance = Wiley()

    assert workflow_instance.workflow_name == "wiley"
    assert workflow_instance.batch_id
    assert mocked_s3.get_object(
        Bucket="dsc", Key=f"{workflow_instance.batch_path}mit_authored_articles_001.csv"
    )
    assert mocked_s3.get_object(
        Bucket="dsc", Key=f"{workflow_instance.batch_path}mit_authored_articles_002.csv"
    )
