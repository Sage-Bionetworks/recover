import boto3
import pandas as pd
import pytest
from moto import mock_s3
from pyarrow import parquet


@pytest.fixture()
def mock_aws_credentials(monkeypatch):
    """A mock AWS credentials environment."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_REGION", "us-east-1")


@pytest.fixture
def s3():
    with mock_s3():
        s3_client = boto3.client("s3", region_name="us-east-1")
        yield s3_client


@pytest.fixture
def mock_aws_session(mock_aws_credentials):
    with mock_s3():
        aws_session = boto3.session.Session(region_name="us-east-1")
        yield aws_session


@pytest.fixture
def parquet_bucket_name():
    yield "test-parquet-bucket"


@pytest.fixture(scope="function")
def valid_staging_parquet_object(tmpdir_factory, valid_staging_dataset):
    filename = str(tmpdir_factory.mktemp("data_folder").join("df.parquet"))
    valid_staging_dataset.to_parquet(path=filename, engine="pyarrow")
    data = parquet.read_table(filename)
    yield data


@pytest.fixture()
def dataset_fixture(request):
    """This allows us to use different fixtures for the same test"""
    return request.getfixturevalue(request.param)


@pytest.fixture
def mock_s3_environment(mock_s3_bucket):
    """This allows us to persist the bucket and s3 client"""
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=mock_s3_bucket)
        yield s3


@pytest.fixture
def mock_s3_bucket():
    bucket_name = "test-bucket"
    yield bucket_name


@pytest.fixture()
def valid_staging_dataset():
    yield pd.DataFrame(
        {
            "ParticipantIdentifier": ["X000000", "X000001", "X000002"],
            "LogId": [
                "44984262767",
                "46096730542",
                "51739302864",
            ],
            "StartDate": [
                "2021-12-24T14:27:39+00:00",
                "2022-02-18T08:26:54+00:00",
                "2022-10-28T11:58:50+00:00",
            ],
            "EndDate": [
                "2021-12-24T14:40:27+00:00",
                "2022-02-18T09:04:30+00:00",
                "2022-10-28T12:35:38+00:00",
            ],
            "ActiveDuration": ["768000", "2256000", "2208000"],
            "Calories": ["89", "473", "478"],
        }
    )


@pytest.fixture()
def valid_main_dataset():
    yield pd.DataFrame(
        {
            "ParticipantIdentifier": ["X000000", "X000001", "X000002"],
            "LogId": [
                "44984262767",
                "46096730542",
                "51739302864",
            ],
            "StartDate": [
                "2021-12-24T14:27:39+00:00",
                "2022-02-18T08:26:54+00:00",
                "2022-10-28T11:58:50+00:00",
            ],
            "EndDate": [
                "2021-12-24T14:40:27+00:00",
                "2022-02-18T09:04:30+00:00",
                "2022-10-28T12:35:38+00:00",
            ],
            "ActiveDuration": ["768000", "2256000", "2208000"],
            "Calories": ["89", "473", "478"],
        }
    )


@pytest.fixture()
def staging_dataset_with_missing_cols():
    yield pd.DataFrame(
        {
            "LogId": [
                "44984262767",
                "46096730542",
                "51739302864",
            ],
            "ActiveDuration": ["768000", "2256000", "2208000"],
            "Calories": ["89", "473", "478"],
        }
    )


@pytest.fixture()
def staging_dataset_with_add_cols():
    yield pd.DataFrame(
        {
            "ParticipantIdentifier": ["X000000", "X000001", "X000002"],
            "LogId": [
                "44984262767",
                "46096730542",
                "51739302864",
            ],
            "StartDate": [
                "2021-12-24T14:27:39+00:00",
                "2022-02-18T08:26:54+00:00",
                "2022-10-28T11:58:50+00:00",
            ],
            "EndDate": [
                "2021-12-24T14:40:27+00:00",
                "2022-02-18T09:04:30+00:00",
                "2022-10-28T12:35:38+00:00",
            ],
            "ActiveDuration": ["768000", "2256000", "2208000"],
            "Calories": ["89", "473", "478"],
            "AverageHeartRate": ["108", "151", "157"],
        }
    )


@pytest.fixture()
def staging_dataset_with_no_common_cols():
    yield pd.DataFrame(
        {
            "Steps": ["866", "6074", "5744"],
            "OriginalDuration": ["768000", "2256000", "2208000"],
        }
    )


@pytest.fixture()
def staging_dataset_with_diff_num_of_rows():
    yield pd.DataFrame(
        {
            "ParticipantIdentifier": ["X000000"],
            "LogId": ["44984262767"],
            "StartDate": ["2021-12-24T14:27:39+00:00"],
            "EndDate": ["2021-12-24T14:40:27+00:00"],
            "ActiveDuration": ["768000"],
            "Calories": ["89"],
        }
    )


@pytest.fixture()
def staging_dataset_with_dup_cols():
    dup_df = pd.DataFrame(
        {
            "LogId": ["44984262767"],
            "StartDate": ["2021-12-24T14:27:39+00:00"],
            "EndDate": ["2021-12-24T14:40:27+00:00"],
            "ActiveDuration": [768000],
            "Calories": [89.0],
        }
    )
    dup_df = dup_df.rename({"StartDate": "EndDate"}, axis=1)
    yield dup_df


@pytest.fixture()
def staging_dataset_with_dup_indexes():
    yield pd.DataFrame(
        {
            "LogId": ["44984262767", "44984262767"],
            "StartDate": ["2021-12-24T14:27:39", "2021-12-24T14:27:40"],
            "EndDate": ["2021-12-24T14:40:27", "2021-12-24T14:40:28"],
        }
    )


@pytest.fixture()
def staging_dataset_with_empty_columns():
    yield pd.DataFrame(
        {
            "ParticipantIdentifier": [],
            "LogId": [],
            "StartDate": [],
            "EndDate": [],
        }
    )


@pytest.fixture()
def staging_dataset_empty():
    yield pd.DataFrame()


def pytest_addoption(parser):
    parser.addoption(
        "--test-synapse-folder-id",
        action="store",
        default=None,
        help="ID of the synapse folder to check STS access. Required.",
    )
    parser.addoption("--namespace", default=None, help="The namespace to test.")
    parser.addoption(
        "--test-sts-permission",
        default="read_only",
        choices=["read_only", "read_write"],
        help="The permission type to use for the STS token credentials. Required.",
    )
    parser.addoption(
        "--test-bucket",
        default=None,
        choices=[
            "recover-input-data",
            "recover-processed-data",
            "recover-dev-input-data",
            "recover-dev-processed-data",
        ],
        help="The bucket to test access with the STS token credentials. Required.",
    )


@pytest.fixture(scope="session")
def namespace(pytestconfig):
    yield pytestconfig.getoption("namespace")


@pytest.fixture(scope="session")
def artifact_bucket():
    yield "recover-dev-cloudformation"


@pytest.fixture(scope="session")
def ssm_parameter():
    yield "synapse-recover-auth"
