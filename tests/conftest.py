import os
from unittest import mock

import boto3
import pytest
import pandas as pd
from pyarrow import fs, parquet
from moto import mock_s3


@pytest.fixture(scope="function")
def mock_aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3(mock_aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture(scope="function")
def mock_aws_session(mock_aws_credentials):
    with mock_s3():
        yield boto3.session.Session(region_name="us-east-1")


@pytest.fixture()
def parquet_bucket_name():
    yield "test-parquet-bucket"


@pytest.fixture(scope="function")
def mock_s3_filesystem(mock_aws_session):
    with mock_s3():
        session_credentials = mock_aws_session.get_credentials()
        yield fs.S3FileSystem(
            region="us-east-1",
            access_key=session_credentials.access_key,
            secret_key=session_credentials.secret_key,
            session_token=session_credentials.token,
        )


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


@pytest.fixture()
def valid_staging_dataset():
    yield pd.DataFrame(
        {
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
            "ParticipantIdentifier": [
                "MDH-9352-3209",
                "MDH-9352-3209",
                "MDH-9352-3209",
            ],
            "Steps": ["866", "6074", "5744"],
            "OriginalDuration": ["768000", "2256000", "2208000"],
        }
    )


@pytest.fixture()
def staging_dataset_with_diff_data_type_cols():
    yield pd.DataFrame(
        {
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
            "ActiveDuration": [768000, 2256000, 2208000],
            "Calories": [89.0, 473.0, 478.0],
        }
    )


@pytest.fixture()
def staging_dataset_with_diff_num_of_rows():
    yield pd.DataFrame(
        {
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
def staging_dataset_with_all_col_val_diff():
    yield pd.DataFrame(
        {
            "LogId": ["44984262767", "44984262767"],
            "StartDate": ["2021-12-24T14:27:39+00:00", "2021-12-24T14:27:39+00:00"],
            "EndDate": ["TESTING1", "TESTING2"],
        }
    )


@pytest.fixture()
def staging_dataset_with_empty_columns():
    return pd.DataFrame(
        {
            "LogId": [],
            "StartDate": [],
            "EndDate": [],
        }
    )


@pytest.fixture()
def staging_dataset_empty():
    return pd.DataFrame()


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
