import os
from unittest import mock

import boto3
import pytest
import pandas as pd
from pyarrow import fs
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


@pytest.fixture()
def parquet_bucket_name():
    yield f"test-parquet-bucket"


@pytest.fixture
def s3_test_bucket(s3, parquet_bucket_name):
    with mock_s3():
        s3.create_bucket(Bucket=parquet_bucket_name)
        yield


@pytest.fixture(scope="function")
def mock_s3_filesystem(mock_aws_credentials):
    with mock_s3():
        yield fs.S3FileSystem(region="us-east-1")


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
            "StartDate": ["2021-12-24T14:27:39+00:00", "2021-12-24T14:27:39+00:00"],
            "EndDate": ["2021-12-24T14:40:27+00:00", "2021-12-24T14:40:27+00:00"],
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
