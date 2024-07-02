import json
import re
import zipfile
from collections import namedtuple
from io import BytesIO
from unittest import mock

import datacompy
import pandas as pd
import pyarrow
import pyarrow.dataset as ds
import pytest
from moto import mock_s3
from moto.server import ThreadedMotoServer
from pandas.testing import assert_frame_equal
from pyarrow import fs, parquet

from src.glue.jobs import compare_parquet_datasets as compare_parquet


@pytest.fixture(scope="module")
def mock_moto_server():
    """A moto server to mock S3 interactions.

    We cannot use the moto because pyarrow's S3 FileSystem
    is not based on boto3 at all. Instead we use the moto_server
    feature
    (http://docs.getmoto.org/en/latest/docs/getting_started.html#stand-alone-server-mode),
    which gives us an endpoint url, that can be used to construct a
    pyarrow S3FileSystem that interacts with the moto server.

    References:
        https://github.com/apache/arrow/issues/31811
    """

    server = ThreadedMotoServer(port=3000)
    server.start()
    yield "http://127.0.0.1:3000"
    server.stop()


@pytest.fixture
def mock_s3_for_filesystem(mock_aws_session, mock_moto_server):
    s3_client = mock_aws_session.client(
        "s3", region_name="us-east-1", endpoint_url=mock_moto_server
    )
    yield s3_client


@pytest.fixture
def mock_s3_filesystem(mock_aws_credentials, mock_aws_session, mock_moto_server):
    session_credentials = mock_aws_session.get_credentials()
    filesystem = fs.S3FileSystem(
        region="us-east-1",
        access_key=session_credentials.access_key,
        secret_key=session_credentials.secret_key,
        session_token=session_credentials.token,
        endpoint_override=mock_moto_server,
    )
    yield filesystem


def add_data_to_mock_bucket(
    mock_s3_client: "boto3.client",
    input_data: pd.DataFrame,
    mock_bucket_name: str,
    dataset_key: str,
) -> None:
    """Helper function that creates a mock bucket and
        adds test data to mock s3 bucket

    Args:
        mock_s3_client (boto3.client): mock s3 client
        input_data (pd.DataFrame): test data
        mock_bucket_name (str): mock s3 bucket name to use
        dataset_key (str): path in mock s3 bucket to put data
    """
    mock_s3_client.create_bucket(Bucket=mock_bucket_name)
    # Create a sample dataframe and upload as a parquet dataset
    buffer = BytesIO()
    table = pyarrow.Table.from_pandas(input_data)
    parquet.write_table(table, buffer)
    buffer.seek(0)
    mock_s3_client.put_object(
        Bucket=mock_bucket_name, Key=dataset_key, Body=buffer.getvalue()
    )
    # Ensure the object is uploaded
    obj_list = mock_s3_client.list_objects_v2(Bucket=mock_bucket_name)
    assert any(obj["Key"] == dataset_key for obj in obj_list.get("Contents", []))

    # Directly access the S3 object to ensure it's there
    response = mock_s3_client.get_object(Bucket=mock_bucket_name, Key=dataset_key)
    assert response["Body"].read() is not None


def test_that_validate_args_raises_exception_when_input_value_is_empty_string():
    with pytest.raises(ValueError):
        compare_parquet.validate_args(value="")


def test_that_validate_args_returns_nothing_when_value_is_not_an_empty_string():
    assert compare_parquet.validate_args(value="TEST") == None


def test_that_get_parquet_dataset_s3_path_returns_correct_filepath(parquet_bucket_name):
    filepath = compare_parquet.get_parquet_dataset_s3_path(
        parquet_bucket_name, "test_namespace", "dataset_fitbitactivitylogs"
    )
    assert (
        filepath
        == "s3://test-parquet-bucket/test_namespace/parquet/dataset_fitbitactivitylogs"
    )


def test_that_get_s3_file_key_for_comparison_results_returns_correct_filepath_for_data_types_compare():
    file_key = compare_parquet.get_s3_file_key_for_comparison_results(
        "staging", data_type=None, file_name="data_types_compare.txt"
    )
    assert file_key == "staging/comparison_result/data_types_compare.txt"


def test_that_get_s3_file_key_for_comparison_results_has_expected_filepath_for_specific_data_type():
    file_key = compare_parquet.get_s3_file_key_for_comparison_results(
        "staging",
        data_type="dataset_fitbitactivitylogs",
        file_name="parquet_compare.txt",
    )
    assert (
        file_key
        == "staging/comparison_result/dataset_fitbitactivitylogs/parquet_compare.txt"
    )


def test_that_get_s3_file_key_for_comparison_results_raises_type_error_if_filename_has_wrong_file_ext():
    with pytest.raises(TypeError):
        file_key = compare_parquet.get_s3_file_key_for_comparison_results(
            "staging",
            data_type="dataset_fitbitactivitylogs",
            file_name="parquet_compare.pdf",
        )


def test_that_get_duplicated_columns_returns_empty_if_no_dup_exist(
    valid_staging_dataset,
):
    assert compare_parquet.get_duplicated_columns(valid_staging_dataset) == []


def test_that_get_duplicated_columns_returns_list_if_dup_exist(
    staging_dataset_with_dup_cols,
):
    assert compare_parquet.get_duplicated_columns(staging_dataset_with_dup_cols) == [
        "EndDate"
    ]


@mock_s3
def test_that_get_S3FileSystem_from_session_returns_filesystem_when_credentials_exist(
    mock_aws_session,
):
    filesystem = compare_parquet.get_S3FileSystem_from_session(
        aws_session=mock_aws_session
    )
    assert isinstance(filesystem, fs.S3FileSystem)


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("HealthKitV2ActivitySummaries_20221026-20221028.json", "2022-10-28T00:00:00"),
        ("EnrolledParticipants_20221027.json", "2022-10-27T00:00:00"),
        (
            "HealthKitV2Samples_WalkingStepLength_Deleted_20221023-20221024.json",
            "2022-10-24T00:00:00",
        ),
        (
            "HealthKitV2Samples_WalkingStepLength_Deleted_20221024.json",
            "2022-10-24T00:00:00",
        ),
        (
            "HealthKitV2Samples.json",
            None,
        ),
        (
            "",
            None,
        ),
        (
            None,
            None,
        ),
    ],
    ids=[
        "filename_with_date_range",
        "filename_with_end_date",
        "filename_with_subtype_and_date_range",
        "filename_with_subtype_and_end_date",
        "filename_with_no_date",
        "filename_is_empty",
        "filename_is_none",
    ],
)
def test_that_get_export_end_date_returns_expected(filename, expected):
    result = compare_parquet.get_export_end_date(filename)
    assert expected == result


@pytest.mark.parametrize(
    "s3_uri,expected",
    [
        (
            "s3://recover-input-data/main/pediatric_v1/2024-06-09T00:15:TEST",
            "pediatric_v1",
        ),
        ("s3://recover-input-data/main/adults_v1/2024-06-09T00:12:49TEST", "adults_v1"),
        ("s3://recover-input-data/main/2024-06-09T00:12:49TEST", None),
    ],
    ids=[
        "peds_cohort",
        "adults_cohort",
        "no_cohort",
    ],
)
def test_that_get_cohort_from_s3_uri_returns_expected(s3_uri, expected):
    result = compare_parquet.get_cohort_from_s3_uri(s3_uri)
    assert expected == result


@pytest.mark.parametrize(
    "filename,expected",
    [
        (
            "HealthKitV2ActivitySummaries_20221026-20221028.json",
            "dataset_healthkitv2activitysummaries",
        ),
        ("EnrolledParticipants_20221027.json", "dataset_enrolledparticipants"),
        (
            "HealthKitV2Samples_WalkingStepLength_Deleted_20221024.json",
            None,
        ),
        (
            "HealthKitV2Workouts_Deleted_20221024.json",
            None,
        ),
        (
            "NonexistentDataType_20221024.json",
            None,
        ),
    ],
    ids=[
        "data_type_with_date_range",
        "data_type",
        "data_type_with_deleted_subtype",
        "deleted_data_type",
        "invalid_data_type",
    ],
)
def test_that_get_data_type_from_filename_returns_expected(filename, expected):
    result = compare_parquet.get_data_type_from_filename(filename)
    assert expected == result


def test_that_get_json_files_in_zip_from_s3_returns_expected_filelist(
    mock_s3_environment, mock_s3_bucket
):
    # Create a zip file with JSON files
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as z:
        z.writestr("file1.json", '{"key": "value"}')
        z.writestr("file2.json", '{"key2": "value2"}')
        z.writestr("Manifest.json", '{"key2": "value2"}')
        z.writestr("/folder/test", '{"key2": "value2"}')
        z.writestr("empty.json", "")

    zip_buffer.seek(0)

    # Upload the zip file to the mock S3 bucket
    s3_uri = f"s3://{mock_s3_bucket}/test.zip"
    mock_s3_environment.put_object(
        Bucket=mock_s3_bucket, Key="test.zip", Body=zip_buffer.getvalue()
    )

    result = compare_parquet.get_json_files_in_zip_from_s3(
        mock_s3_environment, mock_s3_bucket, s3_uri
    )

    # Verify the result
    assert result == ["file1.json", "file2.json"]


def test_that_get_integration_test_exports_json_success(
    mock_s3_environment, mock_s3_bucket
):
    staging_namespace = "staging"

    # Create a sample exports.json file content
    exports_content = ["file1.json", "file2.json", "file3.json"]
    exports_json = json.dumps(exports_content)

    # Upload the exports.json file to the mock S3 bucket
    mock_s3_environment.put_object(
        Bucket=mock_s3_bucket,
        Key=f"{staging_namespace}/integration_test_exports.json",
        Body=exports_json,
    )

    result = compare_parquet.get_integration_test_exports_json(
        mock_s3_environment, mock_s3_bucket, staging_namespace
    )
    assert result == exports_content


def test_that_get_integration_test_exports_json_file_not_exist(
    mock_s3_environment, mock_s3_bucket
):
    staging_namespace = "staging"

    # Call the function and expect an exception
    with pytest.raises(mock_s3_environment.exceptions.NoSuchKey):
        compare_parquet.get_integration_test_exports_json(
            mock_s3_environment, mock_s3_bucket, staging_namespace
        )


@pytest.mark.parametrize(
    "json_body",
    [(""), ("{invalid json}")],
    ids=[
        "empty_json",
        "invalid_json",
    ],
)
def test_that_get_integration_test_exports_json_throws_json_decode_error(
    json_body, mock_s3_environment, mock_s3_bucket
):
    staging_namespace = "staging"

    # Upload an invalid exports.json file to the mock S3 bucket
    mock_s3_environment.put_object(
        Bucket=mock_s3_bucket,
        Key=f"{staging_namespace}/integration_test_exports.json",
        Body=json_body,
    )

    # Call the function and expect a JSON decode error
    with pytest.raises(json.JSONDecodeError):
        compare_parquet.get_integration_test_exports_json(
            mock_s3_environment, mock_s3_bucket, staging_namespace
        )


@pytest.mark.parametrize(
    "data_type, filelist, expected_filter",
    [
        (
            "dataset_healthkitv2activitysummaries",
            ["s3://bucket/adults_v1/file1.zip", "s3://bucket/pediatric_v1/file2.zip"],
            {
                "adults_v1": ["2022-10-28T00:00:00", "2022-10-29T00:00:00"],
                "pediatric_v1": ["2022-10-28T00:00:00", "2022-10-29T00:00:00"],
            },
        ),
        (
            "dataset_enrolledparticipants",
            ["s3://bucket/adults_v1/file1.zip"],
            {"adults_v1": ["2022-10-27T00:00:00"]},
        ),
        (
            "dataset_healthkitv2samples",
            ["s3://bucket/pediatric_v1/file1.zip"],
            {"pediatric_v1": ["2022-10-29T00:00:00", "2022-10-24T00:00:00"]},
        ),
        ("dataset_googlefitsamples", ["s3://bucket/adults_v1/file1.zip"], {}),
        (
            "dataset_healthkitv2samples_deleted",
            ["s3://bucket/pediatric_v1/file1.zip"],
            {},
        ),
    ],
    ids=[
        "empty_filelist",
        "adults_cohort_match",
        "peds_cohort_match",
        "no_data_type_match",
        "deleted_data_type_match",
    ],
)
def test_that_get_exports_filter_values_returns_expected_results(
    s3, data_type, filelist, expected_filter
):
    with mock.patch.object(
        compare_parquet, "get_integration_test_exports_json"
    ) as patch_test_exports, mock.patch.object(
        compare_parquet, "get_json_files_in_zip_from_s3"
    ) as patch_get_json:
        patch_test_exports.return_value = filelist
        patch_get_json.return_value = [
            "HealthKitV2ActivitySummaries_20221026-20221028.json",
            "HealthKitV2ActivitySummaries_20221027-20221029.json",
            "HealthKitV2Samples_BloodPressureDiastolic_20221027-20221029.json",
            "EnrolledParticipants_20221027.json",
            "HealthKitV2Samples_AppleExerciseTime_20221024.json",
            "HealthKitV2Samples_AppleExerciseTime_Deleted_20221024.json",
        ]

        exports_filter = compare_parquet.get_exports_filter_values(
            s3,
            data_type,
            input_bucket="test_input_bucket",
            cfn_bucket="test_cfn_bucket",
            staging_namespace="staging",
        )
        assert exports_filter == expected_filter


@pytest.mark.parametrize(
    "input_filter_values, expected_expression",
    [
        (
            {"adults_v1": ["2022-10-28T00:00:00"]},
            (ds.field("cohort") == "adults_v1")
            & (ds.field("export_end_date").isin(["2022-10-28T00:00:00"])),
        ),
        (
            {
                "adults_v1": ["2022-10-28T00:00:00", "2022-10-29T00:00:00"],
                "pediatric_v1": ["2022-10-28T00:00:00", "2022-10-29T00:00:00"],
            },
            (ds.field("cohort") == "adults_v1")
            & (
                ds.field("export_end_date").isin(
                    ["2022-10-28T00:00:00", "2022-10-29T00:00:00"]
                )
            )
            | (ds.field("cohort") == "pediatric_v1")
            & (
                ds.field("export_end_date").isin(
                    ["2022-10-28T00:00:00", "2022-10-29T00:00:00"]
                )
            ),
        ),
        ({}, None),
    ],
    ids=["single_condition", "multi_condition", "no_filter_values"],
)
def test_that_convert_filter_values_to_expression_returns_correct_exp(
    input_filter_values, expected_expression
):
    result = compare_parquet.convert_filter_values_to_expression(
        filter_values=input_filter_values
    )

    # handle when expression is None
    if expected_expression is not None:
        assert result.equals(expected_expression)
    else:
        assert expected_expression == result


@pytest.mark.parametrize(
    "input_data, filter_values, expected_filtered_data",
    [
        (
            pd.DataFrame(
                {
                    "cohort": ["pediatric_v1", "adult_v1", "pediatric_v1"],
                    "export_end_date": [
                        "2022-10-24T00:00:00",
                        "2022-10-25T00:00:00",
                        "2022-10-23T00:00:00",
                    ],
                    "value": [1, 2, 3],
                }
            ),
            {"pediatric_v1": ["2022-10-24T00:00:00", "2022-10-23T00:00:00"]},
            pd.DataFrame(
                {
                    "cohort": ["pediatric_v1", "pediatric_v1"],
                    "export_end_date": ["2022-10-24T00:00:00", "2022-10-23T00:00:00"],
                    "value": [1, 3],
                }
            ),
        ),
        (
            pd.DataFrame(
                {
                    "cohort": [
                        "pediatric_v1",
                        "adults_v1",
                        "adults_v1",
                        "pediatric_v1",
                    ],
                    "export_end_date": [
                        "2022-10-24T00:00:00",
                        "2022-10-25T00:00:00",
                        "2022-10-27T00:00:00",
                        "2022-10-23T00:00:00",
                    ],
                    "value": [1, 2, 3, 4],
                }
            ),
            {
                "pediatric_v1": ["2022-10-24T00:00:00", "2022-10-23T00:00:00"],
                "adults_v1": ["2022-10-24T00:00:00", "2022-10-25T00:00:00"],
            },
            pd.DataFrame(
                {
                    "cohort": ["pediatric_v1", "adults_v1", "pediatric_v1"],
                    "export_end_date": [
                        "2022-10-24T00:00:00",
                        "2022-10-25T00:00:00",
                        "2022-10-23T00:00:00",
                    ],
                    "value": [1, 2, 4],
                }
            ),
        ),
        (
            pd.DataFrame(
                {
                    "cohort": ["pediatric_v1"],
                    "export_end_date": [
                        "2022-10-24T00:00:00",
                    ],
                    "value": [1],
                }
            ),
            {},
            pd.DataFrame(
                {
                    "cohort": ["pediatric_v1"],
                    "export_end_date": [
                        "2022-10-24T00:00:00",
                    ],
                    "value": [1],
                }
            ),
        ),
        (
            pd.DataFrame(),
            {},
            pd.DataFrame(),
        ),
        (
            pd.DataFrame(
                {
                    "cohort": ["pediatric_v1"],
                    "export_end_date": [
                        "2022-10-24T00:00:00",
                    ],
                    "value": [1],
                }
            ),
            {"adults_v1": ["2022-10-24T00:00:00"]},
            pd.DataFrame(
                {
                    "cohort": pd.Series(dtype="object"),
                    "export_end_date": pd.Series(dtype="object"),
                    "value": pd.Series(dtype="int64"),
                }
            ),
        ),
        (
            pd.DataFrame(
                {
                    "cohort": pd.Series(dtype="object"),
                    "export_end_date": pd.Series(dtype="object"),
                    "value": pd.Series(dtype="int64"),
                }
            ),
            {},
            pd.DataFrame(
                {
                    "cohort": pd.Series(dtype="object"),
                    "export_end_date": pd.Series(dtype="object"),
                    "value": pd.Series(dtype="int64"),
                }
            ),
        ),
    ],
    ids=[
        "regular_filter",
        "multi_condition_filter",
        "no_filter",
        "empty_df_no_filter",
        "empty_cols_after_filter",
        "empty_cols_no_filter",
    ],
)
def test_that_get_parquet_dataset_returns_expected_results(
    mock_s3_for_filesystem,
    mock_s3_filesystem,
    mock_s3_bucket,
    input_data,
    filter_values,
    expected_filtered_data,
):
    """This will check that the main dataset is being chunked and filtered correctly
    based on what is the staging dataset
    """
    dataset_key = "test_dataset/main_dataset.parquet"

    add_data_to_mock_bucket(
        mock_s3_client=mock_s3_for_filesystem,
        input_data=input_data,
        mock_bucket_name=mock_s3_bucket,
        dataset_key=dataset_key,
    )

    # Call the function to test
    result = compare_parquet.get_parquet_dataset(
        filter_values=filter_values,
        dataset_key=f"s3://{mock_s3_bucket}/{dataset_key}",
        s3_filesystem=mock_s3_filesystem,
    )

    # Verify the result
    pd.testing.assert_frame_equal(
        result, expected_filtered_data, check_index_type=False
    )


@pytest.mark.parametrize(
    "input_data, filter_values, expected_exception",
    [
        (
            pd.DataFrame(),
            {"adults_v1": ["2022-10-24T00:00:00"]},
            pyarrow.lib.ArrowInvalid,
        ),
        (
            pd.DataFrame(
                {
                    "cohort": pd.Series(dtype="object"),
                    "export_end_date": pd.Series(dtype="object"),
                    "value": pd.Series(dtype="int64"),
                }
            ),
            {"adults_v1": ["2022-10-24T00:00:00"]},
            pyarrow.lib.ArrowNotImplementedError,
        ),
    ],
    ids=[
        "empty_df_before_filter",
        "empty_cols_before_filter",
    ],
)
def test_that_get_parquet_dataset_raises_expected_exceptions(
    mock_s3_for_filesystem,
    mock_s3_filesystem,
    mock_s3_bucket,
    input_data,
    filter_values,
    expected_exception,
):
    """These are the test cases that will end up with pyarrow
    exceptions when trying to get the dataset
    """
    dataset_key = "test_dataset/main_dataset.parquet"

    add_data_to_mock_bucket(
        mock_s3_client=mock_s3_for_filesystem,
        input_data=input_data,
        mock_bucket_name=mock_s3_bucket,
        dataset_key=dataset_key,
    )

    with pytest.raises(expected_exception):
        compare_parquet.get_parquet_dataset(
            filter_values=filter_values,
            dataset_key=f"s3://{mock_s3_bucket}/{dataset_key}",
            s3_filesystem=mock_s3_filesystem,
        )


@mock_s3
def test_that_get_parquet_dataset_raises_attr_error_if_no_datasets_exist(
    mock_s3_filesystem, parquet_bucket_name
):
    file_key = "staging/parquet/dataset_fitbitactivitylogs/test.parquet"
    with mock.patch.object(ds, "dataset", return_value=None):
        with pytest.raises(AttributeError):
            compare_parquet.get_parquet_dataset(
                dataset_key=f"{parquet_bucket_name}/{file_key}",
                s3_filesystem=mock_s3_filesystem,
            )


def test_that_has_common_cols_returns_false_if_no_common_cols(
    staging_dataset_with_no_common_cols, valid_main_dataset
):
    test_common_cols = compare_parquet.has_common_cols(
        staging_dataset_with_no_common_cols, valid_main_dataset
    )
    assert test_common_cols is False


def test_that_has_common_cols_returns_true_if_common_cols(
    valid_staging_dataset, valid_main_dataset
):
    test_common_cols = compare_parquet.has_common_cols(
        valid_staging_dataset, valid_main_dataset
    )
    assert test_common_cols is True


def test_that_get_missing_cols_returns_empty_list_if_no_missing_cols(
    valid_staging_dataset, valid_main_dataset
):
    test_missing_cols = compare_parquet.get_missing_cols(
        valid_staging_dataset, valid_main_dataset
    )
    assert test_missing_cols == []


def test_that_get_missing_cols_returns_list_of_cols_if_missing_cols(
    staging_dataset_with_missing_cols, valid_main_dataset
):
    test_missing_cols = compare_parquet.get_missing_cols(
        staging_dataset_with_missing_cols, valid_main_dataset
    )
    assert test_missing_cols == ["EndDate", "ParticipantIdentifier", "StartDate"]


def test_that_get_additional_cols_returns_empty_list_if_no_add_cols(
    valid_staging_dataset, valid_main_dataset
):
    test_add_cols = compare_parquet.get_additional_cols(
        valid_staging_dataset, valid_main_dataset
    )
    assert test_add_cols == []


def test_that_get_additional_cols_returns_list_of_cols_if_add_cols(
    staging_dataset_with_add_cols, valid_main_dataset
):
    test_add_cols = compare_parquet.get_additional_cols(
        staging_dataset_with_add_cols, valid_main_dataset
    )
    assert test_add_cols == ["AverageHeartRate"]


@pytest.mark.parametrize(
    "input_dataset,expected_str",
    [
        (pd.DataFrame(dict(col1=[1, 2], col2=[3, 4])), ",col1,col2\n0,1,3\n1,2,4\n"),
        (pd.DataFrame(), ""),
        (pd.DataFrame(columns=["col1", "col2"]), ""),
    ],
    ids=["non_empty_df", "empty_df", "empty_cols"],
)
def test_that_dataframe_to_text_returns_expected_str(input_dataset, expected_str):
    result = compare_parquet.convert_dataframe_to_text(input_dataset)
    assert result == expected_str


def test_that_dataframe_to_text_returns_valid_format_for_s3_put_object(
    mock_s3_bucket, mock_s3_environment, valid_staging_dataset
):
    # shouldn't throw a botocore.exceptions.ParamValidationError
    staging_content = compare_parquet.convert_dataframe_to_text(valid_staging_dataset)
    mock_s3_environment.put_object(
        Bucket=mock_s3_bucket,
        Key=f"staging/parquet/dataset_fitbitactivitylogs/test.csv",
        Body=staging_content,
    )


def test_that_get_duplicates_returns_empty_df_if_no_dups(
    valid_staging_dataset, valid_main_dataset
):
    compare = datacompy.Compare(
        df1=valid_staging_dataset,
        df2=valid_main_dataset,
        join_columns="LogId",
        df1_name="staging",  # Optional, defaults to 'df1'
        df2_name="main",  # Optional, defaults to 'df2'
        cast_column_names_lower=False,
    )
    staging_dups = compare_parquet.get_duplicates(compare, namespace="staging")
    assert staging_dups.empty


def test_that_get_duplicates_returns_dups_df_if_dups_exist(
    staging_dataset_with_dup_indexes, valid_main_dataset
):
    compare = datacompy.Compare(
        df1=staging_dataset_with_dup_indexes,
        df2=valid_main_dataset,
        join_columns="LogId",
        df1_name="staging",  # Optional, defaults to 'df1'
        df2_name="main",  # Optional, defaults to 'df2'
        cast_column_names_lower=False,
    )
    staging_dups = compare_parquet.get_duplicates(compare, namespace="staging")
    assert_frame_equal(
        staging_dups.reset_index(drop=True),
        pd.DataFrame(
            {
                "LogId": ["44984262767"],
                "StartDate": ["2021-12-24T14:27:40"],
                "EndDate": ["2021-12-24T14:40:28"],
            }
        ).reset_index(drop=True),
    )


def test_that_get_duplicates_raises_key_error_if_namespace_invalid():
    with pytest.raises(KeyError):
        compare_parquet.get_duplicates(None, namespace="invalid")


def test_that_compare_row_diffs_returns_empty_df_if_columns_are_not_diff(
    valid_staging_dataset, valid_main_dataset
):
    compare = datacompy.Compare(
        df1=valid_staging_dataset,
        df2=valid_main_dataset,
        join_columns="LogId",
        df1_name="staging",  # Optional, defaults to 'df1'
        df2_name="main",  # Optional, defaults to 'df2'
        cast_column_names_lower=False,
    )
    staging_rows = compare_parquet.compare_row_diffs(compare, namespace="staging")
    main_rows = compare_parquet.compare_row_diffs(compare, namespace="main")
    assert staging_rows.empty
    assert main_rows.empty


def test_that_compare_row_diffs_returns_df_if_columns_are_not_diff(
    staging_dataset_with_diff_num_of_rows, valid_main_dataset
):
    compare = datacompy.Compare(
        df1=staging_dataset_with_diff_num_of_rows,
        df2=valid_main_dataset,
        join_columns=["LogId", "ParticipantIdentifier"],
        df1_name="staging",  # Optional, defaults to 'df1'
        df2_name="main",  # Optional, defaults to 'df2'
        cast_column_names_lower=False,
    )
    staging_rows = compare_parquet.compare_row_diffs(compare, namespace="staging")
    main_rows = compare_parquet.compare_row_diffs(compare, namespace="main")
    assert staging_rows.empty
    assert_frame_equal(
        main_rows.sort_values(by="LogId").reset_index(drop=True),
        pd.DataFrame(
            {
                "ParticipantIdentifier": ["X000001", "X000002"],
                "LogId": [
                    "46096730542",
                    "51739302864",
                ],
                "StartDate": [
                    "2022-02-18T08:26:54+00:00",
                    "2022-10-28T11:58:50+00:00",
                ],
                "EndDate": [
                    "2022-02-18T09:04:30+00:00",
                    "2022-10-28T12:35:38+00:00",
                ],
                "ActiveDuration": ["2256000", "2208000"],
                "Calories": ["473", "478"],
            }
        )
        .sort_values(by="LogId")
        .reset_index(drop=True),
    )


def test_that_compare_row_diffs_raises_key_error_is_namespace_is_invalid():
    with pytest.raises(KeyError):
        compare_parquet.compare_row_diffs(None, namespace="invalid_namespace")


def test_that_compare_column_names_returns_empty_msg_if_cols_are_same(
    valid_staging_dataset, valid_main_dataset
):
    compare_msg = compare_parquet.compare_column_names(
        "dataset_fitbitactivitylogs", valid_staging_dataset, valid_main_dataset
    )
    assert compare_msg == []


def test_that_compare_column_names_returns_msg_if_cols_are_diff(
    staging_dataset_with_no_common_cols, valid_main_dataset
):
    compare_msg = compare_parquet.compare_column_names(
        "dataset_fitbitactivitylogs",
        staging_dataset_with_no_common_cols,
        valid_main_dataset,
    )

    assert compare_msg == [
        "dataset_fitbitactivitylogs: Staging dataset has the following missing columns:\n"
        "['ActiveDuration', 'Calories', 'EndDate', 'LogId', 'ParticipantIdentifier', 'StartDate']",
        "dataset_fitbitactivitylogs: Staging dataset has the following additional columns:\n"
        "['OriginalDuration', 'Steps']",
    ]


@pytest.mark.parametrize(
    "dataset_fixture,expected_error",
    [
        (
            "staging_dataset_empty",
            "The staging dataset is empty. Comparison cannot continue.",
        ),
        (
            "staging_dataset_with_empty_columns",
            "The staging dataset is empty. Comparison cannot continue.",
        ),
        (
            "staging_dataset_with_dup_cols",
            "staging dataset has duplicated columns. Comparison cannot continue.\nDuplicated columns:['EndDate']",
        ),
    ],
    ids=["empty_df_no_rows_no_cols", "empty_df_no_rows", "df_dup_cols"],
    indirect=["dataset_fixture"],
)
def test_that_check_for_valid_dataset_raises_exception_if_dataset_is_invalid(
    dataset_fixture, expected_error
):
    with pytest.raises(ValueError, match=re.escape(expected_error)):
        compare_parquet.check_for_valid_dataset(dataset_fixture, "staging")


def test_that_check_for_valid_dataset_raises_no_exception_if_dataset_is_valid(
    valid_staging_dataset,
):
    compare_parquet.check_for_valid_dataset(valid_staging_dataset, "staging")


@pytest.mark.parametrize(
    "dataset_fixture",
    ["staging_dataset_with_empty_columns", "valid_staging_dataset"],
    indirect=True,
)
def test_that_compare_datasets_and_output_report_outputs_datacompy_compare_obj_if_input_is_valid(
    dataset_fixture, valid_main_dataset
):
    compare = compare_parquet.compare_datasets_and_output_report(
        data_type="dataset_fitbitactivitylogs",
        staging_dataset=dataset_fixture,
        main_dataset=valid_main_dataset,
        staging_namespace="staging",
        main_namespace="main",
    )
    assert isinstance(compare, datacompy.Compare)
    assert_frame_equal(compare.df1, dataset_fixture)
    assert_frame_equal(compare.df2, valid_main_dataset)


def test_that_add_additional_msg_to_comparison_report_returns_correct_updated_msg():
    comparison_report = "some string\n\n"
    add_msgs = ["one message", "two message"]
    result = compare_parquet.add_additional_msg_to_comparison_report(
        comparison_report, add_msgs, msg_type="column_name_diff"
    )
    assert result == (
        "some string\n\nColumn Name Differences\n"
        "-----------------------\n\n"
        "one message\ntwo message"
    )


def test_that_add_additional_msg_to_comparison_report_throws_error_if_msg_type_not_valid():
    comparison_report = "some string\n\n"
    add_msgs = ["one message", "two message"]
    with pytest.raises(ValueError):
        compare_parquet.add_additional_msg_to_comparison_report(
            comparison_report, add_msgs, msg_type="invalid_msg_type"
        )


@mock.patch("src.glue.jobs.compare_parquet_datasets.compare_datasets_and_output_report")
def test_that_compare_datasets_by_data_type_returns_correct_msg_if_input_is_invalid(
    mocked_compare_datasets, s3, parquet_bucket_name, staging_dataset_empty
):
    with mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_parquet_dataset",
        return_value=staging_dataset_empty,
    ), mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_exports_filter_values",
    ):
        with pytest.raises(
            ValueError,
            match="The staging dataset is empty. Comparison cannot continue.",
        ):
            compare_parquet.compare_datasets_by_data_type(
                s3=s3,
                cfn_bucket="test_cfn_bucket",
                input_bucket="test_input_bucket",
                parquet_bucket=parquet_bucket_name,
                staging_namespace="staging",
                main_namespace="main",
                s3_filesystem=None,
                data_type="dataset_fitbitactivitylogs",
            )
            mocked_compare_datasets.assert_not_called()


@mock.patch("src.glue.jobs.compare_parquet_datasets.compare_datasets_and_output_report")
def test_that_compare_datasets_by_data_type_calls_compare_datasets_by_data_type_if_input_is_valid(
    mocked_compare_datasets, parquet_bucket_name, valid_staging_dataset, s3
):
    with mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_parquet_dataset",
        return_value=valid_staging_dataset,
    ) as patch_get_parquet_data, mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_exports_filter_values",
        return_value="some_filter",
    ) as patch_get_filter, mock.patch(
        "src.glue.jobs.compare_parquet_datasets.check_for_valid_dataset",
    ) as patch_check_valid:
        compare_parquet.compare_datasets_by_data_type(
            s3=s3,
            cfn_bucket="test_cfn_bucket",
            input_bucket="test_input_bucket",
            parquet_bucket=parquet_bucket_name,
            staging_namespace="staging",
            main_namespace="main",
            s3_filesystem=None,
            data_type="dataset_fitbitactivitylogs",
        )
        patch_get_parquet_data.assert_has_calls(
            [
                mock.call(
                    dataset_key=f"s3://{parquet_bucket_name}/staging/parquet/dataset_fitbitactivitylogs",
                    s3_filesystem=None,
                ),
                mock.call(
                    filter_values="some_filter",
                    dataset_key=f"s3://{parquet_bucket_name}/main/parquet/dataset_fitbitactivitylogs",
                    s3_filesystem=None,
                ),
            ]
        )
        patch_get_filter.assert_called_once_with(
            s3=s3,
            data_type="dataset_fitbitactivitylogs",
            input_bucket="test_input_bucket",
            cfn_bucket="test_cfn_bucket",
            staging_namespace="staging",
        )
        patch_check_valid.assert_has_calls(
            [
                mock.call(valid_staging_dataset, "staging"),
                mock.call(valid_staging_dataset, "main"),
            ]
        )
        mocked_compare_datasets.assert_called_once_with(
            data_type="dataset_fitbitactivitylogs",
            staging_dataset=valid_staging_dataset,
            main_dataset=valid_staging_dataset,
            staging_namespace="staging",
            main_namespace="main",
        )


@mock.patch("src.glue.jobs.compare_parquet_datasets.compare_datasets_and_output_report")
def test_that_compare_datasets_by_data_type_raises_exception_if_input_has_no_common_cols(
    mocked_compare_datasets,
    parquet_bucket_name,
    valid_staging_dataset,
    s3,
):
    with mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_parquet_dataset",
        return_value=valid_staging_dataset,
    ), mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_exports_filter_values",
    ), mock.patch(
        "src.glue.jobs.compare_parquet_datasets.has_common_cols",
        return_value=False,
    ):
        with pytest.raises(
            ValueError, match="Datasets have no common columns to merge on."
        ):
            compare_parquet.compare_datasets_by_data_type(
                s3=s3,
                cfn_bucket="test_cfn_bucket",
                input_bucket="test_input_bucket",
                parquet_bucket=parquet_bucket_name,
                staging_namespace="staging",
                main_namespace="main",
                s3_filesystem=None,
                data_type="dataset_fitbitactivitylogs",
            )
            mocked_compare_datasets.assert_not_called()


def test_that_has_parquet_files_returns_false_with_incorrect_location(
    mock_s3_environment, mock_s3_bucket
):
    mock_s3_environment.put_object(
        Bucket=mock_s3_bucket, Key="incorrect_location/file1.parquet", Body="data"
    )
    assert (
        compare_parquet.has_parquet_files(
            mock_s3_environment, mock_s3_bucket, "test", "test_data"
        )
        == False
    )


def test_that_has_parquet_files_returns_false_with_no_files(
    mock_s3_environment, mock_s3_bucket
):
    assert (
        compare_parquet.has_parquet_files(
            mock_s3_environment, mock_s3_bucket, "test", "test_data"
        )
        == False
    )


def test_that_has_parquet_files_returns_false_with_no_parquet_files(
    mock_s3_environment, mock_s3_bucket
):
    mock_s3_environment.put_object(
        Bucket=mock_s3_bucket, Key="test/parquet/test_data/file1.txt", Body="data"
    )
    mock_s3_environment.put_object(
        Bucket=mock_s3_bucket, Key="test/parquet/test_data/file2.csv", Body="data"
    )
    assert (
        compare_parquet.has_parquet_files(
            mock_s3_environment, mock_s3_bucket, "test", "test_data"
        )
        == False
    )


def test_that_has_parquet_files_returns_true_with_parquet_files(
    mock_s3_environment, mock_s3_bucket
):
    mock_s3_environment.put_object(
        Bucket=mock_s3_bucket, Key="test/parquet/test_data/file1.parquet", Body="data"
    )
    mock_s3_environment.put_object(
        Bucket=mock_s3_bucket, Key="test/parquet/test_data/file2.txt", Body="data"
    )
    assert (
        compare_parquet.has_parquet_files(
            mock_s3_environment, mock_s3_bucket, "test", "test_data"
        )
        == True
    )


def test_that_upload_reports_to_s3_has_expected_calls(s3):
    """For this test, the empty dataframe should not have a report saved to S3"""
    df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    df2 = pd.DataFrame()
    df3 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]})

    # create our test reports mirroring the functionality in main
    ReportParams = namedtuple("ReportParams", ["file_name", "content"])
    reports = [
        ReportParams(
            file_name="file1.csv",
            content=compare_parquet.convert_dataframe_to_text(df1),
        ),
        ReportParams(
            file_name="file2.csv",
            content=compare_parquet.convert_dataframe_to_text(df2),
        ),
        ReportParams(
            file_name="file3.csv",
            content=compare_parquet.convert_dataframe_to_text(df3),
        ),
    ]

    with mock.patch.object(s3, "put_object") as mock_put_object:
        compare_parquet.upload_reports_to_s3(
            s3=s3,
            reports=reports,
            parquet_bucket="my_bucket",
            data_type="my_data_type",
            staging_namespace="my_namespace",
        )
        mock_put_object.assert_has_calls(
            [
                mock.call(
                    Bucket="my_bucket",
                    Key=compare_parquet.get_s3_file_key_for_comparison_results(
                        staging_namespace="my_namespace",
                        data_type="my_data_type",
                        file_name="file1.csv",
                    ),
                    Body=compare_parquet.convert_dataframe_to_text(df1),
                ),
                mock.call(
                    Bucket="my_bucket",
                    Key=compare_parquet.get_s3_file_key_for_comparison_results(
                        staging_namespace="my_namespace",
                        data_type="my_data_type",
                        file_name="file3.csv",
                    ),
                    Body=compare_parquet.convert_dataframe_to_text(df3),
                ),
            ]
        )
