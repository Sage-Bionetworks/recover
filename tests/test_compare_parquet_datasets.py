import argparse
from unittest import mock

from moto import mock_s3
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from pyarrow import fs, parquet

from src.glue.jobs import compare_parquet_datasets as compare_parquet


def test_that_validate_args_raises_exception_when_input_value_is_empty_string():
    with pytest.raises(argparse.ArgumentTypeError):
        compare_parquet.validate_args(value="")


def test_that_validate_args_returns_value_when_value_is_not_an_empty_string():
    assert compare_parquet.validate_args(value="TEST") == "TEST"


def test_that_get_s3_file_key_for_comparison_results_returns_correct_filepath_for_data_types_compare(
    parquet_bucket_name,
):
    file_key = compare_parquet.get_s3_file_key_for_comparison_results(
        parquet_bucket_name, "staging", data_type=None
    )
    assert (
        file_key
        == "test-parquet-bucket/staging/comparison_result/data_types_compare.txt"
    )


def test_that_get_s3_file_key_for_comparison_results_has_expected_filepath_for_specific_data_type(
    parquet_bucket_name,
):
    file_key = compare_parquet.get_s3_file_key_for_comparison_results(
        parquet_bucket_name, "staging", data_type="dataset_fitbitactivitylogs"
    )
    assert (
        file_key
        == "test-parquet-bucket/staging/comparison_result/dataset_fitbitactivitylogs_parquet_compare.txt"
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


@mock_s3
def test_that_get_parquet_dataset_raises_attr_error_if_no_datasets_exist(
    s3, mock_s3_filesystem, parquet_bucket_name
):
    file_key = "staging/parquet/dataset_fitbitactivitylogs/test.parquet"
    with mock.patch.object(parquet, "read_table", return_value=None) as mock_method:
        with pytest.raises(AttributeError):
            parquet_dataset = compare_parquet.get_parquet_dataset(
                dataset_key=f"{parquet_bucket_name}/{file_key}",
                s3_filesystem=mock_s3_filesystem,
            )


@mock_s3
def test_that_get_parquet_dataset_returns_dataset_if_datasets_exist(
    s3,
    mock_s3_filesystem,
    valid_staging_parquet_object,
    valid_staging_dataset,
    parquet_bucket_name,
):
    file_key = "staging/parquet/dataset_fitbitactivitylogs/test.parquet"
    with mock.patch.object(
        parquet, "read_table", return_value=valid_staging_parquet_object
    ) as mock_method:
        parquet_dataset = compare_parquet.get_parquet_dataset(
            dataset_key=f"{parquet_bucket_name}/{file_key}",
            s3_filesystem=mock_s3_filesystem,
        )

        assert_frame_equal(
            parquet_dataset.reset_index(drop=True),
            valid_staging_dataset.reset_index(drop=True),
        )


@mock_s3
def test_that_get_folders_in_s3_bucket_returns_empty_list_if_no_folders(
    s3, parquet_bucket_name
):
    s3.create_bucket(Bucket=parquet_bucket_name)
    result = compare_parquet.get_folders_in_s3_bucket(
        s3, bucket_name=parquet_bucket_name, namespace="staging"
    )
    assert result == []


@mock_s3
def test_that_get_folders_in_s3_bucket_returns_list_if_folder_exists(
    s3, parquet_bucket_name
):
    s3.create_bucket(Bucket=parquet_bucket_name)

    for obj in [
        "dataset_fitbitactivitylogs",
        "dataset_fitbitactivitylogs/test.txt",
        "dataset_fitbitprofiles",
        "dataset_fitbitactivitylogs/test2.txt",
        "dataset_fitbitprofiles/test.txt",
    ]:
        s3.put_object(Bucket=parquet_bucket_name, Key=f"staging/parquet/{obj}")

    result = compare_parquet.get_folders_in_s3_bucket(
        s3, bucket_name=parquet_bucket_name, namespace="staging"
    )
    assert result == ["dataset_fitbitactivitylogs", "dataset_fitbitprofiles"]


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
    assert test_missing_cols == ["EndDate", "StartDate"]


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
        "['ActiveDuration', 'Calories', 'EndDate', 'LogId', 'StartDate']",
        "dataset_fitbitactivitylogs: Staging dataset has the following additional columns:\n"
        "['OriginalDuration', 'ParticipantIdentifier', 'Steps']",
    ]


@mock_s3
def test_that_get_data_types_to_compare_returns_correct_datatypes_in_common(
    s3, parquet_bucket_name
):
    s3.create_bucket(Bucket=parquet_bucket_name)
    for namespace in ["staging", "main"]:
        s3.put_object(
            Bucket=parquet_bucket_name,
            Key=f"{namespace}/parquet/dataset_fitbitactivitylogs/test.txt",
        )
        s3.put_object(
            Bucket=parquet_bucket_name,
            Key=f"{namespace}/parquet/dataset_fitbitdevices/test.txt",
        )

    data_types = compare_parquet.get_data_types_to_compare(
        s3, parquet_bucket_name, staging_namespace="staging", main_namespace="main"
    )
    assert set(data_types) == set(
        ["dataset_fitbitdevices", "dataset_fitbitactivitylogs"]
    )


@mock_s3
def test_that_get_data_types_to_compare_returns_empty_list_if_no_data_types_in_common(
    s3, parquet_bucket_name
):
    s3.create_bucket(Bucket=parquet_bucket_name)
    s3.put_object(
        Bucket=parquet_bucket_name,
        Key=f"staging/parquet/dataset_fitbitactivitylogs/test.txt",
    )
    s3.put_object(
        Bucket=parquet_bucket_name,
        Key=f"main/parquet/dataset_fitbitdevices/test.txt",
    )

    data_types = compare_parquet.get_data_types_to_compare(
        s3, parquet_bucket_name, staging_namespace="staging", main_namespace="main"
    )
    assert data_types == []


@mock_s3
def test_that_compare_dataset_data_types_returns_empty_msg_if_datatypes_are_equal(
    s3, parquet_bucket_name
):
    s3.create_bucket(Bucket=parquet_bucket_name)
    for namespace in ["staging", "main"]:
        s3.put_object(
            Bucket=parquet_bucket_name,
            Key=f"{namespace}/parquet/dataset_fitbitactivitylogs/test.txt",
        )
    compare_msg = compare_parquet.compare_dataset_data_types(
        s3, parquet_bucket_name, staging_namespace="staging", main_namespace="main"
    )
    assert compare_msg == []


@mock_s3
def test_that_compare_dataset_data_types_returns_msg_if_datatypes_are_not_equal(
    s3, parquet_bucket_name
):
    s3.create_bucket(Bucket=parquet_bucket_name)
    for datatype in [
        "dataset_fitbitactivitylogs/test.txt",
        "dataset_fitbitintradaycombined/test.txt",
    ]:
        s3.put_object(Bucket=parquet_bucket_name, Key=f"staging/parquet/{datatype}")

    for datatype in [
        "dataset_fitbitactivitylogs/test.txt",
        "dataset_fitbitdevices/test.txt",
    ]:
        s3.put_object(Bucket=parquet_bucket_name, Key=f"main/parquet/{datatype}")

    compare_msg = compare_parquet.compare_dataset_data_types(
        s3, parquet_bucket_name, staging_namespace="staging", main_namespace="main"
    )
    assert compare_msg == [
        "Staging dataset has the following missing data types: ['dataset_fitbitdevices']",
        "Staging dataset has the following additional data types: ['dataset_fitbitintradaycombined']",
    ]


def test_that_is_valid_dataset_returns_true_if_dataset_is_valid(valid_staging_dataset):
    is_valid_result = compare_parquet.is_valid_dataset(valid_staging_dataset, "staging")
    assert is_valid_result["result"]
    assert is_valid_result["msg"] == "staging dataset has been validated."


def test_that_is_valid_dataset_returns_false_if_dataset_is_empty(staging_dataset_empty):
    is_valid_result = compare_parquet.is_valid_dataset(staging_dataset_empty, "staging")
    assert is_valid_result["result"] is False
    assert (
        is_valid_result["msg"]
        == "staging dataset has no data. Comparison cannot continue."
    )


def test_that_is_valid_dataset_returns_false_if_dataset_has_dup_cols(
    staging_dataset_with_dup_cols,
):
    is_valid_result = compare_parquet.is_valid_dataset(
        staging_dataset_with_dup_cols, "staging"
    )
    assert is_valid_result["result"] is False
    assert is_valid_result["msg"] == (
        "staging dataset has duplicated columns. Comparison cannot continue.\n"
        "Duplicated columns:['EndDate']"
    )


def test_that_is_valid_dataset_returns_true_if_dataset_has_empty_cols(
    staging_dataset_with_empty_columns,
):
    is_valid_result = compare_parquet.is_valid_dataset(
        staging_dataset_with_empty_columns, "staging"
    )
    assert is_valid_result["result"]
    assert is_valid_result["msg"] == "staging dataset has been validated."


@pytest.mark.parametrize(
    "dataset_fixture",
    ["staging_dataset_with_empty_columns", "valid_staging_dataset"],
    indirect=True,
)
def test_that_compare_datasets_and_output_report_outputs_nonempty_str_if_input_is_valid(
    dataset_fixture, valid_main_dataset
):
    comparison_report = compare_parquet.compare_datasets_and_output_report(
        "dataset_fitbitactivitylogs",
        dataset_fixture,
        valid_main_dataset,
        "staging",
        "main",
    )
    assert isinstance(comparison_report, str)
    assert comparison_report


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
        result = compare_parquet.add_additional_msg_to_comparison_report(
            comparison_report, add_msgs, msg_type="invalid_msg_type"
        )


def test_that_compare_datasets_by_data_type_returns_correct_msg_if_input_is_empty(
    parquet_bucket_name, staging_dataset_empty
):
    with mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_parquet_dataset",
        return_value=staging_dataset_empty,
    ) as mock_parquet:
        compare_msg = compare_parquet.compare_datasets_by_data_type(
            parquet_bucket=parquet_bucket_name,
            staging_namespace="staging",
            main_namespace="main",
            s3_filesystem=None,
            data_type="dataset_fitbitactivitylogs",
        )
        assert compare_msg == (
            "\n\nParquet Dataset Comparison running for Data Type: dataset_fitbitactivitylogs\n"
            "-----------------------------------------------------------------\n\n"
            "staging dataset has no data. Comparison cannot continue.\n"
            "main dataset has no data. Comparison cannot continue."
        )


@mock.patch("src.glue.jobs.compare_parquet_datasets.compare_datasets_and_output_report")
def test_that_compare_datasets_by_data_type_calls_compare_datasets_by_data_type_if_input_is_valid(
    mocked_compare_datasets, parquet_bucket_name, valid_staging_dataset
):
    with mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_parquet_dataset",
        return_value=valid_staging_dataset,
    ) as mock_parquet:
        compare_parquet.compare_datasets_by_data_type(
            parquet_bucket=parquet_bucket_name,
            staging_namespace="staging",
            main_namespace="main",
            s3_filesystem=None,
            data_type="dataset_fitbitactivitylogs",
        )
        mocked_compare_datasets.assert_called_once()


@mock.patch("src.glue.jobs.compare_parquet_datasets.compare_datasets_and_output_report")
@mock.patch("src.glue.jobs.compare_parquet_datasets.has_common_cols", return_value = False)
def test_that_compare_datasets_by_data_type_does_not_call_compare_datasets_by_data_type_if_input_has_no_common_cols(
   mocked_has_common_cols, mocked_compare_datasets, parquet_bucket_name, valid_staging_dataset
):
    with mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_parquet_dataset",
        return_value=valid_staging_dataset,
    ) as mock_parquet:
        compare_parquet.compare_datasets_by_data_type(
            parquet_bucket=parquet_bucket_name,
            staging_namespace="staging",
            main_namespace="main",
            s3_filesystem=None,
            data_type="dataset_fitbitactivitylogs",
        )
        mocked_compare_datasets.assert_not_called()
