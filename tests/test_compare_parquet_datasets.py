from unittest import mock

import pandas as pd
from pyarrow import fs
from moto import mock_s3
from pandas.testing import assert_frame_equal

from src.glue.jobs import compare_parquet_datasets as compare_parquet


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
def test_that_get_parquet_dataset_returns_empty_if_no_datasets_exist(
    s3, mock_s3_filesystem, valid_staging_dataset, parquet_bucket_name
):
    data = valid_staging_dataset.to_parquet()
    s3.create_bucket(Bucket=parquet_bucket_name)
    s3.put_object(
        Bucket=parquet_bucket_name,
        Key="staging/parquet/dataset_fitbitactivitylogs/test.parquet",
        Body=data,
    )

    file_key = "staging/parquet/dataset_fitbitactivitylogs/test.parquet"
    parquet_dataset = compare_parquet.get_parquet_dataset(
        dataset_key=f"{parquet_bucket_name}/{file_key}",
        s3_filesystem=mock_s3_filesystem,
    )
    assert parquet_dataset == None


@mock_s3
def test_that_get_parquet_dataset_returns_dataset_if_datasets_exist(
    s3, mock_s3_filesystem, valid_staging_dataset, parquet_bucket_name
):
    pass


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
def test_that_compare_dataset_data_types_returns_empty_msg_if_datatypes_are_equal(
    s3, parquet_bucket_name
):
    s3.create_bucket(Bucket=parquet_bucket_name)
    for namespace in ["staging", "main"]:
        s3.put_object(
            Bucket=parquet_bucket_name,
            Key=f"{namespace}/parquet/dataset_fitbitactivitylogs",
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
    assert (
        is_valid_result["result"] is True
        and is_valid_result["msg"] == "staging dataset has been validated."
    )


def test_that_is_valid_dataset_returns_false_if_dataset_is_empty(staging_dataset_empty):
    is_valid_result = compare_parquet.is_valid_dataset(staging_dataset_empty, "staging")
    assert (
        is_valid_result["result"] is False
        and is_valid_result["msg"]
        == "staging dataset has no data. Comparison cannot continue."
    )


def test_that_is_valid_dataset_returns_false_if_dataset_has_dup_cols(
    staging_dataset_with_dup_cols,
):
    is_valid_result = compare_parquet.is_valid_dataset(
        staging_dataset_with_dup_cols, "staging"
    )
    assert is_valid_result["result"] is False and is_valid_result["msg"] == (
        "staging dataset has duplicated columns. Comparison cannot continue.\n"
        "Duplicated columns:['EndDate']"
    )


def test_that_is_valid_dataset_returns_true_if_dataset_has_empty_cols(
    staging_dataset_with_empty_columns,
):
    is_valid_result = compare_parquet.is_valid_dataset(
        staging_dataset_with_empty_columns, "staging"
    )
    assert (
        is_valid_result["result"] is True
        and is_valid_result["msg"] == "staging dataset has been validated."
    )


def test_that_compare_datasets_and_output_report_outputs_something_if_input_is_valid(
    valid_staging_dataset, valid_main_dataset
):
    comparison_report = compare_parquet.compare_datasets_and_output_report(
        "dataset_fitbitactivitylogs",
        valid_staging_dataset,
        valid_main_dataset,
        "staging",
        "main",
    )
    assert comparison_report is not False


def test_that_add_additional_msg_to_comparison_report_outputs_correct_updated_msg():
    comparison_report = "some string\n\n"
    add_msgs = ["one message", "two message"]
    result = compare_parquet.add_additional_msg_to_comparison_report(
        comparison_report, add_msgs
    )
    assert result == (
        "some string\n\nColumn Name Differences\n"
        "-----------------------\n\n"
        "one message\ntwo message"
    )
