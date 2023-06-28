import argparse
from unittest import mock

import datacompy
from moto import mock_s3
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from pyarrow import fs, parquet

from src.glue.jobs import compare_parquet_datasets as compare_parquet


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


def test_that_dataframe_to_text_returns_str(valid_staging_dataset):
    staging_content = compare_parquet.convert_dataframe_to_text(valid_staging_dataset)
    assert isinstance(staging_content, str)


def test_that_dataframe_to_text_returns_valid_format_for_s3_put_object(
    s3, parquet_bucket_name, valid_staging_dataset
):
    # shouldn't throw a botocore.exceptions.ParamValidationError
    s3.create_bucket(Bucket=parquet_bucket_name)
    staging_content = compare_parquet.convert_dataframe_to_text(valid_staging_dataset)
    s3.put_object(
        Bucket=parquet_bucket_name,
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
        join_columns="LogId",
        df1_name="staging",  # Optional, defaults to 'df1'
        df2_name="main",  # Optional, defaults to 'df2'
        cast_column_names_lower=False,
    )
    staging_rows = compare_parquet.compare_row_diffs(compare, namespace="staging")
    main_rows = compare_parquet.compare_row_diffs(compare, namespace="main")
    assert staging_rows.empty
    assert_frame_equal(
        main_rows.sort_values(by='LogId').reset_index(drop=True),
        pd.DataFrame(
            {
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
        ).sort_values(by='LogId').reset_index(drop=True),
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
        compare_dict = compare_parquet.compare_datasets_by_data_type(
            parquet_bucket=parquet_bucket_name,
            staging_namespace="staging",
            main_namespace="main",
            s3_filesystem=None,
            data_type="dataset_fitbitactivitylogs",
        )
        assert compare_dict["comparison_report"] == (
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
@mock.patch(
    "src.glue.jobs.compare_parquet_datasets.has_common_cols", return_value=False
)
def test_that_compare_datasets_by_data_type_does_not_call_compare_datasets_by_data_type_if_input_has_no_common_cols(
    mocked_has_common_cols,
    mocked_compare_datasets,
    parquet_bucket_name,
    valid_staging_dataset,
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
