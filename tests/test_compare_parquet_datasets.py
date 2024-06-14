from io import BytesIO, StringIO
import json
from unittest import mock
import zipfile

import datacompy
from moto import mock_s3
import pandas as pd
from pandas.testing import assert_frame_equal
import pyarrow
from pyarrow import fs, parquet
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pytest

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
    ],
    ids=[
        "filename_with_date_range",
        "filename_with_end_date",
        "filename_with_subtype_and_date_range",
        "filename_with_subtype_and_end_date",
        "filename_with_no_date",
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
    ],
    ids=[
        "data_type_with_date_range",
        "data_type",
        "data_type_with_subtype",
    ],
)
def test_that_get_data_type_from_filename_returns_expected(filename, expected):
    result = compare_parquet.get_data_type_from_filename(filename)
    assert expected == result


@mock_s3
def test_that_get_json_files_in_zip_from_s3_returns_expected_filelist(s3):
    # Set up the mock S3 service
    input_bucket = "test-input-bucket"
    s3.create_bucket(Bucket=input_bucket)

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
    s3_uri = f"s3://{input_bucket}/test.zip"
    s3.put_object(Bucket=input_bucket, Key="test.zip", Body=zip_buffer.getvalue())

    result = compare_parquet.get_json_files_in_zip_from_s3(s3, input_bucket, s3_uri)

    # Verify the result
    assert result == ["file1.json", "file2.json"]


@mock_s3
def test_that_get_integration_test_exports_json_success(s3):
    staging_namespace = "staging"
    # Set up the mock S3 service
    mock_cfn_bucket = "test-cfn-bucket"
    s3.create_bucket(Bucket=mock_cfn_bucket)

    # Create a sample exports.json file content
    exports_content = ["file1.json", "file2.json", "file3.json"]
    exports_json = json.dumps(exports_content)

    # Upload the exports.json file to the mock S3 bucket
    s3.put_object(
        Bucket=mock_cfn_bucket,
        Key=f"{staging_namespace}/integration_test_exports.json",
        Body=exports_json,
    )

    result = compare_parquet.get_integration_test_exports_json(
        s3, mock_cfn_bucket, staging_namespace
    )
    assert result == exports_content


@mock_s3
def test_that_get_integration_test_exports_json_file_not_exist(s3):
    # Set up the mock S3 service
    mock_cfn_bucket = "test-cfn-bucket"
    s3.create_bucket(Bucket=mock_cfn_bucket)
    staging_namespace = "staging"

    # Call the function and expect an exception
    with pytest.raises(s3.exceptions.NoSuchKey):
        compare_parquet.get_integration_test_exports_json(
            s3, mock_cfn_bucket, staging_namespace
        )


@mock_s3
@pytest.mark.parametrize(
    "json_body",
    [(""), ("{invalid json}")],
    ids=[
        "empty_json",
        "invalid_json",
    ],
)
def test_that_get_integration_test_exports_json_throws_json_decode_error(s3, json_body):
    # Set up the mock S3 service
    mock_cfn_bucket = "test-cfn-bucket"
    s3.create_bucket(Bucket=mock_cfn_bucket)
    staging_namespace = "staging"

    # Upload an invalid exports.json file to the mock S3 bucket
    s3.put_object(
        Bucket=mock_cfn_bucket,
        Key=f"{staging_namespace}/integration_test_exports.json",
        Body=json_body,
    )

    # Call the function and expect a JSON decode error
    with pytest.raises(json.JSONDecodeError):
        compare_parquet.get_integration_test_exports_json(
            s3, mock_cfn_bucket, staging_namespace
        )


@pytest.mark.parametrize(
    "data_type, filelist, expected_filter",
    [
        (
            "dataset_healthkitv2activitysummaries",
            ["s3://bucket/adults_v1/file1.zip", "s3://bucket/pediatric_v1/file2.zip"],
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
        (
            "dataset_enrolledparticipants",
            ["s3://bucket/adults_v1/file1.zip"],
            (ds.field("cohort") == "adults_v1")
            & (ds.field("export_end_date").isin(["2022-10-27T00:00:00"])),
        ),
        (
            "dataset_healthkitv2samples",
            ["s3://bucket/pediatric_v1/file1.zip"],
            (ds.field("cohort") == "pediatric_v1")
            & (ds.field("export_end_date").isin(["2022-10-24T00:00:00"])),
        ),
        ("dataset_googlefitsamples", ["s3://bucket/adults_v1/file1.zip"], None),
    ],
    ids=[
        "empty_filelist",
        "adults_cohort_match",
        "peds_cohort_match",
        "no_data_type_match",
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
            "EnrolledParticipants_20221027.json",
            "HealthKitV2Samples_20221024.json",
        ]

        exports_filter = compare_parquet.get_exports_filter_values(
            s3,
            data_type,
            input_bucket="test_input_bucket",
            cfn_bucket="test_cfn_bucket",
            staging_namespace="staging",
        )
        # handle condition when the filter is None
        if expected_filter is not None:
            assert exports_filter.equals(expected_filter)
        else:
            assert exports_filter == expected_filter


def test_that_get_filtered_main_dataset_raises_attr_error_if_no_datasets_exist():
    """Mirrors the same test as above"""
    pass


def test_that_get_filtered_main_dataset_returns_expected_results(
    s3
):
    """This will check that the main dataset is being chunked and filtered correctly
    based on what is the staging dataset

    Test cases:
    - None staging dataset?
    - empty staging dataset
    - staging dataset with no UIDs match in main dataset
    - staging dataset with some UIDs match in main dataset
    - empty main dataset
    - None main dataset?
    - None exports_filter should get the entire dataset and throw no error

    """

    # Setup S3 bucket and data
    mock_parquet_bucket = "test-processed-bucket"
    dataset_key = "test-dataset/main_dataset.parquet"
    s3.create_bucket(Bucket=mock_parquet_bucket)

    # Create a sample dataframe and upload as a parquet dataset
    test_data = pd.DataFrame(
        {
            "cohort": ["pediatric_v1", "adult_v1", "pediatric_v1"],
            "export_end_date": [
                "2022-10-24T00:00:00",
                "2022-10-25T00:00:00",
                "2022-10-23T00:00:00",
            ],
            "value": [1, 2, 3],
        }
    )

    table = pyarrow.Table.from_pandas(test_data)

    # create datasets
    buffer = BytesIO()
    table = pyarrow.Table.from_pandas(test_data)
    parquet.write_table(table, buffer)
    buffer.seek(0)
    s3.put_object(Bucket=mock_parquet_bucket, Key=dataset_key, Body=buffer.getvalue())

    exports_filter = (ds.field("cohort") == "pediatric_v1") & (
        ds.field("export_end_date").isin(["2022-10-24T00:00:00", "2022-10-23T00:00:00"])
    )

        # Mock S3FileSystem to behave like a real S3 filesystem
    with mock.patch('pyarrow.fs.S3FileSystem', autospec=True) as mock_s3_filesystem:
        # Create an instance of the mock
        mock_s3_filesystem_instance = mock_s3_filesystem.return_value

        # Ensure it behaves like the real S3FileSystem
        mock_s3_filesystem_instance.get_file_info.return_value = [mock.MagicMock()]
        # Call the function to test
        result = compare_parquet.get_filtered_main_dataset(
            exports_filter,
            f"s3://{mock_parquet_bucket}/{dataset_key}",
            mock_s3_filesystem_instance,
        )

    # Verify the result
    expected_data = pd.DataFrame({
        "cohort": ["pediatric_v1", "pediatric_v1"],
        "export_end_date": ["2022-10-24T00:00:00", "2022-10-23T00:00:00"],
        "value": [1, 3],
    })
    pd.testing.assert_frame_equal(result, expected_data)


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
    s3, parquet_bucket_name, staging_dataset_empty
):
    with mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_parquet_dataset",
        return_value=staging_dataset_empty,
    ), mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_exports_filter_values",
    ), mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_filtered_main_dataset",
        return_value=staging_dataset_empty,
    ):
        compare_dict = compare_parquet.compare_datasets_by_data_type(
            s3=s3,
            cfn_bucket="test_cfn_bucket",
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
    mocked_compare_datasets, parquet_bucket_name, valid_staging_dataset, s3
):
    with mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_parquet_dataset",
        return_value=valid_staging_dataset,
    ), mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_exports_filter_values",
    ), mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_filtered_main_dataset",
        return_value=valid_staging_dataset,
    ):
        compare_parquet.compare_datasets_by_data_type(
            s3=s3,
            cfn_bucket="test_cfn_bucket",
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
    s3,
):
    with mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_parquet_dataset",
        return_value=valid_staging_dataset,
    ), mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_exports_filter_values",
    ), mock.patch(
        "src.glue.jobs.compare_parquet_datasets.get_filtered_main_dataset",
        return_value=valid_staging_dataset,
    ):
        compare_parquet.compare_datasets_by_data_type(
            s3=s3,
            cfn_bucket="test_cfn_bucket",
            parquet_bucket=parquet_bucket_name,
            staging_namespace="staging",
            main_namespace="main",
            s3_filesystem=None,
            data_type="dataset_fitbitactivitylogs",
        )
        mocked_compare_datasets.assert_not_called()
