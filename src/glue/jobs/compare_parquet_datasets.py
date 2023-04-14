import os
import json
import logging
import argparse

import boto3
import datacompy
import pandas as pd
import synapseclient
from pyarrow import fs
import pyarrow.parquet as pq

# from json_to_parquet import INDEX_FIELD_MAP

logger = logging.getLogger()
logger.setLevel(logging.INFO)

INDEX_FIELD_MAP = {
    "dataset_enrolledparticipants": ["ParticipantIdentifier"],
    "dataset_fitbitprofiles": ["ParticipantIdentifier", "ModifiedDate"],
    "dataset_fitbitdevices": ["ParticipantIdentifier", "Date"],
    "dataset_fitbitactivitylogs": ["LogId"],
    "dataset_fitbitdailydata": ["ParticipantIdentifier", "Date"],
    "dataset_fitbitintradaycombined": ["ParticipantIdentifier", "Type", "DateTime"],
    "dataset_fitbitrestingheartrates": ["ParticipantIdentifier", "Date"],
    "dataset_fitbitsleeplogs": ["LogId"],
    "dataset_healthkitv2characteristics": ["HealthKitCharacteristicKey"],
    "dataset_healthkitv2samples": ["HealthKitSampleKey"],
    "dataset_healthkitv2samples_deleted": ["HealthKitSampleKey"],
    "dataset_healthkitv2heartbeat": ["HealthKitHeartbeatSampleKey"],
    "dataset_healthkitv2statistics": ["HealthKitStatisticKey"],
    "dataset_healthkitv2clinicalrecords": ["HealthKitClinicalRecordKey"],
    "dataset_healthkitv2electrocardiogram": ["HealthKitECGSampleKey"],
    "dataset_healthkitv2workouts": ["HealthKitWorkoutKey"],
    "dataset_healthkitv2activitysummaries": ["HealthKitActivitySummaryKey"],
    "dataset_googlefitsamples": ["GoogleFitSampleKey"],
    "dataset_symptomlog": ["DataPointKey"],
}


def read_args():
    parser = argparse.ArgumentParser(
        description=(
            "Compare parquet datasets between two namespaced S3 bucket locations"
        )
    )
    parser.add_argument(
        "--staging-namespace",
        required=True,
        help="The name of the staging namespace to use",
    )
    parser.add_argument(
        "--main-namespace",
        required=True,
        help=("The name of the main namespace to use"),
    )
    parser.add_argument(
        "--parquet-bucket",
        required=True,
        help=("The name of the S3 bucket containing the S3 files to compare"),
    )
    args = parser.parse_args()
    return args


def dataset_is_empty(dataset) -> bool:
    return len(dataset.columns) == 0


def get_duplicated_index_fields(data_type: str, dataset: pd.DataFrame) -> pd.DataFrame:
    """Gets the rows of data that are duplicated based on the index columns by data type
    and returns them
    """
    index_cols = INDEX_FIELD_MAP[data_type]
    return dataset[dataset.duplicated(subset=index_cols)]


def get_duplicated_columns(dataset: pd.DataFrame) -> list:
    """Gets a list of duplicated columns in a dataframe"""
    return dataset.columns[dataset.columns.duplicated()].tolist()


def get_common_cols(staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame) -> list:
    """Gets the list of common columns between two dataframes"""
    common_cols = staging_dataset.columns.intersection(main_dataset.columns).tolist()
    return common_cols


def get_missing_cols(staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame) -> list:
    """Gets the list of missing columns present in main but not in staging"""
    missing_cols = main_dataset.columns.difference(staging_dataset.columns).tolist()
    return missing_cols


def get_additional_cols(
    staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame
) -> list:
    """Gets the list of additional columns present in staging but not in main"""
    add_cols = staging_dataset.columns.difference(main_dataset.columns).tolist()
    return add_cols


def get_S3FileSystem_from_session(
    aws_session: boto3.session.Session,
) -> fs.S3FileSystem:
    """Gets a pyarrow S3 filesystem object given an
    authenticated aws session with credentials

    Args:
        aws_session (boto3.session.Session): authenticated aws session

    Returns:
        fs.S3FileSystem: S3 filesystem object initiated from AWS credentials
    """
    session_credentials = aws_session.get_credentials()
    s3_fs = fs.S3FileSystem(
        access_key=session_credentials.access_key,
        secret_key=session_credentials.secret_key,
        session_token=session_credentials.token,
    )
    return s3_fs


def get_parquet_dataset(
    dataset_key: str, s3_filesystem: fs.S3FileSystem
) -> pd.DataFrame:
    """
    Returns a Parquet dataset on S3 as a pandas dataframe

    Args:
        dataset_key (str): The URI of the parquet dataset.
        s3_filesystem (S3FileSystem): A fs.S3FileSystem object

    Returns:
        pandas.DataFrame
    """
    table_source = dataset_key.split("s3://")[-1]
    parquet_dataset = pq.read_table(source=table_source, filesystem=s3_filesystem)
    return parquet_dataset.to_pandas()


def get_folders_in_s3_bucket(
    s3: boto3.client, bucket_name: str, namespace: str
) -> list:
    """Gets the folders in the S3 bucket under the specific namespace

    Args:
        s3 (boto3.client): authenticated s3 client
        bucket_name (str): name of the S3 bucket to look into
        namespace (str): namespace of the path to look for folders in

    Returns:
        list: folder names inside S3 bucket
    """
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{namespace}/parquet/")
    if "Contents" in response.keys():
        contents = response["Contents"]
        folders = [
            content["Key"].split("/")[-1]
            for content in contents
            if content["Key"].split("/")[-1] != "owner.txt"
        ]
    else:
        folders = []
    return folders


def keep_common_rows_cols(
    data_type: str, staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame
) -> dict:
    """This function keeps the common columns between the two
        given datasets. This function also merges on the index fields in
        common between the two datasets so that the dataset can be
        reduced to the same dimensions and be comparable

    Args:
        data_type (str): current data type
        staging_dataset (pd.DataFrame): "new" data that is to go through processing
        main_dataset (pd.DataFrame): "established" dataset

    Returns:
        dict of staging dataset and main datasets
    """
    index_cols = INDEX_FIELD_MAP[data_type]
    common_cols = get_common_cols(staging_dataset, main_dataset)
    # convert to having same columns
    staging_dataset_subset = staging_dataset[common_cols].add_suffix("_staging")
    main_dataset_subset = main_dataset[common_cols].add_suffix("_main")

    # merging on index to get rid of extra rows
    merged_dataset = staging_dataset_subset.merge(
        main_dataset_subset,
        left_on=[f"{col}_staging" for col in index_cols],
        right_on=[f"{col}_main" for col in index_cols],
        how="inner",
    )
    staging_dataset_common = merged_dataset[staging_dataset_subset.columns]
    main_dataset_common = merged_dataset[main_dataset_subset.columns]

    staging_dataset_common.columns = staging_dataset_common.columns.str.removesuffix(
        "_staging"
    )
    main_dataset_common.columns = main_dataset_common.columns.str.removesuffix("_main")
    return {"staging": staging_dataset_common, "main": main_dataset_common}


def compare_column_data_types(
    data_type: str, staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame
) -> list:
    """This compares the column data types of the common columns between
    two datasets and creates a message if there are differences
    """
    compare_msg = []
    common_cols = get_common_cols(staging_dataset, main_dataset)
    for common_col in common_cols:
        if staging_dataset[common_col].dtype != main_dataset[common_col].dtype:
            compare_msg.append(
                (
                    f"{data_type}: Staging dataset's {common_col} has data type {staging_dataset[common_col].dtype}.\n"
                    f"Main dataset's {common_col} has data type {staging_dataset[common_col].dtype}."
                )
            )
    return compare_msg


def compare_column_names(
    data_type: str, staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame
) -> list:
    """This compares the column names between two datasets and outputs a
    message if there are any differences"""
    compare_msg = []
    missing_cols = get_missing_cols(staging_dataset, main_dataset)
    add_cols = get_additional_cols(staging_dataset, main_dataset)
    if missing_cols:
        compare_msg.append(
            f"{data_type}: Staging dataset has the following missing columns:\n{str(missing_cols)}"
        )
    if add_cols:
        compare_msg.append(
            f"{data_type}: Staging dataset has the following additional columns:\n{str(add_cols)}"
        )
    return compare_msg


def compare_column_vals(
    data_type: str, staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame
) -> list:
    """This compares the column values between the common columns of two
    datasets after the datasets have been reduced to the same dimensions
    and outputs a message if any columns have all of their values as different"""
    compare_msg = []
    dataset_dict = keep_common_rows_cols(data_type, staging_dataset, main_dataset)
    dataset_diff = dataset_dict["staging"].compare(
        other=dataset_dict["main"], align_axis="columns", keep_shape=True
    )
    dataset_diff_cnt = dataset_diff.isna().sum()
    dataset_diff_cnt = dataset_diff_cnt[dataset_diff_cnt == 0].to_dict()
    if dataset_diff_cnt:
        compare_msg.append(
            f"{data_type}: Staging dataset has column(s) with value differences with the main dataset:\n"
            f"{str(list(dataset_diff_cnt.keys()))}"
        )
    return compare_msg


def compare_dataset_data_types(
    s3: boto3.client, bucket_name: str, staging_namespace: str, main_namespace: str
) -> list:
    """This looks at the current datatype folders in the S3 bucket between the
    two namespaced paths and outputs a message if there are any differences
    in the datatype folders"""
    compare_msg = []
    staging_datatype_folders = get_folders_in_s3_bucket(
        s3, bucket_name, namespace=staging_namespace
    )
    main_datatype_folders = get_folders_in_s3_bucket(
        s3, bucket_name, namespace=main_namespace
    )
    missing_datatypes = list(set(main_datatype_folders) - set(staging_datatype_folders))
    add_datatypes = list(set(staging_datatype_folders) - set(main_datatype_folders))

    if missing_datatypes:
        compare_msg.append(
            f"Staging dataset has the following missing data types: {str(missing_datatypes)}"
        )

    if add_datatypes:
        compare_msg.append(
            f"Staging dataset has the following additional data types: {str(add_datatypes)}"
        )
    return compare_msg


def compare_num_of_rows(
    data_type: str, staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame
) -> list:
    """This compares the number of rows between two datasets and outputs a message
    if there are any row count differences"""
    compare_msg = []
    if staging_dataset.shape[0] != main_dataset.shape[0]:
        compare_msg.append(
            f"{data_type}: Staging dataset has {staging_dataset.shape[0]} rows of data.\n"
            f"Main dataset has {main_dataset.shape[0]} rows of data."
        )
    return compare_msg


def compare_dataset_row_vals(
    data_type: str, staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame
) -> list:
    """This compares the row values between the two
    datasets after the datasets have been reduced to the same dimensions
    and outputs a message if any rows have differences"""
    compare_msg = []
    dataset_dict = keep_common_rows_cols(data_type, staging_dataset, main_dataset)
    dataset_diff = dataset_dict["staging"].compare(
        other=dataset_dict["main"], align_axis="columns", keep_equal=False
    )
    if not dataset_diff.empty:
        compare_msg.append(
            f"{data_type}: Staging dataset has value difference(s) with the main dataset."
            f"Here is an example:\n{dataset_diff.head(1)}"
        )
    return compare_msg


def get_data_types_to_compare(
    s3: boto3.client, bucket_name: str, staging_namespace: str, main_namespace: str
) -> list:
    """This gets the common data types to run the comparison of the parquet datasets from
    the two namespaced paths on based on the folders in the s3 bucket"""
    staging_datatype_folders = get_folders_in_s3_bucket(
        s3, bucket_name, namespace=staging_namespace
    )
    main_datatype_folders = get_folders_in_s3_bucket(
        s3, bucket_name, namespace=main_namespace
    )
    return list(set(staging_datatype_folders + main_datatype_folders))


def compare_datasets_and_export_report(
    data_type: str,
    staging_dataset: pd.DataFrame,
    main_dataset: pd.DataFrame,
    staging_namespace: str,
    main_namespace: str,
) -> str:
    """This method prints out a human-readable report summarizing and
    sampling differences between datasets for the given data type.
    A full list of comparisons can be found in the datacompy package site.

    Returns:
        str: large string block of the report
    """
    compare = datacompy.Compare(
        df1=staging_dataset,
        df2=main_dataset,
        join_columns=INDEX_FIELD_MAP[data_type],
        abs_tol=0,  # Optional, defaults to 0
        rel_tol=0,  # Optional, defaults to 0
        df1_name=staging_namespace,  # Optional, defaults to 'df1'
        df2_name=main_namespace,  # Optional, defaults to 'df2'
    )
    compare.matches(ignore_extra_columns=False)
    return compare.report()


def add_additional_msg_to_comparison_report(
    comparison_report: str, add_msgs: list
) -> str:
    """This adds additional messages to the comparison report. Currently, this adds
        messages that specify the names of columns that are different between the
        two datasets

    Args:
        comparison_report (str): report generated using datacompy
        add_msgs (list): list of additional messages to include at the bottom of the report

    Returns:
        str: updated comparison report with more specific messages
    """
    # does some formatting.
    joined_add_msgs = "\n".join(add_msgs)
    updated_comparison_report = (
        f"{comparison_report}"
        f"Column Name Differences\n"
        f"-----------------------\n\n{joined_add_msgs}"
    )
    return updated_comparison_report


def is_valid_dataset(dataset: pd.DataFrame, namespace: str) -> dict:
    """Checks whether the individual dataset is valid under the following criteria:
        - no duplicated columns
        - dataset is not empty (aka has columns)
        before it can go through the comparison

    Args:
        dataset (pd.DataFrame): dataset to be validated
        namespace (str): namespace for the dataset

    Returns:
        dict: containing boolean of the validation result and string message
    """
    # Check that datasets have no emptiness, duplicated columns, or have columns in common
    if dataset_is_empty(dataset):
        msg = f"{namespace} dataset has no data. Comparison cannot continue."
        return {"result": False, "msg": msg}
    elif get_duplicated_columns(dataset) != []:
        msg = (
            f"{namespace} dataset has duplicated columns. Comparison cannot continue.\n"
            f"Duplicated columns:{str(get_duplicated_columns(dataset))}"
        )
        return {"result": False, "msg": msg}
    else:
        msg = f"{namespace} dataset has been validated."
        return {"result": True, "msg": msg}


def compare_datasets_by_data_type(
    args, s3_filesystem: fs.S3FileSystem, data_type: str
) -> str:
    """This runs the bulk of the comparison functions from beginning to end by data type

    Args:
        args: arguments from command line
        s3_filesystem (fs.S3FileSystem): filesystem instantiated by aws credentials
        data_type (str): data type to be compared for the given datasets

    Returns:
        str: final report on the datasets for the given data type
    """
    data_type = "dataset_fitbitactivitylogs"
    header_msg = (
        f"\n\nParquet Dataset Comparison running for Data Type: {data_type}"
        f"\n-------------------------------------------------------------------------------\n\n"
    )
    staging_dataset = get_parquet_dataset(
        dataset_key=f"s3://{args.parquet_bucket}/{args.staging_namespace}/parquet/{data_type}/",
        s3_filesystem=s3_filesystem,
    )
    main_dataset = get_parquet_dataset(
        dataset_key=f"s3://{args.parquet_bucket}/{args.main_namespace}/parquet/{data_type}/",
        s3_filesystem=s3_filesystem,
    )
    # go through specific validation for each dataset prior to comparison
    staging_is_valid_result = is_valid_dataset(staging_dataset, args.staging_namespace)
    main_is_valid_result = is_valid_dataset(main_dataset, args.main_namespace)
    if (
        staging_is_valid_result["result"] == False
        or main_is_valid_result["result"] == False
    ):
        comparison_report = f"{header_msg}{staging_is_valid_result['msg']}\n{main_is_valid_result['msg']}"
        return comparison_report

    # check that they have columns in common to compare
    elif get_common_cols(staging_dataset, main_dataset) == []:
        comparison_report = (
            f"{header_msg}{args.staging_namespace} dataset and {args.main_namespace} has no columns in common."
            f"Comparison cannot continue."
        )
        return comparison_report
    else:
        add_msgs = compare_column_names(data_type, staging_dataset, main_dataset)
        comparison_report = compare_datasets_and_export_report(
            data_type,
            staging_dataset,
            main_dataset,
            args.staging_namespace,
            args.main_namespace,
        )
        comparison_report = f"{header_msg}{comparison_report}"
        comparison_report = add_additional_msg_to_comparison_report(
            comparison_report, add_msgs
        )
        return comparison_report


def main():
    args = read_args()
    comparison_result = {}
    s3 = boto3.client("s3")
    aws_session = boto3.session.Session(profile_name="default", region_name="us-east-1")
    fs = get_S3FileSystem_from_session(aws_session)

    comparison_result["missing_data_types"] = compare_dataset_data_types(
        s3,
        args.parquet_bucket,
        main_namespace=args.main_namespace,
        staging_namespace=args.staging_namespace,
    )
    data_types_to_compare = get_data_types_to_compare(
        s3,
        args.parquet_bucket,
        main_namespace=args.main_namespace,
        staging_namespace=args.staging_namespace,
    )
    for data_type in data_types_to_compare:
        comparison_report = compare_datasets_by_data_type(args, fs, data_type)
        print(comparison_report)
    return


if __name__ == "__main__":
    main()
