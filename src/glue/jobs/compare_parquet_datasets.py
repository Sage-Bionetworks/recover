import os
import logging
import argparse

import boto3
import pandas as pd
import synapseclient
from pyarrow import fs
import pyarrow.parquet as pq


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


def get_duplicated_index_fields(data_type: str, dataset: pd.DataFrame) -> pd.DataFrame:
    index_cols = INDEX_FIELD_MAP[data_type]
    return dataset[dataset.duplicated(subset=index_cols)]


def get_duplicated_columns(dataset: pd.DataFrame) -> list:
    return dataset.columns[dataset.columns.duplicated()].tolist()


def get_common_cols(staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame) -> list:
    common_cols = staging_dataset.columns.intersection(main_dataset.columns).tolist()
    return common_cols


def get_missing_cols(staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame) -> list:
    missing_cols = main_dataset.columns.difference(staging_dataset.columns).tolist()
    return missing_cols


def get_additional_cols(
    staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame
) -> list:
    add_cols = staging_dataset.columns.difference(main_dataset.columns).tolist()
    return add_cols


def get_S3FileSystem_from_session(
    aws_session: boto3.session.Session,
) -> fs.S3FileSystem:
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
    staging_datatype_folders = get_folders_in_s3_bucket(
        s3, bucket_name, namespace=staging_namespace
    )
    main_datatype_folders = get_folders_in_s3_bucket(
        s3, bucket_name, namespace=main_namespace
    )
    return list(set(staging_datatype_folders + main_datatype_folders))


def print_comparison_result(comparison_result: dict) -> None:
    for msg in comparison_result:
        logger.info(comparison_result[msg])
    logger.info("Comparison results complete!")


def compare_datasets_by_data_type(
    args,
    s3_filesystem: fs.S3FileSystem,
    data_type: str,
    comparison_result: dict,
) -> dict:
    data_type = "dataset_fitbitactivitylogs"
    staging_dataset = get_parquet_dataset(
        dataset_key=f"s3://{args.parquet_bucket}/{args.staging_namespace}/parquet/{data_type}/",
        s3_filesystem=s3_filesystem,
    )

    main_dataset = get_parquet_dataset(
        dataset_key=f"s3://{args.parquet_bucket}/{args.main_namespace}/parquet/{data_type}/",
        s3_filesystem=s3_filesystem,
    )

    if staging_dataset.empty or main_dataset.empty:
        comparison_result[
            data_type
        ] = f"One of {args.staging_namespace} or {args.main_namespace} has no data. Comparison cannot continue."
    else:
        # check that the dataset has no dup cols or dup rows and that they have cols in common
        comparison_result[data_type] = []
        # check if one or both of the datasets have no data
        if staging_dataset.empty or main_dataset.empty:
            comparison_result["empty"][
                data_type
            ] = f"One of {args.staging_namespace} or {args.main_namespace} has no data. Comparison cannot continue."
        else:
            comparison_result[data_type].append(
                compare_column_data_types(data_type, staging_dataset, main_dataset)
            )
            comparison_result[data_type].append(
                compare_column_names(data_type, staging_dataset, main_dataset)
            )
            comparison_result[data_type].append(
                compare_column_vals(data_type, staging_dataset, main_dataset)
            )
            comparison_result[data_type].append(
                compare_num_of_rows(data_type, staging_dataset, main_dataset)
            )
            comparison_result[data_type].append(
                compare_dataset_row_vals(data_type, staging_dataset, main_dataset)
            )
        return comparison_result


def main():
    args = read_args()
    comparison_result = {}
    s3 = boto3.client("s3")
    aws_session = boto3.session.Session(profile_name="default", region_name="us-east-1")
    fs = get_S3FileSystem_from_session(aws_session)

    # check if one or both of the datasets have no data
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
        comparison_result = compare_datasets_by_data_type(
            args, fs, data_type, comparison_result
        )
    print_comparison_result(comparison_result)
    return


if __name__ == "__main__":
    main()
