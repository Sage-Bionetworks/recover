import argparse
import datetime
from io import BytesIO, StringIO
import json
import logging
import os
import sys
from typing import Dict, List, Union
import zipfile

from awsglue.utils import getResolvedOptions
import boto3
import datacompy
import pandas as pd
from pyarrow import fs
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(logging.INFO)

INDEX_FIELD_MAP = {
    "dataset_enrolledparticipants": ["ParticipantIdentifier"],
    "dataset_fitbitprofiles": ["ParticipantIdentifier", "ModifiedDate"],
    "dataset_fitbitdevices": ["ParticipantIdentifier", "Date", "Device"],
    "dataset_fitbitactivitylogs": ["ParticipantIdentifier", "LogId"],
    "dataset_fitbitdailydata": ["ParticipantIdentifier", "Date"],
    "dataset_fitbitecg": ["ParticipantIdentifier", "FitbitEcgKey"],
    "dataset_fitbitintradaycombined": ["ParticipantIdentifier", "Type", "DateTime"],
    "dataset_fitbitrestingheartrates": ["ParticipantIdentifier", "Date"],
    "dataset_fitbitsleeplogs": ["ParticipantIdentifier", "LogId"],
    "dataset_healthkitv2characteristics": [
        "ParticipantIdentifier",
        "HealthKitCharacteristicKey",
    ],
    "dataset_healthkitv2samples": ["ParticipantIdentifier", "HealthKitSampleKey"],
    "dataset_healthkitv2heartbeat": ["ParticipantIdentifier", "HealthKitHeartbeatSampleKey"],
    "dataset_healthkitv2statistics": ["ParticipantIdentifier", "HealthKitStatisticKey"],
    "dataset_healthkitv2clinicalrecords": [
        "ParticipantIdentifier",
        "HealthKitClinicalRecordKey",
    ],
    "dataset_healthkitv2electrocardiogram": ["ParticipantIdentifier", "HealthKitECGSampleKey"],
    "dataset_healthkitv2workouts": ["ParticipantIdentifier", "HealthKitWorkoutKey"],
    "dataset_healthkitv2activitysummaries": [
        "ParticipantIdentifier",
        "HealthKitActivitySummaryKey",
    ],
    "dataset_garminactivitydetailssummary": ["ParticipantIdentifier", "SummaryId"],
    "dataset_garminactivitysummary": ["ParticipantIdentifier", "SummaryId"],
    "dataset_garminbloodpressuresummary": ["ParticipantIdentifier", "SummaryId"],
    "dataset_garmindailysummary": ["ParticipantIdentifier", "StartTimeInSeconds"],
    "dataset_garminepochsummary": ["ParticipantIdentifier", "SummaryId"],
    "dataset_garminhealthsnapshotsummary": ["ParticipantIdentifier", "StartTimeInSeconds"],
    "dataset_garminhrvsummary": ["ParticipantIdentifier", "StartTimeInSeconds"],
    "dataset_garminmanuallyupdatedactivitysummary": ["ParticipantIdentifier", "SummaryId"],
    "dataset_garminmoveiqactivitysummary": ["ParticipantIdentifier", "SummaryId"],
    "dataset_garminpulseoxsummary": ["ParticipantIdentifier", "SummaryId"],
    "dataset_garminrespirationsummary": ["ParticipantIdentifier", "SummaryId"],
    "dataset_garminsleepsummary": [
        "ParticipantIdentifier",
        "StartTimeInSeconds",
        "DurationInSeconds",
        "Validation",
    ],
    "dataset_garminstressdetailsummary": ["ParticipantIdentifier", "StartTimeInSeconds"],
    "dataset_garminthirdpartydailysummary": ["ParticipantIdentifier", "StartTimeInSeconds"],
    "dataset_garminusermetricssummary": ["ParticipantIdentifier", "CalenderDate"],
    "dataset_googlefitsamples": ["ParticipantIdentifier", "GoogleFitSampleKey"],
    "dataset_symptomlog": ["ParticipantIdentifier", "DataPointKey"],
    # deleted data types
    "dataset_healthkitv2samples_deleted": ["ParticipantIdentifier", "HealthKitSampleKey"],
    "dataset_healthkitv2heartbeat_deleted":["ParticipantIdentifier", "HealthKitHeartbeatSampleKey"],
    "dataset_healthkitv2statistics_deleted":["ParticipantIdentifier", "HealthKitStatisticKey"],
    "dataset_healthkitv2electrocardiogram_deleted":["ParticipantIdentifier", "HealthKitECGSampleKey"],
    "dataset_healthkitv2workouts_deleted":["ParticipantIdentifier", "HealthKitWorkoutKey"],
    "dataset_healthkitv2activitysummaries_deleted": [
        "ParticipantIdentifier",
        "HealthKitActivitySummaryKey",
    ],

}


def read_args() -> dict:
    """Returns the specific params that our code needs to run"""
    args = getResolvedOptions(
        sys.argv,
        [
            "data-type",
            "staging-namespace",
            "main-namespace",
            "parquet-bucket",
            "input-bucket",
            "cfn-bucket",
        ],
    )
    for arg in args:
        validate_args(args[arg])
    return args


def validate_args(value: str) -> None:
    """Checks to make sure none of the input command line arguments are empty strings

    Args:
        value (str): the value of the command line argument parsed by argparse

    Raises:
        ValueError: when value is an empty string
    """
    if value == "":
        raise ValueError("Argument value cannot be an empty string")
    else:
        return None


def get_s3_file_key_for_comparison_results(
    staging_namespace: str, data_type: str = None, file_name: str = ""
) -> str:
    """Gets the s3 file key for saving the comparison results to
    Note that file_name should contain the file extension.

    NOTE: When using s3.put_object, if the bucket name is part of the the
    Key parameter where we put the filepath to the file to save, it will
    just assume it's another folder and will create another folder inside the
    bucket"""
    s3_folder_prefix = os.path.join(staging_namespace, "comparison_result")
    if file_name.endswith(".csv") or file_name.endswith(".txt"):
        if data_type:
            return os.path.join(s3_folder_prefix, data_type, f"{file_name}")
        else:
            return os.path.join(s3_folder_prefix, f"{file_name}")
    else:
        raise TypeError(
            f"file_name {file_name} should contain one of the following file extensions: [.txt, .csv]"
        )


def get_parquet_dataset_s3_path(parquet_bucket: str, namespace: str, data_type: str):
    """Gets the s3 filepath to the parquet datasets"""
    return os.path.join("s3://", parquet_bucket, namespace, "parquet", data_type)


def get_duplicated_columns(dataset: pd.DataFrame) -> list:
    """Gets a list of duplicated columns in a dataframe"""
    return dataset.columns[dataset.columns.duplicated()].tolist()


def has_common_cols(staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame) -> list:
    """Gets the list of common columns between two dataframes
    TODO: Could look into depreciating this and using datacompy.intersect_columns function
    """
    common_cols = staging_dataset.columns.intersection(main_dataset.columns).tolist()
    return common_cols != []


def get_missing_cols(staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame) -> list:
    """Gets the list of missing columns present in main but not in staging
    TODO: Could look into depreciating this and using datacompy.df2_unq_columns function
    """
    missing_cols = main_dataset.columns.difference(staging_dataset.columns).tolist()
    return missing_cols


def get_additional_cols(
    staging_dataset: pd.DataFrame, main_dataset: pd.DataFrame
) -> list:
    """Gets the list of additional columns present in staging but not in main
    TODO: Could look into depreciating this and using datacompy.df1_unq_columns function
    """
    add_cols = staging_dataset.columns.difference(main_dataset.columns).tolist()
    return add_cols


def convert_dataframe_to_text(dataset: pd.DataFrame) -> str:
    """Converts a pandas DataFrame into a string to save as csv in S3

    NOTE: This is because s3.put_object only allows bytes or string
    objects when saving them to S3"""
    csv_buffer = StringIO()
    dataset.to_csv(csv_buffer)
    csv_content = csv_buffer.getvalue()
    return csv_content


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


def get_export_end_date(filename: str) -> str:
    """Gets the export end date based on the filename

    Args:
        filename (str): name of the input json file

    Returns:
        str: export end date in isoformat
    """
    filename_components = os.path.splitext(filename)[0].split("_")
    if len(filename_components) <= 1:
        export_end_date = None
    else:
        if "-" in filename_components[-1]:
            _, end_date = filename_components[-1].split("-")
            end_date = datetime.datetime.strptime(end_date, "%Y%m%d")
        else:
            end_date = datetime.datetime.strptime(filename_components[-1], "%Y%m%d")
        export_end_date = end_date.isoformat()
    return export_end_date


def get_cohort_from_s3_uri(s3_uri: str) -> str:
    """Gets the cohort (pediatric_v1 or adults_v1)
    from the s3 uri of the export

    Args:
        s3_uri (str): the S3 uri

    Returns:
        str: the cohort
    """
    cohort = None
    if "adults_v1" in s3_uri:
        cohort = "adults_v1"
    elif "pediatric_v1" in s3_uri:
        cohort = "pediatric_v1"
    else:
        pass
    return cohort


def get_data_type_from_filename(filename: str) -> Union[str, None]:
    """_Gets the data type from the JSON filename

    Args:
        filename (str): JSON filename
    Returns:
        Union[str, None]: the data type if it's the top level data type
        otherwise returns None as we currently don't support comparing
        subtypes in data
    """
    filename_components = os.path.splitext(filename)[0].split("_")
    data_type = f"dataset_{filename_components[0].lower()}"
    if data_type in INDEX_FIELD_MAP.keys() and len(filename_components) < 3:
        return data_type
    else:
        return None


def get_json_files_in_zip_from_s3(s3, input_bucket: str, s3_uri: str) -> List[str]:
    """_summary_

    Args:
        s3_uri (str): _description_

    Returns:
        list: _description_
    """
    # Get the object from S3
    s3_key = s3_uri.split(f"s3://{input_bucket}/")[1]
    s3_obj = s3.get_object(Bucket=input_bucket, Key=s3_key)

    # Use BytesIO to treat the zip content as a file-like object
    with zipfile.ZipFile(BytesIO(s3_obj["Body"].read())) as z:
        # Open the specific JSON file within the zip file
        non_empty_contents = [
            f.filename
            for f in z.filelist
            if "/" not in f.filename
            and "Manifest" not in f.filename
            and f.file_size > 0
        ]
        return non_empty_contents


def get_integration_test_exports_json(s3, cfn_bucket: str, staging_namespace: str):
    """_summary_

    Args:
        s3 (_type_): _description_
        cfn_bucket (str): _description_
        staging_namespace (str): _description_

    Returns:
        _type_: _description_
    """
    # read in the json filelist
    s3_response_object = s3.get_object(
        Bucket=cfn_bucket, Key=f"{staging_namespace}/integration_test_exports.json"
    )
    json_content = s3_response_object["Body"].read().decode("utf-8")
    filelist = json.loads(json_content)
    return filelist


def get_exports_filter_values(
    s3, data_type: str, input_bucket: str, cfn_bucket: str, staging_namespace: str
) -> Dict[str, list]:
    """_summary_

    Args:
        s3 (_type_): _description_
        data_type (str): _description_
        input_bucket (str): _description_
        cfn_bucket (str): _description_
        staging_namespace (str): _description_

    Returns:
        Dict[str, list]: _description_
    """
    filelist = get_integration_test_exports_json(s3, cfn_bucket, staging_namespace)

    # create the dictionary of export end dates and cohort
    export_end_date_vals = {}
    for s3_uri in filelist:
        json_files = get_json_files_in_zip_from_s3(s3, input_bucket, s3_uri)
        cur_cohort = get_cohort_from_s3_uri(s3_uri)
        for json_file in json_files:
            cur_data_type = get_data_type_from_filename(json_file)
            cur_export_end_date = get_export_end_date(json_file)
            if cur_data_type == data_type:
                if cur_cohort in export_end_date_vals.keys():
                    export_end_date_vals[cur_cohort].append(cur_export_end_date)
                else:
                    export_end_date_vals[cur_cohort] = [cur_export_end_date]
            else:
                continue

    # create the filter using pyarrow and predicate pushdown
    exports_filter = None
    if export_end_date_vals:
        for cohort, values in export_end_date_vals.items():
            column_filter = (ds.field("cohort") == cohort) & (
                ds.field("export_end_date").isin(values)
            )
            if exports_filter is None:
                exports_filter = column_filter
            else:
                # Combine filters using logical OR
                exports_filter = exports_filter | column_filter

    return exports_filter


def get_filtered_main_dataset(
    exports_filter: dict, dataset_key: str, s3_filesystem: fs.S3FileSystem
):
    """
    Returns a Parquet dataset on S3 as a pandas dataframe

    Args:
        dataset_key (str): The URI of the parquet dataset.
        s3_filesystem (S3FileSystem): A fs.S3FileSystem object

    Returns:
        pandas.DataFrame

    TODO: Currently, internal pyarrow things like to_table as a
    result of the read_table function below takes a while as the dataset
    grows bigger. Could find a way to optimize that.
    """
    # Create the dataset object pointing to the S3 location
    table_source = dataset_key.split("s3://")[-1]
    dataset = ds.dataset(
        source=table_source, filesystem=s3_filesystem, format="parquet"
    )

    # Apply the filter and read the dataset into a table
    filtered_table = dataset.to_table(filter=exports_filter)
    return filtered_table.to_pandas()


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

    TODO: Currently, internal pyarrow things like to_table as a
    result of the read_table function below takes a while as the dataset
    grows bigger. Could find a way to optimize that.
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

    response = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=f"{namespace}/parquet/", Delimiter="/"
    )
    if "CommonPrefixes" in response.keys():
        contents = response["CommonPrefixes"]
        folders = [
            os.path.normpath(content["Prefix"]).split(os.sep)[-1]
            for content in contents
        ]
    else:
        folders = []
    return folders


def get_duplicates(compare_obj: datacompy.Compare, namespace: str) -> pd.DataFrame:
    """Uses the datacompy Compare object to get all duplicates for a given dataset
    Args:
        compare_obj (datacompy.Compare): compare object that was defined earlier
        namespace (str): The dataset we want to get the duplicated rows from

    Returns:
        pd.DataFrame: All the duplicated rows
    """
    if namespace == "staging":
        dup_rows = compare_obj.df1[
            compare_obj.df1.duplicated(subset=compare_obj.join_columns)
        ]
    elif namespace == "main":
        dup_rows = compare_obj.df2[
            compare_obj.df2.duplicated(subset=compare_obj.join_columns)
        ]
    else:
        raise KeyError("namespace can only be one of 'staging', 'main'")
    return dup_rows


def compare_row_diffs(compare_obj: datacompy.Compare, namespace: str) -> pd.DataFrame:
    """Uses the datacompy Compare object to get all rows that are different
        in each dataset
    Args:
        compare_obj (datacompy.Compare): compare object that was defined earlier
        namespace (str): The dataset we want to get the rows that are different from

    Returns:
        pd.DataFrame: All the rows that's in one dataframe but not the other
    """
    if namespace == "staging":
        columns = compare_obj.df1_unq_rows.columns
        rows = compare_obj.df1_unq_rows.sample(compare_obj.df1_unq_rows.shape[0])[
            columns
        ]
    elif namespace == "main":
        columns = compare_obj.df2_unq_rows.columns
        rows = compare_obj.df2_unq_rows.sample(compare_obj.df2_unq_rows.shape[0])[
            columns
        ]
    else:
        raise KeyError("namespace can only be one of 'staging', 'main'")
    return rows


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
    return list(set(staging_datatype_folders) & set(main_datatype_folders))


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


def compare_datasets_and_output_report(
    data_type: str,
    staging_dataset: pd.DataFrame,
    main_dataset: pd.DataFrame,
    staging_namespace: str,
    main_namespace: str,
) -> datacompy.Compare:
    """This method prints out a human-readable report summarizing and
    sampling differences between datasets for the given data type.
    A full list of comparisons can be found in the datacompy package site.

    Returns:
        datacompy.Compare: object containing

    TODO: Look into using datacompy.SparkCompare as in the docs, it mentions
    it works with data that is partitioned Parquet, CSV, or JSON files,
    or Cerebro tables. This will also likely be necessary once we have
    more data coming in over weeks and months
    """
    # there exists folders with data subtypes, but we want to just merge on
    # the main level datatypes
    main_data_type = f'dataset_{data_type.split("_")[1]}'
    compare = datacompy.Compare(
        df1=staging_dataset,
        df2=main_dataset,
        join_columns=INDEX_FIELD_MAP[main_data_type] + ["export_end_date"],
        abs_tol=0,  # Optional, defaults to 0
        rel_tol=0,  # Optional, defaults to 0
        df1_name=staging_namespace,  # Optional, defaults to 'df1'
        df2_name=main_namespace,  # Optional, defaults to 'df2'
        cast_column_names_lower=False,
    )
    compare.matches(ignore_extra_columns=False)
    return compare


def add_additional_msg_to_comparison_report(
    comparison_report: str, add_msgs: list, msg_type: str
) -> str:
    """This adds additional messages to the comparison report. Currently, this adds
        messages that specify the names of columns that are different between the
        two datasets

    Args:
        comparison_report (str): report generated using datacompy
        add_msgs (list): list of additional messages to include at the bottom of the report
        msg_type (str): category of message, current available ones are
            ["column_name_diff", "data_type_diff"]

    Returns:
        str: updated comparison report with more specific messages
    """
    # does some formatting.
    joined_add_msgs = "\n".join(add_msgs)
    if msg_type == "column_name_diff":
        updated_comparison_report = (
            f"{comparison_report}"
            f"Column Name Differences\n"
            f"-----------------------\n\n{joined_add_msgs}"
        )
    elif msg_type == "data_type_diff":
        updated_comparison_report = (
            f"{comparison_report}"
            f"Data Type Differences between the namespaces\n"
            f"--------------------------------------------\n\n{joined_add_msgs}"
        )
    else:
        raise ValueError(
            "msg_type param must be one of 'column_name_diff', 'data_type_diff'"
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
    if len(dataset.columns) == 0:
        msg = f"{namespace} dataset has no data. Comparison cannot continue."
        return {"result": False, "msg": msg}
    elif get_duplicated_columns(dataset):
        msg = (
            f"{namespace} dataset has duplicated columns. Comparison cannot continue.\n"
            f"Duplicated columns:{str(get_duplicated_columns(dataset))}"
        )
        return {"result": False, "msg": msg}
    else:
        msg = f"{namespace} dataset has been validated."
        return {"result": True, "msg": msg}


def compare_datasets_by_data_type(
    s3,
    cfn_bucket: str,
    parquet_bucket: str,
    staging_namespace: str,
    main_namespace: str,
    s3_filesystem: fs.S3FileSystem,
    data_type: str,
) -> dict:
    """This runs the bulk of the comparison functions from beginning to end by data type

    Args:
        parquet_bucket (str): name of the bucket containing the parquet datasets
        staging_namespace (str): name of namespace containing the "new" data
        main_namespace (str): name of namespace containing the "established" data
        s3_filesystem (fs.S3FileSystem): filesystem instantiated by aws credentials
        data_type (str): data type to be compared for the given datasets

    Returns:
        dict:
            compare_obj: the datacompy.Compare obj on the two datasets
            comparison_report:final report on the datasets for the given data type
    """
    header_msg = (
        f"\n\nParquet Dataset Comparison running for Data Type: {data_type}"
        f"\n-----------------------------------------------------------------\n\n"
    )
    staging_dataset = get_parquet_dataset(
        dataset_key=get_parquet_dataset_s3_path(
            parquet_bucket, staging_namespace, data_type
        ),
        s3_filesystem=s3_filesystem,
    )
    filter_values = get_exports_filter_values(
        s3=s3,
        data_type=data_type,
        cfn_bucket=cfn_bucket,
        staging_namespace=staging_namespace,
    )
    main_dataset = get_filtered_main_dataset(
        filter_values=filter_values,
        dataset_key=get_parquet_dataset_s3_path(
            parquet_bucket, main_namespace, data_type
        ),
        s3_filesystem=s3_filesystem,
    )
    # go through specific validation for each dataset prior to comparison
    staging_is_valid_result = is_valid_dataset(staging_dataset, staging_namespace)
    main_is_valid_result = is_valid_dataset(main_dataset, main_namespace)
    if (
        staging_is_valid_result["result"] == False
        or main_is_valid_result["result"] == False
    ):
        comparison_report = (
            f"{staging_is_valid_result['msg']}\n{main_is_valid_result['msg']}"
        )
        compare = None
    # check that they have columns in common to compare
    elif not has_common_cols(staging_dataset, main_dataset):
        comparison_report = (
            f"{staging_namespace} dataset and {main_namespace} have no columns in common."
            f" Comparison cannot continue."
        )
        compare = None
    else:
        logger.info(
            f"{staging_namespace} dataset memory usage:"
            f"{staging_dataset.memory_usage(deep=True).sum()/1e+6} MB"
        )
        logger.info(
            f"{main_namespace} dataset memory usage:"
            f"{main_dataset.memory_usage(deep=True).sum()/1e+6} MB"
        )
        compare = compare_datasets_and_output_report(
            data_type=data_type,
            staging_dataset=staging_dataset,
            main_dataset=main_dataset,
            staging_namespace=staging_namespace,
            main_namespace=main_namespace,
        )

        comparison_report = add_additional_msg_to_comparison_report(
            compare.report(),
            add_msgs=compare_column_names(data_type, staging_dataset, main_dataset),
            msg_type="column_name_diff",
        )
    return {
        "compare_obj": compare,
        "comparison_report": f"{header_msg}{comparison_report}",
    }


def main():
    args = read_args()
    s3 = boto3.client("s3")
    aws_session = boto3.session.Session(region_name="us-east-1")
    fs = get_S3FileSystem_from_session(aws_session)
    data_type = args["data_type"]

    data_types_to_compare = get_data_types_to_compare(
        s3,
        args["parquet_bucket"],
        main_namespace=args["main_namespace"],
        staging_namespace=args["staging_namespace"],
    )
    data_types_diff = compare_dataset_data_types(
        s3,
        args["parquet_bucket"],
        main_namespace=args["main_namespace"],
        staging_namespace=args["staging_namespace"],
    )
    if data_types_to_compare:
        logger.info(f"Running comparison report for {data_type}")
        compare_dict = compare_datasets_by_data_type(
            s3=s3,
            parquet_bucket=args["parquet_bucket"],
            staging_namespace=args["staging_namespace"],
            main_namespace=args["main_namespace"],
            s3_filesystem=fs,
            data_type=data_type,
        )
        # update comparison report with the data_type differences message
        comparison_report = add_additional_msg_to_comparison_report(
            compare_dict["comparison_report"],
            add_msgs=data_types_diff,
            msg_type="data_type_diff",
        )
        # save comparison report to report folder in staging namespace
        s3.put_object(
            Bucket=args["parquet_bucket"],
            Key=get_s3_file_key_for_comparison_results(
                staging_namespace=args["staging_namespace"],
                data_type=data_type,
                file_name="parquet_compare.txt",
            ),
            Body=comparison_report,
        )
        logger.info("Comparison report saved!")
        # additional report print outs
        compare = compare_dict["compare_obj"]
        # TODO: Find out if pandas.to_csv, or direct write to S3
        # is more efficient. s3.put_object is very slow and memory heavy
        # esp. if using StringIO conversion

        # print out all mismatch columns
        mismatch_cols_report = compare.all_mismatch()
        if not mismatch_cols_report.empty:
            s3.put_object(
                Bucket=args["parquet_bucket"],
                Key=get_s3_file_key_for_comparison_results(
                    staging_namespace=args["staging_namespace"],
                    data_type=data_type,
                    file_name="all_mismatch_cols.csv",
                ),
                Body=convert_dataframe_to_text(mismatch_cols_report),
            )
            logger.info("Mismatch columns saved!")
        # print out all staging rows that are different to main
        staging_rows_report = compare_row_diffs(compare, namespace="staging")
        if not staging_rows_report.empty:
            s3.put_object(
                Bucket=args["parquet_bucket"],
                Key=get_s3_file_key_for_comparison_results(
                    staging_namespace=args["staging_namespace"],
                    data_type=data_type,
                    file_name="all_diff_staging_rows.csv",
                ),
                Body=convert_dataframe_to_text(staging_rows_report),
            )
            logger.info("Different staging dataset rows saved!")
        # print out all main rows that are different to staging
        main_rows_report = compare_row_diffs(compare, namespace="main")
        if not main_rows_report.empty:
            s3.put_object(
                Bucket=args["parquet_bucket"],
                Key=get_s3_file_key_for_comparison_results(
                    staging_namespace=args["staging_namespace"],
                    data_type=data_type,
                    file_name="all_diff_main_rows.csv",
                ),
                Body=convert_dataframe_to_text(main_rows_report),
            )
            logger.info("Different main dataset rows saved!")

        # print out all staging duplicated rows
        staging_dups_report = get_duplicates(compare, namespace="staging")
        if not staging_dups_report.empty:
            s3.put_object(
                Bucket=args["parquet_bucket"],
                Key=get_s3_file_key_for_comparison_results(
                    staging_namespace=args["staging_namespace"],
                    data_type=data_type,
                    file_name="all_dup_staging_rows.csv",
                ),
                Body=convert_dataframe_to_text(staging_dups_report),
            )
            logger.info("Staging dataset duplicates saved!")
        # print out all main duplicated rows
        main_dups_report = get_duplicates(compare, namespace="main")
        if not main_dups_report.empty:
            s3.put_object(
                Bucket=args["parquet_bucket"],
                Key=get_s3_file_key_for_comparison_results(
                    staging_namespace=args["staging_namespace"],
                    data_type=data_type,
                    file_name="all_dup_main_rows.csv",
                ),
                Body=convert_dataframe_to_text(main_dups_report),
            )
            logger.info("Main dataset duplicates saved!")
    else:
        # update comparison report with the data_type differences message
        comparison_report = add_additional_msg_to_comparison_report(
            comparison_report,
            add_msgs=data_types_diff,
            msg_type="data_type_diff",
        )
        print(comparison_report)
        s3.put_object(
            Bucket=args["parquet_bucket"],
            Key=get_s3_file_key_for_comparison_results(
                staging_namespace=args["staging_namespace"],
                data_type=None,
                file_name="data_types_compare.txt",
            ),
            Body=comparison_report,
        )
        logger.info("Comparison report saved!")
    return


if __name__ == "__main__":
    main()
