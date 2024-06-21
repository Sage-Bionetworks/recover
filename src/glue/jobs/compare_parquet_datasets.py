from collections import namedtuple
import datetime
import json
import logging
import os
import sys
import zipfile
from io import BytesIO, StringIO
from typing import Dict, List, NamedTuple, Union

import boto3
import datacompy
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from awsglue.utils import getResolvedOptions
from pyarrow import fs

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ADULTS = "adults_v1"
PEDIATRIC = "pediatric_v1"
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
    "dataset_healthkitv2heartbeat": [
        "ParticipantIdentifier",
        "HealthKitHeartbeatSampleKey",
    ],
    "dataset_healthkitv2statistics": ["ParticipantIdentifier", "HealthKitStatisticKey"],
    "dataset_healthkitv2clinicalrecords": [
        "ParticipantIdentifier",
        "HealthKitClinicalRecordKey",
    ],
    "dataset_healthkitv2electrocardiogram": [
        "ParticipantIdentifier",
        "HealthKitECGSampleKey",
    ],
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
    "dataset_garminhealthsnapshotsummary": [
        "ParticipantIdentifier",
        "StartTimeInSeconds",
    ],
    "dataset_garminhrvsummary": ["ParticipantIdentifier", "StartTimeInSeconds"],
    "dataset_garminmanuallyupdatedactivitysummary": [
        "ParticipantIdentifier",
        "SummaryId",
    ],
    "dataset_garminmoveiqactivitysummary": ["ParticipantIdentifier", "SummaryId"],
    "dataset_garminpulseoxsummary": ["ParticipantIdentifier", "SummaryId"],
    "dataset_garminrespirationsummary": ["ParticipantIdentifier", "SummaryId"],
    "dataset_garminsleepsummary": [
        "ParticipantIdentifier",
        "StartTimeInSeconds",
        "DurationInSeconds",
        "Validation",
    ],
    "dataset_garminstressdetailsummary": [
        "ParticipantIdentifier",
        "StartTimeInSeconds",
    ],
    "dataset_garminthirdpartydailysummary": [
        "ParticipantIdentifier",
        "StartTimeInSeconds",
    ],
    "dataset_garminusermetricssummary": ["ParticipantIdentifier", "CalenderDate"],
    "dataset_googlefitsamples": ["ParticipantIdentifier", "GoogleFitSampleKey"],
    "dataset_symptomlog": ["ParticipantIdentifier", "DataPointKey"],
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
    """Converts a pandas DataFrame into a string to save as csv in S3.
        If the dataframe is empty or has empty columns, it should just
        return an empty string

        NOTE: This is because s3.put_object only allows bytes or string
        objects when saving them to S3

    Args:
        dataset (pd.DataFrame): input dataset

    Returns:
        str: the resulting string conversion
    """
    if not dataset.empty:
        csv_buffer = StringIO()
        dataset.to_csv(csv_buffer)
        csv_content = csv_buffer.getvalue()
        return csv_content
    else:
        return ""


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


def get_export_end_date(filename: str) -> Union[str, None]:
    """Gets the export end date (converted to isoformat) based on the filename.

        We can have filenames with with:
        - {DateType}_{YYYYMMDD}-{YYYYMMDD}.json
        - {DateType}_{YYYYMMDD}.json format
        This function does handling for both. The export end date is always the
        last component of the filename.

    Args:
        filename (str): name of the input json file

    Returns:
        Union[str, None]: export end date in isoformat if it exists
    """
    if (
        filename is None
        or filename == ""
        or len(os.path.splitext(filename)[0].split("_")) <= 1
    ):
        return None

    filename_components = os.path.splitext(filename)[0].split("_")

    if "-" in filename_components[-1]:
        _, end_date = filename_components[-1].split("-")
        end_date = datetime.datetime.strptime(end_date, "%Y%m%d")
    else:
        end_date = datetime.datetime.strptime(filename_components[-1], "%Y%m%d")
    export_end_date = end_date.isoformat()
    return export_end_date


def get_cohort_from_s3_uri(s3_uri: str) -> Union[str, None]:
    """Gets the cohort (pediatric_v1 or adults_v1)
    from the s3 uri of the export.

    The s3 uri of the export has the following expected format:
    s3://{bucket_name}/{namespace}/{cohort}/{export_name}

    Args:
        s3_uri (str): the S3 uri

    Returns:
        Union[str, None]: the cohort if it exists
    """
    cohort = None
    if ADULTS in s3_uri:
        cohort = ADULTS
    elif PEDIATRIC in s3_uri:
        cohort = PEDIATRIC
    return cohort


def get_data_type_from_filename(filename: str) -> Union[str, None]:
    """Gets the data type from the JSON filename.

    A filename can be of the following format:
    {DataType}_[{DataSubType}_][Deleted_]{YYYYMMDD}[-{YYYYMMDD}].json

    Since we don't have support for DataSubType, we only support top
    level datatypes but because we only have the subtypes
    of the dataset in the exports, we can still use that to get the metadata
    we need. We parse out deleted healthkit data types as we don't process them
    into parquet.

    Args:
        filename (str): JSON filename

    Returns:
        Union[str, None]: the data type otherwise returns None as we
        currently don't support comparingsubtypes in data
    """
    filename_components = os.path.splitext(filename)[0].split("_")
    data_type = filename_components[0]

    if "HealthKitV2" in data_type and filename_components[-2] == "Deleted":
        data_type = f"{data_type}_Deleted"
    formatted_data_type = f"dataset_{data_type.lower()}"

    if formatted_data_type in INDEX_FIELD_MAP.keys():
        return formatted_data_type
    else:
        return None


def get_json_files_in_zip_from_s3(
    s3: boto3.client, input_bucket: str, s3_uri: str
) -> List[str]:
    """Parses through a zipped export and gets the list of json files.
        Accepted JSON files cannot be:
            - 0 file size
            - be a folder (have / in the name)
            - Have 'Manifest' in the filename
    Args:
        s3 (boto3.client): s3 client connection
        input_bucket (str): name of the input bucket containing the zipped exports
        s3_uri (str): s3 uri to the zipped export

    Returns:
        List[str]: list of json file names
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


def get_integration_test_exports_json(
    s3: boto3.client, cfn_bucket: str, staging_namespace: str
) -> List[str]:
    """Reads in the integration test exports json from the cloudformation
        bucket that contains the ~2 weeks of production data exports
        to use in the staging pipeline

    Args:
        s3 (boto3.client): s3 client connection
        cfn_bucket (str): cloudformation bucket name
        staging_namespace (str): name of namespace containing the "new" data

    Returns:
        List[str]: list of the json exports
    """
    # read in the json filelist
    s3_response_object = s3.get_object(
        Bucket=cfn_bucket, Key=f"{staging_namespace}/integration_test_exports.json"
    )
    json_content = s3_response_object["Body"].read().decode("utf-8")
    filelist = json.loads(json_content)
    return filelist


def get_exports_filter_values(
    s3: boto3.client,
    data_type: str,
    input_bucket: str,
    cfn_bucket: str,
    staging_namespace: str,
) -> Dict[str, str]:
    """Parses through the json exports and gets the values
    for the cohort and export_end_date to filter on for our
    main parquet dataset. The exports_filter will have the following
    structure:
        {
            <cohort_name>: [<list of export_end_date values>],
            ...
        }
    Args:
        s3 (boto3.client): s3 client connection
        data_type (str): data type of the dataset
        input_bucket (str): input data bucket name
        cfn_bucket (str): cloudformation bucket name
        staging_namespace (str): name of namespace containing the "new" data

    Returns:
        Dict[str, str]: a dict containing the column(s) (key(s)) and values to
        filter on
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

    return export_end_date_vals


def convert_filter_values_to_expression(filter_values: Dict[str, str]) -> ds.Expression:
    """Converts the dict of the keys, values to filter on
        into filter conditions in the form of a pyarrow.dataset.Expression object

        The expression object takes the following structure for a single condition:
        (ds.field("cohort") == <cohort_name>) &
        (ds.field("export_end_date").isin([<list of export_end_date values]))

        For multiple conditions:
        (ds.field("cohort") == <cohort_name>) & (ds.field("export_end_date").isin([<list of export_end_date values]))
        | (ds.field("cohort") == <cohort_name>) & (ds.field("export_end_date").isin([<list of export_end_date values]))

        When the filter_values is an empty {}, the filter is None

    Args:
        filter_values (Dict[str, str]): dict of the filter values

    Returns:
        ds.Expression: a pyarrow dataset expression object that contains
        the filter conditions that can be applied to pyarrow datasets
    """
    # create the filter using pyarrow and predicate pushdown
    exports_filter = None
    if filter_values:
        for cohort, values in filter_values.items():
            column_filter = (ds.field("cohort") == cohort) & (
                ds.field("export_end_date").isin(values)
            )
            if exports_filter is None:
                exports_filter = column_filter
            else:
                # Combine filters using logical OR
                exports_filter = exports_filter | column_filter
    return exports_filter


def get_parquet_dataset(
    dataset_key: str,
    s3_filesystem: fs.S3FileSystem,
    filter_values: Dict[str, str] = {},
) -> pd.DataFrame:
    """Returns a parquet dataset on S3 as a pandas dataframe.
    The main dataset is optionally filtered using the filter_values
    converted to a ds.Expression object prior to being
    read into memory.

    Args:
        dataset_key (str): The URI of the parquet dataset.
        s3_filesystem (S3FileSystem): A fs.S3FileSystem object
        filter_values (Dict[str, str]): A dictionary object containing
            the columns (keys) and values to filter the dataset on. Defaults to {}.

    Returns:
        pd.DataFrame: the filtered table as a pandas dataframe
    """
    ds_filter = convert_filter_values_to_expression(filter_values=filter_values)
    # Create the dataset object pointing to the S3 location
    table_source = dataset_key.split("s3://")[-1]
    dataset = ds.dataset(
        source=table_source,
        filesystem=s3_filesystem,
        format="parquet",
        partitioning="hive",  # allows us to read in partitions as columns
    )

    # Apply any filter and read the dataset into a table
    filtered_table = dataset.to_table(filter=ds_filter)
    return filtered_table.to_pandas()


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
        join_columns=INDEX_FIELD_MAP[main_data_type],
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
            ["column_name_diff"]

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
    else:
        raise ValueError("msg_type param must be one of 'column_name_diff'")
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
    s3: boto3.client,
    cfn_bucket: str,
    input_bucket: str,
    parquet_bucket: str,
    staging_namespace: str,
    main_namespace: str,
    s3_filesystem: fs.S3FileSystem,
    data_type: str,
) -> dict:
    """This runs the bulk of the comparison functions from beginning to end by data type

    Args:
        s3 (boto3.client): s3 client connection
        cfn_bucket (str): name of the bucket containing the integration test exports
        input_bucket (str): name of the bucket containing the input data
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
    filter_values = get_exports_filter_values(
        s3=s3,
        data_type=data_type,
        input_bucket=input_bucket,
        cfn_bucket=cfn_bucket,
        staging_namespace=staging_namespace,
    )
    staging_dataset = get_parquet_dataset(
        dataset_key=get_parquet_dataset_s3_path(
            parquet_bucket, staging_namespace, data_type
        ),
        s3_filesystem=s3_filesystem,
    )
    main_dataset = get_parquet_dataset(
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
            f"{staging_namespace} dataset and {main_namespace} dataset have no columns in common."
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


def has_parquet_files(s3: boto3.client, bucket_name: str, prefix: str) -> bool:
    """Quick check that a s3 folder location has parquet data

    Args:
        s3 (boto3.client): s3 client connection
        bucket_name (str): name of the bucket
        prefix (str): prefix of the path to the files

    Returns:
        bool: Whether this folder location has data
    """
    # List objects within a specific S3 bucket and prefix (folder)
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Check if 'Contents' is in the response, which means there are objects in the folder
    if "Contents" in response:
        for obj in response["Contents"]:
            # Check if the object key ends with .parquet
            if obj["Key"].endswith(".parquet"):
                return True
    return False


def upload_reports_to_s3(
    s3: boto3.client,
    reports: List[NamedTuple],
    data_type: str,
    parquet_bucket: str,
    staging_namespace: str,
) -> None:
    """Uploads the various comparison reports to S3 bucket.

    Args:
        s3 (boto3.client): s3 client connection
        reports (List[NamedTuple]): List of report which contain content(str) and
        file_name(str). Content is the string body of the report and file_name is the
        name of the file to be saved to S3.
        data_type (str): data type to be compared for the given datasets
        parquet_bucket (str): name of the bucket containing the parquet datasets
        staging_namespace (str): name of namespace containing the "new" data
    """
    for report in reports:
        if report.content:
            s3.put_object(
                Bucket=parquet_bucket,
                Key=get_s3_file_key_for_comparison_results(
                    staging_namespace=staging_namespace,
                    data_type=data_type,
                    file_name=report.file_name,
                ),
                Body=report.content,
            )


def main():
    args = read_args()
    s3 = boto3.client("s3")
    aws_session = boto3.session.Session(region_name="us-east-1")
    fs = get_S3FileSystem_from_session(aws_session)
    data_type = args["data_type"]
    logger.info(f"Running comparison report for {data_type}")
    staging_has_parquet_files = has_parquet_files(
        s3,
        bucket_name=args["parquet_bucket"],
        prefix=get_parquet_dataset_s3_path(
            args["parquet_bucket"], args["staging_namespace"], data_type
        ))
    main_has_parquet_files = has_parquet_files(
        s3,
        bucket_name=args["parquet_bucket"],
        prefix=get_parquet_dataset_s3_path(
            args["parquet_bucket"], args["main_namespace"], data_type
        ))
    if staging_has_parquet_files and main_has_parquet_files:
        compare_dict = compare_datasets_by_data_type(
            s3=s3,
            cfn_bucket=args["cfn_bucket"],
            input_bucket=args["input_bucket"],
            parquet_bucket=args["parquet_bucket"],
            staging_namespace=args["staging_namespace"],
            main_namespace=args["main_namespace"],
            s3_filesystem=fs,
            data_type=data_type,
        )
        # List of reports with their corresponding parameters
        ReportParams = namedtuple("ReportParams", ["file_name", "content"])
        staging_row_diffs = convert_dataframe_to_text(
            compare_row_diffs(compare_dict["compare_obj"], namespace="staging")
        )
        main_row_diffs = convert_dataframe_to_text(
            compare_row_diffs(compare_dict["compare_obj"], namespace="main")
        )
        staging_dups = convert_dataframe_to_text(
            get_duplicates(compare_dict["compare_obj"], namespace="staging")
        )
        main_dups = convert_dataframe_to_text(
            get_duplicates(compare_dict["compare_obj"], namespace="main")
        )
        reports = [
            ReportParams(
                content=compare_dict["comparison_report"],
                file_name="parquet_compare.txt",
            ),
            ReportParams(
                content=staging_row_diffs, file_name="all_diff_staging_rows.csv"
            ),
            ReportParams(content=main_row_diffs, file_name="all_diff_main_rows.csv"),
            ReportParams(content=staging_dups, file_name="all_dups_staging_rows.csv"),
            ReportParams(content=main_dups, file_name="all_dups_main_rows.csv"),
        ]
        upload_reports_to_s3(
            s3=s3,
            reports=reports,
            data_type=data_type,
            parquet_bucket=args["parquet_bucket"],
            staging_namespace=args["staging_namespace"],
        )
    return


if __name__ == "__main__":
    main()
