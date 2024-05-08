"""
This script runs as a Glue job and converts a collection of JSON files
(whose schema is defined by a Glue table), to a parquet dataset partitioned by
cohort. Additionally, if the table has nested data,
it will be separated into its own dataset with a predictable name. For example,
the healthkitv2heartbeat data type has a field called "SubSamples" which is an
array of objects. We will write out two parquet datasets in this case, a `healthkitv2heartbeat`
dataset and an `healthkitv2heartbeat_subsamples` dataset.
"""

import logging
import os
import sys
from collections import defaultdict
from copy import deepcopy
from enum import Enum

import boto3
import ecs_logging
import pandas
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import StructType
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col
from pyspark.sql.dataframe import DataFrame

# Configure logger to use ECS formatting
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(ecs_logging.StdlibFormatter())
logger.addHandler(handler)
logger.propagate = False

INDEX_FIELD_MAP = {
    "enrolledparticipants": ["ParticipantIdentifier"],
    "fitbitprofiles": ["ParticipantIdentifier", "ModifiedDate"],
    "fitbitdevices": ["ParticipantIdentifier", "Date"],
    "fitbitactivitylogs": ["ParticipantIdentifier", "LogId"],
    "fitbitdailydata": ["ParticipantIdentifier", "Date"],
    "fitbitecg": ["ParticipantIdentifier", "FitbitEcgKey"],
    "fitbitintradaycombined": ["ParticipantIdentifier", "Type", "DateTime"],
    "fitbitrestingheartrates": ["ParticipantIdentifier", "Date"],
    "fitbitsleeplogs": ["ParticipantIdentifier", "LogId"],
    "healthkitv2characteristics": ["ParticipantIdentifier", "HealthKitCharacteristicKey"],
    "healthkitv2samples": ["ParticipantIdentifier", "HealthKitSampleKey"],
    "healthkitv2heartbeat": ["ParticipantIdentifier", "HealthKitHeartbeatSampleKey"],
    "healthkitv2statistics": ["ParticipantIdentifier", "HealthKitStatisticKey"],
    "healthkitv2clinicalrecords": ["ParticipantIdentifier", "HealthKitClinicalRecordKey"],
    "healthkitv2electrocardiogram": ["ParticipantIdentifier", "HealthKitECGSampleKey"],
    "healthkitv2workouts": ["ParticipantIdentifier", "HealthKitWorkoutKey"],
    "healthkitv2activitysummaries": ["ParticipantIdentifier", "HealthKitActivitySummaryKey"],
    "garminactivitydetailssummary": ["ParticipantIdentifier", "SummaryId"],
    "garminactivitysummary": ["ParticipantIdentifier", "SummaryId"],
    "garminbloodpressuresummary": ["ParticipantIdentifier", "SummaryId"],
    "garmindailysummary": ["ParticipantIdentifier", "StartTimeInSeconds"],
    "garminepochsummary": ["ParticipantIdentifier", "SummaryId"],
    "garminhealthsnapshotsummary": ["ParticipantIdentifier", "StartTimeInSeconds"],
    "garminhrvsummary": ["ParticipantIdentifier", "StartTimeInSeconds"],
    "garminmanuallyupdatedactivitysummary": ["ParticipantIdentifier", "SummaryId"],
    "garminmoveiqactivitysummary": ["ParticipantIdentifier", "SummaryId"],
    "garminpulseoxsummary": ["ParticipantIdentifier", "SummaryId"],
    "garminrespirationsummary": ["ParticipantIdentifier", "SummaryId"],
    "garminsleepsummary": [
        "ParticipantIdentifier",
        "StartTimeInSeconds",
        "DurationInSeconds",
        "Validation",
    ],
    "garminstressdetailsummary": ["ParticipantIdentifier", "StartTimeInSeconds"],
    "garminthirdpartydailysummary": ["ParticipantIdentifier", "StartTimeInSeconds"],
    "garminusermetricssummary": ["ParticipantIdentifier", "CalenderDate"],
    "googlefitsamples": ["ParticipantIdentifier", "GoogleFitSampleKey"],
    "symptomlog": ["ParticipantIdentifier", "DataPointKey"],
}


def get_args() -> tuple[dict, dict]:
    """
    Get job and workflow arguments.

    Returns:
        (tuple) : (job arguments (dict), workflow run properties (dict))
    """
    glue_client = boto3.client("glue")
    args = getResolvedOptions(
        sys.argv, ["WORKFLOW_NAME", "WORKFLOW_RUN_ID", "JOB_NAME", "glue-table"]
    )
    workflow_run_properties = glue_client.get_workflow_run_properties(
        Name=args["WORKFLOW_NAME"], RunId=args["WORKFLOW_RUN_ID"]
    )["RunProperties"]
    return args, workflow_run_properties


def has_nested_fields(schema: StructType) -> bool:
    """
    Determine whether a DynamicFrame schema has struct or array fields.

    If the DynamicFrame does not have fields of these data types, it is flat and
    can be written directly to S3.  If it does, then the DynamicFrame will need
    to be 'relationalized' so that all the data contained in the DynamicFrame has
    been flattened.

    Args:
        schema (awsglue.gluetypes.StructType): The schema of a DynamicFrame.

    Returns:
        bool: Whether this schema contains struct or array fields.
    """
    array_struct_cols = [
        col.dataType.typeName() in ["array", "struct"] for col in schema
    ]
    if any(array_struct_cols):
        return True
    return False


def get_table(
    table_name: str,
    database_name: str,
    glue_context: GlueContext,
    record_counts: dict,
    logger_context: dict,
    ) -> DynamicFrame:
    """
    Return a table as a DynamicFrame with an unambiguous schema. Additionally,
    we drop any superfluous partition_* fields which are added by Glue.

    Args:
        table_name (str): The name of the Glue table.
        database_name (str): The name of the Glue database
        glue_context (GlueContext): The glue context
        logger_context (dict): A dictionary containing contextual information
            to include with every log.

    Returns:
        awsglue.DynamicFrame: The Glue Table's data as a DynamicFrame
    """
    table = glue_context.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
        additional_options={"groupFiles": "inPartition"},
        transformation_ctx="create_dynamic_frame",
    ).resolveChoice(
        choice="match_catalog", database=database_name, table_name=table_name
    )
    partition_fields = []
    for field in table.schema():
        if "partition_" in field.name:  # superfluous field added by Glue
            partition_fields.append(field.name)
    if len(partition_fields) > 0:
        table = table.drop_fields(paths=partition_fields)
    count_records_for_event(
        table=table.toDF(),
        event=CountEventType.READ,
        record_counts=record_counts,
        logger_context=logger_context,
    )
    return table


def drop_table_duplicates(
    table: DynamicFrame,
    data_type: str,
    record_counts: dict[str, list],
    logger_context: dict,
    ) -> DataFrame:
    """
    Drop duplicate samples and superflous partition columns.

    Duplicate samples are dropped by referencing the `InsertedDate`
    field, keeping the more recent sample. For any data types which
    don't have this field, we drop duplicates by referencing `export_end_date`.


    Args:
        table (DynamicFrame): The table from which to drop duplicates
        data_type (str): The data type. Helps determine
            the table index in conjunction with INDEX_FIELD_MAP.
        record_counts (dict[str,list]): A dict mapping data types to a list
            of counts, each of which corresponds to an `event` type.
        logger_context (dict): A dictionary containing contextual information
            to include with every log.

    Returns:
        pyspark.sql.dataframe.DataFrame: A Spark dataframe with duplicates removed.
    """
    window_unordered = Window.partitionBy(INDEX_FIELD_MAP[data_type])
    spark_df = table.toDF()
    if "InsertedDate" in spark_df.columns:
        window_ordered = window_unordered.orderBy(
                col("InsertedDate").desc(),
                col("export_end_date").desc()
        )
    else:
        window_ordered = window_unordered.orderBy(
                col("export_end_date").desc()
        )
    table_no_duplicates = (
            spark_df
            .withColumn('ranking', row_number().over(window_ordered))
            .filter("ranking == 1")
            .drop("ranking")
            .cache()
    )
    count_records_for_event(
        table=table_no_duplicates,
        event=CountEventType.DROP_DUPLICATES,
        record_counts=record_counts,
        logger_context=logger_context,
    )
    return table_no_duplicates


def drop_deleted_healthkit_data(
    glue_context: GlueContext,
    table: DataFrame,
    table_name: str,
    data_type: str,
    glue_database: str,
    record_counts: dict[str, list],
    logger_context: dict,
    ) -> DataFrame:
    """
    Drop records from a HealthKit table.

    This function will attempt to fetch the respective *_deleted table
    for a table containing HealthKit data. If no *_deleted table is found,
    then the HealthKit table is return unmodified. Otherwise, a diff is taken
    upon the HealthKit table, using the index field specified in
    `INDEX_FIELD_MAP` as reference.

    Args:
        glue_context (GlueContext): The glue context
        table (DynamicFrame): A DynamicFrame containing HealthKit data
        glue_database (str): The name of the Glue database containing
            the *_deleted table
        record_counts (dict[str,list]): A dict mapping data types to a list
            of counts, each of which corresponds to an `event` type.
        logger_context (dict): A dictionary containing contextual information
            to include with every log.

    Returns:
        DynamicFrame: A DynamicFrame with the respective *_deleted table's
            samples removed.
    """
    glue_client = boto3.client("glue")
    deleted_table_name = f"{table_name}_deleted"
    deleted_data_type = f"{data_type}_deleted"
    try:
        glue_client.get_table(DatabaseName=glue_database, Name=deleted_table_name)
    except glue_client.exceptions.EntityNotFoundException:
        return table
    deleted_table_logger_context = deepcopy(logger_context)
    deleted_table_logger_context["labels"]["glue_table_name"] = deleted_table_name
    deleted_table_logger_context["labels"]["type"] = deleted_data_type
    deleted_table_raw = get_table(
        table_name=deleted_table_name,
        database_name=glue_database,
        glue_context=glue_context,
        record_counts=record_counts,
        logger_context=deleted_table_logger_context,
    )
    # we use `data_type` rather than `deleted_data_type` here because they share
    # an index (we don't bother including `deleted_data_type` in `INDEX_FIELD_MAP`).
    deleted_table = drop_table_duplicates(
        table=deleted_table_raw,
        data_type=data_type,
        record_counts=record_counts,
        logger_context=deleted_table_logger_context,
    )
    table_with_deleted_samples_removed = table.join(
                other=deleted_table,
                on=INDEX_FIELD_MAP[data_type],
                how="left_anti",
    )
    count_records_for_event(
        table=table_with_deleted_samples_removed,
        event=CountEventType.DROP_DELETED_SAMPLES,
        record_counts=record_counts,
        logger_context=logger_context,
    )
    return table_with_deleted_samples_removed


def archive_existing_datasets(
    bucket: str,
    key_prefix: str,
    workflow_name: str,
    workflow_run_id: str,
    delete_upon_completion: bool,
    ) -> list[dict]:
    """
    Archives existing datasets in S3 by copying them to a timestamped subfolder
    within an "archive" folder. The format of the timestamped subfolder is:

        {year}_{month}_{day}_{workflow_run_id}

    If the dataset does not exist at `key_prefix` or the dataset is empty,
    no action will be taken.

    Args:
        glue_context (GlueContext): The Glue context
        bucket (str): The name of the S3 bucket containing the dataset to archive.
        key_prefix (str): The S3 key prefix of the objects to be archived.
            Ensure that this ends with a trailing "/" to avoid matching
            child datasets.
        workflow_name (str): The Glue workflow name.
        workflow_run_id (str): The Glue workflow run ID.
        delete_upon_completion (bool): Whether to delete the dataset after archiving.

    Returns:
        None

    Raises:
        N/A
    """
    glue_client = boto3.client("glue")
    workflow_run = glue_client.get_workflow_run(
        Name=workflow_name, RunId=workflow_run_id
    )
    workflow_date = workflow_run["Run"]["StartedOn"].strftime("%Y_%m_%d")
    this_archive_collection = "_".join([workflow_date, workflow_run_id])
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key_prefix)
    if "Contents" not in response:
        return list()
    source_objects = [obj for obj in response["Contents"] if obj["Size"] > 0]
    archived_objects = []
    for source_object in source_objects:
        this_object = {"Bucket": bucket, "Key": source_object["Key"]}
        destination_key = os.path.join(
            os.path.dirname(key_prefix[:-1]),  # .../parquet/
            "archive",
            this_archive_collection,  # {year}_{month}_{day}_{workflow_run_id}
            os.path.basename(key_prefix[:-1]),  # {dataset}
            os.path.relpath(this_object["Key"], key_prefix),  # some/path/object.parquet
        )
        logger.info(f"Copying {this_object['Key']} to {destination_key}")
        copy_response = s3_client.copy_object(
            CopySource=this_object, Bucket=bucket, Key=destination_key
        )
        archived_objects.append(this_object)
    if delete_upon_completion and archived_objects:
        deleted_response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in archived_objects]},
        )
    return archived_objects


def write_table_to_s3(
    dynamic_frame: DynamicFrame,
    bucket: str,
    key: str,
    glue_context: GlueContext,
    workflow_name: str,
    workflow_run_id: str,
    records_per_partition: int = int(1e6),
    ) -> None:
    """
    Write a DynamicFrame to S3 as a parquet dataset.

    Args:
        dynamic_frame (awsglue.DynamicFrame): A DynamicFrame.
        bucket (str): An S3 bucket name.
        key (str): The key to which we write the `dynamic_frame`.
        glue_context (GlueContext): The glue context
        workflow_name (str): The Glue workflow name
        workflow_run_id (str): The Glue workflow run ID
        records_per_partition (int): The number of records (rows) to include
            in each partition. Defaults to 1e6.

    Returns:
        None
    """
    s3_write_path = os.path.join("s3://", bucket, key)
    num_partitions = int(dynamic_frame.count() // records_per_partition + 1)
    dynamic_frame = dynamic_frame.coalesce(num_partitions)
    logger.info(f"Archiving {os.path.basename(key)}")
    archive_existing_datasets(
        bucket=bucket,
        key_prefix=key + "/",
        workflow_name=workflow_name,
        workflow_run_id=workflow_run_id,
        delete_upon_completion=True,
    )
    logger.info(
        f"Writing {os.path.basename(key)} to {s3_write_path} "
        f"as {num_partitions} partitions."
    )
    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": s3_write_path, "partitionKeys": ["cohort"]},
        format="parquet",
        transformation_ctx="write_dynamic_frame",
    )


class CountEventType(Enum):
    """The event associated with a count."""

    READ = "READ"
    """
    This table has just now been read from the Glue table catalog
    and has not yet had any transformations done to it.
    """

    DROP_DUPLICATES = "DROP_DUPLICATES"
    """
    This table has just now had duplicate records dropped
    (see the function `drop_table_duplicates`).
    """

    DROP_DELETED_SAMPLES = "DROP_DELETED_SAMPLES"
    """
    This table has just now had records which are present in its respective
    "Deleted" table dropped (see the function `drop_deleted_healthkit_data`).
    """

    WRITE = "WRITE"
    """
    This table has just now been written to S3.
    """


def count_records_for_event(
    table: DataFrame,
    event: CountEventType,
    record_counts: dict[str, list],
    logger_context: dict,
    ) -> dict[str, list]:
    """
    Compute record count statistics for each `export_end_date`.

    Note that `record_counts` is a cumulative object. We add counts for each
    `event` to its respective data type.

    Args:
        table (pyspark.sql.dataframe.DataFrame): The dataframe to derive counts from.
        event (CountEventType): The event associated with this count. See class
            `CountEventType` for allowed values and their descriptions.
        record_counts (dict[str,list]): A dict mapping data types to a list
            of counts, each of which corresponds to an `event` type. This object
            is built up cumulatively by this function over the course of this job.
        logger_context (dict): A dictionary containing contextual information
            to include with every log. Must include the keys `process.parent.pid`
            (containing the workflow_run_id) and `.['labels']['type']` (containing
            the data type).

    Returns:
        (dict[str,list]) : The aggregate record counts collected for each data type.
            A record count is a pandas DataFrame with the below schema:

            workflow_run_id (str): the workflow run ID of this workflow
            data_type (str): the data type
            event (str): the event type (see parameter `event`)
            export_end_date (str): An ISO8601-formatted datetime string which
                corresponds to the export which this record originated.
            count (int): The record count
    """
    if table.count() == 0:
        return record_counts
    table_counts = table.groupby(["export_end_date"]).count().toPandas()

    table_counts["workflow_run_id"] = logger_context["process.parent.pid"]
    table_counts["data_type"] = logger_context["labels"]["type"]
    table_counts["event"] = event.value
    record_counts[logger_context["labels"]["type"]].append(table_counts)
    return record_counts


def store_record_counts(
    record_counts: dict[str, list],
    parquet_bucket: str,
    namespace: str,
    workflow_name: str,
    workflow_run_id: str,
    ) -> dict[str, str]:
    """
    Uploads record counts as S3 objects.

    Objects are uploaded to:

        s3://{parquet_bucket}/{namespace}/record_counts/{workflow_start_date}/
             {workflow_run_id}/{data_type}_{workflow_run_id}.csv

    Args:
        record_counts (dict[str,list]): A dict mapping data types to a list
            of counts, each of which corresponds to an `event` type.
        parquet_bucket (str): The name of the bucket which we write Parquet data to.
        namespace (str): The namespace
        workflow_name (str): The name of this workflow
        workflow_run_id (str): The run ID of this workflow.

    Returns:
        dict: Mapping data types to their S3 object key
    """
    glue_client = boto3.client("glue")
    workflow_run = glue_client.get_workflow_run(
        Name=workflow_name, RunId=workflow_run_id
    )
    workflow_start_date = workflow_run["Run"]["StartedOn"].strftime("%Y_%m_%d")
    s3_key_prefix = os.path.join(
        namespace, "record_counts", f"{workflow_start_date}_{workflow_run_id}"
    )
    s3_client = boto3.client("s3")
    uploaded_objects = {}
    for data_type in record_counts:
        basename = f"{data_type}_{workflow_run_id}.csv"
        s3_key = os.path.join(s3_key_prefix, basename)
        all_counts = pandas.concat(record_counts[data_type])
        all_counts.to_csv(basename)
        with open(basename, "rb") as counts_file:
            s3_client.put_object(Body=counts_file, Bucket=parquet_bucket, Key=s3_key)
            uploaded_objects[data_type] = s3_key
    return uploaded_objects


def add_index_to_table(
    table_key: str,
    table_name: str,
    processed_tables: dict[str, DynamicFrame],
    unprocessed_tables: dict[str, DynamicFrame],
    ) -> DataFrame:
    """Add partition and index fields to a DynamicFrame.

    A DynamicFrame containing the top-level fields already includes the index
    fields -- `ParticipantIdentifier` and a primary key which is particular to
    each data type (see global var `INDEX_FIELD_MAP`) -- but DynamicFrame's which
    were flattened as a result of the DynamicFrame.relationalize operation need
    to inherit the index and partition fields from their parent. We also propagate
    the `ParticipantID` field where present, although this field is not included
    in the SymptomLog data type. In order for this function to execute
    successfully, the table's parent must already have the index fields and be
    included in `processed_tables`.

    In addition to adding the index fields, this function formats the names
    of the (non-index) fields which were manipulated by the call to
    DynamicFrame.relationalize.

    Args:
        table_key (str): A key from the dict object returned by DynamicFrame.relationalize
        table_name (str): The name of the top-level parent table. All `table_key` values
            ought to be prefixed by this name.
        processed_tables (dict): A mapping from table keys to DynamicFrames which
            already have an index. Typically, this function will be invoked
            iteratively on a sorted list of table keys so that it is guaranteed
            that a child table may always reference the index of its parent table.
        unprocessed_tables (dict): A mapping from table keys to DynamicFrames which
        don't yet have an index.

    Returns:
        awsglue.DynamicFrame with index columns
    """
    _, table_data_type = table_name.split("_")
    this_table = unprocessed_tables[table_key].toDF()
    logger.info(f"Adding index to {table_name}")
    if table_key == table_name:  # top-level fields already include index
        for c in list(this_table.columns):
            if "." in c:  # a flattened struct field
                c_new = c.replace(".", "_")
                logger.info(f"Renaming field {c} to {c_new}")
                this_table = this_table.withColumnRenamed(c, c_new)
        df_with_index = this_table
    else:
        if ".val." in table_key:
            hierarchy = table_key.split(".")
            parent_key = ".".join(hierarchy[:-1])
            original_field_name = hierarchy[-1]
            parent_table = processed_tables[parent_key]
        else:  # table_key is the value of a top-level field
            parent_key = table_name
            original_field_name = table_key.replace(f"{table_name}_", "")
            parent_table = unprocessed_tables[parent_key].toDF()
        if "." in original_field_name:
            selectable_original_field_name = f"`{original_field_name}`"
        else:
            selectable_original_field_name = original_field_name
        logger.info(f"Adding index to {original_field_name}")
        index_fields = INDEX_FIELD_MAP[table_data_type]
        additional_fields = [selectable_original_field_name, "cohort"]
        if "ParticipantID" in parent_table.columns:
            additional_fields.append("ParticipantID")
        parent_index = parent_table.select(index_fields + additional_fields).distinct()
        this_index = parent_index.withColumnRenamed(original_field_name, "id")
        df_with_index = this_table.join(this_index, on="id", how="inner")
        # remove prefix from field names
        field_prefix = table_key.replace(f"{table_name}_", "") + ".val."
        columns = list(df_with_index.columns)
        for c in columns:
            # do nothing if c is id, index, or partition field
            if f"{original_field_name}.val" == c:  # field is an array
                succinct_name = c.replace(".", "_")
                logger.info(f"Renaming field {c} to {succinct_name}")
                df_with_index = df_with_index.withColumnRenamed(c, succinct_name)
            elif field_prefix in c:
                succinct_name = c.replace(field_prefix, "").replace(".", "_")
                if succinct_name in df_with_index.columns:
                    # If key is a duplicate, use longer format
                    succinct_name = c.replace(".val", "").replace(".", "_")
                if succinct_name in df_with_index.columns:
                    # If key is still duplicate, leave unchanged
                    continue
                logger.info(f"Renaming field {c} to {succinct_name}")
                df_with_index = df_with_index.withColumnRenamed(c, succinct_name)
    return df_with_index


def main() -> None:
    # Get args and setup environment
    args, workflow_run_properties = get_args()
    glue_context = GlueContext(SparkContext.getOrCreate())
    table_name = args["glue_table"]
    data_type = args["glue_table"].split("_")[1]
    logger_context = {
        "labels": {
            "glue_table_name": table_name,
            "type": data_type,
            "job_name": args["JOB_NAME"],
        },
        "process.parent.pid": args["WORKFLOW_RUN_ID"],
        "process.parent.name": args["WORKFLOW_NAME"],
    }
    record_counts = defaultdict(list)
    logger.info(f"Job args: {args}")
    logger.info(f"Workflow run properties: {workflow_run_properties}")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Read table and drop duplicated and deleted samples
    table_raw = get_table(
        table_name=table_name,
        database_name=workflow_run_properties["glue_database"],
        glue_context=glue_context,
        record_counts=record_counts,
        logger_context=logger_context,
    )
    if table_raw.count() == 0:
        return
    table = drop_table_duplicates(
        table=table_raw,
        data_type=data_type,
        record_counts=record_counts,
        logger_context=logger_context,
    )
    if "healthkit" in table_name:
        table = drop_deleted_healthkit_data(
            glue_context=glue_context,
            table=table,
            table_name=table_name,
            data_type=data_type,
            glue_database=workflow_run_properties["glue_database"],
            record_counts=record_counts,
            logger_context=logger_context,
        )
    table_dynamic = DynamicFrame.fromDF(
            dataframe=table,
            glue_ctx=glue_context,
            name=table_name
    )
    # Export new table records to parquet
    if has_nested_fields(table.schema):
        tables_with_index = {}
        table_relationalized = table_dynamic.relationalize(
            root_table_name=table_name,
            staging_path=f"s3://{workflow_run_properties['parquet_bucket']}/tmp/",
            transformation_ctx="relationalize",
        )
        # Inject record identifier into child tables
        ordered_keys = list(sorted(table_relationalized.keys()))
        for k in ordered_keys:
            tables_with_index[k] = add_index_to_table(
                table_key=k,
                table_name=table_name,
                processed_tables=tables_with_index,
                unprocessed_tables=table_relationalized,
            )
        for t in tables_with_index:
            clean_name = t.replace(".", "_").lower()
            dynamic_frame_with_index = DynamicFrame.fromDF(
                tables_with_index[t], glue_ctx=glue_context, name=clean_name
            )
            write_table_to_s3(
                dynamic_frame=dynamic_frame_with_index,
                bucket=workflow_run_properties["parquet_bucket"],
                key=os.path.join(
                    workflow_run_properties["namespace"],
                    workflow_run_properties["parquet_prefix"],
                    clean_name,
                ),
                workflow_name=args["WORKFLOW_NAME"],
                workflow_run_id=args["WORKFLOW_RUN_ID"],
                glue_context=glue_context,
            )
        count_records_for_event(
            table=tables_with_index[ordered_keys[0]],
            event=CountEventType.WRITE,
            record_counts=record_counts,
            logger_context=logger_context,
        )
    else:
        write_table_to_s3(
            dynamic_frame=table_dynamic,
            bucket=workflow_run_properties["parquet_bucket"],
            key=os.path.join(
                workflow_run_properties["namespace"],
                workflow_run_properties["parquet_prefix"],
                args["glue_table"],
            ),
            workflow_name=args["WORKFLOW_NAME"],
            workflow_run_id=args["WORKFLOW_RUN_ID"],
            glue_context=glue_context,
        )
        count_records_for_event(
            table=table_dynamic.toDF(),
            event=CountEventType.WRITE,
            record_counts=record_counts,
            logger_context=logger_context,
        )
    store_record_counts(
        record_counts=record_counts,
        parquet_bucket=workflow_run_properties["parquet_bucket"],
        namespace=workflow_run_properties["namespace"],
        workflow_name=args["WORKFLOW_NAME"],
        workflow_run_id=args["WORKFLOW_RUN_ID"],
    )
    job.commit()


if __name__ == "__main__":
    main()
