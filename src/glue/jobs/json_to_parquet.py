"""
This script runs as a Glue job and converts a collection of JSON files
(whose schema is defined by a Glue table), to a parquet dataset partitioned by
assessmentid / year / month / day. Additionally, if the table has nested data,
it will be separated into its own dataset with a predictable name. For example,
the info table (derived from info.json) has a field called "files" which is an
array of objects. We will write out two parquet datasets in this case, an `info`
dataset and an `info_files` dataset.

Before writing our tables to parquet datasets, we add the recordid,
assessmentid, year, month, and day to each record in each table.
"""

import os
import sys
from datetime import datetime

import boto3
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import StructType
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

INDEX_FIELD_MAP = {
    "enrolledparticipants": ["ParticipantIdentifier"],
    "fitbitprofiles": ["ParticipantIdentifier", "ModifiedDate"],
    "fitbitdevices": ["ParticipantIdentifier", "Date"],
    "fitbitactivitylogs": ["LogId"],
    "fitbitdailydata": ["ParticipantIdentifier", "Date"],
    "fitbitintradaycombined": ["ParticipantIdentifier", "Type", "DateTime"],
    "fitbitrestingheartrates": ["ParticipantIdentifier", "Date"],
    "fitbitsleeplogs": ["LogId"],
    "healthkitv2characteristics": ["HealthKitCharacteristicKey"],
    "healthkitv2samples": ["HealthKitSampleKey"],
    "healthkitv2heartbeat": ["HealthKitHeartbeatSampleKey"],
    "healthkitv2statistics": ["HealthKitStatisticKey"],
    "healthkitv2clinicalrecords": ["HealthKitClinicalRecordKey"],
    "healthkitv2electrocardiogram": ["HealthKitECGSampleKey"],
    "healthkitv2workouts": ["HealthKitWorkoutKey"],
    "healthkitv2activitysummaries": ["HealthKitActivitySummaryKey"],
    "garminactivitydetailssummary": ["ParticipantID", "SummaryId"],
    "garminactivitysummary": ["ParticipantID", "SummaryId"],
    "garminbloodpressuresummary": ["ParticipantID", "SummaryId"],
    "garmindailysummary": ["ParticipantID", "StartTimeInSeconds"],
    "garminepochsummary": ["ParticipantID", "SummaryId"],
    "garminhealthsnapshotsummary": ["ParticipantID", "StartTimeInSeconds"],
    "garminhrvsummary": ["ParticipantID", "StartTimeInSeconds"],
    "garminmanuallyupdatedactivitysummary": ["ParticipantID", "SummaryId"],
    "garminmoveiqactivitysummary": ["ParticipantID", "SummaryId"],
    "garminpulseoxsummary": ["ParticipantID", "SummaryId"],
    "garminrespirationsummary": ["ParticipantID", "SummaryId"],
    "garminsleepsummary": ["ParticipantID", "StartTimeInSeconds", "DurationInSeconds", "Validation"],
    "garminstressdetailsummary": ["ParticipantID", "StartTimeInSeconds"],
    "garminthirdpartydailysummary": ["ParticipantID", "StartTimeInSeconds"],
    "garminusermetricssummary": ["ParticipantID", "CalenderDate"],
    "googlefitsamples": ["GoogleFitSampleKey"],
    "symptomlog": ["DataPointKey"]
}

def get_args() -> tuple[dict, dict]:
    """
    Get job and workflow arguments.

    Returns:
        (tuple) : (job arguments (dict), workflow run properties (dict))
    """
    glue_client = boto3.client("glue")
    args = getResolvedOptions(
             sys.argv,
             ["WORKFLOW_NAME",
              "WORKFLOW_RUN_ID",
              "JOB_NAME",
              "glue-table"])
    workflow_run_properties = glue_client.get_workflow_run_properties(
            Name=args["WORKFLOW_NAME"],
            RunId=args["WORKFLOW_RUN_ID"])["RunProperties"]
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
    for col in schema:
        if col.dataType.typeName() in ["array", "struct"]:
            return True
    return False

def get_table(
        table_name: str,
        database_name: str,
        glue_context: GlueContext
    ) -> DynamicFrame:
    """
    Return a table as a DynamicFrame with an unambiguous schema.
    Duplicate samples are dropped by referencing the `InsertedDate`
    field, keeping the more recent sample. For any data types which
    don't have this field, we drop duplicates by referencing `export_end_date`.

    Args:
        table_name (str): The name of the Glue table.
        database_name (str): The name of the Glue database
        glue_context (GlueContext): The glue context

    Returns:
        awsglue.DynamicFrame: The Glue Table's data as a DynamicFrame
            after the duplicates have been dropped by descending
            export date.
    """
    table_name_components = table_name.split("_")
    table_data_type = table_name_components[1]
    table = (
            glue_context.create_dynamic_frame.from_catalog(
                 database=database_name,
                 table_name=table_name,
                 additional_options={"groupFiles": "inPartition"},
                 transformation_ctx="create_dynamic_frame"
            )
            .resolveChoice(
                choice="match_catalog",
                database=database_name,
                table_name=table_name
            )
    )
    if table.count() == 0:
        return table
    field_names = [field.name for field in table.schema().fields]
    spark_df = table.toDF()
    if "InsertedDate" in field_names:
        sorted_spark_df = spark_df.sort(spark_df.InsertedDate.desc())
    else:
        sorted_spark_df = spark_df.sort(spark_df.export_end_date.desc())
    table = DynamicFrame.fromDF(
            dataframe=(
                sorted_spark_df
                .dropDuplicates(
                    subset=INDEX_FIELD_MAP[table_data_type]
                )
            ),
            glue_ctx=glue_context,
            name=table_name
    )
    return table

def drop_deleted_healthkit_data(
        glue_context: GlueContext,
        table: DynamicFrame,
        glue_database: str
    ) -> DynamicFrame:
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

    Returns:
        DynamicFrame: A DynamicFrame with the respective *_deleted table's
            samples removed.
    """
    glue_client = boto3.client("glue")
    deleted_table_name = f"{table.name}_deleted"
    table_data_type = table.name.split("_")[1]
    try:
        glue_client.get_table(
                DatabaseName=glue_database,
                Name=deleted_table_name
        )
    except glue_client.exceptions.EntityNotFoundException:
        return table
    deleted_table = get_table(
            table_name=deleted_table_name,
            database_name=glue_database,
            glue_context=glue_context
    )
    table_df = table.toDF()
    deleted_table_df = deleted_table.toDF()
    table_with_deleted_samples_removed = DynamicFrame.fromDF(
            dataframe=(
                table_df.join(
                    other=deleted_table_df,
                    on=INDEX_FIELD_MAP[table_data_type],
                    how="left_anti"
                )
            ),
            glue_ctx=glue_context,
            name=table.name
    )
    return table_with_deleted_samples_removed

def archive_existing_datasets(
        glue_context: GlueContext,
        bucket: str,
        key_prefix: str,
        workflow_run_id: str,
        delete_upon_completion: bool
    ) -> None:
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
        workflow_run_id (str): The Glue workflow run ID.
        delete_upon_completion (bool): Whether to delete the dataset after archiving.

    Returns:
        None

    Raises:
        N/A
    """
    logger = glue_context.get_logger()
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=key_prefix
    )
    if "Contents" not in response:
        return
    formatted_datetime = ( # e.g., 2023_06_28
            str(datetime.now())
            .split(" ")[0]
            .replace('-', '_')
    )
    this_archive_collection = "_".join([formatted_datetime, workflow_run_id])
    source_objects = [
            obj for obj in response["Contents"] if obj["Size"] > 0]
    objects_to_delete = []
    for source_object in source_objects:
        this_object = {
                "Bucket": bucket,
                "Key": source_object["Key"]
        }
        destination_key = os.path.join(
                os.path.dirname(key_prefix[:-1]), # .../parquet/
                "archive",
                this_archive_collection, # {year}_{month}_{day}_{workflow_run_id}
                os.path.basename(key_prefix[:-1]), # {dataset}
                os.path.relpath(this_object["Key"], key_prefix) # some/path/object.parquet
        )
        logger.info(f"Copying {this_object['Key']} to {destination_key}")
        copy_response = s3_client.copy_object(
                CopySource=this_object,
                Bucket=bucket,
                Key=destination_key
        )
        this_object.pop("Bucket")
        objects_to_delete.append(this_object)
    if delete_upon_completion and objects_to_delete:
        s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects_to_delete}
        )

def write_table_to_s3(
        dynamic_frame: DynamicFrame,
        bucket: str,
        key: str,
        glue_context: GlueContext,
        workflow_run_id: str,
        records_per_partition: int = int(1e6)
    ) -> None:
    """
    Write a DynamicFrame to S3 as a parquet dataset.

    Args:
        dynamic_frame (awsglue.DynamicFrame): A DynamicFrame.
        bucket (str): An S3 bucket name.
        key (str): The key to which we write the `dynamic_frame`.
        glue_context (GlueContext): The glue context
        workflow_run_id (str): The Glue workflow run ID
        records_per_partition (int): The number of records (rows) to include
            in each partition. Defaults to 1e6.

    Returns:
        None
    """
    logger = glue_context.get_logger()
    s3_write_path = os.path.join("s3://", bucket, key)
    num_partitions = int(dynamic_frame.count() // records_per_partition + 1)
    dynamic_frame = dynamic_frame.coalesce(num_partitions)
    logger.info(f"Archiving {os.path.basename(key)}")
    archive_existing_datasets(
            glue_context=glue_context,
            bucket=bucket,
            key_prefix=key+"/",
            workflow_run_id=workflow_run_id,
            delete_upon_completion=True
    )
    logger.info(f"Writing {os.path.basename(key)} to {s3_write_path} "
                f"as {num_partitions} partitions.")
    glue_context.write_dynamic_frame.from_options(
            frame = dynamic_frame,
            connection_type = "s3",
            connection_options = {
                "path": s3_write_path,
                "partitionKeys": ["cohort"]
            },
            format = "parquet",
            transformation_ctx="write_dynamic_frame")

def add_index_to_table(
        table_key: str,
        table_name: str,
        processed_tables: dict[str, DynamicFrame],
        unprocessed_tables: dict[str, DynamicFrame],
        glue_context: GlueContext
    ) -> DynamicFrame:
    """Add partition and index fields to a DynamicFrame.

    A DynamicFrame containing the top-level fields already includes the index
    fields, but DynamicFrame's which were flattened as a result of the
    DynamicFrame.relationalize operation need to inherit the index and partition
    fields from their parent. In order for this function to execute successfully,
    the table's parent must already have the index fields and be included in
    `processed_tables`.

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
        glue_context (GlueContext): The glue context

    Returns:
        awsglue.DynamicFrame with index columns
    """
    logger = glue_context.get_logger()
    _, table_data_type = table_name.split("_")
    this_table = unprocessed_tables[table_key].toDF()
    logger.info(f"Adding index to {table_name}")
    if table_key == table_name: # top-level fields already include index
        for c in list(this_table.columns):
            if "." in c: # a flattened struct field
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
        else: # table_key is the value of a top-level field
            parent_key = table_name
            original_field_name = table_key.replace(f"{table_name}_", "")
            parent_table = unprocessed_tables[parent_key].toDF()
        if "." in original_field_name:
            selectable_original_field_name = f"`{original_field_name}`"
        else:
            selectable_original_field_name = original_field_name
        logger.info(f"Adding index to {original_field_name}")
        parent_index = (parent_table
                .select(
                    ([selectable_original_field_name, "cohort"]
                     + INDEX_FIELD_MAP[table_data_type])
                ).distinct())
        this_index = parent_index.withColumnRenamed(original_field_name, "id")
        df_with_index = this_table.join(
                this_index,
                on = "id",
                how = "inner")
        # remove prefix from field names
        field_prefix = table_key.replace(f"{table_name}_", "") + ".val."
        columns = list(df_with_index.columns)
        for c in columns:
            # do nothing if c is id, index, or partition field
            if f"{original_field_name}.val" == c: # field is an array
                succinct_name = c.replace(".", "_")
                logger.info(f"Renaming field {c} to {succinct_name}")
                df_with_index = df_with_index.withColumnRenamed(
                        c, succinct_name)
            elif field_prefix in c:
                succinct_name = c.replace(field_prefix, "").replace(".", "_")
                if succinct_name in df_with_index.columns:
                    # If key is a duplicate, use longer format
                    succinct_name = c.replace(".val", "").replace(".", "_")
                if succinct_name in df_with_index.columns:
                    # If key is still duplicate, leave unchanged
                    continue
                logger.info(f"Renaming field {c} to {succinct_name}")
                df_with_index = df_with_index.withColumnRenamed(
                        c, succinct_name)
    return df_with_index

def main() -> None:
    # Get args and setup environment
    args, workflow_run_properties = get_args()
    glue_context = GlueContext(SparkContext.getOrCreate())
    logger = glue_context.get_logger()
    logger.info(f"Job args: {args}")
    logger.info(f"Workflow run properties: {workflow_run_properties}")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Get table info
    table_name = args["glue_table"]
    table = get_table(
            table_name=table_name,
            database_name=workflow_run_properties["glue_database"],
            glue_context=glue_context
    )
    if table.count() == 0:
        return
    if "healthkit" in table_name:
        table = drop_deleted_healthkit_data(
                glue_context=glue_context,
                table=table,
                glue_database=workflow_run_properties["glue_database"]
        )

    # Export new table records to parquet
    if has_nested_fields(table.schema()):
        tables_with_index = {}
        table_relationalized = table.relationalize(
            root_table_name = table_name,
            staging_path = f"s3://{workflow_run_properties['parquet_bucket']}/tmp/",
            transformation_ctx="relationalize")
        # Inject record identifier into child tables
        for k in sorted(table_relationalized.keys()):
            tables_with_index[k] = add_index_to_table(
                    table_key=k,
                    table_name=table_name,
                    processed_tables=tables_with_index,
                    unprocessed_tables=table_relationalized,
                    glue_context=glue_context
            )
        for t in tables_with_index:
            clean_name = t.replace(".", "_").lower()
            dynamic_frame_with_index = DynamicFrame.fromDF(
                    tables_with_index[t],
                    glue_ctx = glue_context,
                    name = clean_name
            )
            write_table_to_s3(
                    dynamic_frame=dynamic_frame_with_index,
                    bucket=workflow_run_properties["parquet_bucket"],
                    key=os.path.join(
                        workflow_run_properties["namespace"],
                        workflow_run_properties["parquet_prefix"],
                        clean_name
                    ),
                    workflow_run_id=args["WORKFLOW_RUN_ID"],
                    glue_context=glue_context
            )
    else:
        write_table_to_s3(
                dynamic_frame=table,
                bucket=workflow_run_properties["parquet_bucket"],
                key=os.path.join(
                    workflow_run_properties["namespace"],
                    workflow_run_properties["parquet_prefix"],
                    args["glue_table"]),
                workflow_run_id=args["WORKFLOW_RUN_ID"],
                glue_context=glue_context
        )
    job.commit()

if __name__ == "__main__":
    main()
