"""
S3 to JSON job

Bucket/sort JSON from a zip archive provided by Care Evolution into
JSON datasets in S3. A JSON dataset is specific to a key prefix
(for example: json/dataset=EnrolledParticipants/) and only contains
files which share a similar schema.
"""

import datetime
import gzip
import io
import json
import logging
import os
import sys
import typing
import zipfile
from typing import Iterator, Optional

import boto3
import ecs_logging
from awsglue.utils import getResolvedOptions

DATA_TYPES_WITH_SUBTYPE = [
    "HealthKitV2Samples",
    "HealthKitV2Statistics",
    "HealthKitV2Samples_Deleted",
    "HealthKitV2Statistics_Deleted",
]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(ecs_logging.StdlibFormatter())
logger.addHandler(handler)
logger.propagate = False


def transform_object_to_array_of_objects(
    json_obj_to_replace: dict,
    key_name: str,
    key_type: type,
    value_name: str,
    value_type: type,
    logger_context: dict = {},
) -> list:
    """
    Transforms a dictionary object into an array of dictionaries with specified
    key and value types.

    This function takes a dictionary object `json_obj_to_replace` and transforms
    it into an array of dictionaries, where each dictionary in the array contains
    two key-value pairs. The keys are specified by the `key_name` and `value_name`
    parameters, and the values are converted to the corresponding types specified
    by the `key_type` and `value_type` parameters.

    Args:
        json_obj_to_replace (dict): The input dictionary object to be
            transformed into an array of dictionaries.
        key_name (str): The name of the key in the output dictionaries.
        key_type (type): The type to which the values corresponding to `key_name`
            should be converted in the output dictionaries.
        value_name (str): The name of the value in the output dictionaries.
        value_type (type): The type to which the values corresponding to `value_name`
            should be converted in the output dictionaries.
        logger_context (dict): A dictionary containing contextual information
            to include with every log.

    Returns:
        list: An array of dictionaries, where each dictionary contains
            two key-value pairs:
              - The key specified by `key_name` with the value converted to the type specified by
                `key_type`.
              - The value specified by `value_name` with the value converted to the type specified
                by `value_type`.

    Examples:
        json_obj = {'TimeOffsetHeartRateSamples': {"0": 62, "1": 63, "2": 62}}
        transformed_array = transform_object_to_array_of_objects(
            json_obj,
            key_name='OffsetInSeconds',
            key_type=int,
            value_name='HeartRate',
            value_type=int
        )

        # Resulting `transformed_array`:
        # [
        #     {'OffsetInSeconds': 0, 'HeartRate': 62},
        #     {'OffsetInSeconds': 1, 'HeartRate': 63},
        #     {'OffsetInSeconds': 2, 'HeartRate': 62},
        # ]
    """
    array_of_obj = []
    for k, v in json_obj_to_replace.items():
        try:
            key_value = key_type(k)
        except ValueError as error:
            key_value = None
            _log_error_transform_object_to_array_of_objects(
                value=k, value_type=key_type, error=error, logger_context=logger_context
            )
        try:
            value_value = value_type(v)
        except ValueError as error:
            value_value = None
            _log_error_transform_object_to_array_of_objects(
                value=v,
                value_type=value_type,
                error=error,
                logger_context=logger_context,
            )
        obj = {key_name: key_value, value_name: value_value}
        array_of_obj.append(obj)
    return array_of_obj


def _log_error_transform_object_to_array_of_objects(
    value: typing.Any, value_type: type, error, logger_context: dict
) -> None:
    """
    Logging helper for `transform_object_to_array_of_objects`

    Args:
        value (Any): The value which we have failed to cast.
        value_type (type): The type to which we have failed to cast `value`.
        error (BaseException): The caught exception.
        logger_context (dict): A dictionary containing contextual information
            to include with every log.

    Returns:
        None
    """
    value_error = "Failed to cast %s to %s."
    logger.error(
        value_error,
        value,
        value_type,
        extra=dict(
            merge_dicts(
                logger_context,
                {
                    "error.message": repr(error),
                    "error.type": type(error).__name__,
                    "event.kind": "alert",
                    "event.category": ["configuration"],
                    "event.type": ["change"],
                    "event.outcome": "failure",
                },
            )
        ),
    )
    logger.warning(
        "Setting %s to None",
        value,
        extra=dict(
            merge_dicts(
                logger_context,
                {
                    "event.kind": "alert",
                    "event.category": ["configuration"],
                    "event.type": ["deletion"],
                    "event.outcome": "success",
                },
            )
        ),
    )


def transform_json(json_obj: dict, metadata: dict, logger_context: dict = {}) -> dict:
    """
    Perform the following transformations:

    For every JSON:
        - Add an export_start_date property (may be None)
        - Add an export_end_date property (may be None)
        - Add a cohort property

    For JSON whose data types have a subtype:
        - Add subtype as "Type" property

    For JSON whose data type is "EnrolledParticipants":
        - Cast "CustomFields.Symptoms" and "CustomFields.Treatments" property
          to an array. (May be an empty array).

    For relevant Garmin data types:
        - Some Garmin data types have one or more properties which are objects
          (usually mapping time to some metric), that would be better formatted
          as an array of objects. We transform these properties into arrays of
          objects.

    Args:
        json_obj (dict): A JSON object sourced from the JSON file of this data type.
        metadata (dict): Metadata about the source JSON file.
        logger_context (dict): A dictionary containing contextual information
            to include with every log.

    Returns:
        json_obj (dict) The JSON object with the relevant transformations applied.
    """
    json_obj = _add_universal_properties(json_obj=json_obj, metadata=metadata)
    if metadata["type"] in DATA_TYPES_WITH_SUBTYPE:
        # This puts the `Type` property back where Apple intended it to be
        json_obj["Type"] = metadata["subtype"]
    if metadata["type"] == "SymptomLog":
        # Load JSON string as dict
        json_obj["Value"] = json.loads(json_obj["Value"])
    if metadata["type"] == "EnrolledParticipants":
        json_obj = _cast_custom_fields_to_array(
            json_obj=json_obj,
            logger_context=logger_context,
        )

    # These Garmin data types have fields which would be better formatted
    # as an array of objects.
    garmin_transform_types = {
        "GarminDailySummary": {
            "TimeOffsetHeartRateSamples": (("OffsetInSeconds", int), ("HeartRate", int))
        },
        "GarminHrvSummary": {"HrvValues": (("OffsetInSeconds", int), ("Hrv", int))},
        "GarminPulseOxSummary": {
            "TimeOffsetSpo2Values": (("OffsetInSeconds", int), ("Spo2Value", int))
        },
        "GarminRespirationSummary": {
            "TimeOffsetEpochToBreaths": (("OffsetInSeconds", int), ("Breaths", float))
        },
        "GarminSleepSummary": {
            "TimeOffsetSleepSpo2": (("OffsetInSeconds", int), ("Spo2Value", int)),
            "TimeOffsetSleepRespiration": (
                ("OffsetInSeconds", int),
                ("Breaths", float),
            ),
        },
        "GarminStressDetailSummary": {
            "TimeOffsetStressLevelValues": (
                ("OffsetInSeconds", int),
                ("StressLevel", int),
            ),
            "TimeOffsetBodyBatteryValues": (
                ("OffsetInSeconds", int),
                ("BodyBattery", int),
            ),
        },
        "GarminThirdPartyDailySummary": {
            "TimeOffsetHeartRateSamples": (("OffsetInSeconds", int), ("HeartRate", int))
        },
        "GarminHealthSnapshotSummary": {
            "Summaries.EpochSummaries": (("OffsetInSeconds", int), ("Value", float))
        },
    }
    if metadata["type"] in garmin_transform_types:
        json_obj = _transform_garmin_data_types(
            json_obj=json_obj,
            data_type_transforms=garmin_transform_types[metadata["type"]],
            logger_context=logger_context,
        )
    return json_obj


def _add_universal_properties(
    json_obj: dict,
    metadata: dict,
) -> dict:
    """
    Adds properties which ought to exist for every JSON object.

    A helper function for `transform_json`. Fulfills the below logic.

    For every JSON:
        - Add an export_start_date property (may be None)
        - Add an export_end_date property (may be None)
        - Add a cohort property

    Args:
        json_obj (dict): A JSON object sourced from the JSON file of this data type.
        metadata (dict): Metadata about the source JSON file.

    Returns:
        json_obj (dict) The JSON object with the relevant transformations applied.
    """
    if metadata["start_date"] is not None:
        json_obj["export_start_date"] = metadata["start_date"].isoformat()
    else:
        json_obj["export_start_date"] = None
    json_obj["export_end_date"] = metadata.get("end_date").isoformat()
    json_obj["cohort"] = metadata["cohort"]
    return json_obj


def _cast_custom_fields_to_array(json_obj: dict, logger_context: dict) -> dict:
    """
    Cast `CustomFields` property values to an array.

    A helper function for `transform_json`. Fulfills the below logic.

    For JSON whose data type is "EnrolledParticipants":
        - Cast "CustomFields.Symptoms" and "CustomFields.Treatments" property
          to an array. (May be an empty array).

    Args:
        json_obj (dict): A JSON object sourced from the JSON file of this data type.
        logger_context (dict): A dictionary containing contextual information
            to include with every log.

    Returns:
        json_obj (dict) The JSON object with the relevant transformations applied.
    """
    for field_name in ["Symptoms", "Treatments"]:
        if field_name in json_obj["CustomFields"] and isinstance(
            json_obj["CustomFields"][field_name], str
        ):
            if len(json_obj["CustomFields"][field_name]) > 0:
                # This JSON string was written in a couple different ways
                # in the testing data: "[{\\\"id\\\": ..." and "[{\"id\": ..."
                # or just an empty string. It's not really clear which format
                # is intended. (The example Care Evolution provided has it
                # written as an object rather than a string).
                try:
                    json_obj["CustomFields"][field_name] = json.loads(
                        json_obj["CustomFields"][field_name]
                    )
                except json.JSONDecodeError as error:
                    # If it's not propertly formatted JSON, then we
                    # can't read it, and instead store an empty list
                    logger.error(
                        (
                            f"Problem CustomFields.{field_name}: "
                            f"{json_obj['CustomFields'][field_name]}"
                        ),
                        extra=dict(
                            merge_dicts(
                                logger_context,
                                {
                                    "error.message": repr(error),
                                    "error.type": "json.JSONDecodeError",
                                    "event.kind": "alert",
                                    "event.category": ["change"],
                                    "event.type": ["error"],
                                    "event.outcome": "failure",
                                },
                            )
                        ),
                    )
                    json_obj["CustomFields"][field_name] = []
            else:
                json_obj["CustomFields"][field_name] = []
    return json_obj


def _transform_garmin_data_types(
    json_obj: dict,
    data_type_transforms: dict,
    logger_context: dict,
) -> dict:
    """
    Transform objects to an array of objects for relevant Garmin data types.

    A helper function for `transform_json`. Fulfills the below logic.

    For relevant Garmin data types:
        - Some Garmin data types have one or more properties which are objects
          (usually mapping time to some metric), that would be better formatted
          as an array of objects. We transform these properties into arrays of
          objects.

    Args:
        json_obj (dict): A JSON object sourced from the JSON file of this data type.
        data_type_transforms (dict): A dictionary mapping property names (corresponding
            to objects in the original JSON) to the new property names and their types
            of each object contained in the array resulting from the transformation.
        logger_context (dict): A dictionary containing contextual information
            to include with every log.

    Returns:
        json_obj (dict) The JSON object with the relevant transformations applied.
    """
    for prop in data_type_transforms:
        key_name = data_type_transforms[prop][0][0]
        key_type = data_type_transforms[prop][0][1]
        value_name = data_type_transforms[prop][1][0]
        value_type = data_type_transforms[prop][1][1]
        property_hierarchy = prop.split(".")
        # consider implementing recursive solution if necessary
        if len(property_hierarchy) == 1:
            prop_name = property_hierarchy[0]
            if prop_name in json_obj:
                array_of_obj = transform_object_to_array_of_objects(
                    json_obj_to_replace=json_obj[prop_name],
                    key_name=key_name,
                    key_type=key_type,
                    value_name=value_name,
                    value_type=value_type,
                    logger_context=logger_context,
                )
                json_obj[prop_name] = array_of_obj
        if len(property_hierarchy) == 2:
            prop_name = property_hierarchy[0]
            sub_prop_name = property_hierarchy[1]
            if prop_name in json_obj:
                for obj in json_obj[prop_name]:
                    if sub_prop_name in obj:
                        array_of_obj = transform_object_to_array_of_objects(
                            json_obj_to_replace=obj[sub_prop_name],
                            key_name=key_name,
                            key_type=key_type,
                            value_name=value_name,
                            value_type=value_type,
                            logger_context=logger_context,
                        )
                        obj[sub_prop_name] = array_of_obj
    return json_obj


def get_file_identifier(metadata: dict) -> str:
    """
    Get an identifier for a file from the source file's metadata.

    This function effectively reverse-engineers the process in `get_metadata`,
    enabling us to reconstuct the source file's identifier.

    The format depends on which metadata fields we have available to us.
    Metadata fields we can potentially use:
        - type
        - subtype
        - start_date
        - end_date

    Args:
        metadata (dict): Metadata about the source JSON file.
        part_number (int): Which part we need a file name for.

    Return:
        str: A formatted file name.
    """
    if metadata["type"] in DATA_TYPES_WITH_SUBTYPE:
        identifier = "{}_{}_{}-{}".format(
            metadata["type"],
            metadata["subtype"],
            metadata["start_date"].strftime("%Y%m%d"),
            metadata["end_date"].strftime("%Y%m%d"),
        )
    elif metadata["start_date"] is None:
        identifier = "{}_{}".format(
            metadata["type"], metadata["end_date"].strftime("%Y%m%d")
        )
    else:
        identifier = "{}_{}-{}".format(
            metadata["type"],
            metadata["start_date"].strftime("%Y%m%d"),
            metadata["end_date"].strftime("%Y%m%d"),
        )
    return identifier


def transform_block(
    input_json: typing.IO,
    metadata: dict,
    logger_context: dict = {},
    block_size: int = 10000,
) -> Iterator[list]:
    """
    A generator function which yields a block of transformed JSON records.

    This function can be used with `write_file_to_json_dataset`. Some JSON files
    are too large to have all of their records kept in memory before being written
    to the resulting transformed NDJSON file. To avoid an OOM error, we do the
    transformations and write the records in blocks.

    Args:
        input_json (typing.IO): A file-like object of the JSON to be transformed.
        metadata (dict): Metadata about the source JSON file. See `get_metadata`.
        logger_context (dict): A dictionary containing contextual information
            to include with every log.
        block_size (int, optional): The number of records to process in each block.
            Default is 10000.

    Yields:
        list: A block of transformed JSON records.
    """
    block = []
    for json_line in input_json:
        json_obj = json.loads(json_line)
        json_obj = transform_json(
            json_obj=json_obj, metadata=metadata, logger_context=logger_context
        )
        block.append(json_obj)
        if len(block) == block_size:
            yield block
            block = []
    if block:  # yield final block
        yield block


def write_file_to_json_dataset(
    z: zipfile.ZipFile,
    json_path: str,
    metadata: dict,
    workflow_run_properties: dict,
    logger_context: dict = {},
    delete_upon_successful_upload: bool = True,
    file_size_limit: float = 1e8,
) -> list:
    """
    Write JSON from a zipfile to a JSON dataset.

    Metadata fields are inserted as top-level fields,
    other fields are transformed (see `transform_json`). The resulting NDJSON(s) are
    written to a JSON dataset in S3 and their cumulative line count is logged.
    Depending on the `file_size_limit`, data from a single JSON may be written to
    one or more NDJSON in the JSON dataset as "part" files. See ETL-519 for more information.

    Args:
        z (zipfile.Zipfile): The zip archive as provided by the data provider.
        json_path (str): A JSON path relative to the root of `z`.
        metadata (dict): Metadata about the source JSON file.
        workflow_run_properties (dict): The workflow arguments
        logger_context (dict): A dictionary containing contextual information
            to include with every log.
        delete_upon_successful_upload (bool): Whether to delete the local
            copy of the JSON file after uploading to S3. Set to False
            during testing.
        file_size_limit (float): The approximate maximum file size in bytes
            before writing to another part file.

    Returns:
        list: A list of files uploaded to S3
    """
    # Configuration related to where we write the JSON
    output_dir = os.path.join(
        f"dataset={metadata['type']}", f"cohort={metadata['cohort']}"
    )
    os.makedirs(output_dir, exist_ok=True)
    part_number = 0
    # we update this value as we write
    current_part_path = get_output_path(
        metadata=metadata, part_number=part_number, output_dir=output_dir, touch=True
    )
    compressed_output_path = get_output_path(
        metadata=metadata, part_number=None, output_dir=output_dir, touch=True
    )

    # We will attach file metadata to the uploaded S3 object
    s3_metadata = _derive_str_metadata(metadata=metadata)
    line_count = 0
    uploaded_files = []
    # fmt: off
    # <python3.10 requires this backslash syntax, we currently run 3.7
    with z.open(json_path, "r") as input_json, \
         open(current_part_path, "a") as f_out, \
         gzip.open(compressed_output_path, "wt", encoding="utf-8") as f_compressed_out:
        for transformed_block in transform_block(
            input_json=input_json, metadata=metadata, logger_context=logger_context
        ):
            current_file_size = os.path.getsize(current_part_path)
            if current_file_size > file_size_limit:
                # Upload completed part file
                _upload_file_to_json_dataset(
                    file_path=current_part_path,
                    s3_metadata=s3_metadata,
                    workflow_run_properties=workflow_run_properties,
                    delete_upon_successful_upload=delete_upon_successful_upload,
                )
                uploaded_files.append(current_part_path)
                # Update output path to next part
                part_number += 1
                current_part_path = get_output_path(
                    metadata=metadata,
                    part_number=part_number,
                    output_dir=output_dir,
                    touch=True,
                )
            # Write block data to both part file and compressed file
            for transformed_record in transformed_block:
                line_count += 1
                record_with_newline = "{}\n".format(json.dumps(transformed_record))
                f_out.write(record_with_newline)
                f_compressed_out.write(record_with_newline)
    # fmt: on
    # Upload final part
    _upload_file_to_json_dataset(
        file_path=current_part_path,
        s3_metadata=s3_metadata,
        workflow_run_properties=workflow_run_properties,
        delete_upon_successful_upload=delete_upon_successful_upload,
    )
    uploaded_files.append(current_part_path)
    logger_extra = dict(
        merge_dicts(
            logger_context,
            {
                "file.LineCount": line_count,
                "event.kind": "metric",
                "event.category": ["file"],
                "event.type": ["info", "creation"],
                "event.action": "list-file-properties",
                "labels": {
                    k: v.isoformat() if isinstance(v, datetime.datetime) else v
                    for k, v in metadata.items()
                },
            },
        )
    )
    logger.info("Output file attributes", extra=logger_extra)
    # Upload compressed file
    _upload_file_to_json_dataset(
        file_path=compressed_output_path,
        s3_metadata=s3_metadata,
        workflow_run_properties=workflow_run_properties,
        delete_upon_successful_upload=delete_upon_successful_upload,
        upload_to_compressed_s3_prefix=True,
    )
    uploaded_files.append(compressed_output_path)
    return uploaded_files


def _derive_str_metadata(metadata: dict) -> dict:
    """
    Format metadata values as strings

    Args:
        metadata (dict): Metadata about the source JSON file.

    Returns:
        (dict) The S3 metadata
    """
    s3_metadata = {k: v for k, v in metadata.items() if v is not None}
    for k, v in s3_metadata.items():
        if isinstance(v, datetime.datetime):
            s3_metadata[k] = v.isoformat()
    return s3_metadata


def _upload_file_to_json_dataset(
    file_path: str,
    s3_metadata: dict,
    workflow_run_properties: dict,
    delete_upon_successful_upload: bool,
    upload_to_compressed_s3_prefix: bool = False,
) -> str:
    """
    A helper function for `write_file_to_json_dataset` which handles
    the actual uploading of the data to S3.

    Args:
        file_path (str): The path of the JSON file to upload.
        s3_metadata (dict): S3 Metadata to include on the object.
        workflow_run_properties (dict): The workflow arguments
        delete_upon_successful_upload (bool): Whether to delete the local
            copy of the JSON file after uploading to S3. Set to False
            during testing.
        upload_to_compressed_s3_prefix (bool): Whether to upload this file
            to the compressed JSON S3 prefix. Effectively, this substitutes
            `workflow_run_properties['json_prefix']` with the string
            "compressed_json".

    Returns:
        str: The S3 object key of the uploaded file.
    """
    s3_client = boto3.client("s3")
    if upload_to_compressed_s3_prefix:
        s3_output_key = os.path.join(
            workflow_run_properties["namespace"],
            "compressed_json",
            file_path,
        )
    else:
        s3_output_key = os.path.join(
            workflow_run_properties["namespace"],
            workflow_run_properties["json_prefix"],
            file_path,
        )
    basic_file_info = get_basic_file_info(file_path=file_path)
    with open(file_path, "rb") as f_in:
        response = s3_client.put_object(
            Body=f_in,
            Bucket=workflow_run_properties["json_bucket"],
            Key=s3_output_key,
            Metadata=s3_metadata,
        )
    logger.info(
        "Upload to S3",
        extra={
            **basic_file_info,
            "event.kind": "event",
            "event.category": ["database"],
            "event.type": ["creation"],
            "event.action": "put-bucket-object",
            "labels": {
                **s3_metadata,
                "bucket": workflow_run_properties["json_bucket"],
                "key": s3_output_key,
            },
        },
    )
    if delete_upon_successful_upload:
        os.remove(file_path)
    return s3_output_key


def merge_dicts(x: dict, y: dict) -> typing.Generator:
    """
    Merge two dictionaries recursively.

    We use the following ruleset:

        1. If a key is only in one of the dicts, we retain that
           key:value pair
        2. If a key is in both dicts but one or none of the values is
           a dict, we retain the key:value pair from `y`.
        3. If a key is in both dicts and both values are
           a dict, we merge the dicts according to the above ruleset.
    """
    all_keys = x.keys() | y.keys()
    overlapping_keys = x.keys() & y.keys()
    for key in all_keys:
        if (
            key in overlapping_keys
            and isinstance(x[key], dict)
            and isinstance(y[key], dict)
        ):
            # Merge child dictionaries
            yield (key, dict(merge_dicts(x[key], y[key])))
        elif key in x and key not in y:
            # This key is only in x
            yield (key, x[key])
        else:
            # IF
            # This key is only in y
            # OR
            # One or none of the values is a dict
            # THEN
            # The key:value pair from y is retained
            yield (key, y[key])


def get_output_path(
    metadata: dict,
    output_dir: str,
    part_number: Optional[int],
    touch: bool,
) -> str:
    """
    A helper function for `write_file_to_json_dataset`

    This function returns a file path where we can write data to. If a part
    number is provided, we assume that this is a part file. Otherwise, we
    assume that this is a gzip file.

    Args:
        metadata (dict): Metadata about the source JSON file.
        output_dir (str): The directory to which we write the file.
        part_number (int): Which part we need a file name for. Set to None if
            this is the path to a gzip file.
        touch (bool): Whether to create an empty file at the path

    Returns:
        str: A new part path

    Raises:
        FileExistsError: If touch is True and a file already exists at
            the part path.
    """
    file_identifier = get_file_identifier(metadata=metadata)
    if part_number is not None:
        output_filename = f"{file_identifier}.part{part_number}.ndjson"
    else:
        output_filename = f"{file_identifier}.ndjson.gz"
    output_path = os.path.join(output_dir, output_filename)
    if touch:
        os.makedirs(output_dir, exist_ok=True)
        with open(output_path, "x") as initial_file:
            # create file
            pass
    return output_path


def get_metadata(basename: str) -> dict:
    """
    Get metadata of a file by parsing its basename.

    Args:
        basename (str): The basename of the file.

    Returns:
        dict: The metadata, formatted as
            type (str): the data type
            start_date (datetime.datetime): The date of the oldest data collected.
                May be None if filename only contains the `end_date`.
            end_date (datetime.datetime): The date of the most recent data collected.
            subtype (str): If this is HealthKitV2Samples or HealthKitV2Statistics,
                the type of the sample data. (Not to be confused with the data type,
                which in this case is HealthKitV2Samples or HealthKitV2Samples_Deleted
                or HealthKitV2Statistics).
    """
    metadata = {}
    basename_components = os.path.splitext(basename)[0].split("_")
    metadata["type"] = basename_components[0]
    if "-" in basename_components[-1]:
        start_date, end_date = basename_components[-1].split("-")
        metadata["start_date"] = datetime.datetime.strptime(start_date, "%Y%m%d")
        metadata["end_date"] = datetime.datetime.strptime(end_date, "%Y%m%d")
    else:
        metadata["start_date"] = None
        metadata["end_date"] = datetime.datetime.strptime(
            basename_components[-1], "%Y%m%d"
        )
    if metadata["type"] in DATA_TYPES_WITH_SUBTYPE:
        metadata["subtype"] = basename_components[1]
    if "HealthKitV2" in metadata["type"] and basename_components[-2] == "Deleted":
        metadata["type"] = "{}_Deleted".format(metadata["type"])
    return metadata


def get_basic_file_info(file_path: str) -> dict:
    """
    Returns a dictionary of basic information about a file.

    Args:
        file_path (str): The path to this file

    Returns:
        dict: Basic file information, formatted as
            file.type (str): Always "file"
            file.path (str): Same as `file_path`
            file.name (str): The basename
            file.extension (str): The file extension with no leading dot.
    """
    basic_file_info = {
        "file.type": "file",
        "file.path": file_path,
        "file.name": os.path.basename(file_path),
        "file.extension": os.path.splitext(file_path)[-1][1:],
    }
    return basic_file_info


def process_record(s3_obj: dict, cohort: str, workflow_run_properties: dict) -> None:
    """
    Write the contents of a .zip archive stored on S3 to their respective
    JSON dataset. Each file contained in the .zip archive has its line count logged.

    Metadata about the source JSON is inserted into each JSON before
    being written to its JSON dataset.

    Args:
        s3_obj (dict): An S3 object as returned by `boto3.get_object`.
        cohort (str): The cohort which this data associates with.
        workflow_run_properties (dict): The workflow arguments

    Returns:
        None
    """
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
        non_empty_contents = [
            f.filename
            for f in z.filelist
            if "/" not in f.filename
            and "Manifest" not in f.filename
            and f.file_size > 0
        ]
        for json_path in non_empty_contents:
            with z.open(json_path, "r") as f:
                line_count = sum(1 for _ in f)
            basic_file_info = get_basic_file_info(file_path=json_path)
            metadata = get_metadata(
                basename=os.path.basename(json_path),
            )
            metadata["cohort"] = cohort
            metadata_str_keys = _derive_str_metadata(metadata=metadata)
            logger_context = {
                **basic_file_info,
                "labels": metadata_str_keys,
                "process.parent.pid": workflow_run_properties["WORKFLOW_RUN_ID"],
                "process.parent.name": workflow_run_properties["WORKFLOW_NAME"],
            }
            logger.info(
                "Input file attributes",
                extra=dict(
                    merge_dicts(
                        logger_context,
                        {
                            "file.size": sys.getsizeof(json_path),
                            "file.LineCount": line_count,
                            "event.kind": "metric",
                            "event.category": ["file"],
                            "event.type": ["info", "access"],
                            "event.action": "list-file-properties",
                        },
                    )
                ),
            )
            write_file_to_json_dataset(
                z=z,
                json_path=json_path,
                metadata=metadata,
                workflow_run_properties=workflow_run_properties,
                logger_context=logger_context,
            )


def main() -> None:
    # Instantiate boto clients
    glue_client = boto3.client("glue")
    s3_client = boto3.client("s3")

    # Get job and workflow arguments
    args = getResolvedOptions(sys.argv, ["WORKFLOW_NAME", "WORKFLOW_RUN_ID"])
    workflow_run_properties = glue_client.get_workflow_run_properties(
        Name=args["WORKFLOW_NAME"], RunId=args["WORKFLOW_RUN_ID"]
    )["RunProperties"]
    workflow_run_properties = {**args, **workflow_run_properties}
    logger.debug(
        "getResolvedOptions",
        extra={
            "event.kind": "event",
            "event.category": ["process"],
            "event.type": ["info"],
            "event.action": "get-job-arguments",
            "labels": args,
        },
    )
    logger.debug(
        "get_workflow_run_properties",
        extra={
            "event.kind": "event",
            "event.category": ["process"],
            "event.type": ["info"],
            "event.action": "get-workflow-arguments",
            "labels": workflow_run_properties,
        },
    )

    # Load messages to be processed
    messages = json.loads(workflow_run_properties["messages"])
    for message in messages:
        logger.info(
            "Retrieving S3 object",
            extra={
                "event.kind": "event",
                "event.category": ["database"],
                "event.type": ["access"],
                "event.action": "get-bucket-object",
                "labels": {
                    "bucket": message["source_bucket"],
                    "key": message["source_key"],
                },
            },
        )
        s3_obj = s3_client.get_object(
            Bucket=message["source_bucket"], Key=message["source_key"]
        )
        s3_obj["Body"] = s3_obj["Body"].read()
        cohort = None
        if "adults_v1" in message["source_key"]:
            cohort = "adults_v1"
        elif "pediatric_v1" in message["source_key"]:
            cohort = "pediatric_v1"
        else:
            logger.warning(
                "Could not determine the cohort of object at %s"
                "This file will not be written to a JSON dataset.",
                f"s3://{message['source_bucket']}/{message['source_key']}. ",
                extra={
                    "file.name": message["source_key"],
                    "event.kind": "alert",
                    "event.category": ["configuration"],
                    "event.type": ["creation"],
                    "event.outcome": "failure",
                    "labels": {
                        "bucket": message["source_bucket"],
                        "key": message["source_key"],
                    },
                },
            )
            continue
        process_record(
            s3_obj=s3_obj,
            cohort=cohort,
            workflow_run_properties=workflow_run_properties,
        )


if __name__ == "__main__":
    main()
