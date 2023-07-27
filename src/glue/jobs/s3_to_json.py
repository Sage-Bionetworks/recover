"""
S3 to JSON job

Bucket/sort JSON from a zip archive provided by Care Evolution into
JSON datasets in S3. A JSON dataset is specific to a key prefix
(for example: json/dataset=EnrolledParticipants/) and only contains
files which share a similar schema.
"""
import datetime
import io
import json
import logging
import os
import sys
import zipfile
import boto3
from awsglue.utils import getResolvedOptions

DATA_TYPES_WITH_SUBTYPE = ["HealthKitV2Samples", "HealthKitV2Statistics"]

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def transform_object_to_array_of_objects(
        json_obj_to_replace: dict,
        key_name: str,
        key_type: type,
        value_name: str,
        value_type: type,) -> list:
    """
    Transforms a dictionary object into an array of dictionaries with specified
    key and value types.

    This function takes a dictionary object `json_obj_to_replace` and transforms
    it into an array of dictionaries, where each dictionary in the array contains
    two key-value pairs. The keys are specified by the `key_name` and `value_name`
    parameters, and the values are converted to the corresponding types specified
    by the `key_type` and `value_type` parameters.

    Parameters:
        json_obj_to_replace (dict): The input dictionary object to be
            transformed into an array of dictionaries.
        key_name (str): The name of the key in the output dictionaries.
        key_type (type): The type to which the values corresponding to `key_name`
            should be converted in the output dictionaries.
        value_name (str): The name of the value in the output dictionaries.
        value_type (type): The type to which the values corresponding to `value_name`
            should be converted in the output dictionaries.

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
    value_error = "Failed to cast %s to %s"
    for k, v in json_obj_to_replace.items():
        try:
            key_value = key_type(k)
        except ValueError:
            logger.warning(value_error, k,key_type)
            key_value = None
        try:
            value_value = value_type(v)
        except ValueError:
            logger.warning(value_error, v, value_type)
            value_value = None
        obj = {
            key_name: key_value,
            value_name: value_value
        }
        array_of_obj.append(obj)
    return array_of_obj

def transform_json(
        json_obj: dict,
        dataset_identifier: str,
        metadata: dict,) -> dict:
    """
    Perform the following transformations:

    For every JSON:
        - Add an export_start_date property (may be None)
        - Add an export_end_date property (may be None)

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
        json_obj (str): A JSON object sourced from the JSON file of this data type.
        dataset_identifier (str): The data type of `json_obj`.
        metadata (dict): Metadata derived from the file basename.

    Returns:
        json_obj (dict) The JSON object with the relevant transformations applied.
    """
    if metadata["start_date"] is not None:
        json_obj["export_start_date"] = metadata["start_date"].isoformat()
    else:
        json_obj["export_start_date"] = None
    json_obj["export_end_date"] = metadata.get("end_date").isoformat()
    if dataset_identifier in DATA_TYPES_WITH_SUBTYPE:
        # This puts the `Type` property back where Apple intended it to be
        json_obj["Type"] = metadata["subtype"]
    if dataset_identifier == "SymptomLog":
        # Load JSON string as dict
        json_obj["Value"] = json.loads(json_obj["Value"])
    if dataset_identifier == "EnrolledParticipants":
        for field_name in ["Symptoms", "Treatments"]:
            if (
                    field_name in json_obj["CustomFields"]
                    and isinstance(json_obj["CustomFields"][field_name], str)
               ):
                if len(json_obj["CustomFields"][field_name]) > 0:
                    # This JSON string was written in a couple different ways
                    # in the testing data: "[{\\\"id\\\": ..." and "[{\"id\": ..."
                    # or just an empty string. It's not really clear which format
                    # is intended, (The example they provided has it written as
                    # an object rather than a string, so...).
                    try:
                        json_obj["CustomFields"][field_name] = json.loads(
                                json_obj["CustomFields"][field_name]
                        )
                    except json.JSONDecodeError as e:
                        # If it's not propertly formatted JSON, then we
                        # can't read it, and instead store an empty list
                        logger.warning(f"Problem CustomFields.{field_name}: "
                                       f"{json_obj['CustomFields'][field_name]}")
                        logger.warning(str(e))
                        json_obj["CustomFields"][field_name] = []
                else:
                    json_obj["CustomFields"][field_name] = []
    garmin_transform_types = {
            "GarminDailySummary": {
                "TimeOffsetHeartRateSamples": (("OffsetInSeconds", int), ("HeartRate", int))
            },
            "GarminHrvSummary": {
                "HrvValues": (("OffsetInSeconds", int), ("Hrv", int))
            },
            "GarminPulseOxSummary": {
                "TimeOffsetSpo2Values": (("OffsetInSeconds", int), ("Spo2Value", int))
            },
            "GarminRespirationSummary": {
                "TimeOffsetEpochToBreaths": (("OffsetInSeconds", int), ("Breaths", float))
            },
            "GarminSleepSummary": {
                "TimeOffsetSleepSpo2": (("OffsetInSeconds", int), ("Spo2Value", int)),
                "TimeOffsetSleepRespiration": (("OffsetInSeconds", int), ("Breaths", float))
            },
            "GarminStressDetailSummary": {
                "TimeOffsetStressLevelValues": (("OffsetInSeconds", int), ("StressLevel", int)),
                "TimeOffsetBodyBatteryValues": (("OffsetInSeconds", int), ("BodyBattery", int))
            },
            "GarminThirdPartyDailySummary": {
                "TimeOffsetHeartRateSamples": (("OffsetInSeconds", int), ("HeartRate", int))
            },
            "GarminHealthSnapshotSummary": {
                "Summaries.EpochSummaries": (("OffsetInSeconds", int), ("Value", float))
            }
    }
    if dataset_identifier in garmin_transform_types:
        this_data_type = garmin_transform_types[dataset_identifier]
        for prop in this_data_type:
            key_name = this_data_type[prop][0][0]
            key_type = this_data_type[prop][0][1]
            value_name = this_data_type[prop][1][0]
            value_type = this_data_type[prop][1][1]
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
                            value_type=value_type
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
                                        value_type=value_type
                            )
                            obj[sub_prop_name] = array_of_obj
    return json_obj

def get_output_filename(metadata: dict) -> str:
    """
    Get a formatted file name.

    The format depends on which metadata fields we have available to us.
    Metadata fields we can potentially use:
        - type
        - subtype
        - start_date
        - end_date

    Args:
        metadata (dict): Metadata derived from the file basename.

    Return:
        str: A formatted file name.
    """
    if metadata["type"] in DATA_TYPES_WITH_SUBTYPE:
        output_fname = "{}_{}_{}-{}.ndjson".format(
                metadata["type"],
                metadata["subtype"],
                metadata["start_date"].strftime("%Y%m%d"),
                metadata["end_date"].strftime("%Y%m%d")
        )
    elif metadata["start_date"] is None:
        output_fname = "{}_{}.ndjson".format(
                metadata["type"],
                metadata["end_date"].strftime("%Y%m%d")
    )
    else:
        output_fname = "{}_{}-{}.ndjson".format(
                metadata["type"],
                metadata["start_date"].strftime("%Y%m%d"),
                metadata["end_date"].strftime("%Y%m%d")
        )
    return output_fname

def write_file_to_json_dataset(
        z: zipfile.ZipFile,
        json_path: str,
        dataset_identifier: str,
        metadata: dict,
        workflow_run_properties: dict,
        delete_upon_successful_upload: bool=True) -> str:
    """
    Write a JSON from a zipfile to a JSON dataset.

    Metadata fields derived from the file basename are inserted as top-level fields,
    other fields are transformed (see `transform_json`). The resulting NDJSON is written
    to a JSON dataset in S3.

    Args:
        z (zipfile.Zipfile): The zip archive as provided by the data provider.
        json_path (str): A JSON path relative to the root of `z`.
        dataset_identifier (str): The data type of `json_path`.
        metadata (dict): Metadata derived from the file basename.
        workflow_run_properties (dict): The workflow arguments
        delete_upon_successful_upload (bool): Whether to delete the local
            copy of the JSON file after uploading to S3. Set to False
            during testing.

    Returns:
        output_path (str) The local path the file was written to.
    """
    s3_client = boto3.client("s3")
    os.makedirs(dataset_identifier, exist_ok=True)
    s3_metadata = metadata.copy()
    if s3_metadata["start_date"] is None:
        s3_metadata.pop("start_date")
    else:
        s3_metadata["start_date"] = metadata["start_date"].isoformat()
    s3_metadata["end_date"] = metadata["end_date"].isoformat()
    data = []
    with z.open(json_path, "r") as p:
        for json_line in p:
            json_obj = json.loads(json_line)
            json_obj = transform_json(
                    json_obj=json_obj,
                    dataset_identifier=dataset_identifier,
                    metadata=metadata
            )
            data.append(json_obj)
    output_filename = get_output_filename(metadata=metadata)
    output_path = os.path.join(dataset_identifier, output_filename)
    s3_output_key = os.path.join(
        workflow_run_properties["namespace"],
        workflow_run_properties["json_prefix"],
        f"dataset={dataset_identifier}",
        output_filename
    )
    logger.debug("Output Key: %s", s3_output_key)
    with open(output_path, "w") as f_out:
        for record in data:
            f_out.write("{}\n".format(json.dumps(record)))
    with open(output_path, "rb") as f_in:
        response = s3_client.put_object(
                Body = f_in,
                Bucket = workflow_run_properties["json_bucket"],
                Key = s3_output_key,
                Metadata = s3_metadata
        )
        logger.debug("S3 Put object response: %s", json.dumps(response))
    if delete_upon_successful_upload:
        os.remove(output_path)
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
        metadata["start_date"] = \
                datetime.datetime.strptime(start_date, "%Y%m%d")
        metadata["end_date"] = \
                datetime.datetime.strptime(end_date, "%Y%m%d")
    else:
        metadata["start_date"] = None
        metadata["end_date"] = \
                datetime.datetime.strptime(basename_components[-1], "%Y%m%d")
    if metadata["type"] in DATA_TYPES_WITH_SUBTYPE:
        metadata["subtype"] = basename_components[1]
    if (
        metadata["type"]
        in [
            "HealthKitV2Samples",
            "HealthKitV2Heartbeat",
            "HealthKitV2Electrocardiogram",
            "HealthKitV2Workouts",
        ]
        and basename_components[-2] == "Deleted"
    ):
        metadata["type"] = "{}_Deleted".format(metadata["type"])
    logger.debug("metadata = %s", metadata)
    return metadata

def process_record(
        s3_obj: dict,
        workflow_run_properties: dict) -> None:
    """
    Write the contents of a .zip archive stored on S3 to their respective
    JSON dataset.

    Metadata derived from the filename is inserted into each JSON before
    being written to its JSON dataset.

    Args:
        s3_obj (dict): An S3 object as returned by `boto3.get_object`.
        workflow_run_properties (dict): The workflow arguments

    Returns:
        None
    """
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
        non_empty_contents = [
                f.filename for f in z.filelist
                if "/" not in f.filename and
                "Manifest" not in f.filename and
                f.file_size > 0
        ]
        logger.debug("contents: %s", z.namelist())
        logger.debug("non-empty contents: %s", non_empty_contents)
        for json_path in non_empty_contents:
            metadata = get_metadata(os.path.basename(json_path))
            dataset_identifier = metadata["type"]
            logger.info("Writing %s to dataset %s",
                        json_path, dataset_identifier)
            write_file_to_json_dataset(
                    z=z,
                    json_path=json_path,
                    dataset_identifier=dataset_identifier,
                    metadata=metadata,
                    workflow_run_properties=workflow_run_properties)

def main() -> None:

    # Instantiate boto clients
    glue_client = boto3.client("glue")
    s3_client = boto3.client("s3")

    # Get job and workflow arguments
    args = getResolvedOptions(
            sys.argv,
            ["WORKFLOW_NAME",
             "WORKFLOW_RUN_ID"
            ]
    )
    workflow_run_properties = glue_client.get_workflow_run_properties(
            Name=args["WORKFLOW_NAME"],
            RunId=args["WORKFLOW_RUN_ID"])["RunProperties"]
    logger.debug("getResolvedOptions: %s", json.dumps(args))
    logger.debug("get_workflow_run_properties: %s", json.dumps(workflow_run_properties))

    # Load messages to be processed
    logger.info("Loading messages")
    messages = json.loads(workflow_run_properties["messages"])
    for message in messages:
        logger.info("Retrieving S3 object for Bucket %s and Key %s",
                    message["source_bucket"], message["source_key"])
        s3_obj = s3_client.get_object(
                Bucket = message["source_bucket"],
                Key = message["source_key"]
        )
        s3_obj["Body"] = s3_obj["Body"].read()
        process_record(
                s3_obj=s3_obj,
                workflow_run_properties=workflow_run_properties
        )

if __name__ == "__main__":
    main()
