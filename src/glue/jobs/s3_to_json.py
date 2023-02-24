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

def write_file_to_json_dataset(
        z: zipfile.ZipFile,
        json_path: str,
        dataset_identifier: str,
        metadata: dict,
        workflow_run_properties: dict) -> str:
    """
    Write a JSON from a zipfile to a JSON dataset.

    Metadata fields derived from the file basename are inserted as top-level fields
    and the NDJSON is written to a JSON dataset in S3.

    Args:
        z (zipfile.Zipfile): The zip archive as provided by the data provider.
        json_path (str): A JSON path relative to the root of `z`.
        dataset_identifier (str): The data type of `json_path`.
        metadata (dict): Metadata derived from the file basename.
        workflow_run_properties (dict): The workflow arguments

    Returns:
        output_path (str) The local path the file was written to.
    """
    s3_client = boto3.client("s3")
    logger = logging.getLogger(__name__)
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
            j = json.loads(json_line)
            j["export_start_date"] = s3_metadata.get("start_date")
            j["export_end_date"] = s3_metadata.get("end_date")
            if dataset_identifier == "HealthKitV2Samples":
                # This puts the `Type` property back where Apple intended it to be
                j["Type"] = metadata["subtype"]
            if dataset_identifier == "SymptomLog":
                # Load JSON string as dict
                j["Value"] = json.loads(j["Value"])
            if dataset_identifier == "EnrolledParticipants":
                for field_name in ["Symptoms", "Treatments"]:
                    if (
                            field_name in j["CustomFields"]
                            and isinstance(j["CustomFields"][field_name], str)
                       ):
                        if len(j["CustomFields"][field_name]) > 0:
                            # This JSON string was written in a couple different ways
                            # in the testing data: "[{\\\"id\\\": ..." and "[{\"id\": ..."
                            # or just an empty string. It's not really clear which format
                            # is intended, (The example they provided has it written as
                            # an object rather than a string, so...).
                            try:
                                j["CustomFields"][field_name] = json.loads(
                                        j["CustomFields"][field_name]
                                )
                            except json.JSONDecodeError as e:
                                # If it's not propertly formatted JSON, then we
                                # can't read it, and instead store an empty list
                                logger.warning(f"Problem CustomFields.{field_name}: "
                                               f"{j['CustomFields'][field_name]}")
                                logger.warning(str(e))
                                j["CustomFields"][field_name] = []
                        else:
                            j["CustomFields"][field_name] = []
            data.append(j)
    if dataset_identifier == "HealthKitV2Samples":
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
    output_path = os.path.join(dataset_identifier, output_fname)
    s3_output_key = os.path.join(
        workflow_run_properties["namespace"],
        workflow_run_properties["json_prefix"],
        f"dataset={dataset_identifier}",
        output_fname)
    logger.debug("Output Key: %s", s3_output_key)
    with open(output_path, "w") as f_out:
        for record in data:
            f_out.write("{}\n".format(json.dumps(record)))
    with open(output_path, "rb") as f_in:
        response = s3_client.put_object(
                Body = f_in,
                Bucket = workflow_run_properties["json_bucket"],
                Key = s3_output_key,
                Metadata = s3_metadata)
        logger.debug("S3 Put object response: %s", json.dumps(response))
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
            subtype (str): If this is HealthKitV2Samples, the type of the
                sample data. (Not to be confused with the data type, which in
                this case is HealthKitV2Samples or HealthKitV2Samples_Deleted)
    """
    logger = logging.getLogger(__name__)
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
    if metadata["type"] == "HealthKitV2Samples":
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
    logger = logging.getLogger(__name__)
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
    # Configure logger
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

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
