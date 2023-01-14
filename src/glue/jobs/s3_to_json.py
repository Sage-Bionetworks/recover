import datetime
import io
import os
import json
import logging
import zipfile
import boto3
#from awsglue.utils import getResolvedOptions

def write_file_to_json_dataset(z, json_path, dataset_identifier,
        metadata, workflow_run_properties):
    """
    Write a JSON from a zipfile to a JSON dataset.

    Additional fields are inserted at the top-level (if the JSON is an object) or
    at the top-level of each object (if the JSON is a list of objects) before
    the JSON is written as NDJSON (e.g., since this is a single JSON document,
    on a single line) to a JSON dataset in S3. Partition fields are inserted into
    every JSON, regardless of its schema identifier. Every field from the S3
    object metadata is inserted into the JSON with schema identifier
    'ArchiveMetadata'.

    Partition fields are as follows:
        * assessmentid
        * year (as derived from the `uploadedon` field of the S3 object metadata)
        * month ("")
        * day ("")

    Args:
        z (zipfile.Zipfile): The zip archive as provided by Bridge.
        json_path (str): A path relative to the root of `z` to a JSON file.
        dataset_identifier (str): A BridgeDownstream dataset identifier.
        s3_obj_metadata (dict): A dictionary of S3 object metadata.
        workflow_run_properties (dict): The workflow arguments

    Returns:
        output_path (str) The local path the file was written to.
    """
    s3_client = boto3.client("s3")
    logger = logging.getLogger(__name__)
    schema_identifier = dataset_identifier.split("_")[0]
    os.makedirs(dataset_identifier, exist_ok=True)
    output_path = None
    with z.open(json_path, "r") as p:
        j = json.load(p)
        # We inject all S3 metadata into the metadata file
        if schema_identifier == "ArchiveMetadata":
            j["year"] = int(uploaded_on.year)
            j["month"] = int(uploaded_on.month)
            j["day"] = int(uploaded_on.day)
            for key in s3_obj_metadata:
                j[key] = s3_obj_metadata[key]
        else: # but only the partition fields and record ID into other files
            if isinstance(j, list):
                for item in j:
                    item["assessmentid"] = s3_obj_metadata["assessmentid"]
                    item["year"] = int(uploaded_on.year)
                    item["month"] = int(uploaded_on.month)
                    item["day"] = int(uploaded_on.day)
                    item["recordid"] = s3_obj_metadata["recordid"]
            else:
                j["assessmentid"] = s3_obj_metadata["assessmentid"]
                j["year"] = int(uploaded_on.year)
                j["month"] = int(uploaded_on.month)
                j["day"] = int(uploaded_on.day)
                j["recordid"] = s3_obj_metadata["recordid"]
        output_fname = s3_obj_metadata["recordid"] + ".ndjson"
        output_path = os.path.join(dataset_identifier, output_fname)
        logger.debug(f'output_path: {output_path}')
        with open(output_path, "w") as f_out:
            json.dump(j, f_out, indent=None)
            s3_output_key = os.path.join(
                workflow_run_properties["namespace"],
                workflow_run_properties["app_name"],
                workflow_run_properties["study_name"],
                workflow_run_properties["json_prefix"],
                f"dataset={dataset_identifier}",
                f"assessmentid={s3_obj_metadata['assessmentid']}",
                f"year={str(uploaded_on.year)}",
                f"month={str(uploaded_on.month)}",
                f"day={str(uploaded_on.day)}",
                output_fname)
        with open(output_path, "rb") as f_in:
            response = s3_client.put_object(
                    Body = f_in,
                    Bucket = workflow_run_properties["json_bucket"],
                    Key = s3_output_key,
                    Metadata = s3_obj_metadata)
            logger.debug(f"put object response: {json.dumps(response)}")
    return output_path

def get_metadata(basename):
    """
    Get metadata of a file by parsing its basename.

    Args:
        basename (str): The basename of the file.

    Returns:
        dict: The metadata
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
        metadata["start_date"] = \
                datetime.datetime.strptime(basename_components[-1], "%Y%m%d")
    if metadata["type"] == "HealthKitV2Samples":
        metadata["subtype"] = basename_components[1]
    if (
            metadata["type"] == "HealthKitV2Samples"
            and basename_components[-2] == "Deleted"
        ):
        metadata["deleted"] = True
    return metadata

def process_record(s3_obj, workflow_run_properties):
    """
    Write the contents of a .zip archive stored on S3 to their respective JSON dataset.

    Every JSON dataset has a dataset identifier. The dataset identifier is
    derived from the $id property of the associated JSON Schema (if the file
    has a JSON schema) or mapped to directly from the assessment ID/revision
    and filename for certain older assessments. If there is no mapping from a
    given assessment ID/revision and filename to a dataset identifier, the file
    is skipped. Partition fields are inserted into every JSON, regardless of its
    schema identifier. Every field from the S3 object metadata is inserted into
    the JSON with schema identifier 'ArchiveMetadata'.

    Args:
        s3_obj (dict): An S3 object as returned by boto3.get_object.
        json_schemas (list): A list of JSON Schema (dict).
        dataset_mapping (dict): A mapping from assessment ID/revision/filename to
            dataset identifiers
        schema_mapping (dict): A mapping from JSON schema $id to dataset identifiers.
        archive_map (dict): The dict representation of archive-map.json from
            Sage-Bionetworks/mobile-client-json
        workflow_run_properties (dict): The workflow arguments

    Returns:
        None
    """
    logger = logging.getLogger(__name__)
    s3_obj_metadata = s3_obj["Metadata"]
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
        non_empty_contents = [f.filename for f in z.filelist if f.file_size > 0]
        logger.debug(f'contents: {z.namelist()}')
        logger.debug(f'non-empty contents: {non_empty_contents}')
        for json_path in non_empty_contents:
            metadata = get_metadata(os.path.basename(json_path))
            dataset_identifier = get_dataset_identifier(
                    json_schema=json_schema["schema"],
                    schema_mapping=schema_mapping,
                    dataset_mapping=dataset_mapping,
                    file_metadata=file_metadata)
            if dataset_identifier is None:
                continue
            logger.info(f"Writing {json_path} to dataset {dataset_identifier}")
            write_file_to_json_dataset(
                    z=z,
                    json_path=json_path,
                    dataset_identifier=dataset_identifier,
                    s3_obj_metadata=s3_obj_metadata,
                    workflow_run_properties=workflow_run_properties)

def main():
    # Configure logger
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Instantiate boto clients
    glue_client = boto3.client("glue")

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
    logger.debug(f"getResolvedOptions: {json.dumps(args)}")
    logger.debug(f"get_workflow_run_properties: {json.dumps(workflow_run_properties)}")

    # Load messages to be processed
    logger.info("Loading messages")
    messages = json.loads(workflow_run_properties["messages"])
    sts_tokens = {}
    json_schemas = []
    for message in messages:
        logger.info(f"Retrieving S3 object for Bucket {message['source_bucket']} "
                    f"and Key {message['source_key']}'")
        bridge_s3_client = boto3.client("s3", **sts_tokens[synapse_data_folder])
        s3_obj = bridge_s3_client.get_object(
                Bucket = message["source_bucket"],
                Key = message["source_key"]
        )
        s3_obj["Body"] = s3_obj["Body"].read()
        process_record(
                s3_obj=s3_obj,
                workflow_run_properties=workflow_run_properties
        )
