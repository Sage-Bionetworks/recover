"""
This Lambda app responds to an SQS event notification and starts a Glue workflow.
The Glue workflow name is set by the environment variable `PRIMARY_WORKFLOW_NAME`.
Subsequently, the S3 objects which were contained in the SQS event are written as a
JSON string to the `messages` workflow run property.
"""
import os
import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def filter_object_info(object_info: dict) -> dict:
    """Filter out object info records that should not be processed. Returns None

       - Records containing owner.txt
       - Records that don't contain a specific object key like <path_to_file>/<file_name>
       - Records that are missing a source_key
       - Records that are missing a bucket_name

    Args:
        object_info (dict): Object information from source S3 bucket

    Returns:
        dict: returns back the object info dict if it passes the filter criteria
            otherwise returns None
    """
    if not object_info["source_key"]:
        logger.info(
            "This object_info record doesn't contain a source key "
            f"and can't be processed.\nMessage: {object_info}",
        )
        return None
    elif not object_info["source_bucket"]:
        logger.info(
            "This object_info record doesn't contain a source bucket "
            f"and can't be processed.\nMessage: {object_info}",
        )
        return None
    elif "owner.txt" in object_info["source_key"]:
        logger.info(
            f"This object_info record is an owner.txt and can't be processed.\nMessage: {object_info}"
        )
        return None
    elif object_info["source_key"].endswith("/"):
        logger.info(
            f"This object_info record is a directory and can't be processed.\nMessage: {object_info}"
        )
        return None
    else:
        return object_info


def submit_s3_to_json_workflow(objects_info: list[dict[str, str]], workflow_name):
    """
    Submits list of dicts with keys `source_bucket` and `source_key`
    to the S3 to JSON Glue workflow.

    Args:
        workflow_name (str): name of the glue workflow to submit to
        objects_info (list): objects from source S3 bucket to submit
        to Glue workflow.

    Returns:
        (None)
    """
    glue_client = boto3.client("glue")
    logger.info(f"Starting workflow run for {workflow_name}")
    workflow_run = glue_client.start_workflow_run(Name=workflow_name)
    glue_client.put_workflow_run_properties(
        Name=workflow_name,
        RunId=workflow_run["RunId"],
        RunProperties={"messages": json.dumps(objects_info)},
    )


def is_s3_test_event(record : dict) -> bool:
    """
    AWS always sends a s3 test event to the SQS queue
    whenever a new file is uploaded. We want to skip those
    and have lambda delete it.
    """
    if "Event" in record.keys():
        if record["Event"] == "s3:TestEvent":
            return True
        else:
            return False
    else:
        return False


def lambda_handler(event, context) -> None:
    """
    This main lambda function will be triggered by a SQS event and will
    poll the SQS queue for all available S3 event messages. If the
    messages were all successfully processed and submitted, lambda will
    automatically delete the messages from the sqs queue

    Args:
        event (json): SQS json event containing S3 events
        context (object): A context object is passed to your function by Lambda at runtime.
            This object provides methods and properties that provide information
            about the invocation, function, and runtime environment.
            Unused by this lambda function.
    """
    s3_objects_info = []
    for record in event["Records"]:
        s3_event_records = json.loads(record["body"])
        if is_s3_test_event(s3_event_records):
            logger.info(f"Found AWS default s3:TestEvent. Skipping.")
        else:
            for s3_event in s3_event_records["Records"]:
                bucket_name = s3_event["s3"]["bucket"]["name"]
                object_key = s3_event["s3"]["object"]["key"]
                object_info = {
                    "source_bucket": bucket_name,
                    "source_key": object_key,
                }
                if filter_object_info(object_info) is not None:
                    s3_objects_info.append(object_info)
                else:
                    logger.info(
                        f"Object doesn't meet the S3 event rules to be processed. Skipping."
                    )

    if len(s3_objects_info) > 0:
        logger.info(
            "Submitting the following files to "
            f"{os.environ['PRIMARY_WORKFLOW_NAME']}: {json.dumps(s3_objects_info)}"
        )
        submit_s3_to_json_workflow(
            objects_info=s3_objects_info,
            workflow_name=os.environ["PRIMARY_WORKFLOW_NAME"],
        )
    else:
        logger.info(
            "NO files were submitted to "
            f"{os.environ['PRIMARY_WORKFLOW_NAME']}: {json.dumps(s3_objects_info)}"
        )
