"""
This Lambda app responds to an S3 event notification and starts a Glue workflow.
The Glue workflow name is set by the environment variable `PRIMARY_WORKFLOW_NAME`.
Subsequently, the S3 objects which were contained in the event are written as a
JSON string to the `messages` workflow run property.
"""
import os
import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def submit_s3_to_json_workflow(messages: list[dict[str, str]], workflow_name):
    """
    Submits list of dicts with keys `source_bucket` and `source_key`
    to the S3 to JSON Glue workflow.

    Args:
        workflow_name (str): name of the glue workflow to submit to
        messages (list): objects from source S3 bucket to submit
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
        RunProperties={"messages": json.dumps(messages)},
    )


def lambda_handler(event, context):
    """
    The Lambda entrypoint

    Given a list of files in s3 bucket to process, submit the keys and bucket
    of the new files to a S3 to JSON Glue workflow.

    Args:
        event (dict): A fileview object
        context (obj) : An object that provides methods and properties that
        provides info about the invocation, function, and execution environment

    Returns:
        (None) Submits new file records to a Glue workflow.
    """
    messages = []
    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        message = {"source_bucket": bucket_name, "source_key": object_key}
        if "owner.txt" not in object_key:
            messages.append(message)
    if len(messages) > 0:
        logger.info(
            "Submitting the following files to "
            f"{os.environ['PRIMARY_WORKFLOW_NAME']}: {json.dumps(messages)}"
        )
        submit_s3_to_json_workflow(
            messages=messages, workflow_name=os.environ["PRIMARY_WORKFLOW_NAME"]
        )
