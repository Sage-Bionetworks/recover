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


def lambda_handler(event, context) -> None:
    """This main lambda function will be triggered by a
        SQS event and will poll the SQS queue for
        all available S3 event messages

    Args:
        event (json): SQS event
        context: NA
    """
    # Initialize SQS client
    sqs = boto3.client("sqs")
    s3_objects_info = []
    if "Records" in event:
        for record in event["Records"]:
            s3_event_records = json.loads(record["body"])
            if "Records" in s3_event_records:
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
                            f"Object doesn't meet the S3 event rules to be processed. Skipping\n{object_info}"
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
        # Delete the messages from the queue after all events are successfully submitted
        for record in event["Records"]:
            receipt_handle = record["receiptHandle"]
            sqs.delete_message(
                QueueUrl=os.environ["SQS_QUEUE_URL"], ReceiptHandle=receipt_handle
            )
        return {"statusCode": 200, "body": "All messages retrieved and processed."}
    else:
        logger.info("No 'Records' key exists in event to be parsed" f"\nEvent: {event}")
        return {"statusCode": 0, "body": "No messages exist"}
