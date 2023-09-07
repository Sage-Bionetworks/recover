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
        cloudwatch scheduler

    Args:
        event (json): CloudWatch scheduled trigger event
        context: NA

    Raises:
        Exception: When polling SQS queue on a loop has reached the maximum iteration
    """
    # Initialize SQS client
    sqs = boto3.client("sqs")

    # Get the URL of the SQS queue
    queue_name = os.environ["SQS_QUEUE_NAME"]
    response = sqs.get_queue_url(QueueName=queue_name)
    queue_url = response["QueueUrl"]

    has_messages = True

    # maximum iterations for any loop in order to timeout a loop
    max_iterations = 1000
    iteration = 0
    while has_messages:
        if iteration > max_iterations:
            raise Exception(
                f"Polling SQS queue reached maximum iterations of {max_iterations}"
            )
        # Receive up to 10 messages at a time
        # Note that this visibility timeout var overrides the SQS queue's default and
        # sets a different visibility timeout for these specific messages
        response = sqs.receive_message(
            QueueUrl=queue_url,
            VisibilityTimeout=int(os.environ["VISIBILITY_TIMEOUT"]),
            MaxNumberOfMessages=int(os.environ["MAX_NUMBER_OF_MESSAGES_TO_RECEIVE"]),
            WaitTimeSeconds=int(os.environ["WAIT_TIME_FOR_MESSAGES"]),
        )
        # Check if messages were received
        if "Messages" in response:
            messages = response["Messages"]
            for message in messages:
                msg_body = json.loads(message["Body"])
                s3_objects_info = []
                # check that records are present per message
                if "Records" in msg_body:
                    for record in msg_body["Records"]:
                        bucket_name = record["s3"]["bucket"]["name"]
                        object_key = record["s3"]["object"]["key"]
                        object_info = {
                            "source_bucket": bucket_name,
                            "source_key": object_key,
                        }
                        if filter_object_info(object_info) is not None:
                            s3_objects_info.append(object_info)
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
                        "No 'Records' key exists in event to be parsed"
                        f"\nEvent: {event}"
                    )
                # Delete the messages from the queue
                receipt_handle = message["ReceiptHandle"]
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            has_messages = False
            iteration += 1
        else:
            has_messages = False
            return {"statusCode": 0, "body": "No messages exist"}

    return {"statusCode": 200, "body": "All messages retrieved and processed."}
