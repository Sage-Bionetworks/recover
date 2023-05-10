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

SUCCESS = "SUCCESS"
FAILED = "FAILED"

logger.info("Loading function")
s3 = boto3.resource("s3")


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event, indent=2)}")
    try:
        if event["RequestType"] == "Delete":
            logger.info(f'Request Type:{event["RequestType"]}')
            bucket = os.environ["S3_SOURCE_BUCKET_NAME"]
            delete_notification(bucket)
            logger.info("Sending response to custom resource after Delete")

        elif event["RequestType"] == "Create" or event["RequestType"] == "Update":
            logger.info(f'Request Type: {event["RequestType"]}')
            lambda_arn = os.environ["S3_TO_GLUE_FUNCTION_ARN"]
            bucket = os.environ["S3_SOURCE_BUCKET_NAME"]
            add_notification(lambda_arn, bucket)
            logger.info("Sending response to custom resource")
    except Exception as e:
        logger.info(f"Failed to process:{e}")


def add_notification(lambda_arn: str, bucket: str) -> None:
    """Adds the S3 notification configuration to an existing bucket

    Args:
        lambda_arn (str): Arn of the lambda s3 event config function
        bucket (str): bucket name of the s3 bucket to add the config to
    """
    bucket_notification = s3.BucketNotification(bucket)
    response = bucket_notification.put(
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {"LambdaFunctionArn": lambda_arn, "Events": ["s3:ObjectCreated:*"]}
            ]
        }
    )
    logger.info("Put request completed....")


def delete_notification(bucket: str) -> None:
    """Deletes the S3 notification configuration from an existing bucket

    Args:
        bucket (str): bucket name of the s3 bucket to delete the config in
    """
    bucket_notification = s3.BucketNotification(bucket)
    response = bucket_notification.put(NotificationConfiguration={})
    logger.info("Delete request completed....")
