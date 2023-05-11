"""
This Lambda app responds to an external trigger (usually github action or aws console) and
puts a s3 event notification configuration for the S3 to Glue lambda in the
input data S3 bucket set by the environment variable `S3_SOURCE_BUCKET_NAME`.

This Lambda app also has the option of deleting the notification configuration
from an S3 bucket
"""
import os
import json
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REQUEST_TYPE_VALS = ["Delete", "Create", "Update"]


def lambda_handler(event, context):
    s3 = boto3.resource("s3")
    logger.info(f"Received event: {json.dumps(event, indent=2)}")
    if event["RequestType"] == "Delete":
        logger.info(f'Request Type:{event["RequestType"]}')
        bucket = os.environ["S3_SOURCE_BUCKET_NAME"]
        delete_notification(s3, bucket)
        logger.info("Sending response to custom resource after Delete")
    elif event["RequestType"] in ["Update", "Create"]:
        logger.info(f'Request Type: {event["RequestType"]}')
        lambda_arn = os.environ["S3_TO_GLUE_FUNCTION_ARN"]
        bucket = os.environ["S3_SOURCE_BUCKET_NAME"]
        add_notification(s3, lambda_arn, bucket)
        logger.info("Sending response to custom resource")
    else:
        err_msg =  f"The 'RequestType' key should have one of the following values: {REQUEST_TYPE_VALS}"
        raise KeyError(err_msg)


def add_notification(
    s3_resource: boto3.resources.base.ServiceResource, lambda_arn: str, bucket: str
):
    """Adds the S3 notification configuration to an existing bucket

    Args:
        s3_resource (boto3.resources.base.ServiceResource) : s3 resource to use for s3 event config
        lambda_arn (str): Arn of the lambda s3 event config function
        bucket (str): bucket name of the s3 bucket to add the config to
    """
    bucket_notification = s3_resource.BucketNotification(bucket)
    response = bucket_notification.put(
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {"LambdaFunctionArn": lambda_arn, "Events": ["s3:ObjectCreated:*"]}
            ]
        }
    )
    logger.info("Put request completed....")


def delete_notification(s3_resource: boto3, bucket: str):
    """Deletes the S3 notification configuration from an existing bucket

    Args:
        s3_resource (boto3.resources.base.ServiceResource) : s3 resource to use for s3 event config
        bucket (str): bucket name of the s3 bucket to delete the config in
    """
    bucket_notification = s3_resource.BucketNotification(bucket)
    response = bucket_notification.put(NotificationConfiguration={})
    logger.info("Delete request completed....")
