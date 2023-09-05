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
    s3 = boto3.client("s3")
    logger.info(f"Received event: {json.dumps(event, indent=2)}")
    if event["RequestType"] == "Delete":
        logger.info(f'Request Type:{event["RequestType"]}')
        delete_notification(s3, bucket=os.environ["S3_SOURCE_BUCKET_NAME"])
        logger.info("Sending response to custom resource after Delete")
    elif event["RequestType"] in ["Update", "Create"]:
        logger.info(f'Request Type: {event["RequestType"]}')
        add_notification(
            s3,
            destination_arn=os.environ["S3_TO_GLUE_DESTINATION_ARN"],
            destination_type=os.environ["S3_TO_GLUE_DESTINATION_TYPE"],
            bucket=os.environ["S3_SOURCE_BUCKET_NAME"],
            bucket_key_prefix=os.environ["BUCKET_KEY_PREFIX"],
        )
        logger.info("Sending response to custom resource")
    else:
        err_msg = f"The 'RequestType' key should have one of the following values: {REQUEST_TYPE_VALS}"
        raise KeyError(err_msg)


def add_notification(
    s3_client: boto3.client,
    destination_type : str,
    destination_arn: str,
    bucket: str,
    bucket_key_prefix: str,
):
    """Adds the S3 notification configuration to an existing bucket

    Args:
        s3_client (boto3.client) : s3 client to use for s3 event config
        destination_type (str): String name of the destination type for the configuration
        destination_arn (str): Arn of the destination's s3 event config
        bucket (str): bucket name of the s3 bucket to add the config to
        bucket_key_prefix (str): bucket key prefix for where to look for s3 object notifications
    """
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={
            f"{destination_type}Configurations": [
                {
                    f"{destination_type}Arn": destination_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {"Name": "prefix", "Value": f"{bucket_key_prefix}/"}
                            ]
                        }
                    },
                }
            ]
        },
    )
    logger.info("Put request completed....")


def delete_notification(s3_client: boto3.client, bucket: str):
    """Deletes the S3 notification configuration from an existing bucket

    Args:
        s3_client (boto3.client) : s3 client to use for s3 event config
        bucket (str): bucket name of the s3 bucket to delete the config in
    """
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket, NotificationConfiguration={}
    )
    logger.info("Delete request completed....")
