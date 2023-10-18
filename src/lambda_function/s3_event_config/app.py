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
        delete_notification(
            s3,
            bucket=os.environ["S3_SOURCE_BUCKET_NAME"],
            destination_type=os.environ["S3_TO_GLUE_DESTINATION_TYPE"],
        )
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
    destination_type: str,
    destination_arn: str,
    bucket: str,
    bucket_key_prefix: str,
):
    """Adds the S3 notification configuration to an existing bucket.

        Use cases:
            1) If a bucket has no `NotificationConfiguration` then create the config
            2) If a bucket has a `NotificationConfiguration` but no matching "{destination_type}Configurations" then merge and add the config
            3) If a bucket has a `NotificationConfiguration` and a matching "{destination_type}Configurations":
                3a) If the config is the same then do nothing - ordering of the dict does not matter
                3b) If the config is different then overwrite the matching "{destination_type}Configurations"

    Args:
        s3_client (boto3.client) : s3 client to use for s3 event config
        destination_type (str): String name of the destination type for the configuration
        destination_arn (str): Arn of the destination's s3 event config
        bucket (str): bucket name of the s3 bucket to add the config to
        bucket_key_prefix (str): bucket key prefix for where to look for s3 object notifications
    """
    existing_bucket_notification_configuration = (
        s3_client.get_bucket_notification_configuration(Bucket=bucket)
    )
    existing_notification_config_for_type = (
        existing_bucket_notification_configuration.get(
            f"{destination_type}Configurations", {}
        )
    )

    new_notification_config = {
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
    }

    # If the configuration we want to add isn't there or is different then create a new that contains the new value along with any previous data.
    if not existing_notification_config_for_type or json.dumps(
        existing_notification_config_for_type, sort_keys=True
    ) != json.dumps(
        new_notification_config[f"{destination_type}Configurations"], sort_keys=True
    ):
        merged_config = {
            **existing_bucket_notification_configuration,
            **new_notification_config,
        }

        s3_client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration=merged_config,
        )
        logger.info(
            f"Put request completed to add a NotificationConfiguration for `{destination_type}Configurations`."
        )
    else:
        logger.info(
            f"Put not required as an existing NotificationConfiguration for `{destination_type}Configurations` already exists."
        )


def delete_notification(s3_client: boto3.client, bucket: str, destination_type: str):
    """Deletes the S3 notification configuration from an existing bucket for a specific destination type.

    Args:
        s3_client (boto3.client) : s3 client to use for s3 event config
        bucket (str): bucket name of the s3 bucket to delete the config in
        destination_type (str): String name of the destination type for the configuration
    """
    existing_bucket_notification_configuration = (
        s3_client.get_bucket_notification_configuration(Bucket=bucket)
    )

    configuration_name = f"{destination_type}Configurations"

    existing_notification_config_for_type = (
        existing_bucket_notification_configuration.get(configuration_name, {})
    )

    if existing_notification_config_for_type:
        del existing_bucket_notification_configuration[configuration_name]
        s3_client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration=existing_bucket_notification_configuration,
        )
        logger.info(
            f"Delete request completed to remove a NotificationConfiguration for `{destination_type}Configurations`."
        )
    else:
        logger.info(
            f"Delete not required as no NotificationConfiguration exists for `{destination_type}Configurations`."
        )
