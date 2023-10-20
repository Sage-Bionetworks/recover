"""
This Lambda app responds to an external trigger (usually github action or aws console) and
puts a s3 event notification configuration for the S3 to Glue lambda in the
input data S3 bucket set by the environment variable `S3_SOURCE_BUCKET_NAME`.

This Lambda app also has the option of deleting the notification configuration
from an S3 bucket.

Only certain notification configurations work. Only `QueueConfigurations` are expected.
"""
import os
import json
import logging
import typing

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


def get_existing_bucket_notification_configuration_and_type(
    s3_client: boto3.client, bucket: str, destination_type: str
) -> typing.Tuple[dict, list]:
    """
    Gets the existing bucket notification configuration and the existing notification
    configurations for a specific destination type.

    Arguments:
        s3_client (boto3.client) : s3 client to use for s3 event config
        bucket (str): bucket name of the s3 bucket to delete the config in
        destination_type (str): String name of the destination type for the configuration

    Returns:
        Tuple: A bucket notifiction configuration,
            and the notification configurations for a specific destination type
    """
    existing_bucket_notification_configuration = (
        s3_client.get_bucket_notification_configuration(Bucket=bucket)
    )

    # Remove ResponseMetadata because we don't want to persist it
    existing_bucket_notification_configuration.pop("ResponseMetadata", None)

    existing_notification_configurations_for_type = (
        existing_bucket_notification_configuration.get(
            f"{destination_type}Configurations", []
        )
    )

    # Initialize this with an empty list to have consistent handling if it's present
    # or missing.
    if not existing_notification_configurations_for_type:
        existing_bucket_notification_configuration[
            f"{destination_type}Configurations"
        ] = []

    return (
        existing_bucket_notification_configuration,
        existing_notification_configurations_for_type,
    )


def get_matching_notification_configuration(
    destination_type_arn: str,
    existing_notification_configurations_for_type: list,
    destination_arn: str,
) -> typing.Union[tuple[int, dict], tuple[None, None]]:
    """
    Search through the list of existing notifications and find the one that has a key of
    `destination_type_arn` and a value of `destination_arn`.

    Arguments:
        destination_type_arn (str): Key value for the destination type arn
        existing_notification_configurations_for_type (list): The existing notification configs
        destination_arn (str): Arn of the destination's s3 event config

    Returns:
        tuple: The index of the matching notification configuration and the matching
            notification configuration or None, None if no match is found
    """
    for index, existing_notification_configuration_for_type in enumerate(
        existing_notification_configurations_for_type
    ):
        if (
            destination_type_arn in existing_notification_configuration_for_type
            and existing_notification_configuration_for_type[destination_type_arn]
            == destination_arn
        ):
            return index, existing_notification_configuration_for_type
    return None, None


def create_formatted_message(
    bucket: str, destination_type: str, destination_arn: str
) -> str:
    """Creates a formatted message for logging purposes.

    Arguments:
        bucket (str): bucket name of the s3 bucket
        destination_type (str): String name of the destination type for the configuration
        destination_arn (str): Arn of the destination's s3 event config

    Returns:
        str: A formatted message
    """
    return f"Bucket: {bucket}, DestinationType: {destination_type}, DestinationArn: {destination_arn}"


def add_notification(
    s3_client: boto3.client,
    destination_type: str,
    destination_arn: str,
    bucket: str,
    bucket_key_prefix: str,
) -> None:
    """Adds the S3 notification configuration to an existing bucket.

        Use cases:
            1) If a bucket has no `NotificationConfiguration` then create the config
            2) If a bucket has a `NotificationConfiguration` but no matching
                "{destination_arn}" for the "{destination_type}" then add the config
            3) If a bucket has a `NotificationConfiguration` and a matching
                                    "{destination_arn}" for the "{destination_type}":
                3a) If the config is the same then do nothing
                3b) If the config is different then overwrite the matching config

    Args:
        s3_client (boto3.client) : s3 client to use for s3 event config
        destination_type (str): String name of the destination type for the configuration
        destination_arn (str): Arn of the destination's s3 event config
        bucket (str): bucket name of the s3 bucket to add the config to
        bucket_key_prefix (str): bucket key prefix for where to look for s3 object notifications
    """
    update_required = False
    destination_type_arn = f"{destination_type}Arn"
    new_notification_configuration = {
        destination_type_arn: destination_arn,
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
            "Key": {
                "FilterRules": [{"Name": "prefix", "Value": f"{bucket_key_prefix}/"}]
            }
        },
    }

    (
        existing_bucket_notification_configuration,
        existing_notification_configurations_for_type,
    ) = get_existing_bucket_notification_configuration_and_type(
        s3_client, bucket, destination_type
    )

    (
        index_of_matching_arn,
        matching_notification_configuration,
    ) = get_matching_notification_configuration(
        destination_type_arn,
        existing_notification_configurations_for_type,
        destination_arn,
    )

    if index_of_matching_arn is not None:
        if json.dumps(
            matching_notification_configuration, sort_keys=True
        ) != json.dumps(new_notification_configuration, sort_keys=True):
            existing_notification_configurations_for_type[
                index_of_matching_arn
            ] = new_notification_configuration
            update_required = True
    else:
        existing_notification_configurations_for_type.append(
            new_notification_configuration
        )
        update_required = True

    if update_required:
        existing_bucket_notification_configuration[
            f"{destination_type}Configurations"
        ] = existing_notification_configurations_for_type
        s3_client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration=existing_bucket_notification_configuration,
        )
        logger.info(
            f"Put request completed to add a NotificationConfiguration for"
            + create_formatted_message(bucket, destination_type, destination_arn)
        )
    else:
        logger.info(
            f"Put not required as an existing NotificationConfiguration already exists for"
            + create_formatted_message(bucket, destination_type, destination_arn)
        )


def delete_notification(
    s3_client: boto3.client, bucket: str, destination_type: str, destination_arn: str
) -> None:
    """Deletes the S3 notification configuration from an existing bucket for a specific destination type.

    Args:
        s3_client (boto3.client) : s3 client to use for s3 event config
        bucket (str): bucket name of the s3 bucket to delete the config in
        destination_type (str): String name of the destination type for the configuration
    """
    destination_type_arn = f"{destination_type}Arn"

    (
        existing_bucket_notification_configuration,
        existing_notification_configurations_for_type,
    ) = get_existing_bucket_notification_configuration_and_type(
        s3_client, bucket, destination_type
    )

    (
        index_of_matching_arn,
        matching_notification_confiugration,
    ) = get_matching_notification_configuration(
        destination_type_arn,
        existing_notification_configurations_for_type,
        destination_arn,
    )

    if index_of_matching_arn is not None:
        del existing_notification_configurations_for_type[index_of_matching_arn]

        if existing_notification_configurations_for_type:
            existing_bucket_notification_configuration[
                f"{destination_type}Configurations"
            ] = existing_notification_configurations_for_type
        else:
            del existing_bucket_notification_configuration[
                f"{destination_type}Configurations"
            ]

        s3_client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration=existing_bucket_notification_configuration,
        )
        logger.info(
            f"Delete request completed to remove a NotificationConfiguration for"
            + create_formatted_message(bucket, destination_type, destination_arn)
        )
    else:
        logger.info(
            f"Delete not required as no NotificationConfiguration exists for"
            + create_formatted_message(bucket, destination_type, destination_arn)
        )
