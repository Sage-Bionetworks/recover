"""
This Lambda app responds to an external trigger (usually github action or aws console) and
puts a s3 event notification configuration for the S3 to Glue lambda in the
input data S3 bucket set by the environment variable `S3_SOURCE_BUCKET_NAME`.

This Lambda app also has the option of deleting the notification configuration
from an S3 bucket.

Only certain notification configurations work. Only `QueueConfigurations` are expected.
"""
import json
import logging
import os
import typing
from collections import defaultdict
from enum import Enum

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REQUEST_TYPE_VALS = ["Delete", "Create", "Update"]


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    logger.info(f"Received event: {json.dumps(event, indent=2)}")
    if event["RequestType"] == "Delete":
        logger.info(f'Request Type:{event["RequestType"]}')
        delete_notification(
            s3_client=s3_client,
            bucket=os.environ["S3_SOURCE_BUCKET_NAME"],
            bucket_key_prefix=os.environ["BUCKET_KEY_PREFIX"],
        )
        logger.info("Sending response to custom resource after Delete")
    elif event["RequestType"] in ["Update", "Create"]:
        logger.info(f'Request Type: {event["RequestType"]}')
        add_notification(
            s3_client=s3_client,
            destination_arn=os.environ["S3_TO_GLUE_DESTINATION_ARN"],
            destination_type=os.environ["S3_TO_GLUE_DESTINATION_TYPE"],
            bucket=os.environ["S3_SOURCE_BUCKET_NAME"],
            bucket_key_prefix=os.environ["BUCKET_KEY_PREFIX"],
        )
        logger.info("Sending response to custom resource")
    else:
        err_msg = f"The 'RequestType' key should have one of the following values: {REQUEST_TYPE_VALS}"
        raise KeyError(err_msg)


class NotificationConfigurationType(Enum):
    """
    Supported types for an S3 event configuration.
    """

    Topic = "Topic"
    Queue = "Queue"
    LambdaFunction = "LambdaFunction"


class NotificationConfiguration:
    """
    An abstraction of S3 event configurations.
    """

    def __init__(self, notification_type: NotificationConfigurationType, value: dict):
        self.type = notification_type.value
        self.value = value
        self.arn = self.get_arn()

    def get_arn(self):
        """
        Assign the ARN of this notification configuration to the `arn` property.
        """
        if self.type == NotificationConfigurationType.Topic.value:
            arn = self.value["TopicArn"]
        elif self.type == NotificationConfigurationType.Queue.value:
            arn = self.value["QueueArn"]
        elif self.type == NotificationConfigurationType.LambdaFunction.value:
            arn = self.value["LambdaFunctionArn"]
        else:
            raise ValueError(
                f"{self.type} is not a recognized notification configuration type."
            )
        return arn


class BucketNotificationConfigurations:
    """
    A convenience class for working with a collection of `NotificationConfiguration`s.
    """

    def __init__(self, notification_configurations: list[NotificationConfiguration]):
        self.configs = notification_configurations

    def to_dict(self):
        """
        A dict representation of this object which can be supplied to
        `put_bucket_notification_configuration`
        """
        return_dict = defaultdict(list)
        for config in self.configs:
            return_dict[f"{config.type}Configurations"].append(config.value)
        return dict(return_dict)


def get_bucket_notification_configurations(
    s3_client: boto3.client,
    bucket: str,
) -> BucketNotificationConfigurations:
    """
    Gets the existing bucket notification configuration and the existing notification
    configurations for a specific destination type.

    Arguments:
        s3_client (boto3.client) : s3 client to use for s3 event config
        bucket (str): bucket name of the s3 bucket to delete the config in
        destination_type (str): String name of the destination type for the configuration

    Returns:
        BucketNotificationConfigurations
    """
    bucket_notification_configuration = s3_client.get_bucket_notification_configuration(
        Bucket=bucket
    )
    all_notification_configurations = []
    for configuration_type in NotificationConfigurationType:
        configuration_type_name = f"{configuration_type.value}Configurations"
        if configuration_type_name in bucket_notification_configuration:
            notification_configurations = [
                NotificationConfiguration(
                    notification_type=configuration_type, value=config
                )
                for config in bucket_notification_configuration[configuration_type_name]
            ]
            all_notification_configurations.extend(notification_configurations)
    bucket_notification_configurations = BucketNotificationConfigurations(
        all_notification_configurations
    )
    return bucket_notification_configurations


def get_notification_configuration(
    bucket_notification_configurations: BucketNotificationConfigurations,
    bucket_key_prefix: typing.Union[str, None] = None,
    bucket_key_suffix: typing.Union[str, None] = None,
) -> typing.Union[NotificationConfiguration, None]:
    """
    Filter the list of existing notifications based on the unique S3 key prefix and suffix.

    Arguments:
        bucket_notification_configuration (BucketNotificationConfigurations): The S3 bucket
            notification configurations
        bucket_key_prefix (str): Optional. The S3 key prefix included with the filter rules
            to match upon.
        bucket_key_suffix (str): Optional. The S3 key suffix. The S3 key suffix included with the filter rules
            to match upon.

    Returns:
        NotificationConfiguration or None if no match is found
    """
    for notification_configuration in bucket_notification_configurations.configs:
        filter_rules = notification_configuration.value["Filter"]["Key"]["FilterRules"]
        rule_names = {rule["Name"] for rule in filter_rules}
        common_prefix_path, common_suffix_path = (False, False)
        if "Prefix" not in rule_names and bucket_key_prefix is None:
            common_prefix_path = True
        if "Suffix" not in rule_names and bucket_key_suffix is None:
            common_suffix_path = True
        for filter_rule in filter_rules:
            if filter_rule["Name"] == "Prefix" and bucket_key_prefix is not None:
                common_prefix_path = bool(
                    os.path.commonpath([filter_rule["Value"], bucket_key_prefix])
                )
            elif filter_rule["Name"] == "Suffix" and bucket_key_suffix is not None:
                common_suffix_path = bool(
                    os.path.commonpath([filter_rule["Value"], bucket_key_suffix])
                )
            if common_prefix_path and common_suffix_path:
                return notification_configuration
    return None


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


def normalize_filter_rules(config: NotificationConfiguration):
    """
    Modify the filter rules of a notification configuration so that it is get/put agnostic.

    There is a bug in moto/AWS where moto only allows filter rules with lowercase
    `Name` values in put_bucket_notification_configuration calls. So to normalize
    our notification configurations when comparing configurations obtained via a
    GET with configurations to be utilized in a PUT (as we do in the function
    `notification_configuration_matches`), we make the `Name` values lower case in
    every filter rule.

    Args:
        config (NotificationConfiguration): A notification configuration to normalize

    Returns:
         NotificationConfiguration: The normalized notification configuration
    """
    filter_rules = config.value["Filter"]["Key"]["FilterRules"]
    new_filter_rules = []
    for rule in filter_rules:
        rule["Name"] = rule["Name"].lower()
        new_filter_rules.append(rule)
    config.value["Filter"]["Key"]["FilterRules"] = new_filter_rules
    return config


def notification_configuration_matches(
    config: NotificationConfiguration, other_config: NotificationConfiguration
) -> bool:
    """Determines if two S3 event notification configurations are functionally equivalent.

    Two notification configurations are considered equivalent if:
    1. They have the same filter rules
    2. They have the same destination (ARN)
    3. They are triggered by the same events

    Arguments:
        config (dict): The existing notification config
        other_config (dict): The new notification config

    Returns:
        bool: Whether the Events, ARN, and filter rules match.
    """
    config = normalize_filter_rules(config)
    other_config = normalize_filter_rules(other_config)
    arn_match = other_config.arn == config.arn
    events_match = set(other_config.value["Events"]) == set(config.value["Events"])
    filter_rule_names_match = {
        filter_rule["Name"]
        for filter_rule in other_config.value["Filter"]["Key"]["FilterRules"]
    } == {
        filter_rule["Name"]
        for filter_rule in config.value["Filter"]["Key"]["FilterRules"]
    }
    filter_rule_values_match = all(
        [
            any(
                [
                    filter_rule["Value"] == other_filter_rule["Value"]
                    for filter_rule in config.value["Filter"]["Key"]["FilterRules"]
                    if filter_rule["Name"] == other_filter_rule["Name"]
                ]
            )
            for other_filter_rule in other_config.value["Filter"]["Key"]["FilterRules"]
        ]
    )
    configurations_match = (
        arn_match
        and events_match
        and filter_rule_names_match
        and filter_rule_values_match
    )
    return configurations_match


def add_notification(
    s3_client: boto3.client,
    destination_type: str,
    destination_arn: str,
    bucket: str,
    bucket_key_prefix: str,
) -> None:
    """Adds the S3 notification configuration to an existing bucket.

    Notification configurations are identified by their unique prefix/suffix filter rules.
    If the notification configuration already exists, and is functionally equivalent,
    then no action is taken. If the notification configuration already exists, but is not
    functionally equivalent (see function `notification_configuration_matches`), then a
    RuntimeError is raised. In this case, the notification configuration must be deleted
    before being added.

    Args:
        s3_client (boto3.client) : s3 client to use for s3 event config
        destination_type (str): String name of the destination type for the configuration
        destination_arn (str): Arn of the destination's s3 event config
        bucket (str): bucket name of the s3 bucket to add the config to
        bucket_key_prefix (str): bucket key prefix for where to look for s3 object notifications
    """
    ### Create new notification configuration
    destination_type_arn = f"{destination_type}Arn"
    new_notification_configuration_value = {
        destination_type_arn: destination_arn,
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
            "Key": {
                "FilterRules": [
                    {"Name": "prefix", "Value": os.path.join(bucket_key_prefix, "")}
                ]
            }
        },
    }
    new_notification_configuration = NotificationConfiguration(
        notification_type=NotificationConfigurationType(destination_type),
        value=new_notification_configuration_value,
    )
    ### Get any matching notification configuration
    bucket_notification_configurations = get_bucket_notification_configurations(
        s3_client=s3_client, bucket=bucket
    )
    matching_notification_configuration = get_notification_configuration(
        bucket_notification_configurations=bucket_notification_configurations,
        bucket_key_prefix=bucket_key_prefix,
    )
    if matching_notification_configuration is not None:
        is_the_same_configuration = notification_configuration_matches(
            config=matching_notification_configuration,
            other_config=new_notification_configuration,
        )
        if is_the_same_configuration:
            logger.info(
                "Put not required as an equivalent NotificationConfiguration already exists at "
                + create_formatted_message(
                    bucket=bucket,
                    destination_type=destination_type,
                    destination_arn=matching_notification_configuration.arn,
                )
            )
            return
        raise RuntimeError(
            f"There already exists an event configuration on S3 bucket {bucket} for "
            f"the key prefix {bucket_key_prefix} which differs from the event "
            "configuration which we wish to add."
        )
    ### Store new notification configuration
    logger.info(
        "Put request started to add a NotificationConfiguration for "
        + create_formatted_message(bucket, destination_type, destination_arn)
    )
    bucket_notification_configurations.configs.append(new_notification_configuration)
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration=bucket_notification_configurations.to_dict(),
        SkipDestinationValidation=True,
    )
    logger.info(
        "Put request completed to add a NotificationConfiguration for "
        + create_formatted_message(bucket, destination_type, destination_arn)
    )


def delete_notification(
    s3_client: boto3.client, bucket: str, bucket_key_prefix: str
) -> None:
    """
    Deletes the S3 notification configuration from an existing bucket
    based on its unique S3 key prefix/suffix filter rules.

    Args:
        s3_client (boto3.client) : S3 client to use for S3 event config
        bucket (str): Name of the S3 bucket to delete the config in
        bucket_key_prefix (str): The S3 key prefix.

    Returns: None
    """
    bucket_notification_configurations = get_bucket_notification_configurations(
        s3_client=s3_client,
        bucket=bucket,
    )
    ### Get any matching notification configuration
    matching_notification_configuration = get_notification_configuration(
        bucket_notification_configurations=bucket_notification_configurations,
        bucket_key_prefix=bucket_key_prefix,
    )
    if matching_notification_configuration is None:
        logger.info(
            "Delete not required as no NotificationConfiguration "
            f"exists for S3 prefix {bucket_key_prefix}"
        )
        return
    bucket_notification_configurations.configs = [
        config
        for config in bucket_notification_configurations.configs
        if config.arn != matching_notification_configuration.arn
    ]
    ### Delete matching notification configuration
    logger.info(
        "Delete request started to remove a NotificationConfiguration for "
        + create_formatted_message(
            bucket=bucket,
            destination_type=matching_notification_configuration.type,
            destination_arn=matching_notification_configuration.arn,
        )
    )
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration=bucket_notification_configurations.to_dict(),
        SkipDestinationValidation=True,
    )
    logger.info(
        "Delete request completed to remove a NotificationConfiguration for "
        + create_formatted_message(
            bucket=bucket,
            destination_type=matching_notification_configuration.type,
            destination_arn=matching_notification_configuration.arn,
        )
    )
