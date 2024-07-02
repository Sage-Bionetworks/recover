import copy
from unittest import mock

import boto3
import pytest
from moto import mock_iam, mock_lambda, mock_s3, mock_sns, mock_sqs

from src.lambda_function.s3_event_config import app


@pytest.fixture(scope="function")
def mock_iam_role():
    with mock_iam():
        iam = boto3.client("iam")
        yield iam.create_role(
            RoleName="some-role",
            AssumeRolePolicyDocument="some policy",
            Path="/some-path/",
        )["Role"]["Arn"]


@pytest.fixture(scope="function")
def mock_lambda_function(mock_iam_role):
    with mock_lambda():
        client = boto3.client("lambda")
        client.create_function(
            FunctionName="some_function",
            Role=mock_iam_role,
            Code={"ZipFile": "print('DONE')"},
            Description="string",
        )
        yield client.get_function(FunctionName="some_function")


@pytest.fixture
def mock_sns_topic_arn():
    with mock_sns():
        client = boto3.client("sns")
        topic = client.create_topic(Name="some_topic")
        yield topic["TopicArn"]


@pytest.fixture(scope="function")
def mock_sqs_queue(mock_aws_credentials):
    with mock_sqs():
        client = boto3.client("sqs")
        client.create_queue(QueueName="test_sqs")
        queue_url = client.get_queue_url(QueueName="test_sqs")
        yield client.get_queue_attributes(
            QueueUrl=queue_url["QueueUrl"], AttributeNames=["QueueArn"]
        )


@pytest.fixture
def notification_configuration():
    return app.NotificationConfiguration(
        notification_type=app.NotificationConfigurationType("Topic"),
        value={
            "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
            "TopicArn": "arn:aws:sns:bla",
            "Filter": {
                "Key": {"FilterRules": [{"Name": "Prefix", "Value": "documents/"}]}
            },
        },
    )


@pytest.fixture
def bucket_notification_configurations(notification_configuration):
    ### Topic Configuration
    topic_configuration = copy.deepcopy(notification_configuration)
    ### SQS Configuration
    queue_configuration_value = copy.deepcopy(notification_configuration.value)
    del queue_configuration_value["TopicArn"]
    queue_configuration_value["QueueArn"] = "arn:aws:sqs:bla"
    queue_configuration_value["Filter"]["Key"]["FilterRules"][0] = {
        "Name": "Suffix",
        "Value": "jpeg",
    }
    queue_configuration = app.NotificationConfiguration(
        notification_type=app.NotificationConfigurationType("Queue"),
        value=queue_configuration_value,
    )
    ### Lambda Configuration
    lambda_configuration_value = copy.deepcopy(notification_configuration.value)
    del lambda_configuration_value["TopicArn"]
    lambda_configuration_value["LambdaFunctionArn"] = "arn:aws:lambda:bla"
    lambda_configuration_value["Filter"]["Key"]["FilterRules"][0] = {
        "Name": "Suffix",
        "Value": "jpeg",
    }
    lambda_configuration_value["Filter"]["Key"]["FilterRules"].append(
        {"Name": "Prefix", "Value": "pictures/"}
    )
    lambda_configuration = app.NotificationConfiguration(
        notification_type=app.NotificationConfigurationType("LambdaFunction"),
        value=lambda_configuration_value,
    )
    bucket_notification_configurations = app.BucketNotificationConfigurations(
        [topic_configuration, queue_configuration, lambda_configuration]
    )
    return bucket_notification_configurations


class TestBucketNotificationConfigurations:
    def test_init(self, notification_configuration):
        configs = [notification_configuration, notification_configuration]
        bucket_notification_configurations = app.BucketNotificationConfigurations(
            configs
        )
        assert bucket_notification_configurations.configs == configs

    def test_to_dict(self, notification_configuration):
        other_notification_configuration = copy.deepcopy(notification_configuration)
        other_notification_configuration.type = "LambdaFunction"
        configs = [notification_configuration, other_notification_configuration]
        bucket_notification_configurations = app.BucketNotificationConfigurations(
            configs
        )
        bnc_as_dict = bucket_notification_configurations.to_dict()
        assert "TopicConfigurations" in bnc_as_dict
        assert "LambdaFunctionConfigurations" in bnc_as_dict


class TestGetBucketNotificationConfigurations:
    @mock_s3
    def test_get_configurations(self, s3, notification_configuration):
        s3.create_bucket(Bucket="some_bucket")
        topic_configuration = copy.deepcopy(notification_configuration.value)
        queue_configuration = copy.deepcopy(notification_configuration.value)
        del queue_configuration["TopicArn"]
        queue_configuration["QueueArn"] = "arn:aws:sqs:bla"
        lambda_configuration = copy.deepcopy(notification_configuration.value)
        del lambda_configuration["TopicArn"]
        lambda_configuration["LambdaFunctionArn"] = "arn:aws:lambda:bla"
        event_bridge_configuration = copy.deepcopy(notification_configuration.value)
        del event_bridge_configuration["TopicArn"]
        event_bridge_configuration["EventBridgeArn"] = "arn:aws:eventbridge:bla"
        with mock.patch.object(
            s3,
            "get_bucket_notification_configuration",
            return_value={
                "QueueConfigurations": [queue_configuration],
                "TopicConfigurations": [topic_configuration],
                "LambdaFunctionConfigurations": [lambda_configuration],
                "EventBridgeConfiguration": event_bridge_configuration,
            },
        ):
            bucket_notification_configurations = (
                app.get_bucket_notification_configurations(
                    s3_client=s3, bucket="some_bucket"
                )
            )
            # We should ignore 'EventBridgeConfiguration'
            assert len(bucket_notification_configurations.configs) == 3


class TestGetNotificationConfiguration:
    def test_no_prefix_matching_suffix(self, bucket_notification_configurations):
        # No prefix provided, suffix provided
        matching_notification_configuration = app.get_notification_configuration(
            bucket_notification_configurations, bucket_key_suffix="jpeg"
        )
        assert matching_notification_configuration is not None
        assert matching_notification_configuration.type == "Queue"

    def test_no_suffix_matching_prefix(self, bucket_notification_configurations):
        matching_notification_configuration = app.get_notification_configuration(
            bucket_notification_configurations, bucket_key_prefix="documents"
        )
        assert matching_notification_configuration is not None
        assert matching_notification_configuration.type == "Topic"

    def test_matching_prefix_not_matching_suffix(
        self, bucket_notification_configurations
    ):
        matching_notification_configuration = app.get_notification_configuration(
            bucket_notification_configurations=bucket_notification_configurations,
            bucket_key_prefix="pictures",
            bucket_key_suffix="png",
        )
        assert matching_notification_configuration is None

    def test_matching_suffix_not_matching_prefix(
        self, bucket_notification_configurations
    ):
        matching_notification_configuration = app.get_notification_configuration(
            bucket_notification_configurations=bucket_notification_configurations,
            bucket_key_prefix="documents",
            bucket_key_suffix="jpeg",
        )
        assert matching_notification_configuration is None

    def test_no_match(self, bucket_notification_configurations):
        matching_notification_configuration = app.get_notification_configuration(
            bucket_notification_configurations=bucket_notification_configurations,
            bucket_key_prefix="downloads",
        )
        assert matching_notification_configuration is None


class TestNormalizeFilterRules:
    def test_normalize_filter_rules(self, notification_configuration):
        normalized_notification_configuration = app.normalize_filter_rules(
            config=notification_configuration
        )
        assert all(
            [
                rule["Name"].lower() == rule["Name"]
                for rule in notification_configuration.value["Filter"]["Key"][
                    "FilterRules"
                ]
            ]
        )


class TestNotificationConfigurationMatches:
    def test_all_true(self, notification_configuration):
        assert app.notification_configuration_matches(
            config=notification_configuration, other_config=notification_configuration
        )

    def test_arn_false(self, notification_configuration):
        other_notification_configuration = copy.deepcopy(notification_configuration)
        other_notification_configuration.arn = "arn:aws:sns:hubba"
        assert not app.notification_configuration_matches(
            config=notification_configuration,
            other_config=other_notification_configuration,
        )

    def test_events_false(self, notification_configuration):
        other_notification_configuration = copy.deepcopy(notification_configuration)
        other_notification_configuration.value["Events"] = ["s3:ObjectCreated*"]
        assert not app.notification_configuration_matches(
            config=notification_configuration,
            other_config=other_notification_configuration,
        )

    def test_filter_rule_names_false(self, notification_configuration):
        other_notification_configuration = copy.deepcopy(notification_configuration)
        other_notification_configuration.value["Filter"]["Key"]["FilterRules"] = [
            {"Name": "Prefix", "Value": "documents/"},
            {"Name": "Suffix", "Value": "jpeg"},
        ]
        assert not app.notification_configuration_matches(
            config=notification_configuration,
            other_config=other_notification_configuration,
        )

    def test_filter_rule_values_false(self, notification_configuration):
        other_notification_configuration = copy.deepcopy(notification_configuration)
        other_notification_configuration.value["Filter"]["Key"]["FilterRules"] = [
            {"Name": "Prefix", "Value": "pictures/"}
        ]
        assert not app.notification_configuration_matches(
            config=notification_configuration,
            other_config=other_notification_configuration,
        )


class TestAddNotification:
    @mock_s3
    def test_adds_expected_settings_for_lambda(self, s3, mock_lambda_function):
        s3.create_bucket(Bucket="some_bucket")
        with mock.patch.object(
            s3,
            "get_bucket_notification_configuration",
            return_value={},
        ):
            app.add_notification(
                s3,
                "LambdaFunction",
                mock_lambda_function["Configuration"]["FunctionArn"],
                "some_bucket",
                "test_folder",
            )
        get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")
        assert (
            get_config["LambdaFunctionConfigurations"][0]["LambdaFunctionArn"]
            == mock_lambda_function["Configuration"]["FunctionArn"]
        )
        assert get_config["LambdaFunctionConfigurations"][0]["Events"] == [
            "s3:ObjectCreated:*"
        ]
        # moto prefix/Prefix discrepancy
        assert (
            len(
                get_config["LambdaFunctionConfigurations"][0]["Filter"]["Key"][
                    "FilterRules"
                ]
            )
            == 1
        )

    @mock_s3
    def test_adds_expected_settings_for_sns(self, s3, mock_sns_topic_arn):
        s3.create_bucket(Bucket="some_bucket")
        with mock.patch.object(
            s3,
            "get_bucket_notification_configuration",
            return_value={},
        ):
            app.add_notification(
                s3,
                "Topic",
                mock_sns_topic_arn,
                "some_bucket",
                "test_folder",
            )
        get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")
        assert get_config["TopicConfigurations"][0]["TopicArn"] == mock_sns_topic_arn
        assert get_config["TopicConfigurations"][0]["Events"] == ["s3:ObjectCreated:*"]
        # moto prefix/Prefix discrepancy
        assert (
            len(get_config["TopicConfigurations"][0]["Filter"]["Key"]["FilterRules"])
            == 1
        )

    @mock_s3
    def test_adds_expected_settings_for_sqs(self, s3, mock_sqs_queue):
        s3.create_bucket(Bucket="some_bucket")
        with mock.patch.object(
            s3,
            "get_bucket_notification_configuration",
            return_value={},
        ):
            app.add_notification(
                s3,
                "Queue",
                mock_sqs_queue["Attributes"]["QueueArn"],
                "some_bucket",
                "test_folder",
            )
        get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")
        assert (
            get_config["QueueConfigurations"][0]["QueueArn"]
            == mock_sqs_queue["Attributes"]["QueueArn"]
        )
        assert get_config["QueueConfigurations"][0]["Events"] == ["s3:ObjectCreated:*"]
        # moto prefix/Prefix discrepancy
        assert (
            len(get_config["QueueConfigurations"][0]["Filter"]["Key"]["FilterRules"])
            == 1
        )

    @mock_s3
    def test_raise_exception_if_config_exists_for_prefix(
        self, s3, notification_configuration
    ):
        # GIVEN an S3 bucket
        s3.create_bucket(Bucket="some_bucket")

        # AND the bucket has an existing notification configuration on the same S3 key prefix
        with mock.patch.object(
            s3,
            "get_bucket_notification_configuration",
            return_value={f"TopicConfigurations": [notification_configuration.value]},
        ):
            with pytest.raises(RuntimeError):
                app.add_notification(
                    s3,
                    "Queue",
                    "arn:aws:sqs:bla",
                    "some_bucket",
                    notification_configuration.value["Filter"]["Key"]["FilterRules"][0][
                        "Value"
                    ],
                )

    @mock_s3
    def test_does_nothing_if_notification_already_exists(self, s3):
        # GIVEN an S3 bucket
        s3.create_bucket(Bucket="some_bucket")

        # AND the bucket has an existing notification configuration that matches the one we will add
        notification_configuration = {
            "TopicArn": "arn:aws:sns:bla",
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {
                "Key": {"FilterRules": [{"Name": "Prefix", "Value": f"documents/"}]}
            },
        }
        with mock.patch.object(
            s3,
            "get_bucket_notification_configuration",
            return_value={f"TopicConfigurations": [notification_configuration]},
        ), mock.patch.object(s3, "put_bucket_notification_configuration") as put_config:
            # WHEN I add the existing matching `LambdaFunction` configuration
            app.add_notification(
                s3_client=s3,
                destination_type="Topic",
                destination_arn=notification_configuration["TopicArn"],
                bucket="some_bucket",
                bucket_key_prefix=notification_configuration["Filter"]["Key"][
                    "FilterRules"
                ][0]["Value"],
            )

        # AND I get the notification configuration
        get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")

        # THEN I expect nothing to have been saved in our mocked environment
        assert not put_config.called

    @mock_s3
    def test_does_nothing_if_notification_already_exists_even_in_different_dict_order(
        self, s3, mock_lambda_function
    ):
        # GIVEN an S3 bucket
        s3.create_bucket(Bucket="some_bucket")

        # AND the bucket has an existing `LambdaFunctionConfigurations` that matches
        # content of the one we will add - But differs in order
        with mock.patch.object(
            s3,
            "get_bucket_notification_configuration",
            return_value={
                f"LambdaFunctionConfigurations": [
                    {
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {"Name": "Prefix", "Value": f"test_folder/"}
                                ]
                            }
                        },
                        "Events": ["s3:ObjectCreated:*"],
                        f"LambdaFunctionArn": mock_lambda_function["Configuration"][
                            "FunctionArn"
                        ],
                    }
                ]
            },
        ), mock.patch.object(s3, "put_bucket_notification_configuration") as put_config:
            # WHEN I add the existing matching `LambdaFunction` configuration
            app.add_notification(
                s3,
                "LambdaFunction",
                mock_lambda_function["Configuration"]["FunctionArn"],
                "some_bucket",
                "test_folder",
            )

        # AND I get the notification configuration
        get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")

        # THEN I expect nothing to have been saved in our mocked environment
        assert not put_config.called

    @mock_s3
    def test_adds_config_if_requested_notification_does_not_exist(
        self, s3, mock_lambda_function, mock_sqs_queue
    ):
        # GIVEN an S3 bucket
        s3.create_bucket(Bucket="some_bucket")

        # AND the bucket has an existing `QueueConfigurations`
        with mock.patch.object(
            s3,
            "get_bucket_notification_configuration",
            return_value={
                f"QueueConfigurations": [
                    {
                        "Id": "123",
                        "QueueArn": mock_sqs_queue["Attributes"]["QueueArn"],
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {"Name": "prefix", "Value": f"test_folder/"}
                                ]
                            }
                        },
                    }
                ]
            },
        ):
            # WHEN I add a new `LambdaFunction` configuration
            app.add_notification(
                s3,
                "LambdaFunction",
                mock_lambda_function["Configuration"]["FunctionArn"],
                "some_bucket",
                "another_test_folder",
            )

        # AND I get the notification configuration
        get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")

        # THEN I expect to see a new `LambdaFunction` configuration
        assert (
            get_config["LambdaFunctionConfigurations"][0]["LambdaFunctionArn"]
            == mock_lambda_function["Configuration"]["FunctionArn"]
        )
        assert get_config["LambdaFunctionConfigurations"][0]["Events"] == [
            "s3:ObjectCreated:*"
        ]
        # moto prefix/Prefix discrepancy
        assert (
            len(
                get_config["LambdaFunctionConfigurations"][0]["Filter"]["Key"][
                    "FilterRules"
                ]
            )
            == 1
        )


class TestDeleteNotification:
    @mock_s3
    def test_is_successful_for_configuration_that_exists(
        self, s3, mock_lambda_function
    ):
        # GIVEN an S3 bucket
        s3.create_bucket(Bucket="some_bucket")

        # AND a configuration exists
        with mock.patch.object(
            s3,
            "get_bucket_notification_configuration",
            return_value={
                f"LambdaFunctionConfigurations": [
                    {
                        f"LambdaFunctionArn": mock_lambda_function["Configuration"][
                            "FunctionArn"
                        ],
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {"Name": "Prefix", "Value": f"test_folder/"}
                                ]
                            }
                        },
                    }
                ]
            },
        ):
            # WHEN I delete the notification
            app.delete_notification(
                s3_client=s3, bucket="some_bucket", bucket_key_prefix="test_folder"
            )
        # THEN the notification should be deleted
        get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")
        assert "LambdaFunctionConfigurations" not in get_config

    @mock_s3
    def test_does_nothing_when_deleting_configuration_that_does_not_exist(
        self, s3, mock_lambda_function
    ):
        # GIVEN an S3 bucket
        s3.create_bucket(Bucket="some_bucket")

        # AND a configuration exists for a different notification type
        with mock.patch.object(
            s3,
            "get_bucket_notification_configuration",
            return_value={
                f"LambdaFunctionConfigurations": [
                    {
                        f"LambdaFunctionArn": mock_lambda_function["Configuration"][
                            "FunctionArn"
                        ],
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {"Name": "Prefix", "Value": f"test_folder/"}
                                ]
                            }
                        },
                    }
                ]
            },
            # AND a mock for the put_bucket_notification_configuration method
        ), mock.patch.object(s3, "put_bucket_notification_configuration") as put_config:
            # WHEN I delete a notification that does not exist
            app.delete_notification(
                s3_client=s3,
                bucket="some_bucket",
                bucket_key_prefix="another_test_folder",
            )
        # THEN nothing should have been called
        assert not put_config.called
