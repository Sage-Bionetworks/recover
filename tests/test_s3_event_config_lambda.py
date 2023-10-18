from unittest import mock
import zipfile
import io
import boto3
from moto import mock_s3, mock_lambda, mock_iam, mock_sqs
import pytest

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


@pytest.fixture(scope="function")
def mock_sqs_queue(mock_aws_credentials):
    with mock_sqs():
        client = boto3.client("sqs")
        client.create_queue(QueueName="test_sqs")
        queue_url = client.get_queue_url(QueueName="test_sqs")
        yield client.get_queue_attributes(
            QueueUrl=queue_url["QueueUrl"], AttributeNames=["QueueArn"]
        )


@mock_s3
def test_that_add_notification_adds_expected_settings_for_lambda(
    s3, mock_lambda_function
):
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
    assert get_config["LambdaFunctionConfigurations"][0]["Filter"] == {
        "Key": {"FilterRules": [{"Name": "prefix", "Value": "test_folder/"}]}
    }


@mock_s3
def test_that_delete_notification_is_successful_for_lambda(s3, mock_lambda_function):
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
    app.delete_notification(s3, "some_bucket")
    get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")
    assert "LambdaFunctionConfigurations" not in get_config


@mock_s3
def test_that_add_notification_adds_expected_settings_for_sqs(s3, mock_sqs_queue):
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
    assert get_config["QueueConfigurations"][0]["Filter"] == {
        "Key": {"FilterRules": [{"Name": "prefix", "Value": "test_folder/"}]}
    }


@mock_s3
def test_that_delete_notification_is_successful_for_sqs(s3, mock_sqs_queue):
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
    app.delete_notification(s3, "some_bucket")
    get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")
    assert "QueueConfigurations" not in get_config


@mock_s3
def test_add_notification_does_nothing_if_notification_already_exists(
    s3, mock_lambda_function
):
    # GIVEN an S3 bucket
    s3.create_bucket(Bucket="some_bucket")

    # AND the bucket has an existing `LambdaFunctionConfigurations` that matches the one we will add
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
                                {"Name": "prefix", "Value": f"test_folder/"}
                            ]
                        }
                    },
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
def test_add_notification_does_nothing_if_notification_already_exists_even_in_different_dict_order(
    s3, mock_lambda_function
):
    # GIVEN an S3 bucket
    s3.create_bucket(Bucket="some_bucket")

    # AND the bucket has an existing `LambdaFunctionConfigurations` that matches content of the one we will add - But differs in order
    with mock.patch.object(
        s3,
        "get_bucket_notification_configuration",
        return_value={
            f"LambdaFunctionConfigurations": [
                {
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {"Name": "prefix", "Value": f"test_folder/"}
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
def test_add_notification_adds_config_if_requested_notification_does_not_exist(
    s3, mock_lambda_function, mock_sqs_queue
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
            "test_folder",
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
    assert get_config["LambdaFunctionConfigurations"][0]["Filter"] == {
        "Key": {"FilterRules": [{"Name": "prefix", "Value": "test_folder/"}]}
    }

    # AND I expect the `QueueConfigurations` to be unchanged
    assert (
        get_config["QueueConfigurations"][0]["QueueArn"]
        == mock_sqs_queue["Attributes"]["QueueArn"]
    )
    assert get_config["QueueConfigurations"][0]["Events"] == ["s3:ObjectCreated:*"]
    assert get_config["QueueConfigurations"][0]["Filter"] == {
        "Key": {"FilterRules": [{"Name": "prefix", "Value": "test_folder/"}]}
    }


@mock_s3
def test_add_notification_adds_config_if_existing_config_does_not_match(
    s3, mock_lambda_function
):
    # GIVEN an S3 bucket
    s3.create_bucket(Bucket="some_bucket")

    # AND the bucket has an existing `LambdaFunctionConfigurations` that does not match the one we are adding
    with mock.patch.object(
        s3,
        "get_bucket_notification_configuration",
        return_value={
            f"LambdaFunctionConfigurations": [
                {
                    f"SomeOtherArn": mock_lambda_function["Configuration"][
                        "FunctionArn"
                    ],
                    "Events": ["s3:SomeOtherS3Event:*"],
                }
            ]
        },
    ):
        # WHEN I add the `LambdaFunction` configuration
        app.add_notification(
            s3,
            "LambdaFunction",
            mock_lambda_function["Configuration"]["FunctionArn"],
            "some_bucket",
            "test_folder",
        )

    # AND I get the notification configuration
    get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")

    # THEN I expect to see the updated `LambdaFunction` configuration
    assert (
        get_config["LambdaFunctionConfigurations"][0]["LambdaFunctionArn"]
        == mock_lambda_function["Configuration"]["FunctionArn"]
    )
    assert get_config["LambdaFunctionConfigurations"][0]["Events"] == [
        "s3:ObjectCreated:*"
    ]
    assert get_config["LambdaFunctionConfigurations"][0]["Filter"] == {
        "Key": {"FilterRules": [{"Name": "prefix", "Value": "test_folder/"}]}
    }
