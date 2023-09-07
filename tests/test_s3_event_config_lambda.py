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
def test_that_add_notification_adds_expected_settings_for_lambda(s3, mock_lambda_function):
    s3.create_bucket(Bucket="some_bucket")
    set_config = app.add_notification(
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
    set_config = app.add_notification(
        s3,
        "Queue",
        mock_sqs_queue['Attributes']['QueueArn'],
        "some_bucket",
        "test_folder",
    )
    get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")
    assert (
        get_config["QueueConfigurations"][0]["QueueArn"]
        == mock_sqs_queue['Attributes']['QueueArn']
    )
    assert get_config["QueueConfigurations"][0]["Events"] == [
        "s3:ObjectCreated:*"
    ]
    assert get_config["QueueConfigurations"][0]["Filter"] == {
        "Key": {"FilterRules": [{"Name": "prefix", "Value": "test_folder/"}]}
    }


@mock_s3
def test_that_delete_notification_is_successful_for_sqs(s3, mock_sqs_queue):
    s3.create_bucket(Bucket="some_bucket")
    app.add_notification(
        s3,
        "Queue",
        mock_sqs_queue['Attributes']['QueueArn'],
        "some_bucket",
        "test_folder",
    )
    app.delete_notification(s3, "some_bucket")
    get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")
    assert "QueueConfigurations" not in get_config
