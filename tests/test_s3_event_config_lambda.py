import zipfile
import io
import boto3
from moto import mock_s3, mock_lambda, mock_iam, mock_logs
import pytest

from src.lambda_function.s3_event_config import app


@pytest.fixture(scope="function")
def mock_iam_role(mock_aws_credentials):
    with mock_iam():
        iam = boto3.client("iam")
        yield iam.create_role(
            RoleName="some-role",
            AssumeRolePolicyDocument="some policy",
            Path="/some-path/",
        )["Role"]["Arn"]


@pytest.fixture(scope="function")
def mock_lambda_function(mock_aws_credentials, mock_iam_role):
    with mock_lambda():
        client = boto3.client("lambda")
        client.create_function(
            FunctionName="some_function",
            Role=mock_iam_role,
            Code={"ZipFile": "print('DONE')"},
            Description="string",
        )
        yield client.get_function(FunctionName="some_function")


@mock_s3
def test_that_add_notification_adds_expected_settings(s3, mock_lambda_function):
    s3.create_bucket(Bucket="some_bucket")
    set_config = app.add_notification(
        s3,
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
        "Key": {"FilterRules": [{"Name": "prefix", "Value": "test_folder"}]}
    }


@mock_s3
def test_that_delete_notification_is_successful(s3, mock_lambda_function):
    s3.create_bucket(Bucket="some_bucket")
    app.add_notification(
        s3,
        mock_lambda_function["Configuration"]["FunctionArn"],
        "some_bucket",
        "test_folder",
    )
    app.delete_notification(s3, "some_bucket")
    get_config = s3.get_bucket_notification_configuration(Bucket="some_bucket")
    assert "LambdaFunctionConfigurations" not in get_config
