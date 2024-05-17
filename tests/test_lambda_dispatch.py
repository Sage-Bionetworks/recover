import json
import os
from pathlib import Path
import shutil
import tempfile
import zipfile

import boto3
import pytest
from moto import mock_sns, mock_s3
from src.lambda_function.dispatch import app
from unittest import mock

@pytest.fixture
def s3_event():
    s3_event = {
        "eventVersion": "2.0",
        "eventSource": "aws:s3",
        "awsRegion": "us-east-1",
        "eventTime": "1970-01-01T00:00:00.000Z",
        "eventName": "ObjectCreated:Put",
        "userIdentity": {"principalId": "EXAMPLE"},
        "requestParameters": {"sourceIPAddress": "127.0.0.1"},
        "responseElements": {
            "x-amz-request-id": "EXAMPLE123456789",
            "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
        },
        "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": "testConfigRule",
            "bucket": {
                "name": "recover-dev-input-data",
                "ownerIdentity": {"principalId": "EXAMPLE"},
                "arn": "arn:aws:s3:::bucket_arn",
            },
            "object": {
                "key": "main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
                "size": 1024,
                "eTag": "0123456789abcdef0123456789abcdef",
                "sequencer": "0A1B2C3D4E5F678901",
            },
        },
    }
    return s3_event

@pytest.fixture
def sns_message_template():
    sns_message_template = {
            "Type": "string",
            "MessageId": "string",
            "TopicArn": "string",
            "Subject": "string",
            "Message": "string",
            "Timestamp": "string"
    }
    return sns_message_template

@pytest.fixture
def sqs_message_template():
    sqs_msg = {
        "Records": [
            {
                "MessageId": "string",
                "receiptHandle": "string",
                "MD5OfBody": "string",
                "body": "string",
                "Attributes": {
                    "string": "string",
                },
                "MD5OfMessageAttributes": "string",
                "MessageAttributes": {
                    "string": {
                        "DataType": "string",
                        "StringValue": "string",
                        "BinaryValue": "string",
                    }
                },
            }
        ]
    }
    yield sqs_msg

@pytest.fixture
def event(s3_event, sns_message_template, sqs_message_template):
    sns_message_template["Message"] = json.dumps({"Records": [s3_event]})
    sqs_message_template["Records"][0]["body"] = json.dumps(sns_message_template)
    return sqs_message_template

@pytest.fixture
def archive_json_paths():
    archive_json_paths = [
            "HealthKitV2Workouts_20240508-20240509.json", # normal file
            "empty.json", # should have file size 0 and be ignored
            "Manifest.json", # should be ignored
            "dir/containing/stuff.json" # should be ignored
    ]
    return archive_json_paths

@pytest.fixture
def temp_zip_file():
    temp_zip_file = tempfile.NamedTemporaryFile(
            delete=False,
            suffix='.zip'
    )
    return temp_zip_file

@pytest.fixture
def archive_path(archive_json_paths, temp_zip_file):
    with zipfile.ZipFile(temp_zip_file.name, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for file_path in archive_json_paths:
            if "/" in file_path:
                os.makedirs(os.path.dirname(file_path))
            if file_path != "empty.json":
                with open(file_path, "w") as this_file:
                    this_file.write("test")
            else:
                Path(file_path).touch()
            zip_file.write(file_path)
            if "/" in file_path:
                shutil.rmtree(file_path.split("/")[0])
            else:
                os.remove(file_path)
    yield temp_zip_file.name
    os.remove(temp_zip_file.name)

def test_get_object_info(s3_event):
    object_info = app.get_object_info(s3_event=s3_event)
    assert object_info["Bucket"] == s3_event["s3"]["bucket"]["name"]
    assert object_info["Key"] == s3_event["s3"]["object"]["key"]

def test_get_object_info_unicode_characters_in_key(s3_event):
    s3_event["s3"]["object"]["key"] = \
            "main/2023-09-26T00%3A06%3A39Z_d873eafb-554f-4f8a-9e61-cdbcb7de07eb"
    object_info = app.get_object_info(s3_event=s3_event)
    assert object_info["Key"] == \
            "main/2023-09-26T00:06:39Z_d873eafb-554f-4f8a-9e61-cdbcb7de07eb"

@pytest.mark.parametrize(
    "object_info,expected",
    [
        (
            {
                "Bucket": "recover-dev-input-data",
                "Key": "main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
            },
            {
                "Bucket": "recover-dev-input-data",
                "Key": "main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
            },
        ),
        (
            {
                "Bucket": "recover-dev-input-data",
                "Key": "main/v1/owner.txt",
            },
            None,
        ),
        (
            {
                "Bucket": "recover-dev-input-data",
                "Key": "main/adults_v2/",
            },
            None,
        ),
        (
            {
                "Bucket": "recover-dev-input-data",
                "Key": None,
            },
            None,
        ),
        (
            {
                "Bucket": None,
                "Key": "main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
            },
            None,
        ),
    ],
    ids=[
        "correct_msg_format",
        "owner_txt",
        "directory",
        "missing_key",
        "missing_bucket",
    ],
)
def test_that_filter_object_info_returns_expected_result(object_info, expected):
        assert app.filter_object_info(object_info) == expected

def test_get_archive_contents(archive_path, archive_json_paths):
    dummy_bucket = "dummy_bucket"
    dummy_key = "dummy_key"
    archive_contents = app.get_archive_contents(
            archive_path=archive_path,
            bucket=dummy_bucket,
            key=dummy_key
    )
    assert all([content["Bucket"] == dummy_bucket for content in archive_contents])
    assert all([content["Key"] == dummy_key for content in archive_contents])
    assert all([content["FileSize"] > 0 for content in archive_contents])
    assert set([content["Path"] for content in archive_contents]) == \
            set(["HealthKitV2Workouts_20240508-20240509.json"])

@mock_sns
@mock_s3
def test_main(event, temp_zip_file, s3_event, archive_path):
    sns_client = boto3.client("sns")
    s3_client = boto3.client("s3")
    bucket = s3_event["s3"]["bucket"]["name"]
    key = s3_event["s3"]["object"]["key"]
    s3_client.create_bucket(Bucket=bucket)
    s3_client.upload_file(
            Filename=archive_path,
            Bucket=bucket,
            Key=key
    )
    dispatch_sns = sns_client.create_topic(Name="test-sns-topic")
    with mock.patch.object(sns_client, "publish", wraps=sns_client.publish) as mock_publish:
        app.main(
                event=event,
                context=dict(),
                sns_client=sns_client,
                s3_client=s3_client,
                dispatch_sns_arn=dispatch_sns["TopicArn"],
                temp_zip_path=temp_zip_file.name
        )
        mock_publish.assert_called()
