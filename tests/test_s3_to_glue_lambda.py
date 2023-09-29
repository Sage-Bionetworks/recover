import pytest
from unittest import mock

import json
import boto3
from moto import mock_sqs

from src.lambda_function.s3_to_glue import app


class MockGlueClient:
    def start_workflow_run(*args, **kwargs):
        return {"RunId": "example"}

    def put_workflow_run_properties(*args, **kwargs):
        return {}


class TestS3ToGlueLambda:
    @pytest.fixture
    def sqs_queue_name(self):
        yield "test_sqs"

    @pytest.fixture
    def sqs_queue(self, sqs_queue_name):
        with mock_sqs():
            client = boto3.client("sqs")
            response = client.create_queue(QueueName=sqs_queue_name)
            yield response

    @pytest.fixture
    def no_s3_event_records(self):
        sqs_msg = {
            "Records": [
                {
                    "MessageId": "string",
                    "receiptHandle": "string",
                    "MD5OfBody": "string",
                    "Body": "string",
                    "Attributes": {
                        "string": "string",
                    },
                    "MD5OfMessageAttributes": "string",
                    "MessageAttributes": {
                        "string": {
                            "DataType": "string",
                            "StringValue": "string",
                            "BinaryValue": "string",
                        },
                    },
                }
            ]
        }
        sqs_msg["Records"][0]["body"] = json.dumps({"Records": []})
        yield sqs_msg

    @pytest.fixture
    def s3_test_event(self):
        s3_event = {
            "Service": "Amazon S3",
            "Event": "s3:TestEvent",
            "Time": "2020-11-30T00:00:00.000Z",
            "Bucket": "the_bucket_name",
            "RequestId": "some_id",
            "HostId": "another_id",
        }
        yield s3_event

    @pytest.fixture
    def s3_event(self):
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
    def sqs_message(self, s3_event, s3_test_event):
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
                },
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
            ],
        }
        sqs_msg["Records"][0]["body"] = json.dumps(
            {"Records": [s3_event]}
        )
        sqs_msg["Records"][1]["body"] = json.dumps(s3_test_event)
        yield sqs_msg

    @pytest.fixture
    def object_info(self):
        object_info = [
            {
                "source_bucket": "recover-dev-input-data",
                "source_key": "main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
            }
        ]
        return object_info

    @pytest.fixture
    def set_env_var(self, monkeypatch, sqs_queue):
        monkeypatch.setenv("PRIMARY_WORKFLOW_NAME", "test_workflow")

    def test_submit_s3_to_json_workflow(self, object_info, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x: MockGlueClient())
        app.submit_s3_to_json_workflow(
            objects_info=object_info, workflow_name="example-workflow"
        )

    def test_that_lambda_handler_does_not_call_submit_s3_to_json_workflow_if_no_s3_records(
        self, no_s3_event_records, sqs_queue, set_env_var
    ):
        with mock_sqs():
            with mock.patch.object(boto3, "client") as patch_client, mock.patch.object(
                app, "submit_s3_to_json_workflow"
            ) as patch_submit:
                patch_client.return_value.get_queue_url.return_value = sqs_queue[
                    "QueueUrl"
                ]
                patch_client.return_value.receive_message.return_value = (
                    no_s3_event_records
                )
                app.lambda_handler(event=no_s3_event_records, context=None)
                patch_submit.assert_not_called()

    def test_that_lambda_handler_calls_submit_s3_to_json_workflow_if_queue_has_message(
        self, sqs_message, object_info, sqs_queue, set_env_var
    ):
        with mock_sqs():
            with mock.patch.object(boto3, "client") as patch_client, mock.patch.object(
                app, "submit_s3_to_json_workflow"
            ) as patch_submit:
                patch_client.return_value.get_queue_url.return_value = (
                    sqs_queue["QueueUrl"],
                )
                patch_client.return_value.receive_message.return_value = sqs_message
                app.lambda_handler(event=sqs_message, context=None)
                patch_submit.assert_called_once_with(
                    objects_info=object_info,
                    workflow_name="test_workflow",
                )

    def test_get_object_info_unicode_characters_in_key(self, s3_event):
        s3_event["s3"]["object"]["key"] = \
                "main/2023-09-26T00%3A06%3A39Z_d873eafb-554f-4f8a-9e61-cdbcb7de07eb"
        object_info = app.get_object_info(s3_event=s3_event)
        assert object_info["source_key"] == \
                "main/2023-09-26T00:06:39Z_d873eafb-554f-4f8a-9e61-cdbcb7de07eb"

    @pytest.mark.parametrize(
        "object_info,expected",
        [
            (
                {
                    "source_bucket": "recover-dev-input-data",
                    "source_key": "main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
                },
                {
                    "source_bucket": "recover-dev-input-data",
                    "source_key": "main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
                },
            ),
            (
                {
                    "source_bucket": "recover-dev-input-data",
                    "source_key": "main/v1/owner.txt",
                },
                None,
            ),
            (
                {
                    "source_bucket": "recover-dev-input-data",
                    "source_key": "main/adults_v2/",
                },
                None,
            ),
            (
                {
                    "source_bucket": "recover-dev-input-data",
                    "source_key": None,
                },
                None,
            ),
            (
                {
                    "source_bucket": None,
                    "source_key": "main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
                },
                None,
            ),
        ],
        ids=[
            "correct_msg_format",
            "owner_txt",
            "directory",
            "missing_source_key",
            "missing_source_bucket",
        ],
    )
    def test_that_filter_object_info_returns_expected_result(
        self, object_info, expected
    ):
        assert app.filter_object_info(object_info) == expected


    def test_that_is_s3_test_event_returns_true_when_s3_test_event_is_present(
        self, s3_test_event
    ):
        assert app.is_s3_test_event(s3_test_event) == True

    def test_that_is_s3_test_event_returns_false_when_s3_test_event_is_not_present(
        self, s3_event
    ):
        assert app.is_s3_test_event(s3_event) == False
