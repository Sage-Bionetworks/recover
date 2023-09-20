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
    def test_sqs_queue_name(self):
        yield "test_sqs"

    @pytest.fixture
    def test_sqs_queue_url(self, test_sqs_queue_name):
        client = boto3.client("sqs")
        client.create_queue(QueueName=test_sqs_queue_name)
        queue_url = client.get_queue_url(QueueName=test_sqs_queue_name)
        yield queue_url["QueueUrl"]

    @pytest.fixture
    def empty_queue(self):
        yield {}

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
    def test_s3_event(self):
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
    def test_sqs_message(self, test_s3_event):
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
        sqs_msg["Records"][0]["body"] = json.dumps({"Records": [test_s3_event]})
        yield sqs_msg

    @pytest.fixture
    def test_object_info(self):
        object_info = [
            {
                "source_bucket": "recover-dev-input-data",
                "source_key": "main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
            }
        ]
        return object_info

    @pytest.fixture
    def test_set_env_var(self, monkeypatch, test_sqs_queue_url):
        monkeypatch.setenv("SQS_QUEUE_URL", test_sqs_queue_url)
        monkeypatch.setenv("PRIMARY_WORKFLOW_NAME", "test_workflow")

    def test_submit_s3_to_json_workflow(self, test_object_info, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x: MockGlueClient())
        app.submit_s3_to_json_workflow(
            objects_info=test_object_info, workflow_name="example-workflow"
        )

    @mock_sqs
    def test_that_lambda_handler_does_not_call_submit_s3_to_json_workflow_if_empty_sqs_event(
        self, empty_queue, test_sqs_queue_url, test_set_env_var
    ):
        with mock_sqs():
            with mock.patch.object(boto3, "client") as patch_client, mock.patch.object(
                app, "submit_s3_to_json_workflow"
            ) as patch_submit:
                patch_client.return_value.get_queue_url.return_value = (
                    test_sqs_queue_url
                )
                patch_client.return_value.receive_message.return_value = empty_queue
                result = app.lambda_handler(event=empty_queue, context=None)
                assert result == {"statusCode": 0, "body": "No messages exist"}
                patch_submit.assert_not_called()
                patch_client.return_value.assert_not_called()

    @mock_sqs
    def test_that_lambda_handler_does_not_call_submit_s3_to_json_workflow_if_no_s3_records(
        self, no_s3_event_records, test_sqs_queue_url, test_set_env_var
    ):
        with mock_sqs():
            with mock.patch.object(boto3, "client") as patch_client, mock.patch.object(
                app, "submit_s3_to_json_workflow"
            ) as patch_submit:
                patch_client.return_value.get_queue_url.return_value = (
                    test_sqs_queue_url
                )
                patch_client.return_value.receive_message.return_value = (
                    no_s3_event_records
                )
                result = app.lambda_handler(event=no_s3_event_records, context=None)
                assert result == {
                    "statusCode": 200,
                    "body": "All messages retrieved and processed.",
                }
                patch_submit.assert_not_called()
                patch_client.return_value.delete_message.assert_called_once_with(
                    QueueUrl=test_sqs_queue_url,
                    ReceiptHandle=no_s3_event_records["Records"][0]["receiptHandle"],
                )

    @mock_sqs
    def test_that_lambda_handler_calls_submit_s3_to_json_workflow_if_queue_has_message(
        self, test_sqs_message, test_object_info, test_sqs_queue_url, test_set_env_var
    ):
        with mock_sqs():
            with mock.patch.object(boto3, "client") as patch_client, mock.patch.object(
                app, "submit_s3_to_json_workflow"
            ) as patch_submit:
                patch_client.return_value.get_queue_url.return_value = (
                    test_sqs_queue_url,
                )
                patch_client.return_value.receive_message.return_value = (
                    test_sqs_message
                )
                result = app.lambda_handler(event=test_sqs_message, context=None)
                assert result == {
                    "statusCode": 200,
                    "body": "All messages retrieved and processed.",
                }
                patch_submit.assert_called_once_with(
                    objects_info=test_object_info,
                    workflow_name="test_workflow",
                )
                patch_client.return_value.delete_message.assert_called_once_with(
                    QueueUrl=test_sqs_queue_url,
                    ReceiptHandle=test_sqs_message["Records"][0]["receiptHandle"],
                )

    def test_that_lambda_handler_does_expected_message_deletion(
        self, test_sqs_queue_name, test_s3_event, test_sqs_message, test_set_env_var
    ):
        with mock_sqs():
            client = boto3.client("sqs")
            client.create_queue(QueueName=test_sqs_queue_name)
            queue_url = client.get_queue_url(QueueName=test_sqs_queue_name)
            test_s3_event_str = json.dumps(test_s3_event)

            # Send a sample message to the SQS queue
            response = client.send_message(
                QueueUrl=queue_url["QueueUrl"], MessageBody=test_s3_event_str
            )
            # immediately put back message after getting receipthandle
            messages = client.receive_message(
                QueueUrl=queue_url["QueueUrl"], VisibilityTimeout=0
            )
            receipt_handle = messages["Messages"][0]["ReceiptHandle"]
            test_sqs_message = {
                "Records": [
                    {"receiptHandle": receipt_handle, "body": test_s3_event_str}
                ]
            }
            with mock.patch.object(app, "submit_s3_to_json_workflow"):
                app.lambda_handler(event=test_sqs_message, context=None)
                response = client.receive_message(QueueUrl=queue_url["QueueUrl"])
                assert "Messages" not in response

    @pytest.mark.parametrize(
        "test_object_info,expected",
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
        self, test_object_info, expected
    ):
        assert app.filter_object_info(test_object_info) == expected
