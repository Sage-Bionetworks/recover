import pytest
from src.lambda_function.s3_to_glue import app

class MockGlueClient:
    def start_workflow_run(*args, **kwargs):
        return {"RunId": "example"}

    def put_workflow_run_properties(*args, **kwargs):
        return {}

class TestS3ToGlueLambda:

    @pytest.fixture
    def test_s3_event(self):
        s3_event = {
          "Records": [
            {
              "eventVersion": "2.0",
              "eventSource": "aws:s3",
              "awsRegion": "us-east-1",
              "eventTime": "1970-01-01T00:00:00.000Z",
              "eventName": "ObjectCreated:Put",
              "userIdentity": {
                "principalId": "EXAMPLE"
              },
              "requestParameters": {
                "sourceIPAddress": "127.0.0.1"
              },
              "responseElements": {
                "x-amz-request-id": "EXAMPLE123456789",
                "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
              },
              "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "testConfigRule",
                "bucket": {
                  "name": "recover-dev-input-data",
                  "ownerIdentity": {
                    "principalId": "EXAMPLE"
                  },
                  "arn": "arn:aws:s3:::bucket_arn"
                },
                "object": {
                  "key": "main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
                  "size": 1024,
                  "eTag": "0123456789abcdef0123456789abcdef",
                  "sequencer": "0A1B2C3D4E5F678901"
                }
              }
            }
          ]
        }
        return s3_event

    @pytest.fixture
    def test_messages(self):
        messages = [
                {
                    "source_bucket": "recover-dev-input-data",
                    "source_object": "main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368"
                }
        ]
        return messages

    def test_submit_s3_to_json_workflow(self, test_messages, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x: MockGlueClient())
        app.submit_s3_to_json_workflow(
                messages=test_messages,
                workflow_name="example-workflow"
        )

