"""
This is a utility script that generates a fake S3 event notification and writes
it out as JSON. This test event can be used to test the lambda which
triggers the Glue pipeline.
"""

import argparse
import copy
import json
import boto3

SINGLE_RECORD_OUTFILE = 'single-record.json'
MULTI_RECORD_OUTFILE = 'records.json'

s3_event_record_template = {
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
      "name": "{bucket}",
      "ownerIdentity": {
        "principalId": "EXAMPLE"
      },
      "arn": "arn:aws:s3:::bucket_arn"
    },
    "object": {
      "key": "{key}",
      "size": 1024,
      "eTag": "0123456789abcdef0123456789abcdef",
      "sequencer": "0A1B2C3D4E5F678901"
    }
  }
}

def read_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
      description="Generate a json file of a mocked SQS event for testing.")
    parser.add_argument("--input-bucket",
      default="recover-dev-input-data",
      help="Optional. S3 bucket name containing input data. Default is 'recover-dev-input-data'.")
    parser.add_argument("--input-key",
      default="main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
      help=("Optional. Name of the Synapse dataset containing test data. "
            "Default is `main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368` "))
    parser.add_argument("--input-key-prefix",
      help=("Optional. Takes precedence over `--input-key`. If you want to generate a "
            "single event containing all data under a specific S3 key prefix, "
            "specify that here."))
    args = parser.parse_args()
    return args

def create_event(bucket: str, key: str, key_prefix: str) -> dict:
    """
    Create an S3 event notification for testing.

    This function accepts either an S3 object key or an S3 key prefix that will
    be included in the test event. If an S3 object key is provided, then the test
    event will include only that S3 object. If an S3 key prefix is provided, the
    test event will include every S3 object that is directly under that prefix.
    The key prefix behavior takes precedence over the single key behavior.

    Args:
        bucket (str): The S3 bucket name
        key (str): An S3 object key
        key_prefix (str): An S3 key prefix containing S3 objects to include
            in the test event. Takes precedence over `key`.

    Returns:
        dict: An S3 event notification
    """
    if key_prefix is not None:
        s3_client = boto3.client("s3")
        test_objects = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=key_prefix
        )
        test_data = [obj["Key"] for obj in test_objects["Contents"]]
    else:
        test_data = [key]
    s3_event = {
            "Records": [create_s3_event_record(bucket=bucket, key=k) for k in test_data]
    }
    return s3_event

def create_s3_event_record(bucket: str, key: str) -> dict:
    """
    Create an S3 event notification "Record" for an individual S3 object.

    Args:
        bucket (str): The S3 bucket name
        key (str): The S3 object key

    Returns:
        dict: A dictionary formatted as a "Record" object would be
            in an S3 event notification
    """
    s3_record = copy.deepcopy(s3_event_record_template)
    s3_record["s3"]["bucket"]["name"] = bucket
    s3_record["s3"]["object"]["key"] = key
    return s3_record

def main() -> None:
    args = read_args()
    print("Generating mock S3 event...")
    s3_event = create_event(
            bucket=args.input_bucket,
            key=args.input_key,
            key_prefix=args.input_key_prefix
    )

    if args.input_key_prefix is not None:
        with open(MULTI_RECORD_OUTFILE, "w") as outfile:
            json.dump(s3_event, outfile)
            print(f"Event with multiple records written to {outfile.name}.")
    else:
        with open(SINGLE_RECORD_OUTFILE, "w") as outfile:
            json.dump(s3_event, outfile)
            print(f"Event with single record written to {outfile.name}.")
    print("Done.")


if __name__ == "__main__":
    main()
