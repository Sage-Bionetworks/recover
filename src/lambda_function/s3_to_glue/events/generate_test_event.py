"""
This is a utility script that generates a fake S3 -> SNS -> SQS event
and writes it out as JSON. This test event can be used to test the Lambda which
triggers the Glue pipeline.

Two types of events can be generated:

    1. An event with a single record (single-records.json).
    2. An event with multiple records (records.json).

See https://github.com/Sage-Bionetworks/recover/tree/main/src/lambda_function
for detailed information how to use this event with the Lambda.
"""

import argparse
import json
import os

import boto3

SINGLE_RECORD_OUTFILE = "single-record.json"
MULTI_RECORD_OUTFILE = "records.json"


def read_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a JSON file of a mocked S3 event for testing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-bucket",
        default="recover-dev-input-data",
        help="S3 bucket name containing input data",
    )
    parser.add_argument(
        "--input-key",
        default="main/2023-01-12T22--02--17Z_77fefff8-b0e2-4c1b-b0c5-405554c92368",
        help="A specific S3 key to generate an event for.",
    )
    parser.add_argument(
        "--input-key-prefix",
        help=(
            "Takes precedence over `--input-key`. If you want "
            "to generate a single event containing all data under a specific "
            "S3 key prefix, specify that here."
        ),
    )
    parser.add_argument(
        "--input-key-file",
        help=(
            "Takes precedence over `--input-key` and `--input-key-prefix`. "
            "If you want to generate a single event containing all keys within"
            "a newline delimited file, specify the path to that file here."
        ),
    )
    parser.add_argument(
        "--output-directory",
        default="./",
        help=(
            "Specifies the directory that the S3 notification json gets saved to. "
            "Defaults to current directory that the script is running from. "
        ),
    )
    args = parser.parse_args()
    return args


def create_event(bucket: str, key: str, key_prefix: str, key_file: str) -> dict:
    """
    Create an SQS event wrapping a SNS notification of an S3 event notification(s)
    for testing.

    Each SNS notification will contain a single S3 event, and each SQS event
    will contain a single SNS notification.

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
        key_file (str): A file path to a newline delimited list of S3 keys to
            include in the test event. Takes precedence over `key` and `key_prefix`.

    Returns:
        dict: An SQS event wrapping S3 event notifications
    """
    if key_file is not None:
        with open(key_file, "r") as key_file_obj:
            key_file_contents = key_file_obj.read().strip()
            test_data = key_file_contents.split("\n")
    elif key_prefix is not None:
        s3_client = boto3.client("s3")
        test_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=key_prefix)
        test_data = [
            obj["Key"]
            for obj in test_objects["Contents"]
            if not obj["Key"].endswith("/")
        ]
    else:
        test_data = [key]
    s3_events = [create_s3_event_record(bucket=bucket, key=k) for k in test_data]
    sns_notifications = [create_sns_notification(s3_event) for s3_event in s3_events]
    sqs_messages = [
        create_sqs_message(sns_notification) for sns_notification in sns_notifications
    ]
    sqs_event = {"Records": sqs_messages}
    return sqs_event


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
    s3_event_record = {
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
                "name": "{bucket}",
                "ownerIdentity": {"principalId": "EXAMPLE"},
                "arn": "arn:aws:s3:::bucket_arn",
            },
            "object": {
                "key": "{key}",
                "size": 1024,
                "eTag": "0123456789abcdef0123456789abcdef",
                "sequencer": "0A1B2C3D4E5F678901",
            },
        },
    }
    s3_event_record["s3"]["bucket"]["name"] = bucket
    s3_event_record["s3"]["object"]["key"] = key
    return s3_event_record


def create_sqs_message(sns_notification: dict) -> dict:
    """
    Create an SQS message wrapper around an individual SNS notification.

    See `create_sns_notification` for creating S3 event notifications.

    Args:
        sns_notification (dict): A dictionary formatted as an SNS
            notification JSON object would be.

    Returns:
        dict: A dictionary formatted as an SQS message
    """
    sqs_event_record = {
        "messageId": "bf7be842",
        "receiptHandle": "AQEBLdQhbUa",
        "body": None,
        "attributes": {
            "ApproximateReceiveCount": "1",
            "SentTimestamp": "1694541052297",
            "SenderId": "AIDAJHIPRHEMV73VRJEBU",
            "ApproximateFirstReceiveTimestamp": "1694541052299",
        },
        "messageAttributes": {},
        "md5OfMessageAttributes": None,
        "md5OfBody": "abdc58591d121b6a0334fb44fd45aceb",
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:us-east-1:914833433684:mynamespace-sqs-S3ToLambda-Queue",
        "awsRegion": "us-east-1",
    }
    sqs_event_record["body"] = json.dumps(sns_notification)
    return sqs_event_record


def create_sns_notification(s3_event_record):
    """
    Create an SNS message wrapper for an individual S3 event notification.

    See `create_s3_event_record` for creating S3 event notifications.

    Args:
        s3_event_record (dict): A dictionary formatted as a "Record"
            object would be in an S3 event notification

    Returns:
        dict: A dictionary formatted as an SQS message
    """
    sns_notification = {
        "Type": "string",
        "MessageId": "string",
        "TopicArn": "string",
        "Subject": "string",
        "Message": "string",
        "Timestamp": "string",
    }
    sns_notification["Message"] = json.dumps({"Records": [s3_event_record]})
    return sns_notification


def main() -> None:
    args = read_args()
    print("Generating mock S3 event...")
    sqs_event = create_event(
        bucket=args.input_bucket,
        key=args.input_key,
        key_prefix=args.input_key_prefix,
        key_file=args.input_key_file,
    )

    if args.input_key_file is not None or args.input_key_prefix is not None:
        with open(
            os.path.join(args.output_directory, MULTI_RECORD_OUTFILE), "w"
        ) as outfile:
            json.dump(sqs_event, outfile)
            print(f"Event with multiple records written to {outfile.name}.")
    else:
        with open(
            os.path.join(args.output_directory, SINGLE_RECORD_OUTFILE), "w"
        ) as outfile:
            json.dump(sqs_event, outfile)
            print(f"Event with single record written to {outfile.name}.")
    print("Done.")


if __name__ == "__main__":
    main()
