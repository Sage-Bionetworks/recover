"""
Dispatch Lambda

This Lambda polls the input-to-dispatch SQS queue and publishes to the dispatch SNS topic.
Its purpose is to inspect each export and dispatch each file as a separate job in which
the file will be decompressed and uploaded to S3.
"""
import json
import logging
import os
import zipfile
from urllib import parse

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_object_info(s3_event: dict) -> dict:
    """
    Derive object info from an S3 event.

    Args:
        s3_event (dict): An S3 event

    Returns:
        object_info (dict) The S3 object info
    """
    bucket_name = s3_event["s3"]["bucket"]["name"]
    object_key = parse.unquote(s3_event["s3"]["object"]["key"])
    object_info = {
        "Bucket": bucket_name,
        "Key": object_key,
    }
    return object_info

def get_archive_contents(archive_path: str, bucket: str, key: str) -> list[dict]:
    """
    Inspect a ZIP archive for its file contents.

    Args:
        archive_path (str): The path of the ZIP archive to inspect.
        bucket (str): The S3 bucket where the ZIP archive originates from.
        key (str): The S3 key where the ZIP archive originates from.

    Returns:
        archive_contents (list) A list of dictionaries. Each dictionary contains
            the keys:

            * Bucket - The name of the S3 bucket
            * Key - The S3 key
            * Path - The path within the archive which identifies this file
            * FileSize - The uncompressed size in bytes of this file
    """
    archive_contents = []
    with zipfile.ZipFile(archive_path, "r") as archive:
        for path in archive.infolist():
            if (
                "/" not in path.filename # necessary for pilot data only
                and "Manifest" not in path.filename
                and path.file_size > 0
            ):
                file_info = {
                        "Bucket": bucket,
                        "Key": key,
                        "Path": path.filename,
                        "FileSize": path.file_size
                }
                archive_contents.append(file_info)
    return archive_contents

def lambda_handler(event: dict, context:dict) -> None:
    """
    This function serves as the entrypoint and will be triggered upon
    polling the input-to-dispatch SQS queue.

    Args:
        event (dict): The input-to-dispatch SQS event.
        context (dict): Information about the runtime environment and
            the current invocation

    Returns:
        (None): Calls the real workhorse of this module: `main`.
    """
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns")
    dispatch_sns_arn = os.environ.get("DISPATCH_SNS_ARN", "")
    # if there are multiple exports, they will overwrite each other
    # since it's not necessary to have access to more than one export at a time.
    temp_zip_path = "/tmp/export.zip"
    main(
        event=event,
        context=context,
        s3_client=s3_client,
        sns_client=sns_client,
        dispatch_sns_arn=dispatch_sns_arn,
        temp_zip_path=temp_zip_path
    )

def main(
        event: dict,
        context: dict,
        sns_client: "botocore.client.SNS",
        s3_client: "botocore.client.S3",
        dispatch_sns_arn: str,
        temp_zip_path: str
) -> None:
    """
    This function should be invoked by `lambda_handler`.

    Args:
        event (dict): The input-to-dispatch SQS event.
        context (dict): Information about the runtime environment and
            the current invocation
        sns_client (botocore.client.SNS): An SNS client
        s3_client (botocore.client.S3): An S3 client
        dispatch_sns_arn: The ARN of the SNS topic we publish to
        temp_zip_path: The path to download the export S3 object to.

    Returns:
        (None): Logs and publishes to the dispatch SNS topic.
    """
    for sqs_record in event["Records"]:
        sns_notification = json.loads(sqs_record["body"])
        sns_message = json.loads(sns_notification["Message"])
        logger.info(f"Received SNS message: {sns_message}")
        for s3_event in sns_message["Records"]:
            object_info = get_object_info(s3_event)
            s3_client.download_file(Filename=temp_zip_path, **object_info)
            logger.info(f"Getting archive contents for {object_info}")
            archive_contents = get_archive_contents(
                    archive_path=temp_zip_path,
                    bucket=object_info["Bucket"],
                    key=object_info["Key"]
            )
            for file_info in archive_contents:
                logger.info(f"Publishing {file_info} to {dispatch_sns_arn}")
                sns_client.publish(
                        TopicArn=dispatch_sns_arn,
                        Message=json.dumps(file_info)
                )
