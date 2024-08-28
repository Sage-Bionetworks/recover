import gzip
import io
import json
import logging
import zipfile

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context: dict):
    """
    Entrypoint for this Lambda.

    Args:
        event (dict) An SQS message from the dispatch-to-raw SQS queue.
        context (dict): Information about the runtime environment and
            the current invocation
    """
    s3_client = boto3.client("s3")
    main(event=event, s3_client=s3_client)


def upload_part(gzip_stream: gzip.GzipFile):
    pass


def yield_compressed_data(object_stream: io.BytesIO, path: str, part_threshold=None):
    if part_threshold is None:
        part_threshold = 50 * 1024 * 1024  # 50 MB
    with zipfile.ZipFile(object_stream, "r") as zip_stream:
        with zip_stream.open(path, "r") as json_stream:
            compressed_data = io.BytesIO()
            with gzip.GzipFile(fileobj=compressed_data, mode="wb") as gzip_file:
                # Read/write the JSON file in 1 MB chunks
                for chunk in iter(lambda: json_stream.read(1024 * 1024), b""):
                    gzip_file.write(chunk)
                    if compressed_data.tell() >= part_threshold:
                        yield compressed_data
                    compressed_data.seek(0)
                    compressed_data.truncate(0)
            yield compressed_data


def main(event: dict, s3_client: boto3.client):
    """
    This function should be invoked by `lambda_handler`.

    Args:
        event (dict): The dispatch-to-raw SQS event.
                The payload from the dispatch Lambda is in .["Records"][0]["body"]["Message"]
                and contains the keys:

                * Bucket - The name of the S3 bucket
                * Key - The S3 key
                * Path - The path within the archive which identifies this file
                * FileSize - The uncompressed size in bytes of this file
        context (dict): Information about the runtime environment and
            the current invocation
        s3_client (botocore.client.S3): An S3 client

    Returns:
        (None): Logs and uploads an object to the raw S3 bucket.
    """
    for sqs_record in event["Records"]:
        sns_notification = json.loads(sqs_record["body"])
        sns_message = json.loads(sns_notification["Message"])
        logger.info(f"Received SNS message: {sns_message}")
        # Step 1: Stream the zip file from S3
        with io.BytesIO() as object_stream:
            s3_client.download_fileobj(
                Bucket=sns_message["Bucket"],
                Key=sns_message["Key"],
                Fileobj=object_stream,
            )
            object_stream.seek(0)
            for compressed_data in yield_compressed_data(
                object_stream=object_stream,
                path=sns_message["Path"],
                part_threshold=102,
            ):
                logger.info(compressed_data.tell())
            object_stream.close()

        #        # Define the S3 key for the gzipped file (you can customize this)
        #        gzip_key = f"{s3_details['Path']}.gz"

        #        # Step 4: Upload the gzipped JSON file to S3
        #        s3_client.upload_fileobj(
        #            compressed_data, s3_details["Bucket"], gzip_key
        #        )

        # print(f"Uploaded gzipped file to s3://{s3_details['Bucket']}/{gzip_key}")
