"""
Raw Lambda

A module for an AWS Lambda function that compresses JSON data contained in an
export (zip archive) from S3 and uploads it to the raw S3 bucket. This module
makes heavy use of Python file objects and multipart uploads and can
decompress/compress/upload with a relatively low, fixed memory overhead
with respect to the size of the uncompressed JSON.

Example Usage:
The module is intended to be deployed as an AWS Lambda function that listens to
events from the `dispatch-to-raw` SQS queue.

Environment Variables:
- `RAW_S3_BUCKET`: The raw S3 bucket name where compressed data will be stored.
- `RAW_S3_KEY_PREFIX`: The S3 key prefix within the raw bucket where data is written.
"""

import gzip
import io
import json
import logging
import os
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
    raw_bucket = os.environ.get("RAW_S3_BUCKET")
    raw_key_prefix = os.environ.get("RAW_S3_KEY_PREFIX")
    main(
        event=event,
        s3_client=s3_client,
        raw_bucket=raw_bucket,
        raw_key_prefix=raw_key_prefix,
    )


def construct_raw_key(path: str, key: str, raw_key_prefix: str):
    """
    Constructs an S3 key for data to be uploaded to the raw S3 bucket.

    Args:
        path (str): The relative file path of the JSON data within its zip archive
        key (str): The S3 key of the export/zip archive, formatted as `{namespace}/{cohort}/{export_basename}`.
        raw_key_prefix (str): The raw S3 bucket key prefix  where all raw data is written.

    Returns:
        str: An S3 key in the format:
            `{raw_key_prefix}/dataset={data_type}/cohort={cohort}/{basename}.ndjson.gz`

    Notes:
        - The function expects the input `key` to be formatted as `{namespace}/{cohort}/{export_basename}`.
        - The data type is derived from the first underscore-delimited component of the file basename.
    """
    key_components = key.split("/")
    # input bucket keys are formatted like `{namespace}/{cohort}/{export_basename}`
    cohort = key_components[1]
    file_basename = os.path.basename(path)
    # The first underscore-delimited component of the JSON basename is the datatype
    data_type = file_basename.split("_")[0]
    raw_basename = f"{ os.path.splitext(file_basename)[0] }.ndjson.gz"
    raw_key = os.path.join(
        raw_key_prefix,
        f"dataset={data_type}",
        f"cohort={cohort}",
        raw_basename,
    )
    return raw_key


def upload_part(
    s3_client: boto3.client,
    body: bytes,
    bucket: str,
    key: str,
    upload_id: str = None,
    part_number: int = None,
):
    """
    Uploads a part of data to an S3 object as part of a multipart upload.

    If an `upload_id` is not provided, the function initiates a new multipart
    upload. Each part is identified by its ETag and part number, which are
    required for the completion the multipart upload.

    Args:
        s3_client (boto3.client): The Boto3 S3 client used to interact with AWS S3.
        body (bytes): The data to be uploaded as a part, in bytes.
        bucket (str): The name of the raw S3 bucket where the object is to be stored.
        key (str): The S3 key of the object being uploaded.
        upload_id (str, optional): The ID of an ongoing multipart upload. If not provided,
            a new multipart upload is initiated.
        part_number (int, optional): The part number for this chunk of the upload.
            If not provided, defaults to 1 when initiating a new upload.

    Returns:
        dict: A dictionary containing the following keys:
            - part (dict): This dictionary must be included with the other parts
                in a list when completing the multipart upload and is provided
                in this format for convenience. A dictionary with keys:
                    * ETag (str): The ETag of this part.
                    * part_number (int) The associated part number.
            - upload_id (str): The ID of the multipart upload.
            - part_number (int): The part number associated with this upload part.

    Logs:
        - Logs the start of a new multipart upload if `upload_id` is not provided.
        - Logs the upload size and S3 destination for each part.
        - Logs the response from the S3 upload operation, including the ETag.

    Example:
        # Upload a part to an existing multipart upload
        s3 = boto3.client('s3')
        part = upload_part(
            s3_client=s3,
            body=b'some data chunk',
            bucket='my-bucket',
            key='my-object',
            upload_id='existing-upload-id',
            part_number=2
        )
        print(part)
        # Output:
        # {
        #   'part':
        #     {
        #       'ETag': 'etag-value',
        #       'PartNumber': 2
        #     },
        #   'upload_id': 'existing-upload-id',
        #   'part_number': 2
        # }

    Notes:
        - Parts must be larger than AWS minimum requirements (typically 5 MB),
          excepting the last part. If an object only has one part, than it can
          be any size.
    """
    if upload_id is None:
        multipart_upload = s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=key,
        )
        upload_id = multipart_upload["UploadId"]
        part_number = 1
        logger.info(f"Began multipart upload {upload_id}")
    logger.info(f"Uploading { len(body) } bytes to s3://{bucket}/{key}")
    upload_response = s3_client.upload_part(
        Body=body,
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        PartNumber=part_number,
    )
    logger.info(f"Upload part response: {upload_response}")
    part_identifier = {
        "ETag": upload_response["ETag"],
        "PartNumber": part_number,
    }
    part_wrapper = {
        "part": part_identifier,
        "upload_id": upload_id,
        "part_number": part_number,
    }
    return part_wrapper


def yield_compressed_data(object_stream: io.BytesIO, path: str, part_threshold=None):
    """
    A generator function which yields chunks of compressed JSON data.

    This function reads chunks from a JSON file in `object_stream`,
    compresses the data using gzip, and yields the compressed data in chunks
    that are at least as large as the `part_threshold`, except for the final
    chunk, potentially. The compressed data is yielded as dictionaries containing
    the compressed bytes and a chunk number to preserve ordinality.

    Args:
        object_stream (io.BytesIO): A BytesIO object over a zip archive which contains
            the JSON file to be compressed.
        path (str): The path within the zip archive to the JSON file.
        part_threshold (int, optional): The size threshold in bytes for each yielded
            compressed chunk. Defaults to 8 MB (8 * 1024 * 1024 bytes).

    Yields:
        dict: A dictionary containing the keys:
            - data (bytes): The content of the compressed BytesIO object as bytes.
            - chunk_number (int): The associated chunk number of the data.

    Notes:
        - We use compression level 6 (out of 9). This is the default compression level
          of the linux `gzip` tool. This is faster to write, but produces slightly (~10-20%)
          larger files.
    """
    if part_threshold is None:
        # 8 MB, supports up to 80 GB compressed multipart upload
        part_threshold = 8 * 1024 * 1024
    with zipfile.ZipFile(object_stream, "r") as zip_stream:
        with zip_stream.open(path, "r") as json_stream:
            compressed_data = io.BytesIO()
            # analogous to the part number of a multipart upload
            chunk_number = 1
            with gzip.GzipFile(
                filename=os.path.basename(path),
                fileobj=compressed_data,
                compresslevel=6,
                mode="wb",
            ) as gzip_file:
                # We can expect at least 10x compression, so reading/writing the
                # JSON in 10*part_threshold chunks ensures we do not flush the
                # gzip buffer too often, which can slow the write process significantly.
                compression_factor = 10
                for chunk in iter(
                    lambda: json_stream.read(compression_factor * part_threshold), b""
                ):
                    gzip_file.write(chunk)
                    # .flush() ensures that .tell() gives us an accurate byte count,
                    gzip_file.flush()
                    if compressed_data.tell() >= part_threshold:
                        yield compressed_data_wrapper(
                            compressed_data=compressed_data, chunk_number=chunk_number
                        )
                        compressed_data.seek(0)
                        compressed_data.truncate(0)
                        chunk_number = chunk_number + 1
            yield compressed_data_wrapper(
                compressed_data=compressed_data, chunk_number=chunk_number
            )


def compressed_data_wrapper(compressed_data: io.BytesIO, chunk_number: int):
    """
    A wrapper for the data produced as part of `yield_compressed_data`

    The chunk number is useful for maintaining ordinality during a
    multipart upload.

    Args:
        compressed_data (io.BytesIO): A BytesIO object containing the compressed data.
        chunk_number (int): The chunk number associated with this segment of data.

    Returns:
        dict: A dictionary containing the keys:
            - data (bytes): The content of the compressed BytesIO object as bytes.
            - chunk_number (int): The associated chunk number of the data.
    """
    compressed_data_wrapper = {
        "data": compressed_data.getvalue(),
        "chunk_number": chunk_number,
    }
    return compressed_data_wrapper


def main(event: dict, s3_client: boto3.client, raw_bucket: str, raw_key_prefix: str):
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
        raw_bucket (str): The name of the raw S3 bucket
        raw_key_prefix (str): The S3 prefix where we write our data to.

    Returns:
        (None): Produces logs and uploads a gzipped JSON object to the raw S3 bucket.
    """
    for sqs_record in event["Records"]:
        sns_notification = json.loads(sqs_record["body"])
        sns_message = json.loads(sns_notification["Message"])
        logger.info(f"Received SNS message: {sns_message}")
        raw_key = construct_raw_key(
            path=sns_message["Path"],
            key=sns_message["Key"],
            raw_key_prefix=raw_key_prefix,
        )
        # Stream the zip file from S3
        with io.BytesIO() as object_stream:
            s3_client.download_fileobj(
                Bucket=sns_message["Bucket"],
                Key=sns_message["Key"],
                Fileobj=object_stream,
            )
            object_stream.seek(0)
            multipart_upload = {
                "parts": [],
                "upload_id": None,
            }
            # Upload each chunk of compressed data to S3 as part
            # of a multipart upload
            for compressed_data in yield_compressed_data(
                object_stream=object_stream,
                path=sns_message["Path"],
            ):
                part = upload_part(
                    s3_client=s3_client,
                    body=compressed_data["data"],
                    bucket=raw_bucket,
                    key=raw_key,
                    upload_id=multipart_upload["upload_id"],
                    part_number=compressed_data["chunk_number"],
                )
                multipart_upload["parts"].append(part["part"])
                multipart_upload["upload_id"] = part["upload_id"]
            # Complete our multipart upload
            completed_upload_response = s3_client.complete_multipart_upload(
                Bucket=raw_bucket,
                Key=raw_key,
                UploadId=multipart_upload["upload_id"],
                MultipartUpload={"Parts": multipart_upload["parts"]},
            )
            logger.info(
                f"Complete multipart upload response: {completed_upload_response}"
            )
            return completed_upload_response
