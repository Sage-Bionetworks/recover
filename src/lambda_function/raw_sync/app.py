"""
Raw Sync Lambda

This script verifies that the input and raw S3 buckets are synchronized.

This is accomplished by verifying that all non-zero sized JSON in each export
in the input S3 bucket, excepting "Manifest.json", have a corresponding object
in the raw S3 bucket.  Because we only download the central directory, typically
located near the end of a zip archive, verification can be done extremely quickly
and without needing to download most of the export.

If a JSON file from an export is found to not have a corresponding object in the raw bucket,
the export is submitted to the raw Lambda (via the dispatch SNS topic) for processing.
"""

import logging
import os
import struct
import zipfile
from collections import defaultdict
from io import BytesIO
from typing import Optional

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context: dict) -> None:
    """
    Entrypoint for this Lambda.

    Args:
        event (dict)
        context (dict): Information about the runtime environment and
            the current invocation
    """
    s3_client = boto3.client("s3")
    input_bucket = os.environ.get("INPUT_S3_BUCKET")
    input_key_prefix = os.environ.get("INPUT_S3_KEY_PREFIX")
    raw_bucket = os.environ.get("RAW_S3_BUCKET")
    raw_key_prefix = os.environ.get("RAW_S3_KEY_PREFIX")
    main(
        event=event,
        s3_client=s3_client,
        input_bucket=input_bucket,
        input_key_prefix=input_key_prefix,
        raw_bucket=raw_bucket,
        raw_key_prefix=raw_key_prefix,
    )


def list_s3_objects(
    s3_client: boto3.client, bucket: str, key_prefix: str, key_format: str
) -> dict:
    """
    Recursively list all objects under an S3 bucket and key prefix which
    conform to a specified format.

    It's assumed that all objects under `key_prefix` have a key prefix
    themselves which conforms to one of two formats:

    "input" format: `{namespace}/{cohort}/
    "raw" format: `{namespace}/json/dataset={data_type}/cohort={cohort}/`

    Args:
        s3_client (boto3.client): An S3 client
        bucket (str): The name of the S3 bucket.
        key_prefix (str): The S3 key prefix to recursively list files from.
        key_format (str): The format used by the keys, either "input" or "raw"

    Returns (dict): A dictionary where each hierarchy corresponds to the
        ordering of the variables in the `key_format`, excepting {namespace}.

        For example:

        `key_format`="raw":

            {
                "data_type_one": {
                    "cohort_one": [
                        "object_one",
                        "object_two",
                        ...
                    ],
                    "cohort_two": [
                        "object_one",
                        ...
                    ]
                }
                "data_type_two": {
                    ...
                },
                ...
            }

        `key_format`="input":

            {
                "cohort_one": [
                    "object_one",
                    "object_two",
                    ...
                ],
                "cohort_two": [
                    "object_one",
                    ...
                ]
            }
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=key_prefix)
    if key_format == "raw":
        result = defaultdict(lambda: defaultdict(list))
    elif key_format == "input":
        result = defaultdict(list)

    for response in response_iterator:
        for obj in response.get("Contents", []):
            key = obj["Key"]
            key_components = key.split("/")
            if key_format == "raw":
                try:
                    data_type = next(
                        part.split("=")[1]
                        for part in key_components
                        if part.startswith("dataset=")
                    )
                    cohort = next(
                        part.split("=")[1]
                        for part in key_components
                        if part.startswith("cohort=")
                    )
                    result[data_type][cohort].append(key)
                except StopIteration:
                    # Skip keys that don't match the expected pattern
                    continue
            elif key_format == "input" and len(key_components) == 3:
                cohort = key_components[1]
                result[cohort].append(key)

    return result


def match_corresponding_raw_object(
    namespace: str,
    data_type: str,
    cohort: str,
    file_identifier: str,
    raw_keys: list[dict],
) -> Optional[str]:
    """
    Find a matching S3 key for a given export file.

    Given a `namespace`, `cohort`, `data_type`, and `filename`, the matching
    S3 key conforms to:

        `{namespace}/json/dataset={data_type}/cohort={cohort}/{file_identifier}.ndjson.gz`

    Args:
        namespace (str): The namespace
        data_type (str): The data type
        cohort (str): The cohort name
        file_identifier (str): The identifier of the original JSON file. The identifier is
            the basename without any extensions.
        raw_keys (dict): A dictionary formatted as the dictionary returned by `list_s3_objects`.

    Returns (str): The matching S3 key from `raw_keys`, or None if no match is found.
    """
    expected_key = f"{namespace}/json/dataset={data_type}/cohort={cohort}/{file_identifier}.ndjson.gz"

    # Navigate through raw_keys to locate the correct `data_type` and `cohort`
    if data_type in raw_keys:
        if cohort in raw_keys[data_type]:
            # Iterate through the list of keys under the specified `data_type`` and `cohort`
            for key in raw_keys[data_type][cohort]:
                if key == expected_key:
                    return key
    return None


def parse_content_range(content_range: str) -> tuple[int, ...]:
    """
    Parse the ContentRange header to extract the start, end, and total size of the object.

    A helper function for `list_files_in_archive`.

    Args:
        content_range (str): The ContentRange header value in the format 'bytes start-end/total'.

    Returns:
        tuple: A tuple containing (range_start, range_end, total_size).
    """
    # ContentRange format: 'bytes start-end/total'
    _, range_info = content_range.split(" ")
    range_start, range_end, total_size = map(
        int, range_info.replace("-", "/").split("/")
    )
    logger.info(
        f"Read 0-indexed bytes from {range_start} to {range_end}, inclusive, "
        f"out of {total_size} bytes."
    )
    return range_start, range_end, total_size


def unpack_eocd_fields(body: bytes, eocd_offset: int) -> list[int]:
    """
    Extract the End of Central Directory (EOCD) fields from the given body.

    A helper function for `list_files_in_archive`.

    The `unpack` method parses out:

    <  - indicates Little-endian byte order
    4s - 4-byte string:
        EOCD signature (4 bytes)
    4H - Four 2-byte unsigned short integers:
        Number of this disk (2 bytes)
        Disk where central directory starts (2 bytes)
        Number of central directory records on this disk (2 bytes)
        Total number of central directory records (2 bytes)
    2L - Two 4-byte unsigned long integers
        Size of central directory (4 bytes)
        Offset of start of central directory (4 bytes)
    H  - 2-byte unsigned short integer
        Comment length (2 bytes)

    Args:
        body (bytes): The byte content from which to extract EOCD fields.
        eocd_offset (int): The offset position of the EOCD signature in the body.

    Returns:
        tuple: A tuple containing (central_directory_offset, central_directory_size).
            Both are int type.
    """
    eocd_fields = struct.unpack("<4s4H2LH", body[eocd_offset : eocd_offset + 22])
    logger.info(f"EOCD Record: {eocd_fields}")
    central_directory_offset = eocd_fields[-2]
    central_directory_size = eocd_fields[-3]
    logger.info(f"Central Directory Offset: {central_directory_offset}")
    logger.info(f"Central Directory Size: {central_directory_size}")
    return central_directory_offset, central_directory_size


def determine_eocd_offset(body: bytes, content_range: str) -> int:
    """
    Determine the offset of the End of Central Directory (EOCD) record in a given byte sequence.

    A helper function for `list_files_in_archive`.

    This function searches for the EOCD signature (`PK\x05\x06`) within the provided byte
    sequence (`body`).

    Args:
        body (bytes): The byte sequence in which to search for the EOCD signature.
        content_range (str): A string representing the content range of the bytes.
            This is used for logging purposes.

    Returns:
        int: The offset of the EOCD signature within the provided byte sequence. Returns -1 if the
             EOCD signature is not found, indicating that the EOCD is not present in the current
             range of bytes.
    """
    eocd_signature = b"PK\x05\x06"
    eocd_offset = body.rfind(eocd_signature)
    logger.debug(f"Found EOCD offset: {eocd_offset}")

    # Check if EOCD is present, else try again with a bigger chunk of data
    if eocd_offset == -1:
        logger.info(
            "Did not find the end of central directory record in "
            f"ContentRange {content_range}."
        )
    return eocd_offset


def list_files_in_archive(
    s3_client: boto3.client, bucket: str, key: str, range_size=64 * 1024
) -> list[str]:
    """
    Recursively lists files in a ZIP archive stored as an S3 object.

    Files are filtered by the same criteria as the dispatch Lambda.

    This function:

        1. Fetches the last `range_size` bytes of an S3 object (assumed to contain a ZIP
           archive) in order to locate and parse the End of Central Directory (EOCD) record.
           If the EOCD record is not contained in the bytes, the function calls itself
           recursively with a larger range size. This scenario is expected to be rare,
           only occurring if there is a comment at the end of the ZIP file exceeding
           `range_size` - 22 bytes (the size of the EOCD record minus the optional comment).
           If a non-zip file was provided, this process will repeat until the entire file
           has been read, since this is the only way to determine that there is no EOCD record.

        2. Having found the EOCD record, the offset of the central directory is determined.
           If the central directory is not fully contained within the retrieved range,
           the function will call itself with the appropriate `range_size`.

        3. The function then reads the central directory to extract the file list.

    Args:
        s3_client (boto3.client): The Boto3 S3 client used to fetch the object from S3.
        bucket (str): The name of the S3 bucket where the object is stored.
        key (str): The key of the S3 object containing the ZIP archive.
        range_size (int): The number of bytes to fetch from the tail of the S3 object on each call.
                          Defaults to 64 KB.

    Returns:
        list[str]: A list of file names contained within the ZIP archive, filtered to exclude:

            - Directories (i.e., paths containing "/").
            - Files named "Manifest".
            - Empty files (file size == 0).

            If no files match the criteria or the EOCD record is not found, it returns an empty list.

    Notes:
        - The function may trigger multiple recursive calls if the EOCD record is large or non-existent,
            but is guaranteed to terminate upon retrieving the entire object.
    """
    file_list = []
    object_response = s3_client.get_object(
        Bucket=bucket, Key=key, Range=f"bytes=-{range_size}"
    )
    logger.debug(f"Object Response: {object_response}")

    # Parse the ContentRange for later reference
    range_start, range_end, total_size = parse_content_range(
        content_range=object_response["ContentRange"]
    )

    # Determine end of central directory offset
    tail = object_response["Body"].read()
    eocd_offset = determine_eocd_offset(
        body=tail, content_range=object_response["ContentRange"]
    )
    if eocd_offset == -1:
        adjusted_range_size = range_size * 2
        if adjusted_range_size > total_size * 2:
            logger.error(
                "Did not find an end of central directory record in "
                f"s3://{bucket}/{key}"
            )
            return []
        logger.warning(
            f"Calling this function recursively with `range_size` = {adjusted_range_size}"
        )
        return list_files_in_archive(
            s3_client=s3_client,
            bucket=bucket,
            key=key,
            range_size=adjusted_range_size,
        )

    # Extract the relevant EOCD fields
    central_directory_offset, central_directory_size = unpack_eocd_fields(
        body=tail, eocd_offset=eocd_offset
    )

    # Check if the entire central directory is contained within the fetched range
    if (
        central_directory_offset < range_start
        or central_directory_offset + central_directory_size > range_end
    ):
        logger.warning(
            "The entire central directory is not contained in "
            f"ContentRange {object_response['ContentRange']}."
        )
        appropriate_range_size = total_size - central_directory_offset
        logger.warning(
            f"Calling this function recursively with `range_size` = {appropriate_range_size}"
        )
        return list_files_in_archive(
            s3_client=s3_client,
            bucket=bucket,
            key=key,
            range_size=appropriate_range_size,
        )

    # Compile a list of file names which satisfy the same conditions used by dispatch Lambda
    with zipfile.ZipFile(BytesIO(tail), "r") as zip_file:
        for zip_info in zip_file.infolist():
            if (
                "/" not in zip_info.filename  # necessary for pilot data only
                and "Manifest" not in zip_info.filename
                and zip_info.file_size > 0
            ):
                file_list.append(zip_info.filename)
    return file_list


def main(
    event: dict,
    s3_client: boto3.client,
    input_bucket: str,
    input_key_prefix: str,
    raw_bucket: str,
    raw_key_prefix: str,
) -> None:
    export_keys = list_s3_objects(
        s3_client=s3_client,
        bucket=input_bucket,
        key_prefix=input_key_prefix,
        key_format="input",
    )
    raw_keys = list_s3_objects(
        s3_client=s3_client,
        bucket=raw_bucket,
        key_prefix=raw_key_prefix,
        key_format="raw",
    )
    for export_key in export_keys:
        # input bucket keys are formatted like `{namespace}/{cohort}/{export_basename}`
        namespace, cohort = export_key.split("/")[:2]
        file_list = list_files_in_archive(
            s3_client=s3_client,
            bucket=input_bucket,
            key=export_key,
        )
        for filename in file_list:
            data_type = filename.split("_")[0]
            corresponding_raw_object = match_corresponding_raw_object(
                namespace=namespace,
                cohort=cohort,
                data_type=data_type,
                file_identifier=filename,
                raw_keys=raw_keys,
            )
            if corresponding_raw_object is None:
                logger.info(
                    f"Did not find corresponding file for {filename} from "
                    f"s3://{raw_bucket}/{export_key} in raw bucket "
                    f"s3://{raw_bucket}/{namespace}/"
                )
