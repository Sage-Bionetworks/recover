import io
import json
import struct
import zipfile
from collections import defaultdict
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_s3, mock_sns, mock_sqs

import src.lambda_function.raw_sync.app as app  # Replace with the actual module name


@pytest.fixture
def s3_client():
    """Fixture to create a mocked S3 client."""
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        yield s3


@pytest.fixture
def out_of_range_central_directory():
    central_directory = (
        b"\x50\x4b\x01\x02"  # Central file header signature (4 bytes)
        + b"\x14\x00"  # Version made by (2 bytes)
        + b"\x0A\x00"  # Version needed to extract (2 bytes)
        + b"\x00\x00"  # General purpose bit flag (2 bytes)
        + b"\x00\x00"  # Compression method (2 bytes, 0 = no compression)
        + b"\x00\x00"  # File last modification time (2 bytes)
        + b"\x00\x00"  # File last modification date (2 bytes)
        + b"\x00\x00\x00\x00"  # CRC-32 (4 bytes, placeholder value)
        + b"\x00\x00\x00\x00"  # Compressed size (4 bytes, placeholder value)
        + b"\x00\x00\x00\x00"  # Uncompressed size (4 bytes, placeholder value)
        + b"\x08\x00"  # File name length (2 bytes, "test.txt" is 8 bytes)
        + b"\x00\x00"  # Extra field length (2 bytes, 0 bytes)
        + b"\x00\x00"  # File comment length (2 bytes, 0 bytes)
        + b"\x00\x00"  # Disk number start (2 bytes)
        + b"\x00\x00"  # Internal file attributes (2 bytes)
        + b"\x00\x00\x00\x00"  # External file attributes (4 bytes)
        + b"\x00\x00\x00\x00"  # Relative offset of local header (4 bytes, placeholder value)
        + b"test.txt"  # File name ("test.txt")
    )
    return central_directory


@pytest.fixture
def out_of_range_eocd_record(out_of_range_central_directory):
    return (
        b"PK\x05\x06"
        + b"\x00" * 8
        + struct.pack("<I", len(out_of_range_central_directory))
        + b"\x00" * 6
    )


@pytest.fixture
def setup_s3(s3_client):
    """Fixture to set up the mocked S3 environment with test data."""
    raw_bucket_name = "test-raw-bucket"
    s3_client.create_bucket(Bucket=raw_bucket_name)

    # Create test objects with the expected key pattern
    s3_client.put_object(
        Bucket=raw_bucket_name,
        Key="namespace/json/dataset=data_type_one/cohort=cohort_one/object_one.ndjson.gz",
        Body=b"",
    )
    s3_client.put_object(
        Bucket=raw_bucket_name,
        Key="namespace/json/dataset=data_type_one/cohort=cohort_two/object_two.ndjson.gz",
        Body=b"",
    )
    s3_client.put_object(
        Bucket=raw_bucket_name,
        Key="namespace/json/dataset=data_type_two/cohort=cohort_one/object_three.ndjson.gz",
        Body=b"",
    )

    # Create objects that should be ignored
    s3_client.put_object(
        Bucket=raw_bucket_name,
        Key="namespace/json/dataset=data_type_missing_key/object_four.ndjson.gz",
        Body=b"",
    )
    s3_client.put_object(
        Bucket=raw_bucket_name,
        Key="namespace/json/dataset=data_type_one/incorrect_format.ndjson.gz",
        Body=b"",
    )
    s3_client.put_object(
        Bucket=raw_bucket_name,
        Key="namespace/json/owner.txt",
        Body=b"",
    )

    input_bucket_name = "test-input-bucket"
    s3_client.create_bucket(Bucket=input_bucket_name)

    # Create test objects with the expected key pattern
    s3_client.put_object(
        Bucket=input_bucket_name,
        Key="namespace/cohort_one/object_one",
        Body=b"",
    )
    s3_client.put_object(
        Bucket=input_bucket_name,
        Key="namespace/cohort_one/object_two",
        Body=b"",
    )
    s3_client.put_object(
        Bucket=input_bucket_name,
        Key="namespace/cohort_two/object_one",
        Body=b"",
    )

    # Create objects that should be ignored
    s3_client.put_object(
        Bucket=input_bucket_name,
        Key="namespace/owner.txt",
        Body=b"",
    )


@pytest.fixture
def setup_list_files_in_archive_s3(
    s3_client, out_of_range_central_directory, out_of_range_eocd_record
):
    # These objects will be used as part of the `list_files_in_archive` tests
    list_files_in_archive_bucket_name = "list-files-in-archive-bucket"
    s3_client.create_bucket(Bucket=list_files_in_archive_bucket_name)

    # Object with valid EOCD but needs recursion
    s3_client.put_object(
        Bucket=list_files_in_archive_bucket_name,
        Key="valid_but_needs_recursion.zip",
        Body=b"Random data" + b"PK\x05\x06" + b"\x00" * 22,
    )
    # Object without EOCD signature at all
    s3_client.put_object(
        Bucket=list_files_in_archive_bucket_name,
        Key="no_eocd.notazip",
        Body=b"Random data with no EOCD signature",
    )
    # Object with central directory not fully contained
    s3_client.put_object(
        Bucket=list_files_in_archive_bucket_name,
        Key="central_directory_out_of_range.zip",
        Body=out_of_range_central_directory + out_of_range_eocd_record,
    )
    # Object with valid EOCD and central directory contained
    zip_file_data = io.BytesIO()
    with zipfile.ZipFile(zip_file_data, "w") as zip_file:
        zip_file.writestr("file1.txt", "Content of file 1")
        zip_file.writestr("file2.txt", "Content of file 2")
    zip_file_data.seek(0)
    s3_client.put_object(
        Bucket=list_files_in_archive_bucket_name,
        Key="valid.zip",
        Body=zip_file_data.read(),
    )
    # Object with an empty ZIP archive
    empty_zip_data = io.BytesIO()
    with zipfile.ZipFile(empty_zip_data, "w") as empty_zip:
        pass  # Create an empty ZIP archive (no files)
    empty_zip_data.seek(0)
    s3_client.put_object(
        Bucket=list_files_in_archive_bucket_name,
        Key="empty.zip",
        Body=empty_zip_data.read(),
    )

    return list_files_in_archive_bucket_name


@pytest.fixture
def mocked_raw_keys():
    """Fixture to provide a mocked raw_keys dictionary formatted like the output of list_s3_objects."""
    return {
        "data_type_one": {
            "cohort_one": [
                "namespace/json/dataset=data_type_one/cohort=cohort_one/object_one.ndjson.gz",
                "namespace/json/dataset=data_type_one/cohort=cohort_one/object_two.ndjson.gz",
            ],
            "cohort_two": [
                "namespace/json/dataset=data_type_one/cohort=cohort_two/object_three.ndjson.gz",
            ],
        },
        "data_type_two": {
            "cohort_one": [
                "namespace/json/dataset=data_type_two/cohort=cohort_one/object_four.ndjson.gz",
            ]
        },
    }


def test_parse_content_range():
    """Test the parse_content_range function."""

    # Example content range input
    content_range = "bytes 55411335-55476870/55476871"

    # Expected output
    expected_start = 55411335
    expected_end = 55476870
    expected_total = 55476871

    range_start, range_end, total_size = app.parse_content_range(content_range)

    assert range_start == expected_start
    assert range_end == expected_end
    assert total_size == expected_total


def test_unpack_eocd_fields():
    """Test the unpack_eocd_fields function."""

    # Sample EOCD record in bytes
    eocd_record = (
        b"PK\x05\x06"  # EOCD signature (4 bytes)
        b"\x00\x00"  # Number of this disk (2 bytes)
        b"\x00\x00"  # Disk where central directory starts (2 bytes)
        b"\x01\x00"  # Number of central directory records on this disk (2 bytes)
        b"\x01\x00"  # Total number of central directory records (2 bytes)
        b"\x15=\x00\x00"  # Size of central directory (15,637 bytes) (4 bytes)
        b"\x02\x00\x00\x00"  # Offset of start of central directory (2 bytes + 2 padding) (4 bytes)
        b"\x00\x00"  # Comment length (2 bytes)
    )

    expected_central_directory_offset = 2
    expected_central_directory_size = 15637

    central_directory_offset, central_directory_size = app.unpack_eocd_fields(
        body=eocd_record, eocd_offset=0
    )

    assert central_directory_offset == expected_central_directory_offset
    assert central_directory_size == expected_central_directory_size


def test_determine_eocd_offset_with_eocd():
    """Test determine_eocd_offset when EOCD signature is present."""
    # Sample body with an EOCD signature at the end
    eocd = b"PK\x05\x06" + b"\x00" * 24
    zip_data = b"Random data"
    body = zip_data + eocd
    content_range = "bytes 0-0/0"

    # Expected offset (where the EOCD signature starts)
    expected_eocd_offset = len(zip_data)

    # Call the function
    eocd_offset = app.determine_eocd_offset(body, content_range)

    # Assert the EOCD offset is correctly identified
    assert eocd_offset == expected_eocd_offset


def test_determine_eocd_offset_missing_eocd():
    """Test determine_eocd_offset when EOCD signature is not present."""
    # Sample body without an EOCD signature
    body = b"Random data with no EOCD signature"
    content_range = "bytes 0-0/0"

    # Call the function
    eocd_offset = app.determine_eocd_offset(body, content_range)

    # Assert the EOCD offset is -1, indicating not found
    assert eocd_offset == -1, "Expected EOCD offset to be -1 when signature is missing."


unpatched_list_files_in_archive = app.list_files_in_archive


@patch("src.lambda_function.raw_sync.app.list_files_in_archive")
def test_list_files_in_archive_missing_eocd_recursion(
    mock_list_files_in_archive,
    s3_client,
    setup_list_files_in_archive_s3,
):
    """Test if the function is called recursively when EOCD is not found in tail."""
    bucket_name = "list-files-in-archive-bucket"
    key = "valid_but_needs_recursion.zip"

    # Call the original function
    mock_list_files_in_archive.side_effect = (
        lambda *args, **kwargs: unpatched_list_files_in_archive(*args, **kwargs)
    )

    result = app.list_files_in_archive(
        s3_client=s3_client, bucket=bucket_name, key=key, range_size=16
    )

    # Check that the function was called recursively
    assert (
        mock_list_files_in_archive.call_count > 1
    ), "Function was not called recursively."


def test_list_files_in_archive_no_eocd_returns_empty_list(
    s3_client, setup_list_files_in_archive_s3, caplog
):
    """Test if the function returns an empty list when EOCD is not found at all."""
    bucket_name = "list-files-in-archive-bucket"
    key = "no_eocd.notazip"
    with caplog.at_level("ERROR"):
        result = app.list_files_in_archive(s3_client, bucket_name, key, range_size=16)
    assert result == []
    assert len(caplog.text)


@patch("src.lambda_function.raw_sync.app.list_files_in_archive")
def test_list_files_in_archive_recursive_central_directory_out_of_range(
    mock_list_files_in_archive,
    s3_client,
    setup_list_files_in_archive_s3,
    out_of_range_eocd_record,
):
    """Test if the function is called recursively when central directory is not fully contained."""
    bucket_name = "list-files-in-archive-bucket"
    key = "central_directory_out_of_range.zip"

    # Call the original function
    mock_list_files_in_archive.side_effect = (
        lambda *args, **kwargs: unpatched_list_files_in_archive(*args, **kwargs)
    )

    # Set `range_size` to be just slightly larger than the EOCD record
    # This way we won't get the entire central directory, initially
    result = app.list_files_in_archive(
        s3_client, bucket_name, key, range_size=len(out_of_range_eocd_record) + 2
    )

    # Check that the function was called recursively
    assert (
        mock_list_files_in_archive.call_count > 1
    ), "Function was not called recursively when central directory was out of range."


def test_list_files_in_archive_returns_filenames(
    s3_client, setup_list_files_in_archive_s3
):
    """Test if the function returns a list of filenames."""
    bucket_name = "list-files-in-archive-bucket"
    key = "valid.zip"

    result = app.list_files_in_archive(
        s3_client=s3_client, bucket=bucket_name, key=key, range_size=64 * 1024
    )

    assert isinstance(result, list), "Expected result to be a list."
    assert len(result) > 0, "Expected list to contain filenames."
    assert "filename" in result[0]
    assert "file_size" in result[0]
    filenames = [file_object["filename"] for file_object in result]
    assert "file1.txt" in filenames, "Expected 'file1.txt' to be in the result."
    assert "file2.txt" in filenames, "Expected 'file2.txt' to be in the result."


def test_list_files_in_archive_empty_zip(
    s3_client, setup_list_files_in_archive_s3, caplog
):
    """
    Test that an empty ZIP archive returns an empty file list.
    """
    # Retrieve the bucket name from the fixture
    bucket_name = setup_list_files_in_archive_s3
    key = "empty.zip"

    with caplog.at_level("WARNING"):
        result = app.list_files_in_archive(s3_client, bucket=bucket_name, key=key)

    assert result == []
    assert len(caplog.text)


def test_append_s3_key_raw():
    """Test append_s3_key with 'raw' format."""
    key = "namespace/json/dataset=example_data/cohort=example_cohort/file1.json"
    key_format = "raw"
    result = defaultdict(lambda: defaultdict(list))

    # Set up the result dictionary with nested structure
    app.append_s3_key(key, key_format, result)

    # Expected result structure after processing the key
    result = expected_result = {"example_data": {"example_cohort": [key]}}

    # Assert that the key was correctly added to the result dictionary
    assert result == expected_result


def test_append_s3_key_input():
    """Test append_s3_key with 'input' format."""
    key = "namespace/example_cohort/file1.json"
    key_format = "input"
    result = defaultdict(list)

    # Set up the result dictionary with the flat structure
    app.append_s3_key(key, key_format, result)

    # Expected result structure after processing the key
    result = expected_result = {"example_cohort": [key]}

    # Assert that the key was correctly added to the result dictionary
    assert result == expected_result


def test_append_s3_key_stop_iteration():
    """Test that result is unmodified when StopIteration is encountered."""
    key = "namespace/json/invalid_key_structure"
    key_format = "raw"

    # Initial result dictionary (should remain unchanged)
    result = {
        "data_type_one": {
            "cohort_one": [
                "namespace/json/dataset=data_type_one/cohort=cohort_one/file1.json"
            ]
        }
    }
    # Copy the result to check for modifications later
    original_result = result.copy()

    result = app.append_s3_key(key, key_format, result)

    assert (
        result == original_result
    ), "Expected result to remain unmodified on StopIteration."


def test_list_s3_objects_raw_format(s3_client, setup_s3):
    """Test the list_s3_objects function with the "raw" key format."""
    bucket_name = "test-raw-bucket"
    key_prefix = "namespace/json/"
    key_format = "raw"

    expected_output = {
        "data_type_one": {
            "cohort_one": [
                "namespace/json/dataset=data_type_one/cohort=cohort_one/object_one.ndjson.gz"
            ],
            "cohort_two": [
                "namespace/json/dataset=data_type_one/cohort=cohort_two/object_two.ndjson.gz"
            ],
        },
        "data_type_two": {
            "cohort_one": [
                "namespace/json/dataset=data_type_two/cohort=cohort_one/object_three.ndjson.gz"
            ],
        },
    }

    result = app.list_s3_objects(s3_client, bucket_name, key_prefix, key_format)
    assert result == expected_output, f"Expected {expected_output}, but got {result}"


def test_list_s3_objects_input_format(s3_client, setup_s3):
    """Test the list_s3_objects function with the "input" key format."""
    bucket_name = "test-input-bucket"
    key_prefix = "namespace/"
    key_format = "input"

    expected_output = {
        "cohort_one": [
            "namespace/cohort_one/object_one",
            "namespace/cohort_one/object_two",
        ],
        "cohort_two": ["namespace/cohort_two/object_one"],
    }

    result = app.list_s3_objects(s3_client, bucket_name, key_prefix, key_format)
    assert result == expected_output, f"Expected {expected_output}, but got {result}"


def test_match_corresponding_raw_object_found(mocked_raw_keys):
    """Test when a matching key is found."""
    namespace = "namespace"
    data_type = "data_type_one"
    cohort = "cohort_one"
    file_identifier = "object_one"

    # Expected matching key
    expected_key = f"{namespace}/json/dataset={data_type}/cohort={cohort}/{file_identifier}.ndjson.gz"

    result = app.match_corresponding_raw_object(
        data_type=data_type,
        cohort=cohort,
        expected_key=expected_key,
        raw_keys=mocked_raw_keys,
    )
    assert result == expected_key


def test_match_corresponding_raw_object_non_matching_data_type(mocked_raw_keys):
    """Test when there is no match due to a non-matching data type."""
    namespace = "namespace"
    data_type = "fake_data_type"
    cohort = "cohort_one"
    file_identifier = "object_one"

    # Expected matching key
    expected_key = f"{namespace}/json/dataset={data_type}/cohort={cohort}/{file_identifier}.ndjson.gz"

    result = app.match_corresponding_raw_object(
        data_type=data_type,
        cohort=cohort,
        expected_key=expected_key,
        raw_keys=mocked_raw_keys,
    )
    assert result is None


def test_match_corresponding_raw_object_non_matching_cohort(mocked_raw_keys):
    """Test when there is no match due to a non-matching cohort."""
    namespace = "namespace"
    data_type = "data_type_one"
    cohort = "fake_cohort"
    file_identifier = "object_four"

    # Expected matching key
    expected_key = f"{namespace}/json/dataset={data_type}/cohort={cohort}/{file_identifier}.ndjson.gz"

    result = app.match_corresponding_raw_object(
        data_type=data_type,
        cohort=cohort,
        expected_key=expected_key,
        raw_keys=mocked_raw_keys,
    )
    assert result is None


def test_match_corresponding_raw_object_non_matching_file_identifier(mocked_raw_keys):
    """Test when there is no match due to a non-matching file identifier."""
    namespace = "namespace"
    data_type = "data_type_one"
    cohort = "cohort_one"
    file_identifier = "nonexistent_file"

    # Expected matching key
    expected_key = f"{namespace}/json/dataset={data_type}/cohort={cohort}/{file_identifier}.ndjson.gz"

    result = app.match_corresponding_raw_object(
        data_type=data_type,
        cohort=cohort,
        expected_key=expected_key,
        raw_keys=mocked_raw_keys,
    )
    assert result is None


@mock_sns
@mock_sqs
def test_publish_to_sns_with_sqs_subscription():
    """
    Test publish_to_sns by subscribing an SQS queue to an SNS topic and verifying
    that the message was delivered to the SQS queue.
    """

    # Step 1: Set up the mock SNS environment
    sns_client = boto3.client("sns", region_name="us-east-1")
    response = sns_client.create_topic(Name="test-topic")
    sns_arn = response["TopicArn"]

    # Step 2: Set up the mock SQS environment
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    sqs_response = sqs_client.create_queue(QueueName="test-queue")
    sqs_url = sqs_response["QueueUrl"]

    # Get the SQS queue ARN
    sqs_arn_response = sqs_client.get_queue_attributes(
        QueueUrl=sqs_url, AttributeNames=["QueueArn"]
    )
    sqs_arn = sqs_arn_response["Attributes"]["QueueArn"]

    # Step 3: Subscribe the SQS queue to the SNS topic
    sns_client.subscribe(TopicArn=sns_arn, Protocol="sqs", Endpoint=sqs_arn)

    # Step 4: Input parameters for the function
    bucket = "test-bucket"
    key = "export/file1.json"
    path = "file1.json"
    file_size = 12345

    # Step 5: Call the function to publish to SNS
    app.publish_to_sns(bucket, key, path, file_size, sns_arn)

    # Step 6: Poll the SQS queue for the message
    messages = sqs_client.receive_message(QueueUrl=sqs_url, MaxNumberOfMessages=1)

    # Step 7: Assert the message was published to the SQS queue
    assert "Messages" in messages, "No messages were found in the SQS queue."

    # Step 8: Verify the message content
    message_body = json.loads(messages["Messages"][0]["Body"])
    sns_message = json.loads(message_body["Message"])

    # Expected message payload
    expected_message = {
        "Bucket": bucket,
        "Key": key,
        "Path": path,
        "FileSize": file_size,
    }

    assert (
        sns_message == expected_message
    ), f"Expected {expected_message}, got {sns_message}"

    # Step 9: Cleanup (delete the message from the queue to avoid polluting future tests)
    sqs_client.delete_message(
        QueueUrl=sqs_url, ReceiptHandle=messages["Messages"][0]["ReceiptHandle"]
    )


def test_get_data_type_from_path_simple():
    """Test a path with no subtype or 'Deleted' component"""
    path = "path/to/FitbitIntradayCombined_20241111-20241112.json"
    data_type = app.get_data_type_from_path(path=path)
    assert data_type == "FitbitIntradayCombined"


def test_get_data_type_from_path_subtype():
    """Test a path with a subtype"""
    path = "path/to/HealthKitV2Samples_AppleStandTime_20241111-20241112.json"
    data_type = app.get_data_type_from_path(path=path)
    assert data_type == "HealthKitV2Samples"


def test_get_data_type_from_path_deleted():
    """Test a path with a subtype and a 'Deleted' component"""
    path = "path/to/HealthKitV2Samples_AppleStandTime_Deleted_20241111-20241112.json"
    data_type = app.get_data_type_from_path(path=path)
    assert data_type == "HealthKitV2Samples_Deleted"


import os


def test_get_expected_raw_key_case1():
    raw_key_prefix = "test-raw_key_prefix/json"
    data_type = "test-data-type"
    cohort = "test-cohort"
    path = "path/to/FitbitIntradayCombined_20241111-20241112.json"
    expected_key = f"{raw_key_prefix}/dataset={data_type}/cohort={cohort}/FitbitIntradayCombined_20241111-20241112.ndjson.gz"
    assert (
        app.get_expected_raw_key(raw_key_prefix, data_type, cohort, path)
        == expected_key
    )


def test_get_expected_raw_key_case2():
    raw_key_prefix = "test-raw_key_prefix/json"
    data_type = "test-data-type"
    cohort = "test-cohort"
    path = "path/to/HealthKitV2Samples_AppleStandTime_20241111-20241112.json"
    expected_key = f"{raw_key_prefix}/dataset={data_type}/cohort={cohort}/HealthKitV2Samples_AppleStandTime_20241111-20241112.ndjson.gz"
    assert (
        app.get_expected_raw_key(raw_key_prefix, data_type, cohort, path)
        == expected_key
    )


def test_get_expected_raw_key_case3():
    raw_key_prefix = "test-raw_key_prefix/json"
    data_type = "test-data-type"
    cohort = "test-cohort"
    path = "path/to/HealthKitV2Samples_AppleStandTime_Deleted_20241111-20241112.json"
    expected_key = f"{raw_key_prefix}/dataset={data_type}/cohort={cohort}/HealthKitV2Samples_AppleStandTime_Deleted_20241111-20241112.ndjson.gz"
    assert (
        app.get_expected_raw_key(raw_key_prefix, data_type, cohort, path)
        == expected_key
    )
