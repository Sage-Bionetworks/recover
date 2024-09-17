import io
import struct
import zipfile
from collections import defaultdict
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_s3

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
        Key="no_eocd.zip",
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


def test_list_files_in_archive_no_eocd_returns_none(
    s3_client, setup_list_files_in_archive_s3
):
    """Test if the function returns an empty list when EOCD is not found at all."""
    bucket_name = "list-files-in-archive-bucket"
    key = "no_eocd.zip"
    result = app.list_files_in_archive(s3_client, bucket_name, key, range_size=16)
    assert result == []


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
    assert "file1.txt" in result, "Expected 'file1.txt' to be in the result."
    assert "file2.txt" in result, "Expected 'file2.txt' to be in the result."


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


def test_match_corresponding_raw_object(mocked_raw_keys):
    """Test the match_corresponding_raw_object function."""

    # Test parameters for a match
    namespace = "namespace"
    data_type = "data_type_one"
    cohort = "cohort_one"
    file_identifier = "object_one"

    # Expected matching key
    expected_key = (
        "namespace/json/dataset=data_type_one/cohort=cohort_one/object_one.ndjson.gz"
    )
    result = app.match_corresponding_raw_object(
        namespace, data_type, cohort, file_identifier, mocked_raw_keys
    )
    assert result == expected_key

    # Test for a non-matching scenario
    file_identifier = "nonexistent_file"
    result = app.match_corresponding_raw_object(
        namespace, data_type, cohort, file_identifier, mocked_raw_keys
    )
    assert result == None, "Expected None for a non-matching file identifier."

    # Test with different data_type and cohort
    data_type = "data_type_two"
    cohort = "cohort_one"
    file_identifier = "object_four"
    expected_key = (
        "namespace/json/dataset=data_type_two/cohort=cohort_one/object_four.ndjson.gz"
    )
    result = app.match_corresponding_raw_object(
        namespace, data_type, cohort, file_identifier, mocked_raw_keys
    )
    assert result == expected_key

    # Test with incorrect cohort
    cohort = "incorrect_cohort"
    result = app.match_corresponding_raw_object(
        namespace, data_type, cohort, file_identifier, mocked_raw_keys
    )
    assert result is None, "Expected None for an incorrect cohort."
