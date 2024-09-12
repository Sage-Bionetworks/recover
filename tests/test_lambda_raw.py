import gzip
import hashlib
import io
import zipfile

import boto3
import pytest  # requires pytest-datadir to be installed
from moto import mock_s3

import src.lambda_function.raw.app as app


def test_construct_raw_key():
    path = "path/to/data/FitbitIntradayCombined_20220401-20230112.json"
    key = "main/adults_v1/FitbitIntradayCombined_20220401-20230112.json"
    raw_key_prefix = "json"
    expected_raw_key = (
        "json/dataset=FitbitIntradayCombined/cohort=adults_v1/"
        "FitbitIntradayCombined_20220401-20230112.ndjson.gz"
    )
    result = app.construct_raw_key(path=path, key=key, raw_key_prefix=raw_key_prefix)
    assert result == expected_raw_key


@pytest.fixture
def s3_setup():
    # Fixture to set up a mock S3 client and a test bucket.
    with mock_s3():
        # Set up the mock S3 client and bucket
        s3_client = boto3.client("s3")
        bucket = "test-bucket"
        key = "test-object"
        s3_client.create_bucket(Bucket=bucket)
        yield {
            "s3_client": s3_client,
            "bucket": bucket,
            "key": key,
        }


def test_upload_part_starts_new_multipart_upload(s3_setup):
    # Retrieve the mock S3 client, bucket, and key from the fixture
    s3_client = s3_setup["s3_client"]
    bucket = s3_setup["bucket"]
    key = s3_setup["key"]

    # Act: Call upload_part without an upload_id, expecting it to start a new multipart upload
    body = b"some data chunk"
    result = app.upload_part(s3_client, body, bucket, key)

    # Assert: Verify that a multipart upload was initiated and the part was uploaded
    assert "upload_id" in result
    assert result["part"]["PartNumber"] == 1
    assert "ETag" in result["part"]

    # Additional checks: Ensure the bucket and object exist in the mocked S3
    uploads = s3_client.list_multipart_uploads(Bucket=bucket)
    assert len(uploads.get("Uploads", [])) == 1


def test_upload_part_to_existing_multipart_upload(s3_setup):
    # Retrieve the mock S3 client, bucket, and key from the fixture
    s3_client = s3_setup["s3_client"]
    bucket = s3_setup["bucket"]
    key = s3_setup["key"]

    # Setup: Initiate a multipart upload
    multipart_upload = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = multipart_upload["UploadId"]

    # Act: Upload a part to the existing multipart upload
    body = b"another data chunk"
    part_number = 2
    result = app.upload_part(
        s3_client, body, bucket, key, upload_id=upload_id, part_number=part_number
    )

    # Assert: Verify the correct part was uploaded with the right part number and ETag
    assert result["upload_id"] == upload_id
    assert result["part"]["PartNumber"] == part_number
    assert "ETag" in result["part"]

    # Additional checks: Ensure the part is listed in the multipart upload parts
    parts = s3_client.list_parts(Bucket=bucket, Key=key, UploadId=upload_id)
    assert len(parts["Parts"]) == 1
    assert parts["Parts"][0]["PartNumber"] == part_number


def md5_hash(data):
    """Utility function to compute the MD5 hash of data."""
    hash_md5 = hashlib.md5()
    hash_md5.update(data)
    return hash_md5.hexdigest()


def concatenate_gzip_parts(parts):
    """Concatenate gzip parts into a complete gzip stream."""
    complete_data = io.BytesIO()
    for part in parts:
        complete_data.write(part["data"])
    complete_data.seek(0)
    return complete_data


def test_yield_compressed_data(shared_datadir):
    # Test parameters
    test_file_path = shared_datadir / "2023-01-13T21--08--51Z_TESTDATA"
    # This file is 21 KB uncompressed
    json_file_path = "FitbitIntradayCombined_20230112-20230114.json"
    part_threshold = 1024

    # Load the original JSON file to compare
    with zipfile.ZipFile(test_file_path, "r") as zipf:
        with zipf.open(json_file_path) as original_json_file:
            original_json_data = original_json_file.read()

    # Calculate MD5 of the original uncompressed JSON file
    original_md5 = md5_hash(original_json_data)

    # Run the yield_compressed_data function
    parts = list(
        app.yield_compressed_data(
            io.BytesIO(test_file_path.read_bytes()), json_file_path, part_threshold
        )
    )

    # Verify that each part surpasses the threshold except possibly the last one
    part_sizes = [len(part["data"]) for part in parts]
    for part_size in part_sizes[:-1]:
        assert (
            part_size >= part_threshold
        ), "Part size is smaller than expected threshold."

    # Concatenate all the parts into a complete gzip file
    complete_data = concatenate_gzip_parts(parts)

    # Decompress the complete gzip archive
    with gzip.GzipFile(fileobj=complete_data, mode="rb") as gzip_file:
        decompressed_data = gzip_file.read()

    # Verify that the decompressed data matches the original MD5 hash
    decompressed_md5 = md5_hash(decompressed_data)
    assert (
        decompressed_md5 == original_md5
    ), "Decompressed data does not match the original file."


def test_compressed_data_wrapper():
    # Setup: Create a BytesIO object with some sample data and define a chunk number
    sample_data = b"compressed data"
    chunk_number = 1
    compressed_data = io.BytesIO(sample_data)

    # Act: Call the function with the sample data and chunk number
    result = app.compressed_data_wrapper(compressed_data, chunk_number)

    # Assert: Verify the output matches the expected format and content
    expected_result = {
        "data": sample_data,
        "chunk_number": chunk_number,
    }
    assert result == expected_result
