import unittest

import great_expectations as gx
import pyspark
import pytest

from src.glue.jobs import run_great_expectations_on_parquet as run_gx_on_pq


@pytest.fixture
def gx_context(scope="function"):
    context = gx.get_context()
    yield context


@pytest.fixture(scope="function")
def spark_session():
    yield pyspark.sql.SparkSession.builder.appName("BatchRequestTest").getOrCreate()


def test_get_spark_df_has_expected_calls():
    glue_context = unittest.mock.MagicMock()
    mock_dynamic_frame = unittest.mock.MagicMock()
    mock_spark_df = unittest.mock.MagicMock()
    mock_dynamic_frame.toDF.return_value = mock_spark_df

    with unittest.mock.patch.object(
        glue_context, "create_dynamic_frame_from_options"
    ) as mock_create_dynamic_frame:
        mock_create_dynamic_frame.return_value = mock_dynamic_frame

        parquet_bucket = "test-bucket"
        namespace = "test-namespace"
        data_type = "test-data"

        result_df = run_gx_on_pq.get_spark_df(
            glue_context, parquet_bucket, namespace, data_type
        )

        # Verify the S3 path and the creation of the DynamicFrame
        expected_path = f"s3://test-bucket/test-namespace/parquet/dataset_test-data/"
        mock_create_dynamic_frame.assert_called_once_with(
            connection_type="s3",
            connection_options={"paths": [expected_path]},
            format="parquet",
        )

        # Verify the conversion to DataFrame
        assert result_df == mock_spark_df


def test_get_batch_request(gx_context):
    spark_dataset = unittest.mock.MagicMock()
    data_type = "test-data"
    batch_request = run_gx_on_pq.get_batch_request(
        gx_context=gx_context, spark_dataset=spark_dataset, data_type=data_type
    )
    assert isinstance(batch_request, gx.datasource.fluent.batch_request.BatchRequest)


def test_read_json_correctly_returns_expected_values():
    s3_bucket = "test-bucket"
    key = "test-key"

    # Mock the S3 response
    mock_s3_response = unittest.mock.MagicMock()
    mock_s3_response["Body"].read.return_value = '{"test_key": "test_value"}'.encode(
        "utf-8"
    )

    # Use patch to mock the boto3 s3 client
    with unittest.mock.patch("boto3.client") as mock_s3_client:
        # Mock get_object method
        mock_s3_client.return_value.get_object.return_value = mock_s3_response

        # Call the function
        result = run_gx_on_pq.read_json(mock_s3_client.return_value, s3_bucket, key)

        # Verify that the S3 client was called with the correct parameters
        mock_s3_client.return_value.get_object.assert_called_once_with(
            Bucket=s3_bucket, Key=key
        )

        # Verify the result
        assert result == {"test_key": "test_value"}


def test_that_add_expectations_from_json_has_expected_call():
    mock_context = unittest.mock.MagicMock()

    # Sample expectations data
    expectations_data = {
        "test-data": {
            "expectation_suite_name": "test_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "test_column"},
                },
            ],
        }
    }

    data_type = "test-data"

    # Call the function
    run_gx_on_pq.add_expectations_from_json(
        expectations_data=expectations_data, context=mock_context
    )

    # Verify expectations were added to the context
    mock_context.add_or_update_expectation_suite.assert_called_once()


@pytest.mark.integration
def test_add_expectations_from_json_adds_details_correctly(gx_context):
    # Mock expectations data
    expectations_data = {
        "user_data": {
            "expectation_suite_name": "user_data_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "user_id"},
                },
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {"column": "age", "min_value": 18, "max_value": 65},
                },
            ],
        }
    }

    # Call the function to add expectations
    run_gx_on_pq.add_expectations_from_json(
        expectations_data=expectations_data, context=gx_context
    )

    # Retrieve the expectation suite to verify that expectations were added
    expectation_suite = gx_context.get_expectation_suite("user_data_suite")

    assert expectation_suite.expectation_suite_name == "user_data_suite"
    assert len(expectation_suite.expectations) == 2

    # Verify the details of the first expectation
    first_expectation = expectation_suite.expectations[0]
    assert first_expectation.expectation_type == "expect_column_to_exist"
    assert first_expectation.kwargs == {"column": "user_id"}

    # Verify the details of the second expectation
    second_expectation = expectation_suite.expectations[1]
    assert second_expectation.expectation_type == "expect_column_values_to_be_between"
    assert second_expectation.kwargs == {
        "column": "age",
        "min_value": 18,
        "max_value": 65,
    }
