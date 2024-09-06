from unittest.mock import MagicMock, patch

import great_expectations as gx
import pytest
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from pyspark.sql import SparkSession

from src.glue.jobs import run_great_expectations_on_parquet as run_gx_on_pq


@pytest.fixture
def test_context(scope="function"):
    context = gx.get_context()
    yield context


@pytest.fixture(scope="function")
def test_spark():
    yield SparkSession.builder.appName("BatchRequestTest").getOrCreate()


def test_create_context():
    with (
        patch.object(gx, "get_context") as mock_get_context,
        patch.object(run_gx_on_pq, "add_datasource") as mock_add_datasource,
        patch.object(
            run_gx_on_pq, "add_validation_stores"
        ) as mock_add_validation_stores,
        patch.object(run_gx_on_pq, "add_data_docs_sites") as mock_add_data_docs_sites,
    ):
        mock_context = MagicMock()
        mock_get_context.return_value = mock_context

        s3_bucket = "test-bucket"
        namespace = "test-namespace"
        key_prefix = "test-prefix"

        # Call the function
        result_context = run_gx_on_pq.create_context(s3_bucket, namespace, key_prefix)

        # Assert that the context returned is the mock context
        assert result_context == mock_context

        # Assert that the other functions were called
        mock_add_datasource.assert_called_once_with(mock_context)
        mock_add_validation_stores.assert_called_once_with(
            mock_context, s3_bucket, namespace, key_prefix
        )
        mock_add_data_docs_sites.assert_called_once_with(
            mock_context, s3_bucket, namespace, key_prefix
        )


def test_that_add_datasource_calls_correctly():
    mock_context = MagicMock()
    result_context = run_gx_on_pq.add_datasource(mock_context)

    # Verify that the datasource was added
    mock_context.add_datasource.assert_called_once()
    assert result_context == mock_context


@pytest.mark.integration
def test_that_add_datasource_adds_correctly(test_context):
    # Assuming you've already added a datasource, you can list it
    run_gx_on_pq.add_datasource(test_context)
    datasources = test_context.list_datasources()

    # Define the expected datasource name
    expected_datasource_name = "spark_datasource"

    # Check that the expected datasource is present and other details are correct
    assert any(
        ds["name"] == expected_datasource_name for ds in datasources
    ), f"Datasource '{expected_datasource_name}' was not added correctly."
    datasource = next(
        ds for ds in datasources if ds["name"] == expected_datasource_name
    )
    assert datasource["class_name"] == "Datasource"
    assert "SparkDFExecutionEngine" in datasource["execution_engine"]["class_name"]


def test_add_validation_stores_has_expected_calls():
    mock_context = MagicMock()
    s3_bucket = "test-bucket"
    namespace = "test-namespace"
    key_prefix = "test-prefix"

    with patch.object(mock_context, "add_store") as mock_add_store:
        # Call the function
        result_context = run_gx_on_pq.add_validation_stores(
            mock_context, s3_bucket, namespace, key_prefix
        )

        # Verify that the validation store is added
        mock_add_store.assert_called_once_with(
            "validation_result_store",
            {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": s3_bucket,
                    "prefix": f"{namespace}/{key_prefix}",
                },
            },
        )

        assert result_context == mock_context


@pytest.mark.integration
def test_validation_store_details(test_context):
    # Mock context and stores
    run_gx_on_pq.add_validation_stores(
        test_context,
        s3_bucket="test-bucket",
        namespace="test",
        key_prefix="test_folder/",
    )

    # Run the test logic
    stores = test_context.list_stores()
    expected_store_name = "validation_result_store"

    assert any(store["name"] == expected_store_name for store in stores)
    # pulls the store we want
    store_config = [store for store in stores if store["name"] == expected_store_name][
        0
    ]

    assert store_config["class_name"] == "ValidationsStore"
    assert store_config["store_backend"]["class_name"] == "TupleS3StoreBackend"
    assert store_config["store_backend"]["bucket"] == "test-bucket"
    assert store_config["store_backend"]["prefix"] == "test/test_folder/"


def test_get_spark_df_has_expected_calls():
    glue_context = MagicMock()
    mock_dynamic_frame = MagicMock()
    mock_spark_df = MagicMock()
    mock_dynamic_frame.toDF.return_value = mock_spark_df

    with patch.object(
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


def test_get_batch_request():
    spark_dataset = MagicMock()
    data_type = "test-data"
    run_id = RunIdentifier(run_name="2023_09_04")

    batch_request = run_gx_on_pq.get_batch_request(spark_dataset, data_type, run_id)

    # Verify the RuntimeBatchRequest is correctly set up
    assert isinstance(batch_request, RuntimeBatchRequest)
    assert batch_request.data_asset_name == f"{data_type}-parquet-data-asset"
    assert batch_request.batch_identifiers == {
        "batch_identifier": f"{data_type}_{run_id.run_name}_batch"
    }
    assert batch_request.runtime_parameters == {"batch_data": spark_dataset}


@pytest.mark.integration
def test_that_get_batch_request_details_are_correct(test_spark):
    # Create a simple PySpark DataFrame to simulate the dataset
    data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
    columns = ["name", "age"]
    spark_dataset = test_spark.createDataFrame(data, columns)

    # Create a RunIdentifier
    run_id = RunIdentifier(run_name="test_run_2023")

    # Call the function and get the RuntimeBatchRequest
    data_type = "user_data"
    batch_request = run_gx_on_pq.get_batch_request(spark_dataset, data_type, run_id)

    # Assertions to check that the batch request is properly populated
    assert isinstance(batch_request, RuntimeBatchRequest)
    assert batch_request.datasource_name == "spark_datasource"
    assert batch_request.data_connector_name == "runtime_data_connector"
    assert batch_request.data_asset_name == "user_data-parquet-data-asset"
    assert (
        batch_request.batch_identifiers["batch_identifier"]
        == "user_data_test_run_2023_batch"
    )
    assert batch_request.runtime_parameters["batch_data"] == spark_dataset


def test_read_json_correctly_returns_expected_values():
    s3_bucket = "test-bucket"
    key_prefix = "test-prefix"

    # Mock the S3 response
    mock_s3_response = MagicMock()
    mock_s3_response["Body"].read.return_value = '{"test_key": "test_value"}'.encode(
        "utf-8"
    )

    # Use patch to mock the boto3 s3 client
    with patch("boto3.client") as mock_s3_client:
        # Mock get_object method
        mock_s3_client.return_value.get_object.return_value = mock_s3_response

        # Call the function
        result = run_gx_on_pq.read_json(
            mock_s3_client.return_value, s3_bucket, key_prefix
        )

        # Verify that the S3 client was called with the correct parameters
        mock_s3_client.return_value.get_object.assert_called_once_with(
            Bucket=s3_bucket, Key=key_prefix
        )

        # Verify the result
        assert result == {"test_key": "test_value"}


def test_that_add_expectations_from_json_has_expected_call():
    mock_context = MagicMock()

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
    run_gx_on_pq.add_expectations_from_json(expectations_data, mock_context, data_type)

    # Verify expectations were added to the context
    mock_context.add_or_update_expectation_suite.assert_called_once()


@pytest.mark.integration
def test_add_expectations_from_json_adds_details_correctly(test_context):
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

    data_type = "user_data"

    # Call the function to add expectations
    test_context = run_gx_on_pq.add_expectations_from_json(
        expectations_data, test_context, data_type
    )

    # Retrieve the expectation suite to verify that expectations were added
    expectation_suite = test_context.get_expectation_suite("user_data_suite")

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


def test_that_add_validation_results_to_store_has_expected_calls():
    # Mock the EphemeralDataContext and the necessary components
    mock_context = MagicMock()
    mock_expectation_suite = MagicMock()
    mock_context.get_expectation_suite.return_value = mock_expectation_suite
    mock_expectation_suite.expectation_suite_name = "test_suite"

    # Mock the validation result data
    validation_result = {"result": "test_result"}

    # Create a mock batch identifier and run identifier
    mock_batch_identifier = MagicMock(spec=RuntimeBatchRequest)
    mock_run_identifier = MagicMock(spec=RunIdentifier)

    # Call the function with mocked inputs
    result_context = run_gx_on_pq.add_validation_results_to_store(
        context=mock_context,
        expectation_suite_name="test_suite",
        validation_result=validation_result,
        batch_identifier=mock_batch_identifier,
        run_identifier=mock_run_identifier,
    )

    # Assert that the expectation suite was retrieved correctly
    mock_context.get_expectation_suite.assert_called_once_with("test_suite")

    expected_expectation_suite_identifier = ExpectationSuiteIdentifier(
        expectation_suite_name="test_suite"
    )
    expected_validation_result_identifier = ValidationResultIdentifier(
        expectation_suite_identifier=expected_expectation_suite_identifier,
        batch_identifier=mock_batch_identifier,
        run_id=mock_run_identifier,
    )

    # Verify that the validation result was added to the validations store
    mock_context.validations_store.set.assert_called_once_with(
        expected_validation_result_identifier, validation_result
    )

    # Check that the context is returned
    assert result_context == mock_context
