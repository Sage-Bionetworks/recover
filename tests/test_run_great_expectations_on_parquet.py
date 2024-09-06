from unittest.mock import MagicMock, patch

import great_expectations
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.yaml_handler import YAMLHandler

from src.glue.jobs import run_great_expectations_on_parquet as run_gx_on_pq


def test_create_context():
    with (
        patch.object(great_expectations, "get_context") as mock_get_context,
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


def test_add_datasource():
    mock_context = MagicMock()
    yaml_handler = YAMLHandler()

    result_context = run_gx_on_pq.add_datasource(mock_context)

    # Verify that the datasource was added
    mock_context.add_datasource.assert_called_once()
    assert result_context == mock_context


def test_add_validation_stores():
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
        mock_add_store.assert_any_call(
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

        # Verify that the evaluation parameter store is added
        mock_add_store.assert_any_call(
            "evaluation_parameter_store",
            {
                "class_name": "EvaluationParameterStore",
            },
        )

        assert result_context == mock_context


def test_get_spark_df():
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
        expected_path = (
            f"s3://{parquet_bucket}/{namespace}/parquet/dataset_{data_type}/"
        )
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


def test_read_json():
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


def test_add_expectations_from_json():
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
