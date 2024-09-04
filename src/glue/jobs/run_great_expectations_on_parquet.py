import logging
import sys

import great_expectations as gx
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.yaml_handler import YAMLHandler
from pyspark.context import SparkContext

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def read_args() -> dict:
    """Returns the specific params that our code needs to run"""
    args = getResolvedOptions(
        sys.argv,
        [
            "data-type",
            "namespace",
            "parquet-bucket",
            "expectation-suite-path" "report-prefix",
        ],
    )
    for arg in args:
        validate_args(args[arg])
    return args


def validate_args(value: str) -> None:
    """Checks to make sure none of the input command line arguments are empty strings

    Args:
        value (str): the value of the command line argument parsed by argparse

    Raises:
        ValueError: when value is an empty string
    """
    if value == "":
        raise ValueError("Argument value cannot be an empty string")
    else:
        return None


def create_context(s3_bucket: str, namespace: str, report_prefix: str):
    context = gx.get_context()
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")
    yaml = YAMLHandler()
    context.add_datasource(
        **yaml.load(
            """
        name: my_spark_datasource
        class_name: Datasource
        execution_engine:
            class_name: SparkDFExecutionEngine
            force_reuse_spark_context: true
        data_connectors:
            my_runtime_data_connector:
                class_name: RuntimeDataConnector
                batch_identifiers:
                    - my_batch_identifier
        """
        )
    )
    # Programmatically configure the validation result store and
    # DataDocs to use S3
    context.add_store(
        "validation_result_store",
        {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": s3_bucket,
                "prefix": f"{namespace}/{report_prefix}",
            },
        },
    )

    # Add DataDocs site configuration to output results to S3
    context.add_site_builder(
        site_name="s3_site",
        site_config={
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": s3_bucket,
                "prefix": f"{namespace}/{report_prefix}",
            },
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
            },
        },
    )

    return context


def get_spark_df(glue_context, parquet_bucket, namespace, datatype):
    s3_parquet_path = f"s3://{parquet_bucket}/{namespace}/parquet/dataset_{datatype}/"
    dynamic_frame = glue_context.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [s3_parquet_path]},
        format="parquet",
    )
    spark_df = dynamic_frame.toDF()
    return spark_df


def get_batch_request(spark_dataset):
    batch_request = RuntimeBatchRequest(
        datasource_name="my_spark_datasource",
        data_connector_name="my_runtime_data_connector",
        data_asset_name="my-parquet-data-asset",
        runtime_parameters={"batch_data": spark_dataset},
        batch_identifiers={"my_batch_identifier": "okaybatchidentifier"},
    )
    return batch_request


def add_expectations(data_context, expectation_suite_name):
    existing_expectations = data_context.get_expectation_suite(
        expectation_suite_name
    ).expectations
    non_null_expectation = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "ParticipantIdentifier"},
    )
    data_context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name,
        expectations=[non_null_expectation, *existing_expectations],
    )


import json


def add_expectations_from_json(data_context, expectations_path: str, data_type: str):
    # Load the JSON file with the expectations
    with open(expectations_path, "r") as file:
        expectations_data = json.load(file)

    # Ensure the dataset exists in the JSON file
    if data_type not in expectations_data:
        raise ValueError(f"Dataset '{data_type}' not found in the JSON file.")

    # Extract the expectation suite and expectations for the dataset
    suite_data = expectations_data[data_type]
    expectation_suite_name = suite_data["expectation_suite_name"]
    new_expectations = suite_data["expectations"]

    # Fetch existing expectations from the data context
    existing_suite = data_context.get_expectation_suite(expectation_suite_name)
    existing_expectations = existing_suite.expectations

    # Convert new expectations from JSON format to ExpectationConfiguration objects
    new_expectations_configs = [
        ExpectationConfiguration(
            expectation_type=exp["expectation_type"], kwargs=exp["kwargs"]
        )
        for exp in new_expectations
    ]

    # Combine existing expectations with new ones
    updated_expectations = existing_expectations + new_expectations_configs

    # Update the expectation suite in the data context
    data_context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name, expectations=updated_expectations
    )


def main():
    # args = read_args()
    import pdb

    pdb.set_trace()
    args = {
        "parquet_bucket": "recover-dev-processed-data",
        "namespace": "etl-616",
        "data_type": "healthkitv2heartbeats_expectations",
        "expectation_suite_path": "recover-dev-cloudformation/etl-616/src/glue/resources/data_values_expectations.json",
    }
    context = create_context(
        s3_bucket=args["parquet_bucket"],
        namespace=args["namespace"],
        report_prefix=f"great_expectation_reports/{args['data_type']}/parquet/",
    )
    glue_context = GlueContext(SparkContext.getOrCreate())
    logger.info("get_spark_df")
    spark_df = get_spark_df(
        glue_context=glue_context,
        parquet_bucket=args["parquet_bucket"],
        namespace=args["namespace"],
        datatype=args["data_type"],
    )
    logger.info("isNull")
    null_rows = spark_df.ParticipantIdentifier.isNull()
    logger.info("filter")
    filtered_results = spark_df.filter(null_rows)
    logger.info("collect")
    result = filtered_results.collect()
    logger.info("null_rows: %s", result)
    logger.info("get_batch_request")
    batch_request = get_batch_request(spark_df)
    logger.info("add_expectations")
    # add_expectations(
    #        data_context=context,
    #        expectation_suite_name="my_expectation_suite"
    # )
    add_expectations_from_json(
        data_context=context,
        json_file_path=args["expectation_suite_path"],
        data_type=args["data_type"],
    )
    logger.info("get_validator")
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="my_expectation_suite"
    )
    logger.info("validator.validate")
    validation_result = validator.validate()
    logger.info("validation_result: %s", validation_result)


if __name__ == "__main__":
    main()
