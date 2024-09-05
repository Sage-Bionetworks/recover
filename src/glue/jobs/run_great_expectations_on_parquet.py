import json
import logging
import sys
from datetime import datetime
from typing import Dict, List

import boto3
import great_expectations as gx
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
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
            "parquet-bucket",
            "shareable-artifacts-bucket",
            "cfn-bucket",
            "namespace",
            "data-type",
            "expectation-suite-key-prefix",
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
    yaml = YAMLHandler()
    context.add_datasource(
        **yaml.load(
            """
        name: spark_datasource
        class_name: Datasource
        execution_engine:
            class_name: SparkDFExecutionEngine
            force_reuse_spark_context: true
        data_connectors:
            runtime_data_connector:
                class_name: RuntimeDataConnector
                batch_identifiers:
                    - batch_identifier
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

    # Add evaluation parameter store
    context.add_store(
        "evaluation_parameter_store",
        {
            "class_name": "EvaluationParameterStore",
        },
    )

    # Update DataDocs sites in the context's configuration
    data_context_config = DataContextConfig()
    data_context_config["data_docs_sites"] = {
        "s3_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": s3_bucket,
                "prefix": f"{namespace}/{report_prefix}",
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        }
    }
    context._project_config["data_docs_sites"] = data_context_config["data_docs_sites"]
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


def get_batch_request(spark_dataset, data_type, run_id):
    batch_request = RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name=f"{data_type}-parquet-data-asset",
        runtime_parameters={"batch_data": spark_dataset},
        batch_identifiers={"batch_identifier": f"{data_type}_{run_id.run_name}_batch"},
    )
    return batch_request


def read_json(
    s3: boto3.client,
    s3_bucket: str,
    expectations_key_prefix: str,
) -> List[str]:
    """ """
    # read in the json filelist
    s3_response_object = s3.get_object(Bucket=s3_bucket, Key=expectations_key_prefix)
    json_content = s3_response_object["Body"].read().decode("utf-8")
    expectations = json.loads(json_content)
    return expectations


def add_expectations_from_json(
    s3_client: boto3.client,
    cfn_bucket: str,
    data_context,
    expectations_key_prefix: str,
    data_type: str,
):
    # Load the JSON file with the expectations
    expectations_data = read_json(
        s3=s3_client,
        s3_bucket=cfn_bucket,
        expectations_key_prefix=expectations_key_prefix,
    )
    # Ensure the dataset exists in the JSON file
    if data_type not in expectations_data:
        raise ValueError(f"Dataset '{data_type}' not found in the JSON file.")

    # Extract the expectation suite and expectations for the dataset
    suite_data = expectations_data[data_type]
    expectation_suite_name = suite_data["expectation_suite_name"]
    new_expectations = suite_data["expectations"]

    # Convert new expectations from JSON format to ExpectationConfiguration objects
    new_expectations_configs = [
        ExpectationConfiguration(
            expectation_type=exp["expectation_type"], kwargs=exp["kwargs"]
        )
        for exp in new_expectations
    ]

    # Update the expectation suite in the data context
    data_context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name,
        expectations=new_expectations_configs,
    )


def main():
    args = read_args()
    run_id = RunIdentifier(run_name=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    s3 = boto3.client("s3")
    context = create_context(
        s3_bucket=args["shareable_artifacts_bucket"],
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
    batch_request = get_batch_request(spark_df, args["data_type"], run_id)
    logger.info("add_expectations")

    add_expectations_from_json(
        s3_client=s3,
        cfn_bucket=args["cfn_bucket"],
        data_context=context,
        expectations_key_prefix=args["expectation_suite_key_prefix"],
        data_type=args["data_type"],
    )
    logger.info("get_validator")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=f"{args['data_type']}_expectations",
    )
    logger.info("validator.validate")
    validation_result = validator.validate()

    logger.info("validation_result: %s", validation_result)

    batch_identifier = batch_request["batch_identifiers"]["batch_identifier"]
    suite = context.get_expectation_suite(f"{args['data_type']}_expectations")
    # Create an ExpectationSuiteIdentifier
    expectation_suite_identifier = ExpectationSuiteIdentifier(
        expectation_suite_name=suite.expectation_suite_name
    )

    # Create a ValidationResultIdentifier using the run_id, expectation suite, and batch identifier
    validation_result_identifier = ValidationResultIdentifier(
        expectation_suite_identifier=expectation_suite_identifier,
        batch_identifier=batch_identifier,
        run_id=run_id,
    )

    context.validations_store.set(validation_result_identifier, validation_result)

    context.build_data_docs(
        site_names=["s3_site"],
    )
    logger.info("data docs saved!")


if __name__ == "__main__":
    main()
