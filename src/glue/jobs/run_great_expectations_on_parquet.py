import json
import logging
import sys
from datetime import datetime
from typing import Dict

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
            "expectation-suite-key",
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


def create_context(
    s3_bucket: str, namespace: str, key_prefix: str
) -> "EphemeralDataContext":
    """Creates the data context and adds stores,
        datasource and data docs configurations

    Args:
        s3_bucket (str): name of s3 bucket to store to
        namespace (str): namespace
        key_prefix (str): s3 key prefix

    Returns:
        EphemeralDataContext: context object with all
            configurations
    """
    context = gx.get_context()
    add_datasource(context)
    add_validation_stores(context, s3_bucket, namespace, key_prefix)
    add_data_docs_sites(context, s3_bucket, namespace, key_prefix)
    return context


def add_datasource(context: "EphemeralDataContext") -> "EphemeralDataContext":
    """Adds the spark datasource

    Args:
        context (EphemeralDataContext): data context to add to

    Returns:
        EphemeralDataContext: data context object with datasource configuration
            added
    """
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
    return context


def add_validation_stores(
    context: "EphemeralDataContext",
    s3_bucket: str,
    namespace: str,
    key_prefix: str,
) -> "EphemeralDataContext":
    """Adds the validation store configurations to the context object

    Args:
        context (EphemeralDataContext): data context to add to
        s3_bucket (str): name of the s3 bucket to save validation results to
        namespace (str): name of the namespace
        key_prefix (str): s3 key prefix to save the
            validation results to

    Returns:
        EphemeralDataContext: data context object with validation stores'
            configuration added
    """
    # Programmatically configure the validation result store and
    # DataDocs to use S3
    context.add_store(
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
    return context


def add_data_docs_sites(
    context: "EphemeralDataContext",
    s3_bucket: str,
    namespace: str,
    key_prefix: str,
) -> "EphemeralDataContext":
    """Adds the data docs sites configuration to the context object
        so data docs can be saved to a s3 location. This is a special
        workaround to add the data docs because we're using EphemeralDataContext
        context objects and they don't store to memory.

    Args:
        context (EphemeralDataContext): data context to add to
        s3_bucket (str): name of the s3 bucket to save gx docs to
        namespace (str): name of the namespace
        key_prefix (str): s3 key prefix to save the
            gx docs to

    Returns:
        EphemeralDataContext: data context object with data docs sites'
            configuration added
    """
    data_context_config = DataContextConfig()
    data_context_config["data_docs_sites"] = {
        "s3_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": s3_bucket,
                "prefix": f"{namespace}/{key_prefix}",
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        }
    }
    context._project_config["data_docs_sites"] = data_context_config["data_docs_sites"]
    return context


def get_spark_df(
    glue_context: GlueContext, parquet_bucket: str, namespace: str, data_type: str
) -> "pyspark.sql.dataframe.DataFrame":
    """Reads in the parquet dataset as a Dynamic Frame and converts it
        to a spark dataframe

    Args:
        glue_context (GlueContext): the aws glue context object
        parquet_bucket (str): the name of the bucket holding parquet files
        namespace (str): the namespace
        data_type (str): the data type name

    Returns:
        pyspark.sql.dataframe.DataFrame: spark dataframe of the read in parquet dataset
    """
    s3_parquet_path = f"s3://{parquet_bucket}/{namespace}/parquet/dataset_{data_type}/"
    dynamic_frame = glue_context.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [s3_parquet_path]},
        format="parquet",
    )
    spark_df = dynamic_frame.toDF()
    return spark_df


def get_batch_request(
    spark_dataset: "pyspark.sql.dataframe.DataFrame",
    data_type: str,
    run_id: RunIdentifier,
) -> RuntimeBatchRequest:
    """Retrieves the unique metadata for this batch request

    Args:
        spark_dataset (pyspark.sql.dataframe.DataFrame): parquet dataset as spark df
        data_type (str): data type name
        run_id (RunIdentifier): contains the run name and
            run time metadata of this batch run

    Returns:
        RuntimeBatchRequest: contains metadata for the batch run request
            to identify this great expectations run
    """
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
    key: str,
) -> Dict[str, str]:
    """Reads in a json object

    Args:
        s3 (boto3.client): s3 client connection
        s3_bucket (str): name of the s3 bucket to read from
        key (str): s3 key prefix of the
            location of the json to read from

    Returns:
        Dict[str, str]: the data read in from json
    """
    # read in the json filelist
    s3_response_object = s3.get_object(Bucket=s3_bucket, Key=key)
    json_content = s3_response_object["Body"].read().decode("utf-8")
    expectations = json.loads(json_content)
    return expectations


def add_expectations_from_json(
    expectations_data: Dict[str, str],
    context: "EphemeralDataContext",
    data_type: str,
) -> "EphemeralDataContext":
    """Adds in the read in expectations to the context object

    Args:
        expectations_data (Dict[str, str]): expectations
        context (EphemeralDataContext): context object
        data_type (str): name of the data type

    Raises:
        ValueError: thrown when no expectations exist for this data type

    Returns:
        EphemeralDataContext: context object with expectations added
    """
    # Ensure the data type exists in the JSON file
    if data_type not in expectations_data:
        raise ValueError(f"No expectations found for data type '{data_type}'")

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
    context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name,
        expectations=new_expectations_configs,
    )
    return context


def add_validation_results_to_store(
    context: "EphemeralDataContext",
    expectation_suite_name: str,
    validation_result: Dict[str, str],
    batch_identifier: RuntimeBatchRequest,
    run_identifier: RunIdentifier,
) -> "EphemeralDataContext":
    """Adds the validation results manually to the validation store.
        This is a workaround for a EphemeralDataContext context object,
        and for us to avoid complicating our folder structure to include
        checkpoints/other more persistent data context object types
        until we need that feature

    Args:
        context (EphemeralDataContext): context object to add results to
        expectation_suite_name (str): name of expectation suite
        validation_result (Dict[str, str]): results outputted by gx
            validator to be stored
        batch_identifier (RuntimeBatchRequest): metadata containing details of
            the batch request
        run_identifier (RunIdentifier): metadata containing details of the gx run

    Returns:
        EphemeralDataContext: context object with validation results added to
    """
    expectation_suite = context.get_expectation_suite(expectation_suite_name)
    # Create an ExpectationSuiteIdentifier
    expectation_suite_identifier = ExpectationSuiteIdentifier(
        expectation_suite_name=expectation_suite.expectation_suite_name
    )

    # Create a ValidationResultIdentifier using the run_id, expectation suite, and batch identifier
    validation_result_identifier = ValidationResultIdentifier(
        expectation_suite_identifier=expectation_suite_identifier,
        batch_identifier=batch_identifier,
        run_id=run_identifier,
    )

    context.validations_store.set(validation_result_identifier, validation_result)
    return context


def main():
    args = read_args()
    run_id = RunIdentifier(run_name=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    expectation_suite_name = f"{args['data_type']}_expectations"
    s3 = boto3.client("s3")
    context = create_context(
        s3_bucket=args["shareable_artifacts_bucket"],
        namespace=args["namespace"],
        key_prefix=f"great_expectation_reports/{args['data_type']}/parquet/",
    )
    glue_context = GlueContext(SparkContext.getOrCreate())
    logger.info("get_spark_df")
    spark_df = get_spark_df(
        glue_context=glue_context,
        parquet_bucket=args["parquet_bucket"],
        namespace=args["namespace"],
        data_type=args["data_type"],
    )
    logger.info("get_batch_request")
    batch_request = get_batch_request(spark_df, args["data_type"], run_id)
    logger.info("add_expectations")

    # Load the JSON file with the expectations
    logger.info("reads_expectations_from_json")
    expectations_data = read_json(
        s3=s3,
        s3_bucket=args["cfn_bucket"],
        key=args["expectation_suite_key"],
    )
    logger.info("adds_expectations_from_json")
    add_expectations_from_json(
        expectations_data=expectations_data,
        context=context,
        data_type=args["data_type"],
    )
    logger.info("get_validator")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    logger.info("validator.validate")
    validation_result = validator.validate()

    logger.info("validation_result: %s", validation_result)

    add_validation_results_to_store(
        context,
        expectation_suite_name,
        validation_result,
        batch_identifier=batch_request["batch_identifiers"]["batch_identifier"],
        run_identifier=run_id,
    )
    context.build_data_docs(
        site_names=["s3_site"],
    )
    logger.info("data docs saved!")


if __name__ == "__main__":
    main()
