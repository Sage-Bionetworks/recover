import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict

import boto3
import great_expectations as gx
import pyspark
import yaml
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

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
            "gx-config-key",
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
    return None


def configure_gx_config(
    gx_config_bucket: str,
    gx_config_key: str,
    shareable_artifacts_bucket: str,
    namespace: str,
) -> dict:
    """Download and configure a `great_expectations.yml` file locally

    This function will download a `great_expectations.yml` file from S3 to a `gx` directory.
    This file will be automatically be used to configue the GX data context when calling
    `gx.get_context()`.

    Args:
        gx_config_bucket (str): S3 bucket containing the `great_expectations.yml` file.
        gx_config_key (str): S3 key where this file is located.
        shareable_artifacts_bucket (str): S3 bucket where shareable artifacts are written.
        namespace (str): The current namespace
    """
    gx_config_path = "gx/great_expectations.yml"
    os.makedirs("gx", exist_ok=True)
    s3_client = boto3.client("s3")
    logger.info(
        f"Downloading s3://{gx_config_bucket}/{gx_config_key} to {gx_config_path}"
    )
    s3_client.download_file(
        Bucket=gx_config_bucket, Key=gx_config_key, Filename=gx_config_path
    )
    with open(gx_config_path, "rb") as gx_config_obj:
        gx_config = yaml.safe_load(gx_config_obj)
    # fmt: off
    gx_config["stores"]["validations_store"]["store_backend"]["bucket"] = (
        shareable_artifacts_bucket
    )
    gx_config["stores"]["validations_store"]["store_backend"]["prefix"] = (
        gx_config["stores"]["validations_store"]["store_backend"]["prefix"].format(
            namespace=namespace
        )
    )
    gx_config["data_docs_sites"]["s3_site"]["store_backend"]["bucket"] = (
        shareable_artifacts_bucket
    )
    gx_config["data_docs_sites"]["s3_site"]["store_backend"]["prefix"] = (
        gx_config["data_docs_sites"]["s3_site"]["store_backend"]["prefix"].format(
            namespace=namespace
        )
    )
    # fmt: on
    with open(gx_config_path, "w", encoding="utf-8") as gx_config_obj:
        yaml.dump(gx_config, gx_config_obj)
    return gx_config


def get_spark_df(
    glue_context: GlueContext, parquet_bucket: str, namespace: str, data_type: str
) -> "pyspark.sql.dataframe.DataFrame":
    """
    Read a data-type-specific Parquet dataset

    Args:
        glue_context (GlueContext): The AWS Glue context object
        parquet_bucket (str): The S3 bucket containing the data-type-specific Parquet dataset
        namespace (str): The associated namespace
        data_type (str): The associated data type

    Returns:
        pyspark.sql.dataframe.DataFrame: A Spark dataframe over our data-type-specific Parquet dataset
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
    gx_context: gx.data_context.AbstractDataContext,
    spark_dataset: "pyspark.sql.dataframe.DataFrame",
    data_type: str,
) -> gx.datasource.fluent.batch_request.BatchRequest:
    """
    Get a GX batch request over a Spark dataframe

    Args:
        spark_dataset (pyspark.sql.dataframe.DataFrame): A Spark dataframe
        data_type (str): The data type

    Returns:
        BatchRequest: A batch request which can be used in conjunction
            with an expectation suite to validate our data.
    """
    data_source = gx_context.sources.add_or_update_spark(name="parquet")
    data_asset = data_source.add_dataframe_asset(name=f"{data_type}_spark_dataframe")
    batch_request = data_asset.build_batch_request(dataframe=spark_dataset)
    return batch_request


def read_json(
    s3: boto3.client,
    s3_bucket: str,
    key: str,
) -> Dict[str, str]:
    """
    Read a JSON file from an S3 bucket

    Args:
        s3 (boto3.client): An S3 client
        s3_bucket (str): The S3 bucket containing the JSON file
        key (str): The S3 key of the JSON file

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
    context: gx.data_context.AbstractDataContext,
) -> gx.data_context.AbstractDataContext:
    """
    Add an expectation suite with expectations to our GX data context for each data type.

    Args:
        expectations_data (Dict[str, str]): A mapping of data types to their expectations.
            The expectations should be formatted like so:

                {
                    "expectation_suite_name": "string",
                    "expectations": {
                        "expectation_type": "str",
                        "kwargs": "readable by `ExpectationConfiguration`"
                    }
                }
        context (gx.data_context.AbstractDataContext): context object

    Returns:
        gx.data_context.AbstractDataContext: A GX data context object with expectation suites added
    """
    for data_type in expectations_data:
        suite_data = expectations_data[data_type]
        expectation_suite_name = suite_data["expectation_suite_name"]
        new_expectations = suite_data["expectations"]

        # Convert new expectations from dict to ExpectationConfiguration objects
        new_expectations_configs = [
            gx.core.expectation_configuration.ExpectationConfiguration(
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


def main():
    args = read_args()
    s3 = boto3.client("s3")
    run_id = gx.core.run_identifier.RunIdentifier(
        run_name=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    expectation_suite_name = f"{args['data_type']}_expectations"

    # Set up Great Expectations
    logger.info("configure_gx_config")
    configure_gx_config(
        gx_config_bucket=args["cfn_bucket"],
        gx_config_key=args["gx_config_key"],
        shareable_artifacts_bucket=args["shareable_artifacts_bucket"],
        namespace=args["namespace"],
    )
    gx_context = gx.get_context()
    logger.info("reads_expectations_from_json")
    expectations_data = read_json(
        s3=s3,
        s3_bucket=args["cfn_bucket"],
        key=args["expectation_suite_key"],
    )
    logger.info("adds_expectations_from_json")
    gx_context = add_expectations_from_json(
        expectations_data=expectations_data,
        context=gx_context,
    )

    # Set up Spark
    glue_context = GlueContext(pyspark.context.SparkContext.getOrCreate())
    logger.info("get_spark_df")
    spark_df = get_spark_df(
        glue_context=glue_context,
        parquet_bucket=args["parquet_bucket"],
        namespace=args["namespace"],
        data_type=args["data_type"],
    )

    # Put the two together and validate the GX expectations
    logger.info("get_batch_request")
    batch_request = get_batch_request(
        gx_context=gx_context, spark_dataset=spark_df, data_type=args["data_type"]
    )
    logger.info("add_or_update_checkpoint")
    # The default checkpoint action list is:
    # StoreValidationResultAction, StoreEvaluationParametersAction, UpdateDataDocsAction
    checkpoint = gx_context.add_or_update_checkpoint(
        name=f"{args['data_type']}-checkpoint",
        expectation_suite_name=expectation_suite_name,
        batch_request=batch_request,
    )
    logger.info("run checkpoint")
    checkpoint_result = checkpoint.run(run_id=run_id)
    logger.info("data docs updated!")


if __name__ == "__main__":
    main()
