import datetime
import json
import os
import time
from collections import defaultdict
from unittest.mock import patch, MagicMock, ANY

import boto3
import pandas
import pytest
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import Row
from pyspark.sql.session import SparkSession
from src.glue.jobs import json_to_parquet

# requires pytest-datadir to be installed


def configure_mock_client_archive_datasets(
    mock_boto3_client, dataset_key_prefix, artifact_bucket
):
    """
    Performs common operations related to setting up the MagicMock client
    for the various *archive_datasets* tests
    """
    mock_client = mock_boto3_client.return_value
    workflow_run_datetime = datetime.datetime(2023, 1, 1)
    mock_client.get_workflow_run.return_value = {
        "Run": {"StartedOn": workflow_run_datetime}
    }
    objects_to_copy = {
        "Contents": [
            {
                "Size": 0,
                "Key": os.path.join(dataset_key_prefix, "object0"),
                "Bucket": artifact_bucket,
            },
            {
                "Size": 1,
                "Key": os.path.join(dataset_key_prefix, "object1"),
                "Bucket": artifact_bucket,
            },
            {
                "Size": 1,
                "Key": os.path.join(dataset_key_prefix, "object2"),
                "Bucket": artifact_bucket,
            },
        ]
    }
    mock_client.list_objects_v2.return_value = objects_to_copy
    result = {
        "mock_client": mock_client,
        "objects_to_copy": objects_to_copy,
        "workflow_run_datetime": workflow_run_datetime,
    }
    return result


@pytest.fixture()
def spark_session():
    spark_session = SparkSession.builder.getOrCreate()
    return spark_session


@pytest.fixture()
def sample_table_data():
    sample_table_data = {
        "name": ["John", "John", "Jane", "Bob"],
        "age": [25, 25, 30, 22],
        "city": ["New York", "Chicago", "San Francisco", "Los Angeles"],
        "GlobalKey": ["1", "1", "2", "3"],
        "export_end_date": [
            "2023-05-12T00:00:00",
            "2023-05-13T00:00:00",
            "2023-05-13T00:00:00",
            "2023-05-14T00:00:00",
        ],
    }
    return sample_table_data


@pytest.fixture()
def sample_table_inserted_date_data():
    sample_table_data = {
        "name": ["John", "John", "Jane", "Bob", "Bob_2"],
        "age": [25, 25, 30, 22, 40],
        "city": ["New York", "Chicago", "San Francisco", "Los Angeles", "Tucson"],
        "GlobalKey": ["1", "1", "2", "3", "3"],
        "InsertedDate": [
            "2023-05-12T00:00:00",
            "2023-05-13T00:00:00",
            "2023-05-13T00:00:00",
            "2023-05-14T00:00:00",
            "2023-05-14T00:00:00",
        ],
        "export_end_date": [
            "2023-05-14T00:00:00",
            "2023-05-14T00:00:00",
            "2023-05-14T00:00:00",
            "2023-05-14T00:00:00",
            "2023-05-15T00:00:00",
        ],
    }
    return sample_table_data


def create_table(
    table_data,
    table_name,
    spark_session,
    glue_context,
) -> DynamicFrame:
    rows = [
        Row(**record)
        for record in [dict(zip(table_data, t)) for t in zip(*table_data.values())]
    ]
    table_spark = spark_session.createDataFrame(rows)
    table = DynamicFrame.fromDF(
        dataframe=table_spark, glue_ctx=glue_context, name=table_name
    )
    return table

@pytest.fixture()
def flat_data_type(glue_flat_table_name):
    flat_data_type = glue_flat_table_name.split("_")[1]
    return flat_data_type

@pytest.fixture()
def flat_inserted_date_data_type(glue_flat_inserted_date_table_name):
    flat_inserted_date_data_type = glue_flat_inserted_date_table_name.split("_")[1]
    return flat_inserted_date_data_type

@pytest.fixture()
def nested_data_type(glue_nested_table_name):
    nested_data_type = glue_nested_table_name.split("_")[1]
    return nested_data_type

@pytest.fixture()
def sample_table(
    spark_session, sample_table_data, glue_context, glue_flat_table_name
) -> DynamicFrame:
    sample_table = create_table(
        table_data=sample_table_data,
        table_name=glue_flat_table_name,
        spark_session=spark_session,
        glue_context=glue_context,
    )
    return sample_table


@pytest.fixture()
def sample_table_inserted_date(
    spark_session,
    sample_table_inserted_date_data,
    glue_context,
    glue_flat_inserted_date_table_name,
) -> DynamicFrame:
    sample_table_inserted_date = create_table(
        table_data=sample_table_inserted_date_data,
        table_name=glue_flat_inserted_date_table_name,
        spark_session=spark_session,
        glue_context=glue_context,
    )
    return sample_table_inserted_date

@pytest.fixture(scope="class")
def glue_database_name(namespace):
    return f"{namespace}-pytest-database"


@pytest.fixture(scope="class")
def glue_nested_table_name():
    return "dataset_testnesteddatatype"


@pytest.fixture(scope="class")
def glue_flat_table_name():
    return "dataset_testflatdatatype"


@pytest.fixture(scope="class")
def data_cohort():
    return "adults_v1"


@pytest.fixture(scope="class")
def glue_flat_inserted_date_table_name():
    return "dataset_testflatinserteddatedatatype"


@pytest.fixture(scope="class")
def glue_deleted_table_name(glue_flat_table_name):
    return f"{glue_flat_table_name}_deleted"

@pytest.fixture(scope="class")
def glue_deleted_nested_table_name(glue_nested_table_name) -> str:
    return f"{glue_nested_table_name}_deleted"


@pytest.fixture(scope="class")
def workspace_key_prefix(namespace, artifact_bucket):
    workspace_key_prefix = os.path.join(namespace, "tests/test_json_to_parquet")
    yield workspace_key_prefix
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(
        Bucket=artifact_bucket, Prefix=workspace_key_prefix
    )
    if "Contents" in response:
        objects_to_delete = []
        for obj in response["Contents"]:
            objects_to_delete.append({"Key": obj["Key"]})
        if objects_to_delete:
            s3_client.delete_objects(
                Bucket=artifact_bucket, Delete={"Objects": objects_to_delete}
            )


@pytest.fixture(scope="class")
def parquet_key_prefix(workspace_key_prefix):
    parquet_key_prefix = os.path.join(workspace_key_prefix, "parquet")
    return parquet_key_prefix


@pytest.fixture(scope="class")
def glue_database_path(artifact_bucket, workspace_key_prefix):
    glue_database_path = os.path.join(
        "s3://", artifact_bucket, workspace_key_prefix, "json"
    )
    return glue_database_path


@pytest.fixture(scope="class")
def glue_database(glue_database_name, glue_database_path):
    glue_client = boto3.client("glue")
    glue_database = glue_client.create_database(
        DatabaseInput={
            "Name": glue_database_name,
            "Description": "A database for pytest unit tests.",
            "LocationUri": glue_database_path,
        }
    )
    yield glue_database
    glue_client.delete_database(Name=glue_database_name)


@pytest.fixture(scope="class")
def glue_nested_table_location(glue_database_path, glue_nested_table_name):
    glue_nested_table_location = (
        os.path.join(glue_database_path, glue_nested_table_name.replace("_", "=", 1))
        + "/"
    )
    return glue_nested_table_location


@pytest.fixture(scope="class")
def glue_nested_table(
    glue_database_name, glue_nested_table_name, glue_nested_table_location
):
    glue_client = boto3.client("glue")
    glue_table = glue_client.create_table(
        DatabaseName=glue_database_name,
        TableInput={
            "Name": glue_nested_table_name,
            "Description": "A table for pytest unit tests.",
            "Retention": 0,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Location": glue_nested_table_location,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": False,
                "StoredAsSubDirectories": False,
                "Columns": [
                    {"Name": "GlobalKey", "Type": "string"},
                    {
                        "Name": "ArrayOfObjectsField",
                        "Type": "array<struct<filename:string,timestamp:string>>",
                    },
                    {
                        "Name": "ObjectField",
                        "Type": "struct<filename:string,timestamp:string>",
                    },
                    {"Name": "export_end_date", "Type": "string"},
                ],
            },
            "PartitionKeys": [{"Name": "cohort", "Type": "string"}],
            "Parameters": {
                "classification": "json",
                "compressionType": "none",
                "typeOfData": "file",
            },
        },
    )
    return glue_table


@pytest.fixture(scope="class")
def glue_flat_table_location(glue_database_path, glue_flat_table_name):
    glue_flat_table_location = (
        os.path.join(glue_database_path, glue_flat_table_name.replace("_", "=", 1))
        + "/"
    )
    return glue_flat_table_location


@pytest.fixture(scope="class")
def glue_flat_inserted_date_table_location(
    glue_database_path, glue_flat_inserted_date_table_name
):
    glue_flat_inserted_date_table_location = (
        os.path.join(
            glue_database_path,
            glue_flat_inserted_date_table_name.replace("_", "=", 1),
        )
        + "/"
    )
    return glue_flat_inserted_date_table_location


@pytest.fixture(scope="class")
def glue_flat_table(glue_database_name, glue_flat_table_name, glue_flat_table_location):
    glue_client = boto3.client("glue")
    glue_table = glue_client.create_table(
        DatabaseName=glue_database_name,
        TableInput={
            "Name": glue_flat_table_name,
            "Description": "A table for pytest unit tests.",
            "Retention": 0,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Location": glue_flat_table_location,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": False,
                "StoredAsSubDirectories": False,
                "Columns": [
                    {"Name": "GlobalKey", "Type": "string"},
                    {"Name": "export_end_date", "Type": "string"},
                ],
            },
            "PartitionKeys": [{"Name": "cohort", "Type": "string"}],
            "Parameters": {
                "classification": "json",
                "compressionType": "none",
                "typeOfData": "file",
            },
        },
    )
    return glue_table


@pytest.fixture(scope="class")
def glue_flat_inserted_date_table(
    glue_database_name,
    glue_flat_inserted_date_table_name,
    glue_flat_inserted_date_table_location,
):
    glue_client = boto3.client("glue")
    glue_table = glue_client.create_table(
        DatabaseName=glue_database_name,
        TableInput={
            "Name": glue_flat_inserted_date_table_name,
            "Description": "A table for pytest unit tests.",
            "Retention": 0,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Location": glue_flat_inserted_date_table_location,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": False,
                "StoredAsSubDirectories": False,
                "Columns": [
                    {"Name": "GlobalKey", "Type": "string"},
                    {"Name": "InsertedDate", "Type": "string"},
                ],
            },
            "PartitionKeys": [{"Name": "cohort", "Type": "string"}],
            "Parameters": {
                "classification": "json",
                "compressionType": "none",
                "typeOfData": "file",
            },
        },
    )
    return glue_table


@pytest.fixture(scope="class")
def glue_deleted_table_location(glue_database_path, glue_deleted_table_name):
    glue_deleted_table_location = (
        os.path.join(glue_database_path, glue_deleted_table_name.replace("_", "=", 1))
        + "/"
    )
    return glue_deleted_table_location

@pytest.fixture(scope="class")
def glue_deleted_nested_table_location(glue_database_path, glue_deleted_nested_table_name):
    glue_deleted_nested_table_location = (
        os.path.join(glue_database_path, glue_deleted_nested_table_name.replace("_", "=", 1))
        + "/"
    )
    return glue_deleted_nested_table_location

@pytest.fixture(scope="class")
def glue_deleted_table(
    glue_database_name, glue_deleted_table_name, glue_deleted_table_location
):
    glue_client = boto3.client("glue")
    glue_table = glue_client.create_table(
        DatabaseName=glue_database_name,
        TableInput={
            "Name": glue_deleted_table_name,
            "Description": "A table for pytest unit tests.",
            "Retention": 0,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Location": glue_deleted_table_location,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": False,
                "StoredAsSubDirectories": False,
                "Columns": [
                    {"Name": "GlobalKey", "Type": "string"},
                    {"Name": "export_end_date", "Type": "string"},
                ],
            },
            "PartitionKeys": [{"Name": "cohort", "Type": "string"}],
            "Parameters": {
                "classification": "json",
                "compressionType": "none",
                "typeOfData": "file",
            },
        },
    )
    return glue_table

@pytest.fixture(scope="class")
def glue_deleted_nested_table(
    glue_database_name, glue_deleted_nested_table_name, glue_deleted_nested_table_location
):
    glue_client = boto3.client("glue")
    glue_table = glue_client.create_table(
        DatabaseName=glue_database_name,
        TableInput={
            "Name": glue_deleted_nested_table_name,
            "Description": "An empty table for pytest unit tests.",
            "Retention": 0,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Location": glue_deleted_nested_table_location,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": False,
                "StoredAsSubDirectories": False,
                "Columns": [
                    {"Name": "GlobalKey", "Type": "string"},
                    {"Name": "export_end_date", "Type": "string"},
                ],
            },
            "PartitionKeys": [{"Name": "cohort", "Type": "string"}],
            "Parameters": {
                "classification": "json",
                "compressionType": "none",
                "typeOfData": "file",
            },
        },
    )
    return glue_table

def upload_test_data_to_s3(path, s3_bucket, table_location, data_cohort):
    s3_client = boto3.client("s3")
    file_basename = os.path.basename(path)
    s3_key = os.path.relpath(
        os.path.join(table_location, f"cohort={data_cohort}", file_basename),
        f"s3://{s3_bucket}",
    )
    s3_client.upload_file(Filename=path, Bucket=s3_bucket, Key=s3_key)


@pytest.fixture()
def glue_test_data(
    glue_flat_table_location,
    glue_nested_table_location,
    glue_deleted_table_location,
    glue_flat_inserted_date_table_location,
    artifact_bucket,
    data_cohort,
    datadir,
):
    for root, _, files in os.walk(datadir):
        for basename in files:
            if "InsertedDate" in basename:
                this_table_location = glue_flat_inserted_date_table_location
            elif "Flat" in basename and "Deleted" not in basename:
                this_table_location = glue_flat_table_location
            elif "Flat" in basename and "Deleted" in basename:
                this_table_location = glue_deleted_table_location
            elif "Nested" in basename and "Deleted" not in basename:
                this_table_location = glue_nested_table_location
            else:
                raise ValueError(
                    "Found test data with an unknown data type and location."
                )
            absolute_path = os.path.join(root, basename)
            upload_test_data_to_s3(
                path=absolute_path,
                s3_bucket=artifact_bucket,
                table_location=this_table_location,
                data_cohort=data_cohort,
            )


@pytest.fixture(scope="class")
def glue_crawler_role(namespace):
    iam_client = boto3.client("iam")
    role_name = f"{namespace}-pytest-crawler-role"
    glue_service_policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
    s3_read_policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    glue_crawler_role = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": ["glue.amazonaws.com"]},
                        "Action": ["sts:AssumeRole"],
                    }
                ],
            }
        ),
    )
    iam_client.attach_role_policy(RoleName=role_name, PolicyArn=glue_service_policy_arn)
    iam_client.attach_role_policy(RoleName=role_name, PolicyArn=s3_read_policy_arn)
    yield glue_crawler_role["Role"]["Arn"]
    iam_client.detach_role_policy(RoleName=role_name, PolicyArn=glue_service_policy_arn)
    iam_client.detach_role_policy(RoleName=role_name, PolicyArn=s3_read_policy_arn)
    iam_client.delete_role(RoleName=role_name)


@pytest.fixture()
def glue_crawler(
    glue_database,
    glue_database_name,
    glue_flat_table,
    glue_nested_table,
    glue_deleted_table,
    glue_flat_inserted_date_table,
    glue_flat_table_name,
    glue_nested_table_name,
    glue_deleted_table_name,
    glue_flat_inserted_date_table_name,
    glue_crawler_role,
    namespace,
):
    glue_client = boto3.client("glue")
    crawler_name = f"{namespace}-pytest-crawler"
    time.sleep(10)  # give time for the IAM role trust policy to set in
    glue_crawler = glue_client.create_crawler(
        Name=crawler_name,
        Role=glue_crawler_role,
        DatabaseName=glue_database_name,
        Description="A crawler for pytest unit test data.",
        Targets={
            "CatalogTargets": [
                {
                    "DatabaseName": glue_database_name,
                    "Tables": [
                        glue_flat_table_name,
                        glue_deleted_table_name,
                        glue_nested_table_name,
                        glue_flat_inserted_date_table_name,
                    ],
                }
            ]
        },
        SchemaChangePolicy={"DeleteBehavior": "LOG", "UpdateBehavior": "LOG"},
        RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
        Configuration=json.dumps(
            {
                "Version": 1.0,
                "CrawlerOutput": {
                    "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}
                },
                "Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas"},
            }
        ),
    )
    glue_client.start_crawler(Name=crawler_name)
    response = {"Crawler": {}}
    for _ in range(60):  # wait up to 10 minutes for crawler to finish
        # This should take about 5 minutes
        response = glue_client.get_crawler(Name=crawler_name)
        if (
            "LastCrawl" in response["Crawler"]
            and "Status" in response["Crawler"]["LastCrawl"]
            and response["Crawler"]["LastCrawl"]["Status"] == "SUCCEEDED"
        ):
            break
        time.sleep(10)
    yield glue_crawler
    glue_client.delete_crawler(Name=crawler_name)


@pytest.fixture(scope="class")
def glue_context():
    glue_context = GlueContext(SparkSession.builder.getOrCreate())
    return glue_context


@pytest.fixture(scope="class")
def logger_context():
    logger_context = {
        "labels": {
            "glue_table_name": "test_table_name",
            "type": "test_type",
            "job_name": "test_job_name",
        },
        "process.parent.pid": "test_workflow_run_id",
        "process.parent.name": "test_workflow_name",
    }
    return logger_context


@patch(
    "src.glue.jobs.json_to_parquet.INDEX_FIELD_MAP",
    {
        "testflatdatatype": ["GlobalKey"],
        "testflatinserteddatedatatype": ["GlobalKey"],
        "testnesteddatatype": ["GlobalKey"],
    },
)
class TestJsonS3ToParquet:
    def test_upload_test_data(self, glue_test_data):
        """
        Upload test data to S3.
        """
        return

    def test_run_glue_crawler(self, glue_crawler):
        """
        Create and run Glue crawler.
        """
        return

    def test_get_table_flat(
        self,
        glue_database_name,
        glue_flat_table_name,
        glue_context,
        logger_context,
    ):
        flat_table = json_to_parquet.get_table(
            table_name=glue_flat_table_name,
            database_name=glue_database_name,
            glue_context=glue_context,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        assert flat_table.count() == 2
        print(flat_table.schema())
        print(flat_table.schema().fields)
        assert len(flat_table.schema().fields) == 3
        assert not any(
            ["partition_" in field.name for field in flat_table.schema().fields]
        )

    def test_get_table_inserted_date(
        self,
        glue_database_name,
        glue_flat_inserted_date_table_name,
        glue_context,
        logger_context,
    ):
        inserted_date_table = json_to_parquet.get_table(
            table_name=glue_flat_inserted_date_table_name,
            database_name=glue_database_name,
            glue_context=glue_context,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        assert inserted_date_table.count() == 2
        assert len(inserted_date_table.schema().fields) == 4
        assert not any(
            [
                "partition_" in field.name
                for field in inserted_date_table.schema().fields
            ]
        )

    def test_get_table_nested(
        self,
        glue_database_name,
        glue_nested_table_name,
        glue_context,
        logger_context,
    ):
        nested_table = json_to_parquet.get_table(
            table_name=glue_nested_table_name,
            database_name=glue_database_name,
            glue_context=glue_context,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        assert nested_table.count() == 2
        assert len(nested_table.schema().fields) == 5
        assert not any(
            ["partition_" in field.name for field in nested_table.schema().fields]
        )

    def test_drop_table_duplicates(
            self, sample_table, flat_data_type, glue_context, logger_context
    ):
        table_no_duplicates = json_to_parquet.drop_table_duplicates(
            table=sample_table,
            data_type=flat_data_type,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        table_no_duplicates_df = table_no_duplicates.toPandas()
        assert len(table_no_duplicates_df) == 3
        assert "Chicago" in set(table_no_duplicates_df.city)

    def test_drop_table_duplicates_inserted_date(
        self, sample_table_inserted_date, flat_data_type, glue_context, logger_context
    ):
        table_no_duplicates = json_to_parquet.drop_table_duplicates(
            table=sample_table_inserted_date,
            data_type=flat_data_type,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        table_no_duplicates_df = table_no_duplicates.toPandas()
        assert len(table_no_duplicates_df) == 3
        assert set(["John", "Jane", "Bob_2"]) == set(
            table_no_duplicates_df["name"].tolist()
        )
        assert set(["Chicago", "San Francisco", "Tucson"]) == set(
            table_no_duplicates_df["city"].tolist()
        )
        assert set(["1", "2", "3"]) == set(table_no_duplicates_df["GlobalKey"].tolist())
        assert set(
            [
                "2023-05-13T00:00:00",
                "2023-05-14T00:00:00",
            ]
        ) == set(table_no_duplicates_df["InsertedDate"].tolist())
        assert set(
            [
                "2023-05-14T00:00:00",
                "2023-05-15T00:00:00",
            ]
        ) == set(table_no_duplicates_df["export_end_date"].tolist())

    @pytest.mark.parametrize(
        "table_name,expected",
        [
            ("glue_flat_table_name", False),
            ("glue_nested_table_name", True),
        ],
    )
    def test_has_nested_fields(
        self,
        table_name,
        expected,
        glue_database_name,
        glue_context,
        logger_context,
        request,
    ):
        table = json_to_parquet.get_table(
            table_name=request.getfixturevalue(table_name),
            database_name=glue_database_name,
            glue_context=glue_context,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        table_schema = table.schema()
        assert json_to_parquet.has_nested_fields(table_schema) == expected

    def test_add_index_to_table(
        self,
        glue_database_name,
        glue_database_path,
        glue_nested_table_name,
        glue_context,
        logger_context,
    ):
        nested_table = json_to_parquet.get_table(
            table_name=glue_nested_table_name,
            database_name=glue_database_name,
            glue_context=glue_context,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        nested_table_relationalized = nested_table.relationalize(
            root_table_name=glue_nested_table_name,
            staging_path=os.path.join(glue_database_path, "tmp/"),
        )
        tables_with_index = {}
        tables_with_index[glue_nested_table_name] = json_to_parquet.add_index_to_table(
            table_key=glue_nested_table_name,
            table_name=glue_nested_table_name,
            processed_tables=tables_with_index,
            unprocessed_tables=nested_table_relationalized,
        )
        assert set(
            tables_with_index[glue_nested_table_name].schema.fieldNames()
        ) == set(
            [
                "GlobalKey",
                "ArrayOfObjectsField",
                "ObjectField_filename",
                "ObjectField_timestamp",
                "export_end_date",
                "cohort",
            ]
        )
        table_key = f"{glue_nested_table_name}_ArrayOfObjectsField"
        tables_with_index[table_key] = json_to_parquet.add_index_to_table(
            table_key=table_key,
            table_name=glue_nested_table_name,
            processed_tables=tables_with_index,
            unprocessed_tables=nested_table_relationalized,
        )
        assert set(tables_with_index[table_key].schema.fieldNames()) == set(
            ["id", "index", "filename", "timestamp", "GlobalKey", "cohort"]
        )
        child_table_with_index = (
            tables_with_index[table_key]
            .toPandas()
            .sort_values("GlobalKey")
            .reset_index(drop=True)
            .drop(columns=["id", "index"])
        )
        print("Child table with index:")
        print(child_table_with_index)
        correct_df = pandas.DataFrame(
            {
                "filename": ["test.json"],
                "timestamp": ["999"],
                "GlobalKey": ["123456789"],
                "cohort": ["adults_v1"],
            }
        )
        print("Correct table:")
        print(correct_df)
        for col in child_table_with_index.columns:
            print(col)
            print(child_table_with_index[col].values == correct_df[col].values)
            assert all(child_table_with_index[col].values == correct_df[col].values)

    def test_write_table_to_s3(
        self,
        artifact_bucket,
        glue_database_name,
        glue_flat_table_name,
        glue_context,
        parquet_key_prefix,
        logger_context,
    ):
        flat_table = json_to_parquet.get_table(
            table_name=glue_flat_table_name,
            database_name=glue_database_name,
            glue_context=glue_context,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        parquet_key = os.path.join(parquet_key_prefix, "dataset_TestFlatDataType")
        with patch.object(boto3, "client") as mock_boto3_client:
            mock_client = mock_boto3_client.return_value
            mock_client.get_workflow_run.return_value = {
                "Run": {"StartedOn": datetime.datetime(2023, 1, 1)}
            }
            json_to_parquet.write_table_to_s3(
                dynamic_frame=flat_table,
                bucket=artifact_bucket,
                key=parquet_key,
                glue_context=glue_context,
                workflow_name="testing",
                workflow_run_id="testing",
            )
        s3_client = boto3.client("s3")
        parquet_dataset = s3_client.list_objects_v2(
            Bucket=artifact_bucket, Prefix=parquet_key
        )
        assert parquet_dataset["KeyCount"] > 0

    def test_archive_existing_datasets_non_existent(
        self,
        artifact_bucket,
        parquet_key_prefix,
    ):
        non_existent_dataset_key_prefix = (
            os.path.join(parquet_key_prefix, "doesnotexist") + "/"
        )
        with patch.object(boto3, "client") as mock_boto3_client:
            mock_client = mock_boto3_client.return_value
            mock_client.get_workflow_run.return_value = {
                "Run": {"StartedOn": datetime.datetime(2023, 1, 1)}
            }
            mock_client.list_objects_v2.return_value = dict()
            archived_objects = json_to_parquet.archive_existing_datasets(
                bucket=artifact_bucket,
                key_prefix=non_existent_dataset_key_prefix,
                workflow_name="testing",
                workflow_run_id="testing",
                delete_upon_completion=True,
            )
        assert len(archived_objects) == 0

    def test_archive_existing_datasets_existing(
        self,
        artifact_bucket,
        parquet_key_prefix,
    ):
        existing_dataset_key_prefix = (
            os.path.join(parquet_key_prefix, "dataset_TestFlatDataType") + "/"
        )
        with patch.object(boto3, "client") as mock_boto3_client:
            mock_client_config = configure_mock_client_archive_datasets(
                mock_boto3_client=mock_boto3_client,
                dataset_key_prefix=existing_dataset_key_prefix,
                artifact_bucket=artifact_bucket,
            )
            mock_client = mock_client_config["mock_client"]
            archived_objects = json_to_parquet.archive_existing_datasets(
                bucket=artifact_bucket,
                key_prefix=existing_dataset_key_prefix,
                workflow_name="testing",
                workflow_run_id="testing",
                delete_upon_completion=False,
            )
            for this_object in [
                obj
                for obj in mock_client_config["objects_to_copy"]["Contents"]
                if obj["Size"] > 0
            ]:
                mock_client.copy_object.assert_any_call(
                    CopySource={
                        "Bucket": this_object["Bucket"],
                        "Key": this_object["Key"],
                    },
                    Bucket=artifact_bucket,
                    Key=ANY,
                )
            assert len(archived_objects) == 2

    def test_archive_existing_datasets_delete(
        self,
        artifact_bucket,
        parquet_key_prefix,
    ):
        existing_dataset_key_prefix = (
            os.path.join(parquet_key_prefix, "dataset_TestFlatDataType") + "/"
        )
        with patch.object(boto3, "client") as mock_boto3_client:
            mock_client_config = configure_mock_client_archive_datasets(
                mock_boto3_client=mock_boto3_client,
                dataset_key_prefix=existing_dataset_key_prefix,
                artifact_bucket=artifact_bucket,
            )
            mock_client = mock_client_config["mock_client"]
            archived_objects = json_to_parquet.archive_existing_datasets(
                bucket=artifact_bucket,
                key_prefix=existing_dataset_key_prefix,
                workflow_name="testing",
                workflow_run_id="testing",
                delete_upon_completion=True,
            )
            mock_client.delete_objects.assert_called_once_with(
                Bucket=artifact_bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in archived_objects]},
            )

    def test_drop_deleted_healthkit_data_nonempty(
        self,
        glue_context,
        glue_flat_table_name,
        flat_data_type,
        glue_database_name,
        logger_context,
    ):
        table = json_to_parquet.get_table(
            table_name=glue_flat_table_name,
            database_name=glue_database_name,
            glue_context=glue_context,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        table_after_drop = json_to_parquet.drop_deleted_healthkit_data(
            glue_context=glue_context,
            table=table.toDF(),
            table_name=table.name,
            data_type=flat_data_type,
            glue_database=glue_database_name,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        assert table_after_drop.count() == 0

    def test_drop_deleted_healthkit_data_empty(
        self,
        glue_context,
        glue_nested_table_name,
        nested_data_type,
        glue_database_name,
        glue_deleted_nested_table,
        logger_context,
    ):
        # We do not upload any data for the deleted nested data type
        # So we should receive our nested table back unaltered.
        table = json_to_parquet.get_table(
            table_name=glue_nested_table_name,
            database_name=glue_database_name,
            glue_context=glue_context,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        table_after_drop = json_to_parquet.drop_deleted_healthkit_data(
            glue_context=glue_context,
            table=table.toDF(),
            table_name=table.name,
            data_type=nested_data_type,
            glue_database=glue_database_name,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        assert table_after_drop.count() == table.count()

    def test_drop_deleted_healthkit_data_nonexistent_table(
        self,
        glue_context,
        glue_flat_inserted_date_table_name,
        flat_inserted_date_data_type,
        glue_database_name,
        logger_context,
    ):
        table = json_to_parquet.get_table(
            table_name=glue_flat_inserted_date_table_name,
            database_name=glue_database_name,
            glue_context=glue_context,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        glue_client = boto3.client("glue")
        with pytest.raises(glue_client.exceptions.EntityNotFoundException):
            table_after_drop = json_to_parquet.drop_deleted_healthkit_data(
                glue_context=glue_context,
                table=table.toDF(),
                table_name=table.name,
                data_type=flat_inserted_date_data_type,
                glue_database=glue_database_name,
                record_counts=defaultdict(list),
                logger_context=logger_context,
            )

    def test_count_records_for_event_empty_table(self, sample_table, logger_context):
        spark_sample_table = sample_table.toDF()
        empty_table = spark_sample_table.filter(spark_sample_table.city == "Atlantis")
        record_counts = json_to_parquet.count_records_for_event(
            table=empty_table,
            event=json_to_parquet.CountEventType.READ,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        assert len(record_counts) == 0

    def test_count_records(self, sample_table, logger_context):
        spark_sample_table = sample_table.toDF()
        record_counts = json_to_parquet.count_records_for_event(
            table=spark_sample_table,
            event=json_to_parquet.CountEventType.READ,
            record_counts=defaultdict(list),
            logger_context=logger_context,
        )
        table_counts = record_counts[logger_context["labels"]["type"]][0]
        table_counts_subset = table_counts.query(
            "export_end_date == '2023-05-13T00:00:00'"
        )
        assert len(table_counts_subset) == 1
        assert table_counts_subset["count"].iloc[0] == 2
        assert all(
            [
                v == logger_context["process.parent.pid"]
                for v in table_counts["workflow_run_id"].values
            ]
        )
        assert all(
            [
                v == logger_context["labels"]["type"]
                for v in table_counts["data_type"].values
            ]
        )
        assert all([v == "READ" for v in table_counts["event"].values])

    def test_store_record_counts(self):
        record_counts = {
            "testing_type": [
                pandas.DataFrame({"a": [1], "b": [2]}),
                pandas.DataFrame({"a": [1], "b": [2]}),
            ],
            "testing_type_2": [pandas.DataFrame({"a": [1], "b": [2]})],
        }
        with patch.object(boto3, "client") as mock_boto3_client:
            mock_client = mock_boto3_client.return_value
            mock_client.get_workflow_run.return_value = {
                "Run": {"StartedOn": datetime.datetime(2023, 1, 1)}
            }
            uploaded_objects = json_to_parquet.store_record_counts(
                record_counts=record_counts,
                parquet_bucket="testing_bucket",
                namespace="testing_namespace",
                workflow_name="testing_workflow",
                workflow_run_id="testing_workflow_run_id",
            )
            mock_client.put_object.assert_called()
            assert len(uploaded_objects) == 2
            assert all([k in uploaded_objects.keys() for k in record_counts.keys()])
