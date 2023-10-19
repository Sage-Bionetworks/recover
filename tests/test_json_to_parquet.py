import json
import os
import time
from unittest.mock import patch

import boto3
import pandas
import pytest
from awsglue.context import GlueContext
from pyspark.sql.session import SparkSession
from src.glue.jobs import json_to_parquet
# requires pytest-datadir to be installed

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
def workspace_key_prefix(namespace, artifact_bucket):
    workspace_key_prefix = os.path.join(
        namespace,
        "tests/test_json_to_parquet"
    )
    yield workspace_key_prefix
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(
            Bucket=artifact_bucket,
            Prefix=workspace_key_prefix
    )
    if "Contents" in response:
        objects_to_delete = []
        for obj in response["Contents"]:
            objects_to_delete.append({"Key": obj["Key"]})
        if objects_to_delete:
            s3_client.delete_objects(
                Bucket=artifact_bucket,
                Delete={"Objects": objects_to_delete}
            )

@pytest.fixture(scope="class")
def parquet_key_prefix(workspace_key_prefix):
    parquet_key_prefix = os.path.join(
        workspace_key_prefix,
        "parquet"
    )
    return parquet_key_prefix

@pytest.fixture(scope="class")
def glue_database_path(artifact_bucket, workspace_key_prefix):
    glue_database_path = os.path.join(
            "s3://",
            artifact_bucket,
            workspace_key_prefix,
            "json"
    )
    return glue_database_path

@pytest.fixture(scope="class")
def glue_database(glue_database_name, glue_database_path):
    glue_client = boto3.client("glue")
    glue_database = glue_client.create_database(
            DatabaseInput={
                "Name": glue_database_name,
                "Description": "A database for pytest unit tests.",
                "LocationUri": glue_database_path
            }
    )
    yield glue_database
    glue_client.delete_database(Name=glue_database_name)

@pytest.fixture(scope="class")
def glue_nested_table_location(glue_database_path, glue_nested_table_name):
    glue_nested_table_location = os.path.join(
            glue_database_path,
            glue_nested_table_name.replace("_", "=", 1)
        ) + "/"
    return glue_nested_table_location

@pytest.fixture(scope="class")
def glue_nested_table(glue_database_name, glue_nested_table_name,
                      glue_nested_table_location):
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
                        {
                            "Name": "GlobalKey",
                            "Type": "string"
                        },
                        {
                            "Name": "ArrayOfObjectsField",
                            "Type": "array<struct<filename:string,timestamp:string>>"
                        },
                        {
                            "Name": "ObjectField",
                            "Type": "struct<filename:string,timestamp:string>"
                        },
                        {
                            "Name": "export_end_date",
                            "Type": "string"
                        }
                    ]
                },
                "PartitionKeys": [
                    {
                        "Name": "cohort",
                        "Type": "string"
                    }
                ],
                "Parameters": {
                    "classification": "json",
                    "compressionType": "none",
                    "typeOfData": "file",
                }
            }
    )
    return glue_table

@pytest.fixture(scope="class")
def glue_flat_table_location(glue_database_path, glue_flat_table_name):
    glue_flat_table_location = os.path.join(
            glue_database_path,
            glue_flat_table_name.replace("_", "=", 1)
        ) + "/"
    return glue_flat_table_location

@pytest.fixture(scope="class")
def glue_flat_inserted_date_table_location(glue_database_path, glue_flat_inserted_date_table_name):
    glue_flat_inserted_date_table_location = os.path.join(
            glue_database_path,
            glue_flat_inserted_date_table_name.replace("_", "=", 1),
        ) + "/"
    return glue_flat_inserted_date_table_location

@pytest.fixture(scope="class")
def glue_flat_table(glue_database_name, glue_flat_table_name,
                    glue_flat_table_location):
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
                        {
                            "Name": "GlobalKey",
                            "Type": "string"
                        },
                        {
                            "Name": "export_end_date",
                            "Type": "string"
                        }
                    ]
                },
                "PartitionKeys": [
                    {
                        "Name": "cohort",
                        "Type": "string"
                    }
                ],
                "Parameters": {
                    "classification": "json",
                    "compressionType": "none",
                    "typeOfData": "file",
                }
            }
    )
    return glue_table

@pytest.fixture(scope="class")
def glue_flat_inserted_date_table(glue_database_name, glue_flat_inserted_date_table_name,
                    glue_flat_inserted_date_table_location):
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
                        {
                            "Name": "GlobalKey",
                            "Type": "string"
                        },
                        {
                            "Name": "InsertedDate",
                            "Type": "string"
                        }
                    ]
                },
                "PartitionKeys": [
                    {
                        "Name": "cohort",
                        "Type": "string"
                    }
                ],
                "Parameters": {
                    "classification": "json",
                    "compressionType": "none",
                    "typeOfData": "file",
                }
            }
    )
    return glue_table

@pytest.fixture(scope="class")
def glue_deleted_table_location(glue_database_path, glue_deleted_table_name):
    glue_deleted_table_location = os.path.join(
            glue_database_path,
            glue_deleted_table_name.replace("_", "=", 1)
        ) + "/"
    return glue_deleted_table_location

@pytest.fixture(scope="class")
def glue_deleted_table(glue_database_name, glue_deleted_table_name,
                       glue_deleted_table_location):
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
                        {
                            "Name": "GlobalKey",
                            "Type": "string"
                        },
                        {
                            "Name": "export_end_date",
                            "Type": "string"
                        }
                    ]
                },
                "PartitionKeys": [
                    {
                        "Name": "cohort",
                        "Type": "string"
                    }
                ],
                "Parameters": {
                    "classification": "json",
                    "compressionType": "none",
                    "typeOfData": "file",
                }
            }
    )
    return glue_table

def upload_test_data_to_s3(path, s3_bucket, table_location, data_cohort):
    s3_client = boto3.client("s3")
    file_basename = os.path.basename(path)
    s3_key = os.path.relpath(
            os.path.join(table_location, f"cohort={data_cohort}", file_basename),
            f"s3://{s3_bucket}"
    )
    s3_client.upload_file(
            Filename=path,
            Bucket=s3_bucket,
            Key=s3_key
    )

@pytest.fixture()
def glue_test_data(glue_flat_table_location, glue_nested_table_location,
                   glue_deleted_table_location, glue_flat_inserted_date_table_location,
                   artifact_bucket, data_cohort, datadir):
    for root, _, files in os.walk(datadir):
        for basename in files:
            if "InsertedDate" in basename:
                this_table_location = glue_flat_inserted_date_table_location
            elif "Flat" in basename and "Deleted" not in basename:
                this_table_location=glue_flat_table_location
            elif "Flat" in basename and "Deleted" in basename:
                this_table_location=glue_deleted_table_location
            elif "Nested" in basename and "Deleted" not in basename:
                this_table_location=glue_nested_table_location
            else:
                raise ValueError("Found test data with an unknown data type and location.")
            absolute_path = os.path.join(root, basename)
            upload_test_data_to_s3(
                    path=absolute_path,
                    s3_bucket=artifact_bucket,
                    table_location=this_table_location,
                    data_cohort=data_cohort
            )

@pytest.fixture(scope="class")
def glue_crawler_role(namespace):
    iam_client = boto3.client("iam")
    role_name=f"{namespace}-pytest-crawler-role"
    glue_service_policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
    s3_read_policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    glue_crawler_role = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": ["glue.amazonaws.com"]
                        },
                        "Action": ["sts:AssumeRole"]
                    }
                ]
            }),
    )
    iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=glue_service_policy_arn
    )
    iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=s3_read_policy_arn
    )
    yield glue_crawler_role["Role"]["Arn"]
    iam_client.detach_role_policy(
            RoleName=role_name,
            PolicyArn=glue_service_policy_arn
    )
    iam_client.detach_role_policy(
            RoleName=role_name,
            PolicyArn=s3_read_policy_arn
    )
    iam_client.delete_role(RoleName=role_name)

@pytest.fixture()
def glue_crawler(glue_database, glue_database_name, glue_flat_table,
                 glue_nested_table, glue_deleted_table, glue_flat_inserted_date_table,
                 glue_flat_table_name, glue_nested_table_name, glue_deleted_table_name,
                 glue_flat_inserted_date_table_name, glue_crawler_role, namespace):
    glue_client = boto3.client("glue")
    crawler_name = f"{namespace}-pytest-crawler"
    time.sleep(10) # give time for the IAM role trust policy to set in
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
                            glue_flat_inserted_date_table_name
                        ]
                    }
                ]
            },
            SchemaChangePolicy={
                "DeleteBehavior": "LOG",
                "UpdateBehavior": "LOG"
            },
            RecrawlPolicy={
                "RecrawlBehavior": "CRAWL_EVERYTHING"
            },
            Configuration=json.dumps({
                "Version":1.0,
                "CrawlerOutput": {
                    "Partitions": {
                        "AddOrUpdateBehavior":"InheritFromTable"
                    }
                },
                "Grouping": {
                    "TableGroupingPolicy":"CombineCompatibleSchemas"
                }
            })
    )
    glue_client.start_crawler(Name=crawler_name)
    response = {"Crawler": {}}
    for _ in range(60): # wait up to 10 minutes for crawler to finish
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

@patch(
        "src.glue.jobs.json_to_parquet.INDEX_FIELD_MAP",
        {"testflatdatatype": ["GlobalKey"],
         "testflatinserteddatedatatype": ["GlobalKey"],
         "testnesteddatatype": ["GlobalKey"]}
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

    def test_get_table_flat(self, glue_database_name,
                            glue_flat_table_name, glue_context):
        flat_table = json_to_parquet.get_table(
                table_name=glue_flat_table_name,
                database_name=glue_database_name,
                glue_context=glue_context
        )
        assert flat_table.count() == 1
        print(flat_table.schema())
        print(flat_table.schema().fields)
        assert len(flat_table.schema().fields) == 3

    def test_get_table_inserted_date(
            self, glue_database_name, glue_flat_inserted_date_table_name, glue_context):
        inserted_date_table = json_to_parquet.get_table(
                table_name=glue_flat_inserted_date_table_name,
                database_name=glue_database_name,
                glue_context=glue_context
        )
        assert inserted_date_table.count() == 1
        assert len(inserted_date_table.schema().fields) == 3

    def test_get_table_nested(self, glue_database_name,
                            glue_nested_table_name, glue_context):
        nested_table = json_to_parquet.get_table(
                table_name=glue_nested_table_name,
                database_name=glue_database_name,
                glue_context=glue_context
        )
        assert nested_table.count() == 1
        assert len(nested_table.schema().fields) == 5

    @pytest.mark.parametrize(
        "table_name,expected",
        [
            ("glue_flat_table_name", False),
            ("glue_nested_table_name", True),
        ],
    )
    def test_has_nested_fields(self, table_name, expected, glue_database_name,
                               glue_context, request):
        table = json_to_parquet.get_table(
                table_name=request.getfixturevalue(table_name),
                database_name=glue_database_name,
                glue_context=glue_context
        )
        table_schema = table.schema()
        assert json_to_parquet.has_nested_fields(table_schema) == expected

    def test_add_index_to_table(self, glue_database_name, glue_database_path,
                                glue_nested_table_name, glue_context):
        nested_table = json_to_parquet.get_table(
                table_name=glue_nested_table_name,
                database_name=glue_database_name,
                glue_context=glue_context
        )
        nested_table_relationalized = nested_table.relationalize(
            root_table_name = glue_nested_table_name,
            staging_path = os.path.join(
                glue_database_path,
                "tmp/"
            )
        )
        tables_with_index = {}
        tables_with_index[glue_nested_table_name] = json_to_parquet.add_index_to_table(
                table_key=glue_nested_table_name,
                table_name=glue_nested_table_name,
                processed_tables=tables_with_index,
                unprocessed_tables=nested_table_relationalized,
                glue_context=glue_context
        )
        assert (
                set(tables_with_index[glue_nested_table_name].schema.fieldNames()) ==
                set(["GlobalKey", "ArrayOfObjectsField", "ObjectField_filename",
                     "ObjectField_timestamp", "export_end_date", "cohort"])
        )
        table_key = f"{glue_nested_table_name}_ArrayOfObjectsField"
        tables_with_index[table_key] = json_to_parquet.add_index_to_table(
                table_key=table_key,
                table_name=glue_nested_table_name,
                processed_tables=tables_with_index,
                unprocessed_tables=nested_table_relationalized,
                glue_context=glue_context
        )
        assert (
                set(tables_with_index[table_key].schema.fieldNames()) ==
                set(['id', 'index', 'filename', 'timestamp', 'GlobalKey', "cohort"])
        )
        child_table_with_index = (tables_with_index[table_key]
              .toPandas()
              .sort_values("GlobalKey")
              .reset_index(drop=True)
              .drop(columns=["id", "index"]))
        print("Child table with index:")
        print(child_table_with_index)
        correct_df = pandas.DataFrame({
            "filename": ["test.json"],
            "timestamp": ["999"],
            "GlobalKey": ["123456789"],
            "cohort": ["adults_v1"]
        })
        print("Correct table:")
        print(correct_df)
        for col in child_table_with_index.columns:
            print(col)
            print(child_table_with_index[col].values == correct_df[col].values)
            assert all(child_table_with_index[col].values == correct_df[col].values)

    def test_write_table_to_s3(self, artifact_bucket, glue_database_name,
                               glue_flat_table_name, glue_context, parquet_key_prefix):
        flat_table = json_to_parquet.get_table(
                table_name=glue_flat_table_name,
                database_name=glue_database_name,
                glue_context=glue_context
        )
        parquet_key = os.path.join(
                    parquet_key_prefix,
                    "dataset_TestFlatDataType"
        )
        json_to_parquet.write_table_to_s3(
                dynamic_frame=flat_table,
                bucket=artifact_bucket,
                key=parquet_key,
                glue_context=glue_context,
                workflow_run_id="testing"
        )
        s3_client = boto3.client("s3")
        parquet_dataset = s3_client.list_objects_v2(
                Bucket=artifact_bucket,
                Prefix=parquet_key)
        assert parquet_dataset["KeyCount"] > 0

    def test_archive_existing_datasets_non_existent(
            self, glue_context, artifact_bucket, parquet_key_prefix):
        s3_client = boto3.client("s3")
        archive_key = os.path.join(
                parquet_key_prefix,
                "archive"
        )
        non_existent_dataset_key_prefix = os.path.join(
                parquet_key_prefix,
                "doesnotexist"
        ) + "/"
        json_to_parquet.archive_existing_datasets(
                glue_context=glue_context,
                bucket=artifact_bucket,
                key_prefix=non_existent_dataset_key_prefix,
                workflow_run_id="testing",
                delete_upon_completion=True
        )
        archive_contents = s3_client.list_objects_v2(
                Bucket=artifact_bucket,
                Prefix=archive_key
        )
        assert archive_contents["KeyCount"] == 0

    def test_archive_existing_datasets_existing(
            self, glue_context, artifact_bucket, parquet_key_prefix):
        s3_client = boto3.client("s3")
        archive_key = os.path.join(
                parquet_key_prefix,
                "archive"
        )
        existing_dataset_key_prefix = os.path.join(
                parquet_key_prefix,
                "dataset_TestFlatDataType"
        ) + "/"
        existing_dataset = s3_client.list_objects_v2(
                Bucket=artifact_bucket,
                Prefix=existing_dataset_key_prefix
        )
        json_to_parquet.archive_existing_datasets(
                glue_context=glue_context,
                bucket=artifact_bucket,
                key_prefix=existing_dataset_key_prefix,
                workflow_run_id="testing",
                delete_upon_completion=False
        )
        archive_contents = s3_client.list_objects_v2(
                Bucket=artifact_bucket,
                Prefix=archive_key
        )
        assert archive_contents["KeyCount"] == existing_dataset["KeyCount"]
        s3_client.delete_objects(
                Bucket=artifact_bucket,
                Delete={"Objects": [
                    {"Key": obj["Key"]} for obj in archive_contents["Contents"]
                ]}
        )

    def test_archive_existing_datasets_delete(
            self, glue_context, artifact_bucket, parquet_key_prefix):
        s3_client = boto3.client("s3")
        existing_dataset_key_prefix = os.path.join(
                parquet_key_prefix,
                "dataset_TestFlatDataType"
        ) + "/"
        json_to_parquet.archive_existing_datasets(
                glue_context=glue_context,
                bucket=artifact_bucket,
                key_prefix=existing_dataset_key_prefix,
                workflow_run_id="testing2",
                delete_upon_completion=True
        )
        deleted_dataset = s3_client.list_objects_v2(
                Bucket=artifact_bucket,
                Prefix=existing_dataset_key_prefix
        )
        assert deleted_dataset["KeyCount"] == 0

    def test_drop_deleted_healthkit_data(self, glue_context, glue_flat_table_name,
                                         glue_database_name):
        table = json_to_parquet.get_table(
                table_name=glue_flat_table_name,
                database_name=glue_database_name,
                glue_context=glue_context
        )
        table_after_drop = json_to_parquet.drop_deleted_healthkit_data(
                glue_context=glue_context,
                table=table,
                glue_database=glue_database_name
        )
        assert table_after_drop.count() == 0
