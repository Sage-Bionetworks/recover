import boto3
import pytest
import synapseclient
from pyarrow import fs, parquet


@pytest.fixture
def test_parquet_folder():
    parquet_folder = "syn51079888"
    return parquet_folder


@pytest.fixture
def test_synapse_client():
    '''Returns a synapse client from credentials stored in SSM'''
    aws_session = boto3.session.Session(profile_name="default",
                                        region_name="us-east-1")
    ssm_parameter = "synapse-recover-auth"
    if ssm_parameter is not None:
        ssm_client = aws_session.client("ssm")
        token = ssm_client.get_parameter(
            Name=ssm_parameter,
            WithDecryption=True)
        test_synapse_client = synapseclient.Synapse()
        test_synapse_client.login(authToken=token["Parameter"]["Value"])
    else: # try cached credentials
        test_synapse_client = synapseclient.login()
    return test_synapse_client


def test_setup_external_storage_success(test_parquet_folder, test_synapse_client):
    # Get STS credentials
    token = test_synapse_client.get_sts_storage_token(
        entity=test_parquet_folder, permission="read_only", output_format="json"
    )

    # Pass STS credentials to Arrow filesystem interface
    s3 = fs.S3FileSystem(
        access_key=token["accessKeyId"],
        secret_key=token["secretAccessKey"],
        session_token=token["sessionToken"],
        region="us-east-1",
    )

    base_s3_uri = "{}/{}".format(token["bucket"], token["baseKey"])
    parquet_datasets = s3.get_file_info(fs.FileSelector(base_s3_uri, recursive=False))
