import boto3
import pytest
import synapseclient
from pyarrow import fs

from src.scripts.setup_external_storage import setup_external_storage


@pytest.fixture()
def test_synapse_folder_id(pytestconfig):
    return pytestconfig.getoption("test_synapse_folder_id")


@pytest.fixture()
def test_ssm_parameter(pytestconfig):
    return pytestconfig.getoption("test_ssm_parameter")


def test_setup_external_storage_success(test_synapse_folder_id, test_ssm_parameter):
    """This test tests that it can get the STS token credentials and view and list the
    files in the S3 bucket location to verify that it has access"""
    test_synapse_client = setup_external_storage.get_synapse_client(
        ssm_parameter=test_ssm_parameter, aws_session=boto3
    )
    # Get STS credentials
    token = test_synapse_client.get_sts_storage_token(
        entity=test_synapse_folder_id, permission="read_only", output_format="json"
    )
    # Pass STS credentials to Arrow filesystem interface
    s3 = fs.S3FileSystem(
        access_key=token["accessKeyId"],
        secret_key=token["secretAccessKey"],
        session_token=token["sessionToken"],
        region="us-east-1",
    )
    # get file info
    base_s3_uri = "{}/{}".format(token["bucket"], token["baseKey"])
    datasets = s3.get_file_info(fs.FileSelector(base_s3_uri, recursive=False))
