import boto3
import pytest
import synapseclient
from pyarrow import fs

from src.scripts.setup_external_storage import setup_external_storage


@pytest.fixture()
def test_synapse_folder_id(pytestconfig):
    yield pytestconfig.getoption("test_synapse_folder_id")


@pytest.fixture()
def test_sts_permission(pytestconfig):
    yield pytestconfig.getoption("test_sts_permission")


@pytest.fixture()
def test_bucket(pytestconfig):
    yield pytestconfig.getoption("test_bucket")


@pytest.mark.integration()
def test_setup_external_storage_success(
    test_bucket: str,
    namespace: str,
    test_synapse_folder_id: str,
    test_sts_permission: str,
    ssm_parameter : str
):
    """This test tests that it can get the STS token credentials and view and list the
    files in the S3 bucket location to verify that it has access"""
    test_synapse_client = setup_external_storage.get_synapse_client(
        ssm_parameter=ssm_parameter, aws_session=boto3
    )
    # Get STS credentials
    token = test_synapse_client.get_sts_storage_token(
        entity=test_synapse_folder_id,
        permission=test_sts_permission,
        output_format="boto",
    )
    # login with the credentials and list objects
    s3_client = boto3.Session(**token).client("s3")
    s3_client.list_objects_v2(Bucket=test_bucket, Prefix=namespace)
