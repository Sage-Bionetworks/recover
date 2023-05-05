import boto3
import pytest
import synapseclient
from pyarrow import fs

from src.scripts.setup_external_storage import setup_external_storage


@pytest.fixture()
def test_synapse_folder_id(pytestconfig):
    yield pytestconfig.getoption("test_synapse_folder_id")


@pytest.fixture()
def test_ssm_parameter(pytestconfig):
    yield pytestconfig.getoption("test_ssm_parameter")


@pytest.mark.integration()
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
