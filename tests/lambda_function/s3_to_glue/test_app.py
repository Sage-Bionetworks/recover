import os
import boto3
import pytest
import datetime
from dateutil.tz import tzutc
from dateutil import parser
#from moto import mock_s3
from src.lambda_function.s3_to_glue import app


@pytest.fixture
def test_empty_s3_bucket_objects():
    test_empty_s3_bucket_objects = {
        'ResponseMetadata':
            {'RequestId': 'TEST',
             'HostId': 'TEST',
             'HTTPStatusCode': 200,
             'HTTPHeaders': {},
             'RetryAttempts': 0
             },
            'IsTruncated': False,
            'Name': 'test-bucket',
            'Prefix': '',
            'MaxKeys': 1000,
            'EncodingType': 'url',
            'KeyCount': 0
            }
    return test_empty_s3_bucket_objects

@pytest.fixture
def test_s3_bucket_objects():
    test_s3_bucket_objects = {
        'ResponseMetadata':
            {'RequestId': 'TEST',
             'HostId': 'TEST',
             'HTTPStatusCode': 200,
             'HTTPHeaders': {},
             'RetryAttempts': 0
             },
            'IsTruncated': False,
            'Name': 'test-bucket',
            'Prefix': '',
            'MaxKeys': 1000,
            'EncodingType': 'url',
            'KeyCount': 0,
            'Contents': [
                {'Key': '2023-01-03T20--19--TEST',
                 'LastModified': datetime.datetime(2023, 1, 19, 21, 13, 23, tzinfo=tzutc()),
                 'ETag': '"TEST"',
                 'Size': 100,
                 'StorageClass': 'STANDARD'}]
            }
    return test_s3_bucket_objects


@pytest.fixture
def test_trigger_event():
    test_trigger_event = {
        'version': '0',
        'id': 'TEST',
        'detail-type': 'Scheduled Event',
        'source': 'aws.events',
        'account': 'TEST',
        'time': '2023-01-19T23:21:00Z',
        'region': 'us-east-1',
        'resources': [],
        'detail': {}}
    return test_trigger_event

@pytest.fixture
def test_s3_client():
    test_s3_client = boto3.resource('s3')
    return test_s3_client


@pytest.fixture
def test_bucket_list():
    s3 = boto3.resource('s3')
    test_bucket_list = [bucket.name for bucket in s3.buckets.all()]
    return test_bucket_list


@pytest.fixture
def test_glue_client():
    test_glue_client = boto3.client('glue')
    return test_glue_client


def test_bucket_exists(test_bucket_list):
    assert (app.BUCKET_NAME in test_bucket_list) == True


def test_glue_workflow_exists(test_glue_client):
    test_glue_client.get_workflow(
        Name=app.WORKFLOW_NAME)


def test_query_files_to_submit_success(test_s3_bucket_objects, test_trigger_event):
    test_submit_files = app.query_files_to_submit(
                          objects=test_s3_bucket_objects,
                          files_to_submit=[],
                          trigger_event_date=parser.isoparse(test_trigger_event['time']).date())
    assert test_submit_files == [{
        'source_bucket': test_s3_bucket_objects['Name'],
        'source_key': test_s3_bucket_objects['Contents'][0]['Key']}]


def test_no_files_to_submit_on_date(test_s3_bucket_objects):
    test_submit_files = app.query_files_to_submit(
                          objects=test_s3_bucket_objects,
                          files_to_submit=[],
                          trigger_event_date=datetime.datetime.now().date())
    assert test_submit_files == []


def test_empty_bucket_contents(test_trigger_event):
    test_submit_files = app.query_files_to_submit(
                          objects={'Contents':[]},
                          files_to_submit=[],
                          trigger_event_date=parser.isoparse(test_trigger_event['time']).date())
    assert test_submit_files == []


def test_empty_bucket_contents(test_trigger_event):
    test_submit_files = app.query_files_to_submit(
                          objects={'Contents':[]},
                          files_to_submit=[],
                          trigger_event_date=parser.isoparse(test_trigger_event['time']).date())
    assert test_submit_files == []