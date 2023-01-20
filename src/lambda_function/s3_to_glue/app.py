import os
import json
import boto3
import logging
import datetime
from dateutil import parser

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_NAME = 'recover-pilot-data'
WORKFLOW_NAME = 'S3_to_JSON'


def query_files_to_submit(objects : dict,
                          files_to_submit : list,
                          trigger_event_date : datetime):
    ''' Queries the files in bucket for ones with the same
        date as the trigger event
    '''
    files_current = objects["Contents"]
    logger.info('Querying S3 bucket for new data from source')

    # query bucket for new/updated files
    for fi in files_current:
        # skip objects with no last modified
        if "LastModified" not in fi:
            continue
        file_date = fi["LastModified"].date()
        # if object's last modified date is same as today
        if file_date == trigger_event_date:
            # grab Bucket and Key
            message_parameters = {
                "source_bucket": objects["Name"],
                "source_key": fi["Key"]
            }
            files_to_submit.append(message_parameters)
        else:
            pass
    return(files_to_submit)


def submit_s3_to_json_workflow(workflow_name : str,
                               files_to_submit : list):
    ''' Submits list of dicts with keys source_bucket and source_key
        for the new files added to the S3 to Json glue workflow
    '''
    glue_client = boto3.client("glue")
    logger.info(f'Starting workflow run for workflow {workflow_name}')
    workflow_run = glue_client.start_workflow_run(
        Name=workflow_name)
    glue_client.put_workflow_run_properties(
        Name=workflow_name,
        RunId=workflow_run["RunId"],
        RunProperties={
            "messages": json.dumps(files_to_submit)
        })
    return(None)


def lambda_handler(event, context):
    """
    The Lambda entrypoint

    Given a list of files in s3 bucket to process, submit the keys and bucket
    of the new files to a S3 to JSON Glue workflow.

    Args:
        event (dict): A fileview object
    Returns:
        (None) Submits new file records to a Glue workflow.
    """
    s3 = boto3.client('s3')
    trigger_event_date = parser.isoparse(event['time']).date()
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME)
    files_to_submit = []
    if "Contents" in objects:
        files_to_submit = query_files_to_submit(
            objects, files_to_submit, trigger_event_date)
    else:
        logger.info('No files found in {} bucket'.format(BUCKET_NAME))

    # submit new files to new glue workflow
    if len(files_to_submit) > 0:
        submit_s3_to_json_workflow(WORKFLOW_NAME, files_to_submit)
    else:
        logger.info('No new files to be submitted in {} bucket'.format(BUCKET_NAME))
