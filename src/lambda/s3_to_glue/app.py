import os
import json
import logging
import boto3
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_NAME = 'recover-pilot-data'

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
    today_date = datetime.now(timezone.utc).date()
    namespace = os.environ.get('NAMESPACE')
    s3 = boto3.client('s3')

    # query bucket for new/updated files
    files_new = []
    files_current = s3.list_objects_v2(Bucket=BUCKET_NAME)
    logger.info('Querying S3 bucket for new data from source')
    for fi in files_current["Contents"]:
        file_date = fi["LastModified"].date()
        # if object's last modified date is same as today
        if file_date == today_date:
            # grab Bucket and Key
            message_parameters = {
                "source_bucket": BUCKET_NAME,
                "source_key": fi["Key"]
            }
            files_new.append(message_parameters)
        else:
            pass

    # submit new files to new glue workflow
    if len(files_new) > 0:
        glue_client = boto3.client("glue")
        workflow_name = "S3_to_JSON"
        logger.info(f'Starting workflow run for workflow {workflow_name}')
        workflow_run = glue_client.start_workflow_run(
            Name=workflow_name)
        glue_client.put_workflow_run_properties(
            Name=workflow_name,
            RunId=workflow_run["RunId"],
            RunProperties={
                "messages": json.dumps(files_new)
            })
    else:
        logger.info('No new files found in {} bucket'.format(BUCKET_NAME))

    return(None)
