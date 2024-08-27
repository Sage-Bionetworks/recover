import json

import boto3


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
