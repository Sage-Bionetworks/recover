"""
Create an STS-enabled folder on Synapse over an S3 location.
"""

import argparse
import json
import os

import boto3
import synapseclient


def read_args():
    parser = argparse.ArgumentParser(
        description="Create an STS-enabled folder on Synapse over an S3 location."
    )
    parser.add_argument(
        "--synapse-parent", help="Synapse ID of the parent folder/project"
    )
    parser.add_argument(
        "--synapse-folder-name",
        default="parquet",
        help=(
            "Name of the Synapse folder to use with the external "
            "storage location. Default is parquet"
        ),
    )
    parser.add_argument("--s3-bucket", help="S3 bucket name")
    parser.add_argument(
        "--s3-key",
        default="recover/parquet",
        help="S3 key to serve as the root. Default is recover/parquet.",
    )
    parser.add_argument(
        "--sts-enabled",
        action="store_true",
        help="Whether this storage location should be STS enabled",
    )
    parser.add_argument(
        "--profile",
        help=(
            "Optional. The AWS profile to use. Uses the default "
            "profile if not specified."
        ),
    )
    parser.add_argument(
        "--ssm-parameter",
        help=(
            "Optional. The name of the SSM parameter containing "
            "the Synapse personal access token. "
            "If not provided, cached credentials are used"
        ),
    )
    args = parser.parse_args()
    return args


def get_synapse_client(ssm_parameter=None, aws_session=None):
    """
    Return an authenticated Synapse client.

    Args:
        ssm_parameter (str): Name of the SSM parameter containing the
            recoverETL Synapse authentication token.
        aws_session (boto3.session.Session)

    Returns:
        synapseclient.Synapse
    """
    if ssm_parameter is not None:
        ssm_client = aws_session.client("ssm")
        token = ssm_client.get_parameter(Name=ssm_parameter, WithDecryption=True)
        syn = synapseclient.Synapse()
        syn.login(authToken=token["Parameter"]["Value"])
    else:  # try cached credentials
        syn = synapseclient.login()
    return syn


def main():
    args = read_args()
    aws_session = boto3.session.Session(
        profile_name=args.profile, region_name="us-east-1"
    )
    syn = get_synapse_client(ssm_parameter=args.ssm_parameter, aws_session=aws_session)
    synapse_folder, storage_location, synapse_project = syn.create_s3_storage_location(
        parent=args.synapse_parent,
        folder_name=args.synapse_folder_name,
        bucket_name=args.s3_bucket,
        base_key=args.s3_key,
        sts_enabled=args.sts_enabled,
    )

    storage_location_info = {
        "synapse_folder": synapse_folder,
        "storage_location": storage_location,
        "synapse_project": synapse_project,
    }
    print(storage_location_info)
    return storage_location_info


if __name__ == "__main__":
    main()
