"""
Create an STS-enabled folder on Synapse over an S3 location.
"""

import os
import json
import argparse

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
        help=(
            "Name of the Synapse folder to use with the external " "storage location."
        ),
    )
    parser.add_argument("--s3-bucket", help="S3 bucket name")
    parser.add_argument(
        "--s3-key",
        default=None,
        help="S3 key to serve as the root. `/` (root) by default.",
    )
    parser.add_argument(
        "--sts-enabled",
        action="store_true",
        help="Whether this storage location should be STS enabled",
    )
    args = parser.parse_args()
    return args


def main():
    args = read_args()
    syn = synapseclient.login()
    storage_location = syn.create_s3_storage_location(
        parent=args.synapse_parent,
        folder_name=args.synapse_folder_name,
        bucket_name=args.s3_bucket,
        base_key=args.s3_key,
        sts_enabled=args.sts_enabled,
    )
    storage_location_info = {
        k: v
        for k, v in zip(
            ["synapse_folder", "storage_location", "synapse_project"], storage_location
        )
    }
    print(storage_location_info)
    return storage_location_info


if __name__ == "__main__":
    main()
