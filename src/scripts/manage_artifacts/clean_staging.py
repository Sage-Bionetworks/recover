"""Handles cleaning the staging directory when a new release occurs"""
import argparse
import boto3


def read_args() -> argparse.Namespace:
    """Read the arguments passed to the script."""
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--bucket_prefix", required=True)
    args = parser.parse_args()
    return args


def delete_objects(bucket_prefix: str, bucket: str) -> None:
    """Handle cleaning up bucket in S3. This will allow the owner.txt file
    to be kept.

    Arguments:
        bucket_prefix: The prefix of the bucket to clean. Should end with a '/'
        bucket: The name of the bucket to clean
    """
    print(f"Cleaning bucket: {bucket} with prefix: {bucket_prefix}")

    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=bucket_prefix)

    # Check if objects are found
    if 'Contents' in response:
        for obj in response['Contents']:
            object_key = obj['Key']

            # Skip the owner.txt file so it does not need to be re-created
            if object_key.endswith('owner.txt'):
                continue

            s3_client.delete_object(Bucket=bucket, Key=object_key)


def main() -> None:
    """Main function to handle cleaning the staging directory in S3. This will
    allow the owner.txt file to be kept."""
    args = read_args()
    try:
        delete_objects(bucket_prefix=args.bucket_prefix, bucket=args.bucket)
    except Exception as ex:
        print(f"Error deleting objects: {ex}")
        # Fail, but let the job pass


if __name__ == "__main__":
    main()
