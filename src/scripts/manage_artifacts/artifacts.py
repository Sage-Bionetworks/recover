import argparse
import os
import subprocess


def read_args():
    descriptions = """
  Uploading to S3 and deletion from S3 of scripts and templates.
  """
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--namespace")
    parser.add_argument("--cfn_bucket", required=True)
    parser.add_argument("--shareable-artifacts-bucket", required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--upload", action="store_true")
    group.add_argument("--remove", action="store_true")
    group.add_argument("--list", action="store_true")
    args = parser.parse_args()
    return args


def execute_command(cmd: list[str]):
    print(f'Invoking command: {" ".join(cmd)}')
    subprocess.run(cmd, check=True)


def upload(namespace: str, cfn_bucket: str):
    """Copy Glue scripts to the artifacts bucket"""

    scripts_local_path = "src/glue/"
    scripts_s3_path = os.path.join("s3://", cfn_bucket, namespace, "src/glue/")
    cmd = ["aws", "s3", "sync", scripts_local_path, scripts_s3_path]
    execute_command(cmd)

    """Copies Lambda code and template to the artifacts bucket"""
    lambda_local_path = "src/lambda_function/"
    lambda_s3_path = os.path.join(
        "s3://", cfn_bucket, namespace, "src/lambda_function/"
    )
    cmd = ["aws", "s3", "sync", lambda_local_path, lambda_s3_path]
    execute_command(cmd)

    """Copy CFN templates to the artifacts bucket"""
    templates_local_path = "templates/"
    templates_s3_path = os.path.join("s3://", cfn_bucket, namespace, "templates/")
    cmd = ["aws", "s3", "sync", templates_local_path, templates_s3_path]
    execute_command(cmd)


def sync(namespace: str, shareable_artifacts_bucket: str):
    """Sync resources which are not version controlled to this namespace.

    In some cases, we do not want to version control some data (like Great Expectations artifacts)
    but we need to duplicate this data from the main namespace to a development namespace.

    Args:
        namespace (str): The development namespace
        shareable_artifacts_bucket (str): The S3 bucket containing shareable artifacts
    """
    # Copy Great Expectations artifacts to this namespace
    source_gx_artifacts = os.path.join(
        "s3://", shareable_artifacts_bucket, "main/great_expectation_resources/"
    )
    target_gx_artifacts = os.path.join(
        "s3://", shareable_artifacts_bucket, namespace, "great_expectation_resources/"
    )
    gx_artifacts_clean_up_cmd = ["aws", "s3", "rm", "--recursive", target_gx_artifacts]
    execute_command(gx_artifacts_clean_up_cmd)
    gx_artifacts_sync_cmd = [
        "aws",
        "s3",
        "sync",
        source_gx_artifacts,
        target_gx_artifacts,
    ]
    execute_command(gx_artifacts_sync_cmd)


def delete(namespace: str, cfn_bucket: str):
    """Removes all files recursively for namespace"""
    s3_path = os.path.join("s3://", cfn_bucket, namespace)
    cmd = ["aws", "s3", "rm", "--recursive", s3_path]
    execute_command(cmd)


def list_namespaces(cfn_bucket: str):
    """List all namespaces"""
    s3_path = os.path.join("s3://", cfn_bucket)
    cmd = ["aws", "s3", "ls", s3_path]
    execute_command(cmd)


def main(args):
    if args.upload:
        upload(args.namespace, args.cfn_bucket)
        if args.namespace != "main":
            sync(
                namespace=args.namespace,
                shareable_artifacts_bucket=args.shareable_artifacts_bucket,
            )
    elif args.remove:
        delete(args.namespace, args.cfn_bucket)
    else:
        list_namespaces(args.cfn_bucket)


if __name__ == "__main__":
    args = read_args()
    main(args)
