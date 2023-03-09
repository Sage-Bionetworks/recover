import argparse
import subprocess

REPO_NAME = "recover"


def read_args():
    descriptions = """
  Uploading to S3 and deletion from S3 of scripts and templates.
  """
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--namespace")
    parser.add_argument("--cfn_bucket", required = True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--upload", action="store_true")
    group.add_argument("--remove", action="store_true")
    group.add_argument("--list", action="store_true")
    args = parser.parse_args()
    return args


def execute_command(cmd : str):
    print(f'Invoking command: {" ".join(cmd)}')
    subprocess.run(cmd)


def upload(namespace : str, cfn_bucket : str):
    """Copy Glue scripts to the artifacts bucket"""
    scripts_local_path = "src/glue/"
    scripts_s3_path = f"s3://{cfn_bucket}/{namespace}/{REPO_NAME}/src/glue/"
    cmd = ["aws", "s3", "sync", scripts_local_path, scripts_s3_path]
    execute_command(cmd)

    """Copies Lambda code and template to the artifacts bucket"""
    lambda_local_path = "src/lambda_function/"
    lambda_s3_path = f"s3://{cfn_bucket}/{namespace}/{REPO_NAME}/src/lambda_function/"
    cmd = ["aws", "s3", "sync", lambda_local_path, lambda_s3_path]
    execute_command(cmd)

    """Copy CFN templates to the artifacts bucket"""
    templates_local_path = "templates/"
    templates_s3_path = f"s3://{cfn_bucket}/{namespace}/{REPO_NAME}/templates/"
    cmd = ["aws", "s3", "sync", templates_local_path, templates_s3_path]
    execute_command(cmd)


def delete(namespace : str, cfn_bucket : str):
    """Removes all files recursively for namespace"""
    s3_path = f"s3://{cfn_bucket}/{namespace}/{REPO_NAME}/"
    cmd = ["aws", "s3", "rm", "--recursive", s3_path]
    execute_command(cmd)


def list_namespaces(cfn_bucket : str):
    """List all namespaces"""
    s3_path = f"s3://{cfn_bucket}/"
    cmd = ["aws", "s3", "ls", s3_path]
    execute_command(cmd)


def main(args):
    if args.upload:
        upload(args.namespace, args.cfn_bucket)
    elif args.remove:
        delete(args.namespace, args.cfn_bucket)
    else:
        list_namespaces(args.cfn_bucket)


if __name__ == "__main__":
    args = read_args()
    main(args)
