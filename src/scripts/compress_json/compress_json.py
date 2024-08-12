"""
This script combines and compresses NDJSON part files in our intermediate S3 bucket
into a single gzipped NDJSON file per part set.

Which S3 bucket is used is dependent on whether `--testing` is passed.

-- Data Input --
Each part set is located at:

  `s3://<bucket-name>/<namespace>/json/dataset={dataset}/cohort={cohort}/`

where `{dataset}` is the dataset identifier (datatype) and `{cohort}` is
either 'adults_v1' or 'pediatric_v1'. Each file in the part set is named:

  `{file_identifier}.part{part_number}.ndjson`

-- Data Output --
Each combined and gzipped part set is written to:

  `s3://<bucket-name>/<namespace>/compressed_json/dataset={dataset}/cohort={cohort}/{file_identifier}.ndjson.gz`

Command-line Arguments:
--testing     : flag: Use data from recover-dev-intermediate-data. Omitting
                        this argument will use data from recover-intermediate-data.
--namespace   : str : The namespace in the S3 bucket (default: 'main').
--num-cores   : int : Number of cores to use for parallel processing
                        (default: all cores, if `--testing` is not specified).
"""

import argparse
import concurrent.futures
import gzip
from io import BytesIO

import boto3

DEV_DATASETS = [
    "EnrolledParticipants",
    "FitbitActivityLogs",
    "FitbitDailyData",
    "FitbitDevices",
    "FitbitEcg",
    "FitbitIntradayCombined",
    "FitbitRestingHeartRates",
    "FitbitSleepLogs",
    "GarminDailySummary",
    "GarminEpochSummary",
    "GarminHrvSummary",
    "GarminPulseOxSummary",
    "GarminRespirationSummary",
    "GarminSleepSummary",
    "GarminStressDetailSummary",
    "GoogleFitSamples",
    "HealthKitV2ActivitySummaries",
    "HealthKitV2Electrocardiogram",
    "HealthKitV2Heartbeat",
    "HealthKitV2Samples",
    "HealthKitV2Samples_Deleted",
    "HealthKitV2Statistics",
    "HealthKitV2Workouts",
    "SymptomLog",
]
PROD_DATASETS = [
    "EnrolledParticipants",
    "FitbitActivityLogs",
    "FitbitDailyData",
    "FitbitDevices",
    "FitbitEcg",
    "FitbitIntradayCombined",
    "FitbitRestingHeartRates",
    "FitbitSleepLogs",
    "GoogleFitSamples",
    "HealthKitV2ActivitySummaries",
    "HealthKitV2Electrocardiogram",
    "HealthKitV2Heartbeat",
    "HealthKitV2Heartbeat_Deleted",
    "HealthKitV2Samples",
    "HealthKitV2Samples_Deleted",
    "HealthKitV2Statistics",
    "HealthKitV2Workouts",
    "SymptomLog",
]
COHORTS = ["adults_v1", "pediatric_v1"]


def read_args():
    parser = argparse.ArgumentParser(
        description="Combine and compress part files in S3."
    )
    parser.add_argument(
        "--namespace", type=str, default="main", help="The namespace in the S3 bucket"
    )
    parser.add_argument(
        "--num-cores",
        type=int,
        help=(
            "Number of cores to use for parallel processing. ",
            "If `--testing` is not specified, omit this parameter to "
            "use all available cores by default.",
        ),
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help="Run the processing synchronously for testing",
    )
    return parser.parse_args()


def list_files(s3_client: boto3.client, bucket: str, prefix: str):
    """
    List all files in an S3 bucket with the given prefix using a paginator.

    Args:
        s3_client (boto3.client): The S3 client.
        bucket (str): The name of the S3 bucket.
        prefix (str): The S3 prefix of the files to list.

    Returns:
        list: A list of file keys in the bucket with the given prefix.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    files = []
    for page in page_iterator:
        if "Contents" in page:
            files.extend([obj["Key"] for obj in page["Contents"]])
    return files


def combine_and_compress_files(bucket: str, prefix: str):
    """
    Combine and compress part files from an S3 bucket.

    Args:
        bucket (str): The name of the S3 bucket.
        prefix (str): The S3 prefix of the part files to combine and compress.
    """
    print(f"Compressing: {prefix}")
    s3_client = boto3.client("s3")
    files = list_files(s3_client=s3_client, bucket=bucket, prefix=prefix)

    part_sets = {}
    for file in files:
        parts = file.split("/")[-1].split(".")
        file_identifier = parts[0]
        part_sets.setdefault(file_identifier, []).append(file)

    for file_identifier, parts in part_sets.items():
        combined_data = BytesIO()
        with gzip.GzipFile(fileobj=combined_data, mode="wb") as gz:
            for part in sorted(parts):
                response = s3_client.get_object(Bucket=bucket, Key=part)
                data = response["Body"].read()
                gz.write(data)
        output_key = (
            prefix.replace("json", "compressed_json") + f"{file_identifier}.ndjson.gz"
        )
        content = combined_data.getvalue()
        s3_client.put_object(Bucket=bucket, Key=output_key, Body=content)
    print(f"Compressed: {prefix}")


def main(namespace: str, num_cores: int, testing: bool):
    """
    The main function.

    Args:
        namespace (str): The namespace in the S3 bucket.
        num_cores (int): The number of cores to use for parallel processing.
        testing (bool): Flag to run the processing synchronously for testing.
    """
    if testing:
        datasets = DEV_DATASETS
        bucket_name = "recover-dev-intermediate-data"
    else:
        datasets = PROD_DATASETS
        bucket_name = "recover-intermediate-data"
    s3_prefixes = [
        "{namespace}/json/dataset={dataset}/cohort={cohort}/".format(
            namespace=namespace, dataset=dataset, cohort=cohort
        )
        for dataset in datasets
        for cohort in COHORTS
    ]
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [
            executor.submit(combine_and_compress_files, bucket_name, prefix)
            for prefix in s3_prefixes
        ]
        for future in concurrent.futures.as_completed(futures):
            future.result()


if __name__ == "__main__":
    args = read_args()
    main(
        namespace=args.namespace,
        num_cores=args.num_cores,
        testing=args.testing,
    )
