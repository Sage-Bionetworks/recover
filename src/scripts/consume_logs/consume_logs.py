"""
This script will compare read/write line counts of files processed by the
S3 to JSON job by querying the Cloudwatch log group where that job writes
its logs.

A message is printed whether a mismatch between read/write counts was discovered,
indicating that data went missing or, potentially, that additional data not present
in the original JSON file was written to the JSON dataset.

A CSV file is written (consume_logs_comparison_report.csv) to the current directory
containing the read/write counts for every file matched in the query. This CSV file
contains the fields:

    * workflow_run_id : the ID of the workflow run
    * cohort : the cohort this data belongs to (adults_v1, pediatric_v1, etc.)
    * file_name : the file name.
    * line_count_access : the number of lines counted during read of the original JSON file.
    * line_count_creation : the number of lines counted during write to the JSON dataset.
    * line_count_difference : line_count_access - line_count_creation

If missing data is discovered, another CSV file (consume_logs_missing_data_report.csv)
is written which contains only the subset of files which have a line_count_difference != 0.
The schema is the same as above.
"""
import datetime
import json
import time
from collections import defaultdict
from typing import List, Dict

import argparse
import boto3
import pandas

def read_args():
    parser = argparse.ArgumentParser(
            description="Query S3 to JSON CloudWatch logs and compare line counts.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
            "--log-group-name",
            help="The name of the log group to query.",
            default="/aws-glue/python-jobs/error"
    )
    parser.add_argument(
            "--query",
            help="The query to run against the log group.",
            default='fields @message | filter event.action = "list-file-properties"'
    )
    parser.add_argument(
            "--start-datetime",
            help=(
                "Query start time (local time) expressed in a format parseable by "
                "datetime.datetime.strptime. This argument should be "
                "formatted as `--time-format`."),
            required=True,
    )
    parser.add_argument(
            "--end-datetime",
            help=(
                "Default is \"now\". Query end time (local time) expressed "
                "in a format parseable by datetime.datetime.strptime. This "
                "argument should be formatted as `--time-format`."),
    )
    parser.add_argument(
            "--datetime-format",
            help="The time format to use with datetime.datetime.strptime",
            default="%Y-%m-%d %H:%M:%S",
    )
    args = parser.parse_args()
    return args

def get_seconds_since_epoch(datetime_str: str, datetime_format: str) -> int:
    """
    Returns the seconds since epoch for a specific datetime (local time)

    Args:
        datetime_str (str): A datetime represented by a format parseable by
            datetime.datetime.strptime.
        datetime_format (str): The datetime.datetime.strptime format used
            to parse `datetime`.

    Returns:
        (int) The number of seconds since January 1, 1970 (local time)
    """
    if datetime_str is None:
        parsed_datetime = datetime.datetime.now()
    else:
        parsed_datetime = datetime.datetime.strptime(datetime_str, datetime_format)
    seconds_since_epoch = int(parsed_datetime.timestamp())
    return seconds_since_epoch

def query_logs(
        log_group_name: str,
        query_string: str,
        start_unix_time: int,
        end_unix_time: int,
        **kwargs: dict,
) -> list:
    """
    Query a CloudWatch log group.

    Args:
        log_group_name (str): The log group on which to perform the query.
        query_string (str): The query string to use.
        start_unix_time (int): The beginning of the time range to query. The range
            is inclusive, so the specified start time is included in the query.
            Specified as epoch (Unix) time.
        end_unix_time (int): The end of the time range to query. The range
            is inclusive, so the specified end time is included in the query.
            Specified as epoch (Unix) time.
        kwargs (dict): Used during tests to include boto client stubs

    Returns:
        (list) The results of the query. Empty if no matches found.

    Raises:
        UserWarning: If the call to the get query results API does not succeed.
    """
    logs_client = kwargs.get("logs_client", boto3.client("logs"))
    start_query_response = logs_client.start_query(
            logGroupName=log_group_name,
            startTime=start_unix_time,
            endTime=end_unix_time,
            queryString=query_string
    )
    query_response = logs_client.get_query_results(
            queryId=start_query_response["queryId"]
    )
    _check_for_failed_query(query_response)
    while query_response["status"] != "Complete":
        time.sleep(1)
        query_response = logs_client.get_query_results(
                queryId=start_query_response["queryId"]
        )
        _check_for_failed_query(query_response)
    return query_response["results"]

def _check_for_failed_query(query_response) -> None:
    """
    Helper function for checking whether a query response has not succeeded.
    """
    if query_response["status"] in ["Failed", "Cancelled", "Timeout", "Unknown"]:
        raise UserWarning(f"Query failed with status \"{query_response['status']}\"")

def group_query_result_by_workflow_run(query_results: List[List[dict]]) -> Dict[str, List[dict]]:
    """
    Associates log records with their workflow run ID.

    Since our log query may have picked up logs from multiple workflow runs,
    we organize our log records by mapping the workflow run ID to its respective
    log records.

    Args:
        query_results (list[list[dict]]): The query results. See `get_query_results`.

    Returns:
       (dict[str, list[dict]]): A dict of workflow run ID mapped to its respective logs.
    """
    # e.g., [{'@message': '{...}', '@ptr': '...'}, ...]
    log_records = [
            {field['field']: field['value'] for field in log_message}
            for log_message in query_results
    ]
    workflow_run_logs = defaultdict(list)
    for log_record in log_records:
        log_message = json.loads(log_record["@message"])
        workflow_run_logs[log_message["process"]["parent"]["pid"]].append(log_message)
    return workflow_run_logs

def transform_logs_to_dataframe(log_messages: List[dict]) -> pandas.DataFrame:
    """
    Construct a pandas DataFrame from log records

    Dataframes will contain information necessary for comparing read/write
    line counts of individual files from a workflow run.

    Args:
        log_messages (list[dict]): A list of log messages

    Returns:
        pandas.DataFrame: A pandas DataFrame with columns:
            * cohort (str)
            * file_name (str)
            * event_type (str, either 'access' or 'creation')
            * line_count (int)
    """
    dataframe_records = []
    for log_message in log_messages:
        if (
                "event" not in log_message
                or "type" not in log_message["event"]
                or not any(
                    [
                        k in log_message["event"]["type"]
                        for k in ["access", "creation"]
                    ]
                )
                or all(
                    [
                        k in log_message["event"]["type"]
                        for k in ["access", "creation"]
                    ]
                )
        ):
            raise KeyError(
                    "Did not find event.type in log message or "
                    "event.type contained unexpected values "
                    "for workflow run ID {log_message['process']['parent']['pid']} and "
                    f"file {json.dumps(log_message['file']['labels'])}"
            )
        if "access" in log_message["event"]["type"]:
            event_type = "access"
        else:
            event_type = "creation"
        dataframe_record = {
                "cohort": log_message["labels"]["cohort"],
                "file_name": log_message["file"]["name"],
                "event_type": event_type,
                "line_count": log_message["file"]["LineCount"]
        }
        dataframe_records.append(dataframe_record)
    log_dataframe = pandas.DataFrame.from_records(dataframe_records)
    return log_dataframe

def report_results(
        workflow_run_event_comparison: dict,
        comparison_report_path: str="consume_logs_comparison_report.csv",
        missing_data_report_path: str="consume_logs_missing_data_report.csv",
        testing:bool=False,
    ) -> pandas.DataFrame:
    """
    Report any missing data and save the results.

    This function reports any differences in the 'line_count_difference' column
    for each workflow run and saves the results to a CSV.

    Args:
        workflow_run_event_comparison (dict): A dictionary mapping workflow runs to
            a pandas DataFrame comparing read/write record counts.
        comparison_report_path (str, optional): The file path to save the comparison report CSV.
            This file contains every read/write comparison that matched in the logs query.
            Defaults to "consume_logs_comparison_report.csv".
        missing_data_report_path (str, optional): The file path to save the missing data report CSV.
            Contains the same information as the comparison report, but subset to comparisons
            where a difference in the read/write record count was discovered.
            Defaults to "consume_logs_missing_data_report.csv".
        testing (bool): Set to True to avoid writing any files. Defaults to False.

    Returns:
        pandas.DataFrame
    """
    all_comparisons = pandas.concat(
            workflow_run_event_comparison,
            names=["workflow_run_id", "index"]
    )
    all_missing_data = pandas.DataFrame()
    for workflow_run in workflow_run_event_comparison:
        missing_data = workflow_run_event_comparison[workflow_run].query(
            "line_count_difference != 0"
        )
        if len(missing_data) != 0:
            print(
                    "Discovered differences between records read/write "
                    f"in workflow run {workflow_run}"
            )
            missing_data = missing_data.assign(workflow_run_id = workflow_run)
            all_missing_data = pandas.concat([all_missing_data, missing_data])
    if len(all_missing_data) > 0:
        print(f"Writing missing data information to {missing_data_report_path}")
        all_missing_data = all_missing_data.set_index("workflow_run_id")
        if not testing:
            all_missing_data.to_csv(missing_data_report_path)
    else:
        print("Did not find any differences between records read/write")
    print(f"Writing read/write comparison information to {comparison_report_path}")
    all_comparisons = all_comparisons.droplevel("index")
    if not testing:
        all_comparisons.to_csv(comparison_report_path)
    return all_missing_data

def main() -> None:
    args = read_args()
    start_unix_time = get_seconds_since_epoch(
            datetime_str=args.start_datetime,
            datetime_format=args.datetime_format,
    )
    end_unix_time = get_seconds_since_epoch(
            datetime_str=args.end_datetime,
            datetime_format=args.datetime_format,
    )
    file_property_logs = query_logs(
            log_group_name=args.log_group_name,
            query_string=args.query,
            start_unix_time=start_unix_time,
            end_unix_time=end_unix_time,
    )
    if len(file_property_logs) == 0:
        print(
                f"The query '{args.query}' did not return any results "
                f"in the time range {start_unix_time}-{end_unix_time}"
        )
        return
    workflow_run_logs = group_query_result_by_workflow_run(query_results=file_property_logs)
    workflow_run_event_comparison = {}
    for workflow_run in workflow_run_logs:
        workflow_run_dataframe = transform_logs_to_dataframe(
                log_messages=workflow_run_logs[workflow_run]
        )
        access_events = (
                workflow_run_dataframe
                .query("event_type == 'access'")
                .drop("event_type", axis=1)
        )
        creation_events = (
                workflow_run_dataframe
                .query("event_type == 'creation'")
                .drop("event_type", axis=1)
        )
        event_comparison = access_events.merge(
                creation_events,
                how="left",
                on=["cohort", "file_name"],
                suffixes=("_access", "_creation")
        )
        event_comparison["line_count_difference"] = \
                event_comparison["line_count_access"] - event_comparison["line_count_creation"]
        workflow_run_event_comparison[workflow_run] = event_comparison
    report_results(
            workflow_run_event_comparison=workflow_run_event_comparison
    )

if __name__ == "__main__":
    main()
