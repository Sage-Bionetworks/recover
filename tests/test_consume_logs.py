import datetime

import boto3
import pytest
from botocore.stub import Stubber
from pandas import DataFrame, Index

from src.scripts.consume_logs import consume_logs


@pytest.fixture
def log_group_name():
    return "test-log-group"


@pytest.fixture
def start_unix_time():
    return 1635619200


@pytest.fixture
def end_unix_time(start_unix_time):
    return start_unix_time + 10


@pytest.fixture
def query_string():
    return "display message"


def test_get_seconds_since_epoch():
    now = datetime.datetime.now()
    actual_seconds_since_epoch = int(now.timestamp())
    datetime_format = "%Y-%m-%d %H:%M:%S"
    datetime_str = now.strftime(datetime_format)
    calculated_seconds_since_epoch = consume_logs.get_seconds_since_epoch(
        datetime_str=datetime_str,
        datetime_format=datetime_format,
    )
    assert calculated_seconds_since_epoch == actual_seconds_since_epoch


def test_get_seconds_since_epoch_datetime_str_is_none():
    now = datetime.datetime.now()
    actual_seconds_since_epoch = int(now.timestamp())
    calculated_seconds_since_epoch = consume_logs.get_seconds_since_epoch(
        datetime_str=None,
        datetime_format=None,
    )
    # We don't know what should be the exact time computed within
    # `get_seconds_since_epoch`, but it ought to be within a second of
    # `actual_seconds_since_epoch`
    assert abs(calculated_seconds_since_epoch - actual_seconds_since_epoch) <= 1


def test_query_logs_status_complete(
    log_group_name, start_unix_time, end_unix_time, query_string
):
    logs_client = _stub_logs_client(
        status="Complete",
        log_group_name=log_group_name,
        start_unix_time=start_unix_time,
        end_unix_time=end_unix_time,
        query_string=query_string,
    )
    query_result = consume_logs.query_logs(
        log_group_name=log_group_name,
        query_string=query_string,
        start_unix_time=start_unix_time,
        end_unix_time=end_unix_time,
        logs_client=logs_client,
    )
    assert len(query_result) == 1
    assert query_result[0][0]["field"] == "message"


def test_query_logs_status_failed(
    log_group_name, start_unix_time, end_unix_time, query_string
):
    logs_client = _stub_logs_client(
        status="Failed",
        log_group_name=log_group_name,
        start_unix_time=start_unix_time,
        end_unix_time=end_unix_time,
        query_string=query_string,
    )
    with pytest.raises(UserWarning):
        query_result = consume_logs.query_logs(
            log_group_name=log_group_name,
            query_string=query_string,
            start_unix_time=start_unix_time,
            end_unix_time=end_unix_time,
            logs_client=logs_client,
        )


def test_check_for_failed_query():
    with pytest.raises(UserWarning):
        consume_logs._check_for_failed_query({"status": "Failed"})
    with pytest.raises(UserWarning):
        consume_logs._check_for_failed_query({"status": "Cancelled"})
    with pytest.raises(UserWarning):
        consume_logs._check_for_failed_query({"status": "Timeout"})
    with pytest.raises(UserWarning):
        consume_logs._check_for_failed_query({"status": "Unknown"})


def test_group_query_results_by_workflow_run():
    query_results = [
        [{"field": "@message", "value": '{"process": {"parent": {"pid": "one"}}}'}],
        [{"field": "@message", "value": '{"process": {"parent": {"pid": "one"}}}'}],
        [{"field": "@message", "value": '{"process": {"parent": {"pid": "two"}}}'}],
    ]
    workflow_run_groups = consume_logs.group_query_result_by_workflow_run(
        query_results=query_results
    )
    assert len(workflow_run_groups["one"]) == 2
    assert len(workflow_run_groups["two"]) == 1


def test_transform_logs_to_dataframe():
    log_messages = [
        {
            "labels": {"cohort": "adults_v1"},
            "file": {"name": "file_one", "LineCount": 1},
            "event": {"type": ["access"]},
        },
        {
            "labels": {"cohort": "adults_v1"},
            "file": {"name": "file_one", "LineCount": 1},
            "event": {"type": ["creation"]},
        },
    ]
    resulting_dataframe = consume_logs.transform_logs_to_dataframe(
        log_messages=log_messages
    )
    expected_columns = ["cohort", "file_name", "event_type", "line_count"]
    for col in expected_columns:
        assert col in resulting_dataframe.columns


@pytest.mark.parametrize(
    "log_messages",
    [
        [{"notevent": None}],
        [{"event": {"nottype": None}}],
        [{"event": {"type": ["notaccess", "info"]}}],
        [{"event": {"type": ["access", "creation"]}}],
    ],
)
def test_transform_logs_to_dataframe_malformatted_event_type(log_messages):
    with pytest.raises(KeyError):
        consume_logs.transform_logs_to_dataframe(log_messages=log_messages)


def _stub_logs_client(
    status, log_group_name, start_unix_time, end_unix_time, query_string
):
    """
    A helper function for stubbing the boto3 logs client

    This function provides a convenient way for obtaining a different
    `status` in the `get_query_results` response.
    """
    logs_client = boto3.client("logs")
    logs_client_stub = Stubber(logs_client)
    start_query_response = {"queryId": "a-query-id"}
    get_query_results_response = {
        "status": status,
        "results": [[{"field": "message", "value": "Test log message 1"}]],
    }

    # Set up the Stubber to mock the CloudWatch Logs API calls
    logs_client_stub.add_response(
        "start_query",
        start_query_response,
        expected_params={
            "logGroupName": log_group_name,
            "startTime": start_unix_time,
            "endTime": end_unix_time,
            "queryString": query_string,
        },
    )
    logs_client_stub.add_response(
        "get_query_results",
        get_query_results_response,
        expected_params={"queryId": start_query_response["queryId"]},
    )
    logs_client_stub.activate()
    return logs_client


@pytest.mark.parametrize(
    "workflow_run_event_comparison,missing_data",
    [
        ({"one": DataFrame({"line_count_difference": [0]})}, DataFrame()),
        (
            {"one": DataFrame({"line_count_difference": [0, 1]})},
            DataFrame(
                {"line_count_difference": [1]},
                index=Index(["one"], name="workflow_run_id"),
            ),
        ),
    ],
)
def test_report_results(workflow_run_event_comparison, missing_data):
    this_missing_data = consume_logs.report_results(
        workflow_run_event_comparison=workflow_run_event_comparison, testing=True
    )
    assert this_missing_data.equals(missing_data)
