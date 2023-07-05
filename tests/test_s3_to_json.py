import os
import io
import json
import zipfile
import datetime
from dateutil.tz import tzutc

import boto3
import pytest

from src.glue.jobs import s3_to_json


class MockAWSClient:
    def put_object(*args, **kwargs):
        return None


class TestS3ToJsonS3:
    @pytest.fixture
    def namespace(self):
        namespace = "test-recover"
        return namespace

    @pytest.fixture
    def s3_obj(self, shared_datadir):
        s3_obj = {
            "ResponseMetadata": {
                "RequestId": "TEST",
                "HostId": "TESTING",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "x-amz-id-2": "TESTING",
                    "x-amz-request-id": "TEST",
                    "date": "Thu, 23 Feb 2023 22:28:37 GMT",
                    "last-modified": "Wed, 18 Jan 2023 21:13:23 GMT",
                    "etag": '"DUMMYETAG"',
                    "x-amz-server-side-encryption": "AES256",
                    "accept-ranges": "bytes",
                    "content-type": "application/x-www-form-urlencoded; charset=utf-8",
                    "server": "AmazonS3",
                    "content-length": "87280534",
                },
                "RetryAttempts": 0,
            },
            "AcceptRanges": "bytes",
            "LastModified": datetime.datetime(2023, 1, 18, 21, 13, 23, tzinfo=tzutc()),
            "ContentLength": 87280534,
            "ETag": '"DUMMYETAG"',
            "ContentType": "application/x-www-form-urlencoded; charset=utf-8",
            "ServerSideEncryption": "AES256",
        }
        # sample test data
        with open(
            shared_datadir
            / "2023-01-13T21--08--51Z_TESTDATA",
            "rb",
        ) as z:
            s3_obj["Body"] = z.read()
        return s3_obj

    @pytest.fixture
    def json_file_basenames_dict(self):
        # keep examples of all possible data types and valid filename
        json_file_basenames = {
            "EnrolledParticipants": "EnrolledParticipants_20230103.json",
            "FitbitActivityLogs": "FitbitActivityLogs_20220111-20230103.json",
            "FitbitDailyData": "FitbitDailyData_20220111-20230103.json",
            "FitbitDevices": "FitbitDevices_20230103.json",
            "FitbitIntradayCombined": "FitbitIntradayCombined_20220111-20230103.json",
            "FitbitRestingHeartRates": "FitbitRestingHeartRates_20220111-20230103.json",
            "FitbitSleepLogs": "FitbitSleepLogs_20220111-20230103.json",
            "GoogleFitSamples": "GoogleFitSamples_20220111-20230103.json",
            "HealthKitV2ActivitySummaries": "HealthKitV2ActivitySummaries_20220111-20230103.json",
            "HealthKitV2Electrocardiogram": "HealthKitV2Electrocardiogram_Samples_20220111-20230103.json",
            "HealthKitV2Heartbeat": "HealthKitV2Heartbeat_Samples_20220401-20230112.json",
            "HealthKitV2Heartbeat_Deleted": "HealthKitV2Heartbeat_Samples_Deleted_20220401-20230112.json",
            "HealthKitV2Samples": "HealthKitV2Samples_AbdominalCramps_20220111-20230103.json",
            "HealthKitV2Samples_Deleted": "HealthKitV2Samples_AbdominalCramps_Deleted_20220111-20230103.json",
            "HealthKitV2Statistics": "HealthKitV2Statistics_HourlySteps_20201022-20211022.json",
            "HealthKitV2Workouts": "HealthKitV2Workouts_20220111-20230103.json",
            "SymptomLog": "SymptomLog_20220401-20230112.json",
        }
        return json_file_basenames

    def test_write_healthkitv2samples_file_to_json_dataset(self, s3_obj, namespace, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x: MockAWSClient())
        sample_metadata = {
            "Metadata": {
                "type": "HealthKitV2Samples",
                "start_date": datetime.datetime(2022, 1, 12, 0, 0),
                "end_date": datetime.datetime(2023, 1, 14, 0, 0),
                "subtype": "Weight",
            }
        }
        workflow_run_properties = {
            "namespace": namespace,
            "json_prefix": "raw-json",
            "json_bucket": "json-bucket",
        }
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            output_file = s3_to_json.write_file_to_json_dataset(
                z=z,
                json_path="HealthKitV2Samples_Weight_20230112-20230114.json",
                dataset_identifier="HealthKitV2Samples",
                metadata=sample_metadata["Metadata"],
                workflow_run_properties=workflow_run_properties,
                delete_upon_successful_upload=False,
            )

            with open(output_file, "r") as f_out:
                for json_line in f_out:
                    metadata = json.loads(json_line)
                    assert sample_metadata["Metadata"]["subtype"] == metadata["Type"]
                    assert (
                        sample_metadata["Metadata"]["start_date"].isoformat()
                        == metadata["export_start_date"]
                    )
                    assert (
                        sample_metadata["Metadata"]["end_date"].isoformat()
                        == metadata["export_end_date"]
                    )
                    break

    def test_write_file_to_json_dataset_delete_local_copy(self, s3_obj, namespace, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x: MockAWSClient())
        sample_metadata = {
            "Metadata": {
                "type": "HealthKitV2Samples",
                "start_date": datetime.datetime(2022, 1, 12, 0, 0),
                "end_date": datetime.datetime(2023, 1, 14, 0, 0),
                "subtype": "Weight",
            }
        }
        workflow_run_properties = {
            "namespace": namespace,
            "json_prefix": "raw-json",
            "json_bucket": "json-bucket",
        }
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            output_file = s3_to_json.write_file_to_json_dataset(
                z=z,
                json_path="HealthKitV2Samples_Weight_20230112-20230114.json",
                dataset_identifier="HealthKitV2Samples",
                metadata=sample_metadata["Metadata"],
                workflow_run_properties=workflow_run_properties,
                delete_upon_successful_upload=True,
            )

        assert not os.path.exists(output_file)

    def test_write_symptom_log_file_to_json_dataset(self, s3_obj, namespace, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x: MockAWSClient())
        sample_metadata = {
            "Metadata": {
                "type": "SymptomLog",
                "start_date": datetime.datetime(2022, 1, 12, 0, 0),
                "end_date": datetime.datetime(2023, 1, 14, 0, 0)
            }
        }
        workflow_run_properties = {
            "namespace": namespace,
            "json_prefix": "raw-json",
            "json_bucket": "json-bucket",
        }
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            output_file = s3_to_json.write_file_to_json_dataset(
                z=z,
                json_path="SymptomLog_20230112-20230114.json",
                dataset_identifier="SymptomLog",
                metadata=sample_metadata["Metadata"],
                workflow_run_properties=workflow_run_properties,
                delete_upon_successful_upload=False,
            )

            with open(output_file, "r") as f_out:
                for json_line in f_out:
                    metadata = json.loads(json_line)
                    assert (
                        sample_metadata["Metadata"]["start_date"].isoformat()
                        == metadata["export_start_date"]
                    )
                    assert (
                        sample_metadata["Metadata"]["end_date"].isoformat()
                        == metadata["export_end_date"]
                    )
                    assert isinstance(metadata['Value'], dict)
                    break

    def test_write_enrolled_participants_file_to_json_dataset(self, s3_obj, namespace, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x: MockAWSClient())
        sample_metadata = {
            "Metadata": {
                "type": "EnrolledParticipants",
                "start_date": None,
                "end_date": datetime.datetime(2023, 1, 14, 0, 0)
            }
        }
        workflow_run_properties = {
            "namespace": namespace,
            "json_prefix": "raw-json",
            "json_bucket": "json-bucket",
        }
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            output_file = s3_to_json.write_file_to_json_dataset(
                z=z,
                json_path="EnrolledParticipants_20230114.json",
                dataset_identifier="EnrolledParticipants",
                metadata=sample_metadata["Metadata"],
                workflow_run_properties=workflow_run_properties,
                delete_upon_successful_upload=False,
            )

            with open(output_file, "r") as f_out:
                for json_line in f_out:
                    metadata = json.loads(json_line)
                    assert (
                        sample_metadata["Metadata"]["end_date"].isoformat()
                        == metadata["export_end_date"]
                    )
                    if "Symptoms" in metadata['CustomFields']:
                        assert isinstance(metadata['CustomFields']['Symptoms'], list)
                    if "Treatments" in metadata['CustomFields']:
                        assert isinstance(metadata['CustomFields']['Treatments'], list)
                    break

    def test_write_file_to_json_dataset_record_consistency(self, s3_obj, namespace, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x: MockAWSClient())
        sample_metadata = {
            "Metadata": {
                "type": "FitbitDevices",
                "start_date": None,
                "end_date": datetime.datetime(2023, 1, 14, 0, 0)
            }
        }
        workflow_run_properties = {
            "namespace": namespace,
            "json_prefix": "raw-json",
            "json_bucket": "json-bucket",
        }
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            with z.open("FitbitDevices_20230114.json", "r") as fitbit_data:
                input_line_cnt = len(fitbit_data.readlines())

            output_file = s3_to_json.write_file_to_json_dataset(
                z=z,
                json_path="FitbitDevices_20230114.json",
                dataset_identifier="FitbitDevices",
                metadata=sample_metadata["Metadata"],
                workflow_run_properties=workflow_run_properties,
                delete_upon_successful_upload=False,
            )

            with open(output_file, "r") as f_out:
                output_line_cnt = 0
                for json_line in f_out:
                    metadata = json.loads(json_line)
                    assert metadata["export_start_date"] is None
                    assert (
                        sample_metadata["Metadata"]["end_date"].isoformat()
                        == metadata["export_end_date"]
                    )
                    output_line_cnt += 1
            # gets line count of input json and exported json and checks the two
            assert input_line_cnt == output_line_cnt


    def test_get_metadata_startdate_enddate(self, json_file_basenames_dict):
        basename = json_file_basenames_dict["HealthKitV2Samples_Deleted"]
        assert s3_to_json.get_metadata(basename)["start_date"] == datetime.datetime(
            2022, 1, 11, 0, 0
        ) and s3_to_json.get_metadata(basename)["end_date"] == datetime.datetime(
            2023, 1, 3, 0, 0
        )

    def test_get_metadata_no_startdate(self, json_file_basenames_dict):
        basename = json_file_basenames_dict["EnrolledParticipants"]
        assert s3_to_json.get_metadata(basename)[
            "start_date"
        ] == None and s3_to_json.get_metadata(basename)[
            "end_date"
        ] == datetime.datetime(
            2023, 1, 3, 0, 0
        )

    def test_get_metadata_subtype(self, json_file_basenames_dict):
        basename = json_file_basenames_dict["HealthKitV2Samples"]
        assert s3_to_json.get_metadata(basename)["subtype"] == "AbdominalCramps"

        basename = json_file_basenames_dict["HealthKitV2Statistics"]
        assert s3_to_json.get_metadata(basename)["subtype"] == "HourlySteps"

        basename_delete = json_file_basenames_dict["HealthKitV2Samples_Deleted"]
        assert s3_to_json.get_metadata(basename_delete)["subtype"] == "AbdominalCramps"

    def test_get_metadata_no_subtype(self, json_file_basenames_dict):
        # test that these have no subtype keys in metadata minus
        # HealthKitV2Samples, HealthKitV2Samples_Deleted and HealthKitV2Statistics
        metadata = [
            s3_to_json.get_metadata(basename)
            for basename in list(json_file_basenames_dict.values())
        ]
        subtypes = [
            "subtype" in record.keys()
            for record in metadata
            if record["type"]
            not in ["HealthKitV2Samples", "HealthKitV2Samples_Deleted", "HealthKitV2Statistics"]
        ]
        assert not any(subtypes),\
            "Some data types that are not HealthKitV2Samples or HealthKitV2Statistics have the metadata subtype key"

    def test_get_metadata_type(self, json_file_basenames_dict):
        # check that all file basenames match their type
        metadata_check = [
            s3_to_json.get_metadata(json_file_basenames_dict[basename])["type"]
            == basename
            for basename in json_file_basenames_dict.keys()
        ]
        assert all(metadata_check),\
            "Some data types' metadata type key are incorrect"
