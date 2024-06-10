import os
import io
import json
import shutil
import zipfile
import datetime
from dateutil.tz import tzutc

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
    def sample_metadata(self):
        sample_metadata = {
                "type": "FitbitDevices",
                "start_date": datetime.datetime(2022, 1, 12, 0, 0),
                "end_date": datetime.datetime(2023, 1, 14, 0, 0),
                "subtype": "FakeSubtype",
                "cohort": "adults_v1"
        }
        return sample_metadata

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
            "HealthKitV2ActivitySummaries_Deleted": "HealthKitV2ActivitySummaries_Deleted_20220111-20230103.json",
            "HealthKitV2Electrocardiogram": "HealthKitV2Electrocardiogram_Samples_20220111-20230103.json",
            "HealthKitV2Electrocardiogram_Deleted": "HealthKitV2Electrocardiogram_Samples_Deleted_20220111-20230103.json",
            "HealthKitV2Heartbeat": "HealthKitV2Heartbeat_Samples_20220401-20230112.json",
            "HealthKitV2Heartbeat_Deleted": "HealthKitV2Heartbeat_Samples_Deleted_20220401-20230112.json",
            "HealthKitV2Samples": "HealthKitV2Samples_AbdominalCramps_20220111-20230103.json",
            "HealthKitV2Samples_Deleted": "HealthKitV2Samples_AbdominalCramps_Deleted_20220111-20230103.json",
            "HealthKitV2Statistics": "HealthKitV2Statistics_HourlySteps_20201022-20211022.json",
            "HealthKitV2Statistics_Deleted": "HealthKitV2Statistics_HourlySteps_Deleted_20201022-20211022.json",
            "HealthKitV2Workouts": "HealthKitV2Workouts_20220111-20230103.json",
            "HealthKitV2Workouts_Deleted": "HealthKitV2Workouts_Deleted_20220111-20230103.json",
            "SymptomLog": "SymptomLog_20220401-20230112.json",
        }
        return json_file_basenames

    def test_transform_object_to_array_of_objects(self):
        json_obj_to_replace = {
                "0": 60.0,
                "1": 61.2,
                "2": "99"
        }
        transformed_object = s3_to_json.transform_object_to_array_of_objects(
                json_obj_to_replace=json_obj_to_replace,
                key_name="key",
                key_type=int,
                value_name="value",
                value_type=int
        )
        expected_object = [
                {
                    "key": 0,
                    "value": 60
                },
                {
                    "key": 1,
                    "value": 61
                },
                {
                    "key":2,
                    "value": 99
                }
        ]
        assert all([obj in expected_object for obj in transformed_object])

    def test_log_error_transform_object_to_array_of_objects(self, caplog):
        s3_to_json.logger.propagate = True
        s3_to_json._log_error_transform_object_to_array_of_objects(
                value="a",
                value_type=int,
                error=ValueError,
                logger_context={}
        )
        s3_to_json.logger.propagate = False
        assert len(caplog.records) == 2

    def test_transform_json_with_subtype(self, sample_metadata):
        sample_metadata["type"] = "HealthKitV2Samples"
        transformed_json = s3_to_json.transform_json(
                json_obj={},
                metadata=sample_metadata
        )

        assert sample_metadata["subtype"] == transformed_json["Type"]
        assert (
            sample_metadata["start_date"].isoformat()
            == transformed_json["export_start_date"]
        )
        assert (
            sample_metadata["end_date"].isoformat()
            == transformed_json["export_end_date"]
        )

    def test_transform_json_symptom_log(self, sample_metadata):
        sample_metadata["type"] = "SymptomLog"
        transformed_value = {"a": 1, "b": 2}
        transformed_json = s3_to_json.transform_json(
                json_obj={"Value": json.dumps(transformed_value)},
                metadata=sample_metadata
        )

        assert (
            sample_metadata["start_date"].isoformat()
            == transformed_json["export_start_date"]
        )
        assert (
            sample_metadata["end_date"].isoformat()
            == transformed_json["export_end_date"]
        )
        assert transformed_json["Value"] == transformed_value

    def test_add_universal_properties_start_date(self, sample_metadata):
        json_obj = s3_to_json._add_universal_properties(
                json_obj={},
                metadata=sample_metadata,
        )
        assert json_obj["export_start_date"] == sample_metadata["start_date"].isoformat()

    def test_add_universal_properties_no_start_date(self, sample_metadata):
        sample_metadata["start_date"] = None
        json_obj = s3_to_json._add_universal_properties(
                json_obj={},
                metadata=sample_metadata,
        )
        assert json_obj["export_start_date"] is None

    def test_add_universal_properties_generic(self, sample_metadata):
        json_obj = s3_to_json._add_universal_properties(
                json_obj={},
                metadata=sample_metadata,
        )
        assert json_obj["export_end_date"] == sample_metadata["end_date"].isoformat()
        assert json_obj["cohort"] == sample_metadata["cohort"]

    def test_cast_custom_fields_to_array(self):
        sample_symptoms = {"id": "123", "symptom": "sick"}
        transformed_json = s3_to_json._cast_custom_fields_to_array(
                json_obj={
                    "CustomFields": {
                        "Symptoms": json.dumps(sample_symptoms)
                    }
                },
                logger_context={},
        )
        assert all(
                [
                    item in transformed_json["CustomFields"]["Symptoms"].items()
                    for item in sample_symptoms.items()
                ]
        )

    def test_cast_custom_fields_to_array_malformatted_str(self, caplog):
        s3_to_json.logger.propagate = True
        transformed_json = s3_to_json._cast_custom_fields_to_array(
                json_obj={
                    "CustomFields": {
                        "Symptoms": r'[{\\\"id\\\": "123", \\\"symptom\\\": "sick"}]'
                    }
                },
                logger_context={},
        )
        s3_to_json.logger.propagate = False
        assert len(caplog.records) == 1
        assert transformed_json["CustomFields"]["Symptoms"] == []

    def test_transform_garmin_data_types_one_level_hierarchy(self):
        time_offset_heartrate_samples = {
                "0": 60.0,
                "1": 61.0,
                "2": 99.0
        }
        transformed_time_offset_heartrate_samples = [
                {
                    "OffsetInSeconds": 0,
                    "HeartRate": 60
                },
                {
                    "OffsetInSeconds": 1,
                    "HeartRate": 61
                },
                {
                    "OffsetInSeconds":2,
                    "HeartRate": 99
                }
        ]
        data_type_transforms={"TimeOffsetHeartRateSamples": (("OffsetInSeconds", int), ("HeartRate", int))}
        transformed_json = s3_to_json._transform_garmin_data_types(
                json_obj={"TimeOffsetHeartRateSamples": time_offset_heartrate_samples},
                data_type_transforms=data_type_transforms,
                logger_context={},
        )
        assert all(
                [
                    obj in transformed_json["TimeOffsetHeartRateSamples"]
                    for obj in transformed_time_offset_heartrate_samples
                ]
        )

    def test_transform_garmin_data_types_two_level_hierarchy(self):
        epoch_summaries = {
                "0": 60.0,
                "1": 61.0,
                "2": 99.0
        }
        transformed_epoch_summaries = [
                {
                    "OffsetInSeconds": 0,
                    "Value": 60.0
                },
                {
                    "OffsetInSeconds": 1,
                    "Value": 61.0
                },
                {
                    "OffsetInSeconds":2,
                    "Value": 99.0
                }
        ]
        data_type_transforms= {
                "Summaries.EpochSummaries": (("OffsetInSeconds", int), ("Value", float))
        }
        transformed_json = s3_to_json._transform_garmin_data_types(
                json_obj={
                    "Summaries": [
                        {
                            "EpochSummaries": epoch_summaries,
                            "Dummy": 1
                        },
                        {
                            "EpochSummaries": epoch_summaries,
                        },
                    ]
                },
                data_type_transforms=data_type_transforms,
                logger_context={},
        )
        print(transformed_json)
        assert all(
                [
                    obj in transformed_json["Summaries"][0]["EpochSummaries"]
                    for obj in transformed_epoch_summaries
                ]
        )
        assert transformed_json["Summaries"][0]["Dummy"] == 1
        assert all(
                [
                    obj in transformed_json["Summaries"][1]["EpochSummaries"]
                    for obj in transformed_epoch_summaries
                ]
        )

    def test_transform_block_empty_file(self, s3_obj, sample_metadata):
        sample_metadata["type"] = "HealthKitV2Samples"
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            json_path = "HealthKitV2Samples_Weight_20230112-20230114.json"
            with z.open(json_path, "r") as input_json:
                transformed_block = s3_to_json.transform_block(
                    input_json=input_json,
                    metadata=sample_metadata,
                    block_size=2
                )
                with pytest.raises(StopIteration):
                    next(transformed_block)

    def test_transform_block_non_empty_file_block_size(self, s3_obj, sample_metadata):
        sample_metadata["type"] = "FitbitSleepLogs"
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            json_path = "FitbitSleepLogs_20230112-20230114.json"
            with z.open(json_path, "r") as input_json:
                transformed_block = s3_to_json.transform_block(
                    input_json=input_json,
                    metadata=sample_metadata,
                    block_size=2
                )
                first_block = next(transformed_block)
                assert len(first_block) == 2
                assert (
                        isinstance(first_block[0], dict)
                        and isinstance(first_block[1], dict)
                )

    def test_transform_block_non_empty_file_all_blocks(self, s3_obj, sample_metadata):
        sample_metadata["type"] = "FitbitSleepLogs"
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            json_path = "FitbitSleepLogs_20230112-20230114.json"
            with z.open(json_path, "r") as input_json:
                record_count = len(input_json.readlines())
            with z.open(json_path, "r") as input_json:
                transformed_block = s3_to_json.transform_block(
                    input_json=input_json,
                    metadata=sample_metadata,
                    block_size=10
                )
                counter = 0
                for block in transformed_block:
                    counter += len(block)
                # Should be 12
                assert counter == record_count

    def test_get_output_filename_generic(self, sample_metadata):
        output_filename = s3_to_json.get_output_filename(
                metadata=sample_metadata,
                part_number=0
        )
        assert output_filename == f"{sample_metadata['type']}_20220112-20230114.part0.ndjson"

    def test_get_output_filename_no_start_date(self, sample_metadata):
        sample_metadata["start_date"] = None
        output_filename = s3_to_json.get_output_filename(
                metadata=sample_metadata,
                part_number=0
        )
        assert output_filename == f"{sample_metadata['type']}_20230114.part0.ndjson"

    def test_get_output_filename_subtype(self, sample_metadata):
        sample_metadata["type"] = "HealthKitV2Samples"
        output_filename = s3_to_json.get_output_filename(
                metadata=sample_metadata,
                part_number=0
        )
        assert output_filename == "HealthKitV2Samples_FakeSubtype_20220112-20230114.part0.ndjson"

    def test_upload_file_to_json_dataset_delete_local_copy(self, namespace, sample_metadata, monkeypatch, shared_datadir):
        monkeypatch.setattr("boto3.client", lambda x: MockAWSClient())
        workflow_run_properties = {
            "namespace": namespace,
            "json_prefix": "raw-json",
            "json_bucket": "json-bucket",
        }
        original_file_path = os.path.join(shared_datadir, "2023-01-13T21--08--51Z_TESTDATA")
        temp_dir = f"dataset={sample_metadata['type']}"
        os.makedirs(temp_dir)
        new_file_path = shutil.copy(original_file_path, temp_dir)
        s3_response = s3_to_json._upload_file_to_json_dataset(
            file_path=new_file_path,
            s3_metadata=sample_metadata,
            workflow_run_properties=workflow_run_properties,
            delete_upon_successful_upload=True,
        )

        assert not os.path.exists(new_file_path)
        shutil.rmtree(temp_dir)

    def test_upload_file_to_json_dataset_s3_key(self, namespace, monkeypatch, shared_datadir):
        monkeypatch.setattr("boto3.client", lambda x: MockAWSClient())
        sample_metadata = {
                "type": "HealthKitV2Samples",
                "subtype": "Weight",
        }
        workflow_run_properties = {
            "namespace": namespace,
            "json_prefix": "raw-json",
            "json_bucket": "json-bucket",
        }
        original_file_path = os.path.join(shared_datadir, "2023-01-13T21--08--51Z_TESTDATA")
        temp_dir = f"dataset={sample_metadata['type']}"
        os.makedirs(temp_dir)
        new_file_path = shutil.copy(original_file_path, temp_dir)
        s3_key = s3_to_json._upload_file_to_json_dataset(
            file_path=new_file_path,
            s3_metadata=sample_metadata,
            workflow_run_properties=workflow_run_properties,
            delete_upon_successful_upload=True,
        )

        correct_s3_key = os.path.join(
            workflow_run_properties["namespace"],
            workflow_run_properties["json_prefix"],
            new_file_path,
        )
        assert s3_key == correct_s3_key
        shutil.rmtree(temp_dir)

    def test_write_file_to_json_dataset_delete_local_copy(self, s3_obj, sample_metadata, namespace, monkeypatch):
        sample_metadata["type"] = "HealthKitV2Samples"
        monkeypatch.setattr("boto3.client", lambda x: MockAWSClient())
        workflow_run_properties = {
            "namespace": namespace,
            "json_prefix": "raw-json",
            "json_bucket": "json-bucket",
        }
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            output_files = s3_to_json.write_file_to_json_dataset(
                z=z,
                json_path="HealthKitV2Samples_Weight_20230112-20230114.json",
                metadata=sample_metadata,
                workflow_run_properties=workflow_run_properties,
                delete_upon_successful_upload=True,
            )
        output_file = output_files[0]

        assert not os.path.exists(output_file)
        shutil.rmtree(f"dataset={sample_metadata['type']}")

    def test_write_file_to_json_dataset_record_consistency(
            self, s3_obj, sample_metadata, namespace, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x: MockAWSClient())
        sample_metadata["start_date"] = None
        workflow_run_properties = {
            "namespace": namespace,
            "json_prefix": "raw-json",
            "json_bucket": "json-bucket",
        }
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            with z.open("FitbitDevices_20230114.json", "r") as fitbit_data:
                input_line_cnt = len(fitbit_data.readlines())

            output_files = s3_to_json.write_file_to_json_dataset(
                z=z,
                json_path="FitbitDevices_20230114.json",
                metadata=sample_metadata,
                workflow_run_properties=workflow_run_properties,
                delete_upon_successful_upload=False,
            )
            output_file = output_files[0]

            with open(output_file, "r") as f_out:
                output_line_cnt = 0
                for json_line in f_out:
                    metadata = json.loads(json_line)
                    assert metadata["export_start_date"] is None
                    assert (
                        sample_metadata["end_date"].isoformat()
                        == metadata["export_end_date"]
                    )
                    output_line_cnt += 1
            # gets line count of input json and exported json and checks the two
            assert input_line_cnt == output_line_cnt
            shutil.rmtree(f"dataset={sample_metadata['type']}", ignore_errors=True)

    def test_write_file_to_json_dataset_multiple_parts(
            self, s3_obj, sample_metadata, namespace, monkeypatch):
        monkeypatch.setattr("boto3.client", lambda x: MockAWSClient())
        sample_metadata["type"] = "FitbitIntradayCombined"
        workflow_run_properties = {
            "namespace": namespace,
            "json_prefix": "raw-json",
            "json_bucket": "json-bucket",
        }
        with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
            json_path = "FitbitIntradayCombined_20230112-20230114.json"
            with z.open(json_path, "r") as fitbit_data:
                input_line_cnt = len(fitbit_data.readlines())
            output_files = s3_to_json.write_file_to_json_dataset(
                z=z,
                json_path=json_path,
                metadata=sample_metadata,
                workflow_run_properties=workflow_run_properties,
                delete_upon_successful_upload=False,
                file_size_limit=1e6
            )
            output_line_count = 0
            for output_file in output_files:
                with open(output_file, "r") as f_out:
                    for json_line in f_out:
                        output_line_count += 1
                os.remove(output_file)
            assert input_line_cnt == output_line_count
            shutil.rmtree(f"dataset={sample_metadata['type']}", ignore_errors=True)

    def test_derive_str_metadata(self, sample_metadata):
        str_metadata = s3_to_json._derive_str_metadata(metadata=sample_metadata)
        assert isinstance(str_metadata["start_date"], str)
        assert isinstance(str_metadata["end_date"], str)

    def test_get_part_path_no_touch(self, sample_metadata):
        sample_metadata["start_date"] = None
        part_path = s3_to_json.get_part_path(
                metadata=sample_metadata,
                part_number=0,
                part_dir=sample_metadata["type"],
                touch=False
        )
        assert part_path == "FitbitDevices/FitbitDevices_20230114.part0.ndjson"

    def test_get_part_path_touch(self, sample_metadata):
        part_path = s3_to_json.get_part_path(
                metadata=sample_metadata,
                part_number=0,
                part_dir=sample_metadata["type"],
                touch=True
        )
        assert os.path.exists(part_path)
        shutil.rmtree(sample_metadata["type"], ignore_errors=True)

    def test_get_metadata_startdate_enddate(self, json_file_basenames_dict):
        basename = json_file_basenames_dict["HealthKitV2Samples_Deleted"]
        assert s3_to_json.get_metadata(basename)["start_date"] == datetime.datetime(
            2022, 1, 11, 0, 0
        ) and s3_to_json.get_metadata(basename)["end_date"] == datetime.datetime(
            2023, 1, 3, 0, 0
        )

    def test_get_metadata_no_startdate(self, json_file_basenames_dict):
        basename = json_file_basenames_dict["EnrolledParticipants"]
        assert s3_to_json.get_metadata(basename)["start_date"] is None
        assert s3_to_json.get_metadata(basename)["end_date"] == \
                datetime.datetime(2023, 1, 3, 0, 0)

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
            if record["type"] not in [
                "HealthKitV2Samples",
                "HealthKitV2Samples_Deleted",
                "HealthKitV2Statistics",
                "HealthKitV2Statistics_Deleted"
            ]
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

    def test_get_basic_file_info(self):
        file_path = "my/dir/HealthKitV2Samples_Weight_20230112-20230114.json"
        basic_file_info = s3_to_json.get_basic_file_info(file_path=file_path)
        required_fields = ["file.type", "file.path", "file.name", "file.extension"]
        assert all([field in basic_file_info for field in required_fields])
        assert basic_file_info["file.name"] == "HealthKitV2Samples_Weight_20230112-20230114.json"
        assert basic_file_info["file.extension"] == "json"
