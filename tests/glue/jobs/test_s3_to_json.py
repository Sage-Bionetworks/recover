import os
import io
import json
import zipfile

import boto3
import pytest

from src.glue.jobs import s3_to_json


class TestS3ToJsonS3:
    def test_get_metadata_type(self):
        assert (
            s3_to_json.get_metadata("HealthKitV2Samples_20201022-20211022.json")["type"]
            == "HealthKitV2Samples"
        )

        assert (
            s3_to_json.get_metadata(
                "HealthKitV2Statistics_Samples_20201022-20211022.json"
            )["type"]
            == "HealthKitV2Statistics"
        )

        assert (
            s3_to_json.get_metadata("HealthKitV2Statistics_20201022-20211022.json")[
                "type"
            ]
            == "HealthKitV2Statistics"
        )
        # these tests test that the healthkit sample data will
        # contain deleted in its type
        assert (
            s3_to_json.get_metadata(
                "HealthKitV2Samples_Deleted_20201022-20211022.json"
            )["type"]
            == "HealthKitV2Samples_Deleted"
        )

        assert (
            s3_to_json.get_metadata(
                "HealthKitV2Heartbeat_Deleted_20201022-20211022.json"
            )["type"]
            == "HealthKitV2Heartbeat_Deleted"
        )

        assert (
            s3_to_json.get_metadata(
                "HealthKitV2Electrocardiogram_Deleted_20201022-20211022.json"
            )["type"]
            == "HealthKitV2Electrocardiogram_Deleted"
        )

        assert (
            s3_to_json.get_metadata(
                "HealthKitV2Workouts_Deleted_20201022-20211022.json"
            )["type"]
            == "HealthKitV2Workouts_Deleted"
        )
