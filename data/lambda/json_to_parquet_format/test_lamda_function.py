"""Local data checks with real pandas/PyArrow and mocked AWS boundaries."""

import importlib.util
import io
import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

HERE = Path(__file__).resolve().parent
DATA = HERE.parents[1]
KEY = "youtube/raw_statistics_reference_data/region=ca/CA_category_id.json"

# Importing the module does not initialize AWS libraries or clients.
spec = importlib.util.spec_from_file_location("category_lambda", HERE / "lamda_function.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class CategoryLambdaTests(unittest.TestCase):
    def setUp(self):
        self.s3 = MagicMock()
        self.wr = MagicMock()
        self.wr.s3.to_parquet.return_value = {"paths": ["s3://tej-sliver-data/test.parquet"]}
        self.raw = json.loads((DATA / "CA_category_id.json").read_text(encoding="utf-8"))
        self.s3.get_object.side_effect = lambda **kwargs: {
            "Body": io.BytesIO(json.dumps(self.raw).encode("utf-8"))
        }
        settings = {
            "s3_client": self.s3,
            "wr": self.wr,
            "BRONZE_BUCKET": "tej-data-1",
            "GLUE_DB": "yt_pipeline_silver_dev",
            "GLUE_TABLE": "clean_reference_data",
            "SILVER_PATH": "s3://tej-sliver-data/youtube/reference_data/",
            "SNS_TOPIC": "",
        }
        self.patches = [patch.object(module, name, value) for name, value in settings.items()]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def test_all_ten_country_files_round_trip_as_parquet(self):
        files = sorted(DATA.glob("*_category_id.json"))
        self.assertEqual(len(files), 10)
        for file in files:
            with self.subTest(country=file.name[:2]):
                raw = json.loads(file.read_text(encoding="utf-8"))
                region = file.name[:2].lower()
                key = f"youtube/raw_statistics_reference_data/region={region}/{file.name}"
                df = module.normalize_categories(raw, region, key)
                self.assertEqual(len(df), len({item["id"] for item in raw["items"]}))
                self.assertEqual(set(df["region"]), {region})
                self.assertEqual(set(df["snippet_title"]), {item["snippet"]["title"] for item in raw["items"]})
                # Region is a directory partition, not a physical Parquet column.
                expected = df.drop(columns=["region"])
                buffer = io.BytesIO()
                expected.to_parquet(buffer, index=False, compression="snappy")
                buffer.seek(0)
                pd.testing.assert_frame_equal(pd.read_parquet(buffer), expected)

    def test_deduplication_keeps_last_category(self):
        duplicate = json.loads(json.dumps(self.raw["items"][0]))
        duplicate["snippet"]["title"] = "Updated category"
        self.raw["items"].append(duplicate)
        df = module.normalize_categories(self.raw, "ca", KEY)
        self.assertEqual(df.loc[df["id"] == duplicate["id"], "snippet_title"].tolist(), ["Updated category"])

    def test_bad_category_data_is_rejected(self):
        bad_data = [[], {}, {"items": []}, {"items": [1]},
                    {"items": [{"id": "1"}]},
                    {"items": [{"id": None, "snippet": {"title": "Title"}}]},
                    {"items": [{"id": "1", "snippet": {"title": " "}}]},
                    {"items": [{"id": "1", "snippet": {"title": "Title", "assignable": "false"}}]}]
        for raw in bad_data:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                module.normalize_categories(raw, "ca", KEY)

    def test_missing_optional_fields_keep_stable_types(self):
        df = module.normalize_categories({"items": [{"id": "1", "snippet": {"title": "Music"}}]}, "ca", KEY)
        self.assertTrue(pd.isna(df.loc[0, "snippet_assignable"]))
        self.assertEqual(str(df["snippet_assignable"].dtype), "boolean")
        self.assertEqual(str(df["snippet_channel_id"].dtype), "string")

    def test_country_mismatch_is_rejected(self):
        with self.assertRaises(ValueError):
            module.source_region("tej-data-1", KEY.replace("region=ca", "region=us"))

    def test_direct_invocation_writes_expected_dataset(self):
        result = module.lambda_handler({"bucket": "tej-data-1", "key": KEY}, None)
        self.assertEqual(result["processed"][0]["region"], "ca")
        self.s3.get_object.assert_called_once_with(Bucket="tej-data-1", Key=KEY)
        self.wr.catalog.create_database.assert_called_once_with(name="yt_pipeline_silver_dev", exist_ok=True)
        args = self.wr.s3.to_parquet.call_args.kwargs
        self.assertEqual(args["path"], "s3://tej-sliver-data/youtube/reference_data/")
        self.assertEqual(args["partition_cols"], ["region"])
        self.assertEqual(args["mode"], "overwrite_partitions")
        self.assertEqual(args["table"], "clean_reference_data")

    def test_s3_key_decoding_and_delete_event_filtering(self):
        info = {"bucket": {"name": "tej-data-1"}, "object": {"key": KEY.replace("=", "%3D")}}
        record = {"eventSource": "aws:s3", "eventName": "ObjectCreated:Put", "s3": info}
        self.assertEqual(module.source_objects({"Records": [record]}), [("tej-data-1", KEY)])
        record["eventName"] = "ObjectRemoved:Delete"
        self.assertEqual(module.source_objects({"Records": [record]}), [])

    def test_eventbridge_key_is_literal(self):
        event = {"source": "aws.s3", "detail-type": "Object Created",
                 "detail": {"bucket": {"name": "tej-data-1"}, "object": {"key": "literal+key"}}}
        self.assertEqual(module.source_objects(event), [("tej-data-1", "literal+key")])

    def test_unrelated_csv_and_bucket_do_not_write(self):
        for bucket, key in [("tej-data-1", KEY.replace("_category_id.json", "videos.csv")), ("other-bucket", KEY)]:
            result = module.lambda_handler({"bucket": bucket, "key": key}, None)
            self.assertEqual(len(result["skipped"]), 1)
        self.wr.s3.to_parquet.assert_not_called()
        self.s3.get_object.assert_not_called()

    def test_backfill_pagination_and_duplicate_notifications(self):
        self.s3.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": KEY}]},
            {"Contents": [{"Key": KEY}, {"Key": KEY.replace(".json", ".csv")}]},
        ]
        result = module.lambda_handler({"backfill": True}, None)
        self.assertEqual(len(result["processed"]), 1)
        self.wr.s3.to_parquet.assert_called_once()

    def test_empty_backfill_and_unknown_event_are_errors(self):
        self.s3.get_paginator.return_value.paginate.return_value = [{}]
        for event in [{"backfill": True}, {}]:
            with self.assertRaises(ValueError):
                module.lambda_handler(event, None)
        self.assertEqual(module.lambda_handler({"Event": "s3:TestEvent"}, None)["processed"], [])

    def test_s3_or_parquet_failure_is_raised_for_lambda_retries(self):
        for boundary in [self.s3.get_object, self.wr.s3.to_parquet]:
            with self.subTest(boundary=boundary):
                boundary.side_effect = RuntimeError("AWS write/read failure")
                with self.assertLogs(module.logger, level="ERROR"), self.assertRaises(RuntimeError):
                    module.lambda_handler({"bucket": "tej-data-1", "key": KEY}, None)
                boundary.side_effect = None
                self.s3.get_object.side_effect = lambda **kwargs: {"Body": io.BytesIO(json.dumps(self.raw).encode())}


if __name__ == "__main__":
    unittest.main()
