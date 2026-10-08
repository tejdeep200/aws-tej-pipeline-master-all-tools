"""Local transformation/catalog tests; Spark, Glue, and AWS calls are not run."""

import csv
import importlib.util
import unittest
from pathlib import Path
from unittest.mock import MagicMock

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("statistics_job", HERE / "bronze_to_silver_satistics.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
CSV_SOURCE = "s3://tej-data-1/youtube/raw_statistics/region=ca/CAvideos.csv"
JSON_SOURCE = "s3://tej-data-1/youtube/raw_statistics/region=ca/date=2026-10-07/hour=13/20261007T133215Z_example.json"


class StatisticsTests(unittest.TestCase):
    def setUp(self):
        self.video = {
            "id": "test-video",
            "snippet": {"title": "Música", "channelTitle": "Channel", "categoryId": "10",
                        "publishedAt": "2026-10-06T11:00:00Z", "tags": ["music", "live"],
                        "thumbnails": {"default": {"url": "https://example.com/image.jpg"}}},
            "statistics": {"viewCount": "1000", "likeCount": "100", "commentCount": "20"},
        }
        self.metadata = {"ingestion_timestamp": "2026-10-07T13:32:15Z", "region": "ca"}

    def test_real_kaggle_rows_and_day_month_order(self):
        with (HERE.parent / "CAvideos.csv").open(encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            for _ in range(20):
                raw = next(reader)
                row = module.normalize_statistics(raw, CSV_SOURCE, "test-job")
                self.assertTrue(row["_is_valid"], row["_dq_errors"])
                self.assertEqual(row["date"], "2017-11-14")
                self.assertEqual(row["trending_date"], "17.14.11")
                self.assertEqual(row["trending_date_parsed"].isoformat(), "2017-11-14")
                self.assertIsInstance(row["views"], int)
                self.assertIsInstance(row["comments_disabled"], bool)
                self.assertEqual(row["region"], "ca")

    def test_live_json_fields_and_percentages(self):
        row = module.normalize_statistics(self.video, JSON_SOURCE, "job", self.metadata)
        self.assertTrue(row["_is_valid"])
        self.assertEqual(row["date"], "2026-10-07")
        self.assertEqual(row["trending_date_parsed"].isoformat(), "2026-10-07")
        self.assertEqual(row["tags"], "music|live")
        self.assertEqual(row["like_ratio"], 10.0)
        self.assertEqual(row["engagement_rate"], 12.0)
        self.assertEqual(row["thumbnail_link"], "https://example.com/image.jpg")
        self.assertEqual(row["_observed_at"].isoformat(), "2026-10-07T13:32:15")
        self.assertIsNone(row["dislikes"])
        self.assertIsNone(row["comments_disabled"])

    def test_snapshot_time_falls_back_to_filename(self):
        row = module.normalize_statistics(self.video, JSON_SOURCE, "job")
        self.assertEqual(row["_observed_at"].isoformat(), "2026-10-07T13:32:15")
        self.assertEqual(row["date"], "2026-10-07")

    def test_missing_likes_are_not_invented_or_flagged_as_disabled(self):
        self.video["statistics"].pop("likeCount")
        row = module.normalize_statistics(self.video, JSON_SOURCE, "job", self.metadata)
        self.assertTrue(row["_is_valid"])
        self.assertIsNone(row["likes"])
        self.assertIsNone(row["like_ratio"])
        self.assertIsNone(row["ratings_disabled"])

    def test_bad_required_fields_counts_and_dates_are_flagged(self):
        self.video["statistics"]["viewCount"] = "-1"
        self.video["snippet"]["publishedAt"] = "bad-date"
        self.video["snippet"]["title"] = " "
        row = module.normalize_statistics(self.video, "s3://b/region=ca/snapshot.json", "job")
        self.assertFalse(row["_is_valid"])
        for expected in ("negative_views", "invalid_publish_time", "missing_title", "invalid_trending_date"):
            self.assertIn(expected, row["_dq_errors"])
        self.video["statistics"]["viewCount"] = "bad-number"
        row = module.normalize_statistics(self.video, JSON_SOURCE, "job", self.metadata)
        self.assertIn("invalid_views", row["_dq_errors"])

    def test_integer_bounds_boolean_and_date_validation(self):
        for value in [True, 1.5, "1.2", str(2**63), "bad"]:
            self.assertIsNone(module.parse_integer(value))
        self.assertEqual(module.parse_integer(" 123 "), 123)
        self.assertIsNone(module.parse_trending_date("17.31.02"))
        self.assertEqual(module.parse_trending_date("17.14.11"), "2017-11-14")
        self.assertFalse(module.parse_boolean("False"))

    def test_region_from_partition_or_filename(self):
        self.assertEqual(module.file_region(CSV_SOURCE.replace("region=ca", "region=CA")), "ca")
        self.assertEqual(module.file_region("s3://b/USvideos.csv"), "us")
        with self.assertRaises(ValueError):
            module.file_region("s3://b/unknown.csv")

    def test_items_array_is_exploded_with_snapshot_metadata(self):
        spark_row = MagicMock()
        spark_row.asDict.return_value = {"items": [self.video, self.video],
                                       "_pipeline_metadata": self.metadata, "_source_file": JSON_SOURCE}
        rows = list(module.normalize_partition([spark_row], "job", "json"))
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["_is_valid"] for row in rows))
        spark_row.asDict.return_value["items"] = None
        with self.assertRaises(ValueError):
            list(module.normalize_partition([spark_row], "job", "json"))

    def test_same_snapshot_has_deterministic_tie_breaker(self):
        first = module.normalize_statistics(self.video, JSON_SOURCE, "job", self.metadata)
        second = module.normalize_statistics(self.video, JSON_SOURCE, "job", self.metadata)
        self.assertEqual(first["_record_hash"], second["_record_hash"])
        self.video["statistics"]["viewCount"] = "2000"
        changed = module.normalize_statistics(self.video, JSON_SOURCE, "job", self.metadata)
        self.assertNotEqual(first["_record_hash"], changed["_record_hash"])

    def test_existing_catalog_schema_or_location_mismatch_is_rejected(self):
        client = MagicMock()
        path = "s3://tej-sliver-data/youtube/statistics/"
        columns = [{"Name": "video_id", "Type": "string"}]
        client.get_table.return_value = {"Table": {"StorageDescriptor": {"Columns": columns, "Location": path},
            "PartitionKeys": [{"Name": "region", "Type": "string"}, {"Name": "date", "Type": "string"}]}}
        module.prepare_catalog(client, "tej-pipeline-silver-dev", "clean_statistics", path, columns)
        client.create_table.assert_not_called()
        client.get_table.return_value["Table"]["StorageDescriptor"]["Location"] = "s3://b/other/"
        with self.assertRaises(ValueError):
            module.prepare_catalog(client, "db", "table", path, columns)

    def test_partition_registration_batches_and_checks_errors(self):
        client = MagicMock()
        client.batch_create_partition.return_value = {"Errors": [{"ErrorDetail": {"ErrorCode": "AlreadyExistsException"}}]}
        partitions = [{"region": "ca", "date": f"2020-{i:03d}"} for i in range(205)]
        module.register_partitions(client, "db", "table", "s3://b/statistics/", [], partitions)
        self.assertEqual([len(call.kwargs["PartitionInputList"]) for call in client.batch_create_partition.call_args_list], [100, 100, 5])
        client.batch_create_partition.return_value = {"Errors": [{"ErrorDetail": {"ErrorCode": "AccessDeniedException"}}]}
        with self.assertRaises(RuntimeError):
            module.register_partitions(client, "db", "table", "s3://b/statistics/", [], partitions)


if __name__ == "__main__":
    unittest.main()
