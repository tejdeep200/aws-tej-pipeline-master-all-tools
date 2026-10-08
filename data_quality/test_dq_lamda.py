import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("dq_lamda", HERE / "dq_lamda.py")
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class DataQualityTests(unittest.TestCase):
    def statistics_types(self):
        return {
            "video_id": "string",
            "title": "string",
            "channel_title": "string",
            "views": "bigint",
            "category_id": "bigint",
            "likes": "bigint",
            "dislikes": "bigint",
            "comment_count": "bigint",
            "trending_date_parsed": "date",
            "_processed_at": "timestamp",
            "region": "string",
        }

    def test_empty_event_uses_both_real_catalog_databases(self):
        self.assertEqual(
            module.resolve_targets({}),
            [
                ("tej-pipeline-silver-dev", "clean_statistics"),
                ("yt_pipeline_silver_dev", "clean_reference_data"),
            ],
        )

    def test_table_selection_and_database_override(self):
        self.assertEqual(
            module.resolve_targets({
                "database": "tej-pipeline-silver-dev",
                "tables": ["clean_statistics"],
            }),
            [("tej-pipeline-silver-dev", "clean_statistics")],
        )

    def test_rejects_unknown_table_and_sql_injection(self):
        with self.assertRaises(ValueError):
            module.resolve_targets({"tables": ["other"]})
        with self.assertRaises(ValueError):
            module.resolve_targets({
                "targets": [{"database": 'db"; DROP TABLE x', "table": "clean_statistics"}]
            })

    def test_schema_uses_partition_columns(self):
        glue = MagicMock()
        glue.get_table.return_value = {
            "Table": {
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "video_id", "Type": "string"},
                        {"Name": "title", "Type": "string"},
                        {"Name": "channel_title", "Type": "string"},
                        {"Name": "views", "Type": "bigint"},
                    ],
                    "Location": "s3://tej-sliver-data/youtube/statistics/",
                },
                "PartitionKeys": [{"Name": "region", "Type": "string"}],
            }
        }
        types, location = module.catalog_details(
            glue, "tej-pipeline-silver-dev", "clean_statistics"
        )
        self.assertEqual(types["region"], "string")
        self.assertEqual(location, "s3://tej-sliver-data/youtube/statistics/")
        self.assertTrue(
            module.check_schema(
                "tej-pipeline-silver-dev", "clean_statistics", types
            )["passed"]
        )

    def test_schema_reports_missing_and_wrong_types(self):
        result = module.check_schema(
            "tej-pipeline-silver-dev",
            "clean_statistics",
            {"video_id": "bigint"},
        )
        self.assertFalse(result["passed"])
        self.assertIn("views", result["missing_columns"])
        self.assertEqual(result["type_mismatches"][0]["column"], "video_id")

    def test_metrics_query_uses_exact_count_and_freshness(self):
        query, timestamp_column = module.build_metrics_query(
            "tej-pipeline-silver-dev", "clean_statistics", self.statistics_types()
        )
        self.assertIn("COUNT(*) AS row_count", query)
        self.assertIn("null_video_id", query)
        self.assertIn("invalid_views", query)
        self.assertIn('MAX("_processed_at")', query)
        self.assertEqual(timestamp_column, "_processed_at")

    def test_duplicate_query_uses_business_key(self):
        query = module.build_duplicate_query(
            "tej-pipeline-silver-dev", "clean_statistics", self.statistics_types()
        )
        self.assertIn('GROUP BY "video_id", "region", "trending_date_parsed"', query)
        self.assertIn("duplicate_count", query)

    def test_good_metrics_pass(self):
        metrics = {
            "row_count": 100,
            "null_video_id": 0,
            "null_title": 1,
            "null_channel_title": 0,
            "null_views": 0,
            "null_region": 0,
            "invalid_category_id": 0,
            "invalid_views": 0,
            "invalid_likes": 0,
            "invalid_dislikes": 0,
            "invalid_comment_count": 0,
            "latest_timestamp": "2026-10-07 19:00:00.000 UTC",
        }
        checked_at = datetime(2026, 10, 7, 20, 0, tzinfo=timezone.utc)
        results = module.evaluate_metrics(
            "tej-pipeline-silver-dev",
            "clean_statistics",
            metrics,
            "_processed_at",
            checked_at,
        )
        self.assertTrue(all(result["passed"] for result in results))

    def test_bad_metrics_fail_row_null_range_and_freshness(self):
        metrics = {
            "row_count": 5,
            "null_video_id": 1,
            "null_title": 0,
            "null_channel_title": 0,
            "null_views": 0,
            "null_region": 0,
            "invalid_views": 2,
            "latest_timestamp": "2026-10-01T00:00:00+00:00",
        }
        checked_at = datetime(2026, 10, 7, 20, 0, tzinfo=timezone.utc)
        results = module.evaluate_metrics(
            "tej-pipeline-silver-dev",
            "clean_statistics",
            metrics,
            "_processed_at",
            checked_at,
        )
        self.assertFalse(results[0]["passed"])
        self.assertFalse(next(r for r in results if r.get("column") == "video_id")["passed"])
        self.assertFalse(next(r for r in results if r["check"] == "value_range")["passed"])
        self.assertFalse(next(r for r in results if r["check"] == "freshness")["passed"])

    def test_subscription_arn_is_not_accepted_as_topic_arn(self):
        subscription = (
            "arn:aws:sns:us-east-2:592505727819:tej-data-pipeline-alerts-dev:"
            "470223e0-ef38-48a2-a78f-8f120ae52205"
        )
        self.assertIsNone(module.SNS_TOPIC_PATTERN.fullmatch(subscription))
        self.assertIsNotNone(module.SNS_TOPIC_PATTERN.fullmatch(
            "arn:aws:sns:us-east-2:592505727819:tej-data-pipeline-alerts-dev"
        ))

    def test_handler_returns_failed_quality_and_sends_alert(self):
        glue = MagicMock()
        glue.get_table.return_value = {
            "Table": {
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": name, "Type": data_type}
                        for name, data_type in self.statistics_types().items()
                        if name != "region"
                    ],
                    "Location": "s3://tej-sliver-data/youtube/statistics/",
                },
                "PartitionKeys": [{"Name": "region", "Type": "string"}],
            }
        }
        sns = MagicMock()
        boto3_module = MagicMock()
        boto3_module.client.side_effect = lambda service: glue if service == "glue" else sns
        wr_module = MagicMock()
        wr_module.athena.read_sql_query.side_effect = [
            pd.DataFrame([{
                "row_count": 0,
                "null_video_id": 0,
                "null_title": 0,
                "null_channel_title": 0,
                "null_views": 0,
                "null_region": 0,
                "invalid_category_id": 0,
                "invalid_views": 0,
                "invalid_likes": 0,
                "invalid_dislikes": 0,
                "invalid_comment_count": 0,
                "latest_timestamp": None,
            }]),
            pd.DataFrame([{"duplicate_count": 0}]),
        ]
        topic = "arn:aws:sns:us-east-2:592505727819:tej-data-pipeline-alerts-dev"
        with patch.object(module, "SNS_TOPIC", topic), patch.object(
            module, "load_aws_dependencies", return_value=(boto3_module, wr_module)
        ):
            response = module.lambda_handler(
                {"tables": ["clean_statistics"]}, None
            )
        self.assertFalse(response["quality_passed"])
        self.assertTrue(response["alert_sent"])
        sns.publish.assert_called_once()


if __name__ == "__main__":
    unittest.main()
