import importlib.util
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "silver_to_gold", HERE / "sliver_to_gold_analytics.py"
)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class MissingTable(Exception):
    pass


class SilverToGoldTests(unittest.TestCase):
    def test_default_options_match_project(self):
        options = module.parse_options([])
        self.assertEqual(options.statistics_database, "tej-pipeline-silver-dev")
        self.assertEqual(options.reference_database, "yt_pipeline_silver_dev")
        self.assertEqual(options.silver_bucket, "tej-sliver-data")
        self.assertEqual(options.gold_bucket, "gold-bucket-name")
        self.assertEqual(options.gold_database, "tej-data-gold-dev")

    def test_silver_database_alias_is_supported(self):
        options = module.parse_options(["--silver_database", "silver-db"])
        self.assertEqual(options.statistics_database, "silver-db")

    def test_bookmarks_must_be_disabled(self):
        with self.assertRaises(ValueError):
            module.parse_options(["--job-bookmark-option", "job-bookmark-enable"])

    def test_invalid_catalog_and_bucket_names_are_rejected(self):
        with self.assertRaises(ValueError):
            module.parse_options(["--gold_bucket", "Bad_Bucket"])
        with self.assertRaises(ValueError):
            module.parse_options(["--gold_database", "gold;drop"])

    def test_source_table_must_use_expected_path(self):
        catalog = MagicMock()
        catalog.get_table.return_value = {
            "Table": {"StorageDescriptor": {"Location": "s3://wrong/path/"}}
        }
        with self.assertRaises(ValueError):
            module.validate_source_table(
                catalog,
                "tej-pipeline-silver-dev",
                "clean_statistics",
                "s3://tej-sliver-data/youtube/statistics/",
            )

    def test_required_columns_are_checked(self):
        frame = SimpleNamespace(columns=["video_id", "region"])
        with self.assertRaisesRegex(ValueError, "category_id"):
            module.require_columns(
                frame, module.REQUIRED_STATISTICS_COLUMNS, "statistics"
            )

    def test_partition_locations_are_scoped(self):
        path = module.partition_location(
            "s3://gold-bucket-name/youtube/trending_analytics/",
            ["region", "date"],
            ["ca", "2026-10-07"],
        )
        self.assertEqual(
            path,
            "s3://gold-bucket-name/youtube/trending_analytics/region=ca/date=2026-10-07/",
        )
        with self.assertRaises(ValueError):
            module.partition_location("s3://b/x/", ["region"], ["../bad"])

    def test_new_catalog_table_uses_parquet_and_partitions(self):
        catalog = MagicMock()
        catalog.exceptions.EntityNotFoundException = MissingTable
        catalog.get_table.side_effect = MissingTable()
        columns = [{"Name": "total_views", "Type": "bigint"}]
        module.prepare_catalog(
            catalog,
            "tej-data-gold-dev",
            "trending_analytics",
            "s3://gold-bucket-name/youtube/trending_analytics/",
            columns,
            ["region", "date"],
        )
        table_input = catalog.create_table.call_args.kwargs["TableInput"]
        self.assertEqual(table_input["StorageDescriptor"]["Columns"], columns)
        self.assertEqual(
            table_input["PartitionKeys"],
            [
                {"Name": "region", "Type": "string"},
                {"Name": "date", "Type": "string"},
            ],
        )

    def test_existing_catalog_mismatch_stops_before_write(self):
        catalog = MagicMock()
        catalog.exceptions.EntityNotFoundException = MissingTable
        catalog.get_table.return_value = {
            "Table": {
                "StorageDescriptor": {
                    "Location": "s3://another-bucket/wrong/",
                    "Columns": [],
                },
                "PartitionKeys": [],
            }
        }
        with self.assertRaises(ValueError):
            module.prepare_catalog(
                catalog,
                "tej-data-gold-dev",
                "channel_analytics",
                "s3://gold-bucket-name/youtube/channel_analytics/",
                [],
                ["region"],
            )

    def test_partition_replacement_batches_delete_and_create(self):
        catalog = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Partitions": [
                    {"Values": ["ca"]},
                    {"Values": ["us"]},
                ]
            }
        ]
        catalog.get_paginator.return_value = paginator
        catalog.batch_delete_partition.return_value = {}
        catalog.batch_create_partition.return_value = {}
        module.replace_partitions(
            catalog,
            "tej-data-gold-dev",
            "channel_analytics",
            "s3://gold-bucket-name/youtube/channel_analytics/",
            [{"Name": "total_views", "Type": "bigint"}],
            ["region"],
            [{"region": "ca"}, {"region": "us"}, {"region": "ca"}],
        )
        deleted = catalog.batch_delete_partition.call_args.kwargs[
            "PartitionsToDelete"
        ]
        created = catalog.batch_create_partition.call_args.kwargs[
            "PartitionInputList"
        ]
        self.assertEqual(len(deleted), 2)
        self.assertEqual(len(created), 2)

    def test_all_three_gold_tables_have_dedicated_paths(self):
        prefixes = {spec["prefix"] for spec in module.GOLD_TABLES.values()}
        self.assertEqual(len(prefixes), 3)
        self.assertEqual(
            module.GOLD_TABLES["trending_analytics"]["partitions"],
            ["region", "date"],
        )

    def test_reference_window_uses_renamed_category_id(self):
        source = (HERE / "sliver_to_gold_analytics.py").read_text(encoding="utf-8")
        self.assertIn('reference_order.append(F.col("category_id").asc())', source)
        self.assertNotIn('reference_order.append(F.col("id").asc())', source)


if __name__ == "__main__":
    unittest.main()
