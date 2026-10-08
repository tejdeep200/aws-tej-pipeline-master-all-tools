"""Verify ingestion request parameters, output paths, and failure handling."""

import importlib.util
import io
import json
import os
import unittest
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from unittest.mock import MagicMock, patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("youtube_ingestion", HERE / "lamda_functions.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
TEST_KEY = "unit-test-key-only"


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"YOUTUBE_API_KEY": TEST_KEY, "YOUTUBE_REGIONS": "CA"}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.s3 = MagicMock()
        self.client_patch = patch.object(module, "s3_client", self.s3)
        self.client_patch.start()
        self.addCleanup(self.client_patch.stop)

    def mock_response(self, data):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(data).encode("utf-8")
        return response

    def test_country_normalization_validation_and_deduplication(self):
        self.assertEqual(module.configured_regions(" ca ,US,CA"), ["CA", "US"])
        for value in ["", "CA,", "USA", "CA,1A"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                module.configured_regions(value)

    def test_google_parameters_use_uppercase_countries_and_fifty_results(self):
        with patch.object(module, "urlopen", return_value=self.mock_response({"items": [{}]})) as http:
            module.fetch_trending_videos("ca", TEST_KEY)
            request = http.call_args.args[0]
            query = parse_qs(urlsplit(request.full_url).query)
            self.assertEqual(query["regionCode"], ["CA"])
            self.assertEqual(query["chart"], ["mostPopular"])
            self.assertEqual(query["maxResults"], ["50"])
            self.assertEqual(query["part"], ["snippet,statistics,contentDetails"])
            self.assertEqual(http.call_args.kwargs["timeout"], 30)
            module.fetch_video_categories("ca", TEST_KEY)
            request = http.call_args.args[0]
            self.assertEqual(urlsplit(request.full_url).path, "/youtube/v3/videoCategories")
            self.assertEqual(parse_qs(urlsplit(request.full_url).query)["regionCode"], ["CA"])

    def test_http_and_network_errors_do_not_expose_api_key(self):
        payload = {"error": {"errors": [{"reason": "quotaExceeded", "message": TEST_KEY}]}}
        error = HTTPError(f"https://example.com/?key={TEST_KEY}", 403, "Forbidden", {}, io.BytesIO(json.dumps(payload).encode()))
        for failure in [error, URLError(f"Bad URL with {TEST_KEY}")]:
            with self.subTest(failure=type(failure).__name__), patch.object(module, "urlopen", side_effect=failure):
                with self.assertRaises(RuntimeError) as raised:
                    module.fetch_trending_videos("CA", TEST_KEY)
                self.assertNotIn(TEST_KEY, str(raised.exception))

    def test_invalid_api_responses_are_rejected(self):
        for data in [{}, {"items": {}}, []]:
            with self.subTest(data=data), patch.object(module, "urlopen", return_value=self.mock_response(data)):
                with self.assertRaises(ValueError):
                    module.fetch_trending_videos("CA", TEST_KEY)
        with patch.object(module, "urlopen", return_value=self.mock_response({"items": []})):
            with self.assertRaises(ValueError):
                module.fetch_video_categories("CA", TEST_KEY)

    def test_s3_preserves_utf8_json_and_metadata(self):
        module.write_to_s3({"items": [{"title": "Música"}]}, "tej-data-1", "test.json", "2026-10-07T00:00:00+00:00")
        args = self.s3.put_object.call_args.kwargs
        self.assertEqual(args["ContentType"], "application/json")
        self.assertIn("Música", args["Body"].decode("utf-8"))
        self.assertEqual(args["Metadata"]["source"], "youtube_data_api_v3")

    def test_success_uses_compatible_reference_path_and_snapshot_partitions(self):
        event = {"id": "scheduled-event-1", "time": "2026-10-07T04:00:00Z"}
        with patch.object(module, "fetch_trending_videos", return_value={"items": [{"id": "video"}]}), \
             patch.object(module, "fetch_video_categories", return_value={"items": [{"id": "1"}]}):
            result = module.lambda_handler(event, None)
        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(result["results"]["success"], ["ca"])
        self.assertEqual(self.s3.put_object.call_count, 2)
        writes = [call.kwargs for call in self.s3.put_object.call_args_list]
        self.assertTrue(writes[0]["Key"].startswith("youtube/raw_statistics/region=ca/date=2026-10-07/hour=04/"))
        self.assertEqual(writes[1]["Key"], "youtube/raw_statistics_reference_data/region=ca/CA_category_id.json")
        self.assertTrue(all(write["Bucket"] == "tej-data-1" for write in writes))
        metadata = json.loads(writes[0]["Body"])["_pipeline_metadata"]
        self.assertEqual(metadata["item_count"], 1)
        self.assertEqual(metadata["region"], "ca")

    def test_categories_still_run_when_chart_fails_and_lambda_raises(self):
        with patch.object(module, "fetch_trending_videos", side_effect=RuntimeError("Chart unavailable")), \
             patch.object(module, "fetch_video_categories", return_value={"items": [{"id": "1"}]}), \
             self.assertLogs(module.logger, level="ERROR"), self.assertRaises(RuntimeError) as raised:
            module.lambda_handler({}, None)
        result = json.loads(str(raised.exception))["results"]
        self.assertEqual(result["failed"][0]["type"], "trending")
        self.assertEqual(result["success"], [])
        self.assertEqual(result["objects"][0]["type"], "categories")
        self.s3.put_object.assert_called_once()

    def test_s3_failure_is_not_reported_as_success_and_redacts_key(self):
        self.s3.put_object.side_effect = RuntimeError(f"Simulated failure {TEST_KEY}")
        with patch.object(module, "fetch_trending_videos", return_value={"items": []}), \
             patch.object(module, "fetch_video_categories", return_value={"items": [{"id": "1"}]}), \
             self.assertLogs(module.logger, level="ERROR") as logs, self.assertRaises(RuntimeError) as raised:
            module.lambda_handler({}, None)
        self.assertNotIn(TEST_KEY, str(raised.exception))
        self.assertNotIn(TEST_KEY, " ".join(logs.output))
        self.assertEqual(len(json.loads(str(raised.exception))["results"]["failed"]), 2)

    def test_scheduled_retries_reuse_snapshot_key(self):
        event = {"id": "same-event", "time": "2026-10-07T02:00:00-04:00"}
        with patch.object(module, "fetch_trending_videos", side_effect=lambda *args: {"items": []}), \
             patch.object(module, "fetch_video_categories", side_effect=lambda *args: {"items": [{"id": "1"}]}):
            first = module.lambda_handler(event, None)
            second = module.lambda_handler(event, None)
        self.assertEqual(first["ingestion_id"], second["ingestion_id"])
        self.assertIn("date=2026-10-07/hour=06/", first["results"]["objects"][0]["key"])

    def test_missing_api_key_fails_before_writing(self):
        os.environ.pop("YOUTUBE_API_KEY")
        with self.assertRaisesRegex(ValueError, "YOUTUBE_API_KEY"):
            module.lambda_handler({}, None)
        self.s3.put_object.assert_not_called()


if __name__ == "__main__":
    unittest.main()
