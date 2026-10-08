"""Fetch YouTube popular videos and category mappings into the Bronze bucket.

Handler: lamda_functions.lambda_handler
Required environment variable: YOUTUBE_API_KEY.
Optional: S3_BUCKET_BRONZE, YOUTUBE_REGIONS, SNS_ALERT_TOPIC_ARN.
Uses Python's standard HTTP library and boto3 provided by the Lambda runtime.
"""

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_BASE = "https://www.googleapis.com/youtube/v3"
DEFAULT_REGIONS = "US,GB,CA,DE,FR,IN,JP,KR,MX,RU"
MAX_RESULTS = 50
s3_client = None


def configured_regions(value):
    """Normalize API country codes and remove duplicates without reordering."""
    regions = list(dict.fromkeys(part.strip().upper() for part in value.split(",")))
    if not regions or any(not re.fullmatch(r"[A-Z]{2}", region) for region in regions):
        raise ValueError("YOUTUBE_REGIONS must contain comma-separated two-letter country codes")
    return regions


def fetch_youtube(endpoint, parameters, api_key):
    """Fetch one API response and report failures without exposing the API key."""
    parameters = {**parameters, "key": api_key}
    request = Request(
        f"{API_BASE}/{endpoint}?{urlencode(parameters)}",
        headers={"Accept": "application/json"},
    )
    try:
        with urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        # HTTP errors carry the full request URL. Do not log it or API messages.
        reason = "requestRejected"
        try:
            payload = json.loads(exc.read().decode("utf-8"))
            candidate = payload["error"]["errors"][0]["reason"]
            if isinstance(candidate, str) and re.fullmatch(r"[A-Za-z0-9_]{1,80}", candidate):
                reason = candidate
        except (ValueError, KeyError, IndexError, TypeError):
            pass
        finally:
            exc.close()
        reason = reason.replace(api_key, "REDACTED")
        raise RuntimeError(f"YouTube {endpoint}: HTTP {exc.code}, reason={reason}") from None
    except (URLError, TimeoutError, OSError):
        raise RuntimeError(f"YouTube {endpoint}: network request failed or timed out") from None
    except (ValueError, UnicodeError):
        raise RuntimeError(f"YouTube {endpoint}: response was not valid JSON") from None
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        raise ValueError(f"YouTube {endpoint}: expected an items array")
    if endpoint == "videoCategories" and not data["items"]:
        raise ValueError("YouTube videoCategories: no category records returned")
    return data


def fetch_trending_videos(region_code, api_key):
    """Fetch the first page of up to 50 most-popular videos for a country."""
    return fetch_youtube(
        "videos",
        {"part": "snippet,statistics,contentDetails", "chart": "mostPopular",
         "regionCode": region_code.upper(), "maxResults": MAX_RESULTS},
        api_key,
    )


def fetch_video_categories(region_code, api_key):
    return fetch_youtube(
        "videoCategories", {"part": "snippet", "regionCode": region_code.upper()}, api_key
    )


def write_to_s3(data, bucket, key, ingestion_timestamp):
    global s3_client
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")
    return s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
        Metadata={"ingestion_timestamp": ingestion_timestamp, "source": "youtube_data_api_v3"},
    )


def send_alert(topic, results):
    if topic:
        try:
            import boto3

            boto3.client("sns").publish(
                TopicArn=topic,
                Subject="YouTube ingestion failed",
                Message=json.dumps(results, ensure_ascii=False),
            )
        except Exception:
            # An alert failure must not hide the ingestion error.
            logger.error("Unable to publish the optional SNS alert")


def lambda_handler(event, context):
    """Accept an EventBridge scheduled event or {} for a manual invocation."""
    if not isinstance(event, dict):
        raise ValueError("Lambda event must be a JSON object")
    api_key = os.environ.get("YOUTUBE_API_KEY", "").strip()
    if not api_key:
        raise ValueError("Set the YOUTUBE_API_KEY environment variable before running ingestion")
    bucket = os.environ.get("S3_BUCKET_BRONZE", "tej-data-1").strip()
    if not bucket:
        raise ValueError("S3_BUCKET_BRONZE must not be empty")
    regions = configured_regions(os.environ.get("YOUTUBE_REGIONS", DEFAULT_REGIONS))
    now = datetime.now(timezone.utc)
    snapshot_time = now
    if event.get("time"):
        snapshot_time = datetime.fromisoformat(event["time"].replace("Z", "+00:00"))
        if snapshot_time.tzinfo is None:
            raise ValueError("The event time must include a timezone")
        snapshot_time = snapshot_time.astimezone(timezone.utc)
    event_id = event.get("id") or getattr(context, "aws_request_id", None) or uuid.uuid4().hex
    event_hash = hashlib.sha256(str(event_id).encode("utf-8")).hexdigest()[:12]
    ingestion_id = f"{snapshot_time:%Y%m%dT%H%M%SZ}_{event_hash}"
    results = {"success": [], "failed": [], "objects": []}

    for country in regions:
        region = country.lower()
        succeeded = True
        operations = (
            ("trending", fetch_trending_videos,
             f"youtube/raw_statistics/region={region}/date={snapshot_time:%Y-%m-%d}/"
             f"hour={snapshot_time:%H}/{ingestion_id}.json"),
            ("categories", fetch_video_categories,
             f"youtube/raw_statistics_reference_data/region={region}/{country}_category_id.json"),
        )
        # Fetch categories even if the popular-video chart is unavailable.
        for data_type, fetch, key in operations:
            try:
                data = fetch(country, api_key)
                data["_pipeline_metadata"] = {
                    "ingestion_id": ingestion_id,
                    "region": region,
                    "ingestion_timestamp": now.isoformat(),
                    "source": "youtube_data_api_v3",
                    "item_count": len(data["items"]),
                }
                write_to_s3(data, bucket, key, now.isoformat())
                results["objects"].append(
                    {"region": region, "type": data_type, "key": key, "items": len(data["items"])}
                )
                logger.info("Wrote %s %s records to s3://%s/%s", len(data["items"]), data_type, bucket, key)
            except Exception as exc:
                succeeded = False
                message = str(exc).replace(api_key, "REDACTED")
                logger.error("Failed %s for %s: %s", data_type, country, message)
                results["failed"].append({"region": region, "type": data_type, "error": message})
        if succeeded:
            results["success"].append(region)

    logger.info("Ingestion %s: %s/%s countries succeeded", ingestion_id, len(results["success"]), len(regions))
    if results["failed"]:
        send_alert(os.environ.get("SNS_ALERT_TOPIC_ARN", ""), results)
        # An error is necessary for Lambda/EventBridge retry and failure handling.
        raise RuntimeError(json.dumps({"ingestion_id": ingestion_id, "results": results}))
    return {"statusCode": 200, "ingestion_id": ingestion_id, "results": results}
