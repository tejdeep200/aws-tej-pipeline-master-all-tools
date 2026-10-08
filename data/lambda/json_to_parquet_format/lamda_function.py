"""Convert YouTube category JSON in Bronze to partitioned Parquet in Silver.

Lambda handler: lamda_function.lambda_handler (matches this file's spelling).
Accepts S3 ObjectCreated events, a direct {"bucket": ..., "key": ...} event,
or {"backfill": true} to process the already uploaded reference JSON files.
AWS execution requires the AWS SDK for pandas Lambda layer.
Run this file with Python for local conversion using pandas and PyArrow only.
For AWS, grant the execution role Bronze reads, Silver partition writes/deletes,
and Glue database/table/partition permissions. Use reserved concurrency 1.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote_plus

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BRONZE_BUCKET = os.environ.get("S3_BUCKET_BRONZE", "tej-data-1")
BRONZE_PREFIX = "youtube/raw_statistics_reference_data/"
SILVER_BUCKET = os.environ.get("S3_BUCKET_SILVER", "tej-sliver-data")
SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/reference_data/"
GLUE_DB = os.environ.get("GLUE_DB_SILVER", "yt_pipeline_silver_dev")
GLUE_TABLE = os.environ.get("GLUE_TABLE_REFERENCE", "clean_reference_data")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

SOURCE_PATTERN = re.compile(
    rf"^{re.escape(BRONZE_PREFIX)}region=([a-zA-Z]{{2}})/([a-zA-Z]{{2}})_category_id\.json$"
)
STRING_COLUMNS = ["id", "kind", "etag", "snippet_channel_id", "snippet_title"]
PARQUET_TYPES = {
    **{column: "string" for column in STRING_COLUMNS},
    "snippet_assignable": "boolean",
    "region": "string",
    "_source_file": "string",
    "_ingestion_timestamp": "string",
}
s3_client = None
wr = None


def load_aws_dependencies():
    """Initialize AWS libraries only when running against AWS."""
    global s3_client, wr
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")
    if wr is None:
        import awswrangler

        wr = awswrangler


def source_region(bucket, key):
    """Ignore unrelated objects; reject a file stored under the wrong country."""
    if bucket != BRONZE_BUCKET:
        return None
    match = SOURCE_PATTERN.fullmatch(key)
    if not match:
        if key.startswith(BRONZE_PREFIX) and key.endswith("_category_id.json"):
            raise ValueError(f"Expected region=<country>/<COUNTRY>_category_id.json: {key}")
        return None
    region, file_region = (value.lower() for value in match.groups())
    if region != file_region:
        raise ValueError(f"Country partition and filename disagree: {key}")
    return region


def normalize_categories(raw_data, region, key):
    """Flatten the items array while retaining a consistent Athena schema."""
    if not isinstance(raw_data, dict):
        raise ValueError("Category JSON must be an object with an items array")
    items = raw_data.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("Category JSON must contain a nonempty items array")
    if not all(isinstance(item, dict) for item in items):
        raise ValueError("Every category item must be an object")

    df = pd.json_normalize(items, sep="_").rename(
        columns={"snippet_channelId": "snippet_channel_id"}
    )
    for column in ("id", "snippet_title"):
        if column not in df.columns:
            raise ValueError(f"Missing required category field: {column}")
        df[column] = df[column].astype("string").str.strip()
        if df[column].isna().any() or df[column].eq("").any():
            raise ValueError(f"Category field must not be null or blank: {column}")
    for column in STRING_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = df[column].astype("string")
    if "snippet_assignable" not in df.columns:
        df["snippet_assignable"] = pd.NA
    if not df["snippet_assignable"].dropna().map(
        lambda value: isinstance(value, bool)
    ).all():
        raise ValueError("snippet.assignable must be a JSON boolean")
    df["snippet_assignable"] = df["snippet_assignable"].astype("boolean")
    df = df[STRING_COLUMNS + ["snippet_assignable"]].drop_duplicates(
        subset=["id"], keep="last"
    ).copy()
    df["region"] = region
    df["_source_file"] = key
    df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    return df


def source_objects(event):
    """S3 notifications encode keys; direct invocation keys are literal."""
    if not isinstance(event, dict):
        raise ValueError("Lambda event must be a JSON object")
    if event.get("backfill") is True:
        load_aws_dependencies()
        paginator = s3_client.get_paginator("list_objects_v2")
        return [
            (BRONZE_BUCKET, obj["Key"])
            for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=BRONZE_PREFIX)
            for obj in page.get("Contents", [])
            if obj["Key"].endswith("_category_id.json")
        ]
    if event.get("Event") == "s3:TestEvent":
        return []
    if "Records" in event:
        objects = []
        for record in event["Records"]:
            if record.get("eventSource") != "aws:s3":
                raise ValueError("Expected an S3 notification record")
            if not record.get("eventName", "").startswith("ObjectCreated:"):
                continue
            info = record["s3"]
            objects.append((info["bucket"]["name"], unquote_plus(info["object"]["key"])))
        return objects
    if event.get("source") == "aws.s3" and event.get("detail-type") == "Object Created":
        info = event["detail"]
        return [(info["bucket"]["name"], info["object"]["key"])]
    if "s3" in event:
        info = event["s3"]
        return [(info["bucket"]["name"], unquote_plus(info["object"]["key"]))]
    if "bucket" in event and "key" in event:
        return [(event["bucket"], event["key"])]
    raise ValueError('Use an S3 event, {"bucket": ..., "key": ...}, or {"backfill": true}')


def lambda_handler(event, context):
    load_aws_dependencies()
    objects = source_objects(event)
    if event.get("backfill") is True and not objects:
        raise ValueError(f"No category JSON files found under s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}")
    processed, skipped, errors = [], [], []
    catalog_ready = False
    for bucket, key in dict.fromkeys(objects):
        try:
            region = source_region(bucket, key)
            if region is None:
                skipped.append({"bucket": bucket, "key": key})
                continue
            logger.info("Reading s3://%s/%s", bucket, key)
            response = s3_client.get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            try:
                raw_data = json.loads(body.read().decode("utf-8-sig"))
            finally:
                body.close()
            df = normalize_categories(raw_data, region, key)
            if not catalog_ready:
                wr.catalog.create_database(name=GLUE_DB, exist_ok=True)
                catalog_ready = True
            result = wr.s3.to_parquet(
                df=df,
                path=SILVER_PATH,
                dataset=True,
                index=False,
                compression="snappy",
                partition_cols=["region"],
                mode="overwrite_partitions",
                database=GLUE_DB,
                table=GLUE_TABLE,
                dtype=PARQUET_TYPES,
                schema_evolution=False,
            )
            processed.append({"key": key, "region": region, "rows": len(df), "paths": result["paths"]})
            logger.info("Wrote %s rows for %s to %s", len(df), region, SILVER_PATH)
        except Exception as exc:
            logger.exception("Failed processing s3://%s/%s", bucket, key)
            errors.append({"bucket": bucket, "key": key, "error": str(exc)})

    if errors:
        if SNS_TOPIC:
            try:
                import boto3

                boto3.client("sns").publish(
                    TopicArn=SNS_TOPIC,
                    Subject="YouTube JSON-to-Parquet conversion failed",
                    Message=json.dumps(errors, ensure_ascii=False),
                )
            except Exception:
                logger.exception("Unable to publish the optional SNS alert")
        # Returning statusCode=500 would still count as success for async Lambda.
        raise RuntimeError(json.dumps({"processed": processed, "errors": errors}))
    return {"statusCode": 200, "processed": processed, "skipped": skipped}


def run_local(input_dir, output_dir):
    """Convert local category files and verify the saved Parquet contents."""
    input_dir, output_dir = Path(input_dir), Path(output_dir)
    files = sorted(input_dir.glob("*_category_id.json"))
    if not files:
        raise ValueError(f"No *_category_id.json files found in {input_dir.resolve()}")
    processed = []
    for file in files:
        if not re.fullmatch(r"[a-zA-Z]{2}_category_id\.json", file.name):
            raise ValueError(f"Expected <COUNTRY>_category_id.json: {file.name}")
        region = file.name[:2].lower()
        key = f"{BRONZE_PREFIX}region={region}/{file.name}"
        raw_data = json.loads(file.read_text(encoding="utf-8-sig"))
        df = normalize_categories(raw_data, region, key)
        partition_dir = output_dir / f"region={region}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        destination = partition_dir / f"{file.stem}.snappy.parquet"
        # Match AWS's dataset layout: country is encoded in the directory name.
        parquet_df = df.drop(columns=["region"])
        parquet_df.to_parquet(destination, index=False, compression="snappy", engine="pyarrow")
        pd.testing.assert_frame_equal(pd.read_parquet(destination, engine="pyarrow"), parquet_df)
        processed.append({"file": file.name, "region": region, "rows": len(df), "path": str(destination.resolve())})
        print(f"Verified {file.name}: {len(df)} rows -> {destination}")
    print(f"Successfully converted and verified {len(processed)} local Parquet files.")
    return processed


if __name__ == "__main__":
    import argparse

    data_dir = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description="Convert local category JSON files to Parquet.")
    parser.add_argument("--input-dir", type=Path, default=data_dir)
    parser.add_argument(
        "--output-dir", type=Path,
        default=data_dir / "local_silver" / "youtube" / "reference_data",
    )
    args = parser.parse_args()
    run_local(args.input_dir, args.output_dir)
