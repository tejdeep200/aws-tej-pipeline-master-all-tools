"""Glue Spark job: Bronze statistics -> cleansed Silver Parquet.

Default source: tej-data-bronze-dev.raw_statistics (tej-data-1).
Default output: s3://tej-sliver-data/youtube/statistics/.
Default catalog: tej-pipeline-silver-dev.clean_statistics.

Optional job arguments: --bronze_database, --bronze_table, --silver_bucket,
--silver_database, --silver_table, --input_format (both/csv/json), --regions
(comma-separated countries), --silver_path (prefix inside the Silver bucket).
Use Glue 5.0, job bookmarks DISABLED, and maximum concurrent runs 1.
The job rereads complete input for the selected countries and replaces only
their region/date partitions. Bookmarked partial input cannot replace a full
day safely. Overwrite is not transactional; retry failed runs before querying.

Role permissions: Bronze s3:ListBucket/GetObject; Silver ListBucket/GetObject/
PutObject/DeleteObject; Glue GetTable/GetDatabase/CreateTable/GetPartitions/
BatchCreatePartition (including catalog/database/table resources); CloudWatch.
This is a Glue job, not a Lambda. No local CSV or JSON files are uploaded by it.
"""

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from urllib.parse import urlsplit

DEFAULT_REGIONS = "US,GB,CA,DE,FR,IN,JP,KR,MX,RU"
STRING_FIELDS = ["video_id", "trending_date", "title", "channel_title", "publish_time", "tags",
                 "thumbnail_link", "description"]
COUNT_FIELDS = ["category_id", "views", "likes", "dislikes", "comment_count"]
BOOLEAN_FIELDS = ["comments_disabled", "ratings_disabled", "video_error_or_removed"]


def parse_timestamp(value):
    if not value:
        return None
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def parse_trending_date(value):
    if not value:
        return None
    try:
        text = str(value).strip()
        if re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", text):
            return datetime.strptime(text, "%y.%d.%m").date().isoformat()
        return datetime.strptime(text, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def parse_integer(value):
    # Spark bigint is signed 64-bit. Reject fractional, Boolean, and huge values.
    if value is None or isinstance(value, bool):
        return None
    if not re.fullmatch(r"[+-]?\d+", str(value).strip()):
        return None
    result = int(str(value).strip())
    return result if -(2**63) <= result < 2**63 else None


def parse_boolean(value):
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return {"true": True, "false": False}.get(text)


def file_region(source):
    match = re.search(r"(?:^|/)region=([A-Za-z]{2})(?:/|$)", source)
    if match:
        return match.group(1).lower()
    match = re.search(r"/([A-Za-z]{2})videos\.csv$", source)
    if match:
        return match.group(1).lower()
    raise ValueError(f"Cannot determine country from statistics source: {source}")


def normalize_statistics(record, source, job_name, metadata=None):
    """Normalize one CSV row or one item from a live API response."""
    metadata = metadata or {}
    api_record = isinstance(record.get("snippet"), dict)
    observed = parse_timestamp(metadata.get("ingestion_timestamp"))
    if observed is None:
        match = re.search(r"/(\d{8}T\d{6}Z)_", source)
        if match:
            observed = datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    if api_record:
        snippet, statistics = record["snippet"], record.get("statistics") or {}
        thumbnails = snippet.get("thumbnails") or {}
        thumbnail = next((thumbnails[name].get("url") for name in ("default", "medium", "high")
                          if isinstance(thumbnails.get(name), dict)), None)
        tags = snippet.get("tags")
        if isinstance(tags, list):
            tags = "|".join(str(tag) for tag in tags)
        fields = {
            "video_id": record.get("id"), "title": snippet.get("title"),
            "channel_title": snippet.get("channelTitle"), "category_id": snippet.get("categoryId"),
            "publish_time": snippet.get("publishedAt"), "tags": tags,
            "views": statistics.get("viewCount"), "likes": statistics.get("likeCount"),
            "dislikes": statistics.get("dislikeCount"), "comment_count": statistics.get("commentCount"),
            "thumbnail_link": thumbnail, "description": snippet.get("description"),
            # Missing API metrics do not prove comments/ratings were disabled.
            **{field: None for field in BOOLEAN_FIELDS},
        }
        date = observed.date().isoformat() if observed else None
        if date is None:
            match = re.search(r"/date=(\d{4}-\d{2}-\d{2})/", source)
            date = parse_trending_date(match.group(1)) if match else None
        fields["trending_date"] = date
        source_format = "youtube_api_json"
    else:
        fields = record
        date = parse_trending_date(record.get("trending_date"))
        source_format = "kaggle_csv"

    row = {name: None if fields.get(name) is None else str(fields[name]) for name in STRING_FIELDS}
    row["video_id"] = row["video_id"].strip() if row["video_id"] else None
    row.update({name: parse_integer(fields.get(name)) for name in COUNT_FIELDS})
    row.update({name: parse_boolean(fields.get(name)) for name in BOOLEAN_FIELDS})
    row["region"], row["date"] = file_region(source), date
    row["trending_date_parsed"] = datetime.strptime(date, "%Y-%m-%d").date() if date else None
    errors = []
    for name in ("video_id", "title", "channel_title"):
        if not row[name] or not row[name].strip():
            errors.append(f"missing_{name}")
    if not date:
        errors.append("invalid_trending_date")
    if row["category_id"] is None or row["category_id"] < 0:
        errors.append("invalid_category_id")
    for name in ("views", "likes", "dislikes", "comment_count"):
        raw_value = fields.get(name)
        if row[name] is None and (name == "views" or raw_value not in (None, "")):
            errors.append(f"invalid_{name}")
        elif row[name] is not None and row[name] < 0:
            errors.append(f"negative_{name}")
    publication = parse_timestamp(row["publish_time"])
    if publication is None:
        errors.append("invalid_publish_time")
    row["publish_time_parsed"] = publication.replace(tzinfo=None) if publication else None
    views, likes, comments = row["views"], row["likes"], row["comment_count"]
    row["like_ratio"] = round(likes / views * 100, 4) if views and views > 0 and likes is not None else None
    row["engagement_rate"] = (
        round((likes + (row["dislikes"] or 0) + comments) / views * 100, 4)
        if views and views > 0 and likes is not None and comments is not None else None
    )
    if observed is None and date:
        observed = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    row["_observed_at"] = observed.replace(tzinfo=None) if observed else None
    row["_source_file"], row["_source_format"] = source, source_format
    row["_processed_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    row["_job_name"] = job_name
    row["_dq_errors"], row["_is_valid"] = "|".join(errors), not errors
    # A deterministic tie-breaker when two snapshots have the same timestamp.
    row["_record_hash"] = hashlib.sha256(json.dumps(record, sort_keys=True, default=str).encode()).hexdigest()
    return row


def normalize_partition(rows, job_name, source_format):
    """Run on Spark executors without importing Glue or creating AWS clients."""
    for spark_row in rows:
        raw = spark_row.asDict(recursive=True)
        source = raw["_source_file"]
        if source_format == "json":
            items = raw.get("items")
            if not isinstance(items, list):
                raise ValueError(f"Expected a YouTube items array in {source}")
            for item in items:
                if not isinstance(item, dict) or not isinstance(item.get("snippet"), dict):
                    raise ValueError(f"Expected a video with snippet fields in {source}")
                yield normalize_statistics(item, source, job_name, raw.get("_pipeline_metadata"))
        else:
            yield normalize_statistics(raw, source, job_name)


def output_schema():
    from pyspark.sql.types import BooleanType, DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

    fields = [StructField(name, StringType(), True) for name in STRING_FIELDS]
    fields += [StructField(name, LongType(), True) for name in COUNT_FIELDS]
    fields += [StructField(name, BooleanType(), True) for name in BOOLEAN_FIELDS]
    fields += [StructField(name, StringType(), True) for name in
               ("region", "date", "_source_file", "_source_format", "_job_name", "_dq_errors", "_record_hash")]
    fields += [StructField(name, TimestampType(), True) for name in
               ("publish_time_parsed", "_observed_at", "_processed_at")]
    fields += [StructField(name, DoubleType(), True) for name in ("like_ratio", "engagement_rate")]
    fields.append(StructField("trending_date_parsed", DateType(), True))
    fields.append(StructField("_is_valid", BooleanType(), False))
    return StructType(fields)


def catalog_descriptor(path, columns):
    return {"Columns": columns, "Location": path,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}}


def prepare_catalog(client, database, table, path, columns):
    """Validate existing output metadata before writing any Parquet."""
    client.get_database(Name=database)
    partitions = [{"Name": "region", "Type": "string"}, {"Name": "date", "Type": "string"}]
    try:
        existing = client.get_table(DatabaseName=database, Name=table)["Table"]
    except client.exceptions.EntityNotFoundException:
        client.create_table(DatabaseName=database, TableInput={
            "Name": table, "TableType": "EXTERNAL_TABLE", "PartitionKeys": partitions,
            "StorageDescriptor": catalog_descriptor(path, columns),
            "Parameters": {"classification": "parquet", "compressionType": "snappy", "EXTERNAL": "TRUE"},
        })
        return
    descriptor = existing["StorageDescriptor"]
    existing_columns = [{"Name": item["Name"], "Type": item["Type"]} for item in descriptor["Columns"]]
    existing_partitions = [{"Name": item["Name"], "Type": item["Type"]} for item in existing.get("PartitionKeys", [])]
    if descriptor["Location"].rstrip("/") != path.rstrip("/") or existing_columns != columns or existing_partitions != partitions:
        raise ValueError("Silver table has a different location/schema/partition layout; choose a new --silver_table")


def register_partitions(client, database, table, path, columns, partitions):
    for offset in range(0, len(partitions), 100):
        inputs = [{"Values": [item["region"], item["date"]],
                   "StorageDescriptor": catalog_descriptor(
                       f"{path}region={item['region']}/date={item['date']}/", columns)}
                  for item in partitions[offset:offset + 100]]
        result = client.batch_create_partition(DatabaseName=database, TableName=table, PartitionInputList=inputs)
        failures = [error for error in result.get("Errors", [])
                    if error["ErrorDetail"]["ErrorCode"] != "AlreadyExistsException"]
        if failures:
            raise RuntimeError(f"Unable to register Silver partitions: {failures}")


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze_database", default="tej-data-bronze-dev")
    parser.add_argument("--bronze_table", default="raw_statistics")
    parser.add_argument("--silver_bucket", default="tej-sliver-data")
    parser.add_argument("--silver_database", default="tej-pipeline-silver-dev")
    parser.add_argument("--silver_table", default="clean_statistics")
    parser.add_argument("--silver_path", default="youtube/statistics/")
    parser.add_argument("--input_format", choices=["both", "csv", "json"], default="both")
    parser.add_argument("--regions", default=DEFAULT_REGIONS)
    parser.add_argument("--job-bookmark-option", default="job-bookmark-disable")
    options, _ = parser.parse_known_args()
    if options.job_bookmark_option != "job-bookmark-disable":
        raise ValueError("Disable job bookmarks: replacing daily partitions requires their complete source history")
    regions = {value.strip().lower() for value in options.regions.split(",")}
    if any(not re.fullmatch(r"[a-z]{2}", region) for region in regions):
        raise ValueError("--regions must contain comma-separated two-letter country codes")
    if options.silver_path.strip("/") != "youtube/statistics":
        raise ValueError("Use the dedicated youtube/statistics/ output prefix to keep replacement scoped to statistics")
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    args.update(vars(options))
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark, logger = glue_context.spark_session, glue_context.get_logger()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    catalog, s3 = boto3.client("glue"), boto3.client("s3")

    source = catalog.get_table(DatabaseName=options.bronze_database, Name=options.bronze_table)["Table"]
    source_path = source["StorageDescriptor"]["Location"].rstrip("/") + "/"
    uri = urlsplit(source_path)
    if uri.scheme != "s3" or uri.path.strip("/") != "youtube/raw_statistics":
        raise ValueError("The Bronze table must point to an S3 youtube/raw_statistics/ prefix")
    target = f"s3://{options.silver_bucket}/youtube/statistics/"
    files = {"csv": [], "json": []}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=uri.netloc, Prefix=uri.path.lstrip("/")):
        for obj in page.get("Contents", []):
            extension = obj["Key"].rsplit(".", 1)[-1].lower()
            if extension in files and (options.input_format == "both" or options.input_format == extension):
                path = f"s3://{uri.netloc}/{obj['Key']}"
                if file_region(path) in regions:
                    files[extension].append(path)

    frames = []
    schema, job_name = output_schema(), args["JOB_NAME"]
    for source_format, paths in files.items():
        if not paths:
            continue
        reader = spark.read
        if source_format == "csv":
            frame = reader.option("header", True).option("multiLine", True).option("escape", '"').csv(paths)
            if "video_id" not in frame.columns:
                raise ValueError("CSV source does not contain the required video_id header")
        else:
            frame = reader.option("multiLine", True).json(paths)
        frame = frame.withColumn("_source_file", F.input_file_name())
        normalized = frame.rdd.mapPartitions(
            lambda rows, format_name=source_format: normalize_partition(rows, job_name, format_name)
        )
        frames.append(spark.createDataFrame(normalized, schema))
    if not frames:
        logger.info("No statistics files found for the selected countries and formats")
        job.commit()
        return
    df = frames[0]
    for frame in frames[1:]:
        df = df.unionByName(frame)
    df = df.cache()
    initial_count = df.count()
    clean = df.filter(F.col("_is_valid"))
    valid_count = clean.count()
    logger.info(f"Read {initial_count} statistics records; rejected {initial_count - valid_count} invalid records")
    if initial_count and not valid_count:
        raise ValueError("All statistics records failed validation; Silver data has not been replaced")
    if not valid_count:
        df.unpersist()
        job.commit()
        return

    window = Window.partitionBy("video_id", "region", "date").orderBy(
        F.col("_observed_at").desc_nulls_last(), F.col("_source_file").desc(), F.col("_record_hash").desc()
    )
    clean = clean.withColumn("_rank", F.row_number().over(window)).filter(F.col("_rank") == 1).drop("_rank", "_record_hash").cache()
    count = clean.count()
    columns = [{"Name": field.name, "Type": field.dataType.simpleString()}
               for field in clean.schema.fields if field.name not in ("region", "date")]
    partitions = [row.asDict() for row in clean.select("region", "date").distinct().collect()]
    prepare_catalog(catalog, options.silver_database, options.silver_table, target, columns)
    clean.write.mode("overwrite").option("partitionOverwriteMode", "dynamic").option("compression", "snappy").partitionBy("region", "date").parquet(target)
    register_partitions(catalog, options.silver_database, options.silver_table, target, columns, partitions)
    logger.info(f"Wrote {count} rows to {target}; catalog {options.silver_database}.{options.silver_table}; {len(partitions)} partitions")
    clean.unpersist()
    df.unpersist()
    job.commit()


if __name__ == "__main__":
    main()
