"""AWS Glue job: Silver YouTube data -> Gold analytics tables.

Defaults match this project:
  Statistics: tej-pipeline-silver-dev.clean_statistics
  Reference:  yt_pipeline_silver_dev.clean_reference_data
  Gold:       tej-data-gold-dev in s3://gold-bucket-name/youtube/

The job produces:
  - trending_analytics, partitioned by region and date
  - channel_analytics, partitioned by region
  - category_analytics, partitioned by region and date

It performs a complete refresh from Silver and overwrites each dedicated Gold
prefix, so Glue job bookmarks must be disabled and maximum concurrent runs must
be 1. Writes are not transactional across all three tables; retry a failed run
before querying Gold.

Optional Glue job parameters:
  --silver_database (alias: --statistics_database)
  --silver_bucket
  --statistics_table
  --reference_database
  --reference_table
  --gold_bucket
  --gold_database
  --job-bookmark-option (must be job-bookmark-disable)

Role permissions: Glue GetDatabase/GetTable/CreateTable/GetPartitions/
BatchCreatePartition/BatchDeletePartition, Silver S3 List/Get, Gold S3
List/Get/Put/Delete, and CloudWatch Logs.
"""

import argparse
import re
import sys
from urllib.parse import urlsplit

DEFAULT_STATISTICS_DATABASE = "tej-pipeline-silver-dev"
DEFAULT_STATISTICS_TABLE = "clean_statistics"
DEFAULT_REFERENCE_DATABASE = "yt_pipeline_silver_dev"
DEFAULT_REFERENCE_TABLE = "clean_reference_data"
DEFAULT_GOLD_BUCKET = "gold-bucket-name"
DEFAULT_GOLD_DATABASE = "tej-data-gold-dev"

REQUIRED_STATISTICS_COLUMNS = {
    "video_id",
    "title",
    "channel_title",
    "category_id",
    "views",
    "likes",
    "dislikes",
    "comment_count",
    "like_ratio",
    "engagement_rate",
    "trending_date_parsed",
    "region",
}
REQUIRED_REFERENCE_COLUMNS = {"id", "snippet_title", "region"}

GOLD_TABLES = {
    "trending_analytics": {
        "prefix": "youtube/trending_analytics/",
        "partitions": ["region", "date"],
    },
    "channel_analytics": {
        "prefix": "youtube/channel_analytics/",
        "partitions": ["region"],
    },
    "category_analytics": {
        "prefix": "youtube/category_analytics/",
        "partitions": ["region", "date"],
    },
}

S3_BUCKET_PATTERN = re.compile(
    r"^(?!\d+\.\d+\.\d+\.\d+$)(?!-)(?!.*--)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$"
)
PARTITION_VALUE_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


def parse_options(arguments=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--silver_database",
        "--statistics_database",
        dest="statistics_database",
        default=DEFAULT_STATISTICS_DATABASE,
    )
    parser.add_argument("--silver_bucket", default="tej-sliver-data")
    parser.add_argument("--statistics_table", default=DEFAULT_STATISTICS_TABLE)
    parser.add_argument("--reference_database", default=DEFAULT_REFERENCE_DATABASE)
    parser.add_argument("--reference_table", default=DEFAULT_REFERENCE_TABLE)
    parser.add_argument("--gold_bucket", default=DEFAULT_GOLD_BUCKET)
    parser.add_argument("--gold_database", default=DEFAULT_GOLD_DATABASE)
    parser.add_argument(
        "--job-bookmark-option", default="job-bookmark-disable"
    )
    options, _ = parser.parse_known_args(arguments)
    validate_options(options)
    return options


def validate_options(options):
    if options.job_bookmark_option != "job-bookmark-disable":
        raise ValueError("Disable Glue job bookmarks because Gold is rebuilt from complete Silver data")
    for option_name, bucket in (
        ("--silver_bucket", options.silver_bucket),
        ("--gold_bucket", options.gold_bucket),
    ):
        if not S3_BUCKET_PATTERN.fullmatch(bucket):
            raise ValueError(f"{option_name} is not a valid S3 bucket name")
    for name in (
        options.statistics_database,
        options.statistics_table,
        options.reference_database,
        options.reference_table,
        options.gold_database,
    ):
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,255}", name):
            raise ValueError(f"Invalid Glue catalog name: {name!r}")


def expected_source_path(bucket, dataset):
    return f"s3://{bucket}/youtube/{dataset}/"


def validate_source_table(catalog, database, table, expected_path):
    """Require each input table to point to its dedicated Silver prefix."""
    result = catalog.get_table(DatabaseName=database, Name=table)["Table"]
    descriptor = result.get("StorageDescriptor", {})
    actual_path = descriptor.get("Location", "").rstrip("/")
    if actual_path != expected_path.rstrip("/"):
        raise ValueError(
            f"{database}.{table} points to {actual_path!r}; expected {expected_path!r}"
        )
    return result


def require_columns(frame, expected, label):
    missing = sorted(set(expected) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {', '.join(missing)}")


def catalog_descriptor(path, columns):
    return {
        "Columns": columns,
        "Location": path,
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        },
    }


def prepare_catalog(catalog, database, table, path, columns, partition_names):
    """Create a Gold table or verify that an existing table is compatible."""
    catalog.get_database(Name=database)
    partition_keys = [
        {"Name": name, "Type": "string"} for name in partition_names
    ]
    try:
        existing = catalog.get_table(DatabaseName=database, Name=table)["Table"]
    except catalog.exceptions.EntityNotFoundException:
        catalog.create_table(
            DatabaseName=database,
            TableInput={
                "Name": table,
                "TableType": "EXTERNAL_TABLE",
                "PartitionKeys": partition_keys,
                "StorageDescriptor": catalog_descriptor(path, columns),
                "Parameters": {
                    "classification": "parquet",
                    "compressionType": "snappy",
                    "EXTERNAL": "TRUE",
                },
            },
        )
        return

    descriptor = existing["StorageDescriptor"]
    existing_columns = [
        {"Name": item["Name"], "Type": item["Type"]}
        for item in descriptor.get("Columns", [])
    ]
    existing_partitions = [
        {"Name": item["Name"], "Type": item["Type"]}
        for item in existing.get("PartitionKeys", [])
    ]
    if (
        descriptor.get("Location", "").rstrip("/") != path.rstrip("/")
        or existing_columns != columns
        or existing_partitions != partition_keys
    ):
        raise ValueError(
            f"Gold table {database}.{table} has a different path, schema, or partition layout"
        )


def partition_location(path, partition_names, values):
    if len(partition_names) != len(values):
        raise ValueError("Partition names and values must have the same length")
    segments = []
    for name, value in zip(partition_names, values):
        text = str(value)
        if not PARTITION_VALUE_PATTERN.fullmatch(text):
            raise ValueError(f"Unsafe partition value for {name}: {text!r}")
        segments.append(f"{name}={text}")
    return f"{path.rstrip('/')}/{'/'.join(segments)}/"


def existing_partitions(catalog, database, table):
    values = []
    paginator = catalog.get_paginator("get_partitions")
    for page in paginator.paginate(DatabaseName=database, TableName=table):
        values.extend(partition["Values"] for partition in page.get("Partitions", []))
    return values


def replace_partitions(
    catalog, database, table, path, columns, partition_names, partitions
):
    """Replace Glue partition metadata after a successful full S3 refresh."""
    old_values = existing_partitions(catalog, database, table)
    for offset in range(0, len(old_values), 25):
        response = catalog.batch_delete_partition(
            DatabaseName=database,
            TableName=table,
            PartitionsToDelete=[
                {"Values": values} for values in old_values[offset : offset + 25]
            ],
        )
        if response.get("Errors"):
            raise RuntimeError(f"Unable to remove stale Gold partitions: {response['Errors']}")

    unique = []
    for partition in partitions:
        values = [str(partition[name]) for name in partition_names]
        if values not in unique:
            unique.append(values)
    for offset in range(0, len(unique), 100):
        inputs = []
        for values in unique[offset : offset + 100]:
            descriptor = catalog_descriptor(
                partition_location(path, partition_names, values), columns
            )
            inputs.append({"Values": values, "StorageDescriptor": descriptor})
        response = catalog.batch_create_partition(
            DatabaseName=database,
            TableName=table,
            PartitionInputList=inputs,
        )
        failures = [
            error
            for error in response.get("Errors", [])
            if error.get("ErrorDetail", {}).get("ErrorCode")
            != "AlreadyExistsException"
        ]
        if failures:
            raise RuntimeError(f"Unable to register Gold partitions: {failures}")


def dataframe_columns(frame, partition_names):
    return [
        {"Name": field.name, "Type": field.dataType.simpleString()}
        for field in frame.schema.fields
        if field.name not in partition_names
    ]


def write_gold_table(
    frame, catalog, database, bucket, table, logger
):
    spec = GOLD_TABLES[table]
    path = f"s3://{bucket}/{spec['prefix']}"
    partition_names = spec["partitions"]
    frame = frame.cache()
    row_count = frame.count()
    if row_count == 0:
        frame.unpersist()
        raise ValueError(f"Gold aggregation {table} produced no rows")
    columns = dataframe_columns(frame, partition_names)
    partitions = [
        row.asDict() for row in frame.select(*partition_names).distinct().collect()
    ]

    # Validate catalog metadata before overwrite so a wrong table cannot redirect
    # a full refresh to an unrelated location.
    prepare_catalog(
        catalog, database, table, path, columns, partition_names
    )
    (
        frame.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy(*partition_names)
        .parquet(path)
    )
    replace_partitions(
        catalog,
        database,
        table,
        path,
        columns,
        partition_names,
        partitions,
    )
    frame.unpersist()
    logger.info(
        f"Wrote {row_count} rows to {path} ({database}.{table})"
    )
    return row_count


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    options = parse_options(sys.argv[1:])
    resolved = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job_name = resolved["JOB_NAME"]

    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    logger = glue_context.get_logger()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(glue_context)
    job.init(job_name, {"JOB_NAME": job_name, **vars(options)})
    catalog = boto3.client("glue")

    statistics_path = expected_source_path(options.silver_bucket, "statistics")
    reference_path = expected_source_path(options.silver_bucket, "reference_data")
    validate_source_table(
        catalog,
        options.statistics_database,
        options.statistics_table,
        statistics_path,
    )
    validate_source_table(
        catalog,
        options.reference_database,
        options.reference_table,
        reference_path,
    )

    logger.info(
        f"Reading statistics from "
        f"{options.statistics_database}.{options.statistics_table}"
    )
    statistics = glue_context.create_dynamic_frame.from_catalog(
        database=options.statistics_database,
        table_name=options.statistics_table,
        transformation_ctx="silver_statistics",
    ).toDF()
    references = glue_context.create_dynamic_frame.from_catalog(
        database=options.reference_database,
        table_name=options.reference_table,
        transformation_ctx="silver_reference",
    ).toDF()
    require_columns(statistics, REQUIRED_STATISTICS_COLUMNS, "Silver statistics")
    require_columns(references, REQUIRED_REFERENCE_COLUMNS, "Silver reference data")

    statistics = (
        statistics.withColumn("region", F.lower(F.trim(F.col("region"))))
        .withColumn("category_id", F.col("category_id").cast("long"))
        .withColumn("trending_date_parsed", F.to_date("trending_date_parsed"))
    )
    invalid = statistics.filter(
        F.col("video_id").isNull()
        | (F.trim(F.col("video_id")) == "")
        | F.col("channel_title").isNull()
        | F.col("trending_date_parsed").isNull()
        | F.col("region").isNull()
        | (F.col("views") < 0)
    ).count()
    if invalid:
        raise ValueError(
            f"Silver statistics contains {invalid} invalid rows; run data-quality checks before Gold"
        )

    order_columns = []
    for candidate in ("_observed_at", "_processed_at"):
        if candidate in statistics.columns:
            order_columns.append(F.col(candidate).desc_nulls_last())
            break
    if "_source_file" in statistics.columns:
        order_columns.append(F.col("_source_file").desc())
    order_columns.append(F.col("video_id").asc())
    dedup_window = Window.partitionBy(
        "video_id", "region", "trending_date_parsed"
    ).orderBy(*order_columns)
    statistics = (
        statistics.withColumn("_gold_rank", F.row_number().over(dedup_window))
        .filter(F.col("_gold_rank") == 1)
        .drop("_gold_rank")
    )

    reference_order = []
    if "_ingestion_timestamp" in references.columns:
        reference_order.append(F.col("_ingestion_timestamp").desc_nulls_last())
    if "_source_file" in references.columns:
        reference_order.append(F.col("_source_file").desc())
    # The select below renames the source `id` column to `category_id` before
    # this window is evaluated, so every window expression must use the
    # post-select column name.
    reference_order.append(F.col("category_id").asc())
    reference_window = Window.partitionBy("category_id", "region").orderBy(
        *reference_order
    )
    category_lookup = (
        references.select(
            F.col("id").cast("long").alias("category_id"),
            F.lower(F.trim(F.col("region"))).alias("region"),
            F.trim(F.col("snippet_title")).alias("category_name"),
            *(
                [F.col("_ingestion_timestamp")]
                if "_ingestion_timestamp" in references.columns
                else []
            ),
            *(
                [F.col("_source_file")]
                if "_source_file" in references.columns
                else []
            ),
        )
        .filter(
            F.col("category_id").isNotNull()
            & F.col("region").isNotNull()
            & F.col("category_name").isNotNull()
            & (F.col("category_name") != "")
        )
        .withColumn("_reference_rank", F.row_number().over(reference_window))
        .filter(F.col("_reference_rank") == 1)
        .select("category_id", "region", "category_name")
    )
    if category_lookup.limit(1).count() == 0:
        raise ValueError("Silver reference data produced an empty category lookup")

    statistics = (
        statistics.join(
            F.broadcast(category_lookup),
            on=["category_id", "region"],
            how="left",
        )
        .fillna("Unknown", subset=["category_name"])
        .withColumn("date", F.date_format("trending_date_parsed", "yyyy-MM-dd"))
    ).cache()
    silver_count = statistics.count()
    if silver_count == 0:
        statistics.unpersist()
        raise ValueError("Silver statistics table is empty; Gold was not replaced")
    unmatched = statistics.filter(F.col("category_name") == "Unknown").count()
    if unmatched:
        logger.warn(
            f"{unmatched} Silver rows have no matching regional category name"
        )

    aggregated_at = F.current_timestamp()
    trending = (
        statistics.groupBy("region", "date")
        .agg(
            F.countDistinct("video_id").alias("total_videos"),
            F.sum("views").alias("total_views"),
            F.sum("likes").alias("total_likes"),
            F.sum("dislikes").alias("total_dislikes"),
            F.sum("comment_count").alias("total_comments"),
            F.avg("views").alias("avg_views_per_video"),
            F.avg("like_ratio").alias("avg_like_ratio"),
            F.avg("engagement_rate").alias("avg_engagement_rate"),
            F.max("views").alias("max_views"),
            F.countDistinct("channel_title").alias("unique_channels"),
            F.countDistinct("category_id").alias("unique_categories"),
        )
        .withColumn("_aggregated_at", aggregated_at)
    )

    channel = statistics.groupBy("channel_title", "region").agg(
        F.countDistinct("video_id").alias("total_videos"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("comment_count").alias("total_comments"),
        F.avg("views").alias("avg_views_per_video"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.max("views").alias("peak_views"),
        F.count("video_id").alias("trending_appearances"),
        F.countDistinct("date").alias("trending_days"),
        F.min("trending_date_parsed").alias("first_trending"),
        F.max("trending_date_parsed").alias("last_trending"),
        F.sort_array(F.collect_set("category_name")).alias("categories"),
    )
    channel_rank = Window.partitionBy("region").orderBy(
        F.col("total_views").desc(), F.col("channel_title").asc()
    )
    channel = (
        channel.withColumn("rank_in_region", F.row_number().over(channel_rank))
        .withColumn("_aggregated_at", aggregated_at)
    )

    category = statistics.groupBy(
        "category_name", "category_id", "region", "date"
    ).agg(
        F.countDistinct("video_id").alias("video_count"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("comment_count").alias("total_comments"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.countDistinct("channel_title").alias("unique_channels"),
    )
    daily_window = Window.partitionBy("region", "date")
    category = (
        category.withColumn("_daily_views", F.sum("total_views").over(daily_window))
        .withColumn(
            "view_share_pct",
            F.when(
                F.col("_daily_views") > 0,
                F.round(F.col("total_views") / F.col("_daily_views") * 100, 4),
            ),
        )
        .drop("_daily_views")
        .withColumn("_aggregated_at", aggregated_at)
    )

    for table, frame in (
        ("trending_analytics", trending),
        ("channel_analytics", channel),
        ("category_analytics", category),
    ):
        write_gold_table(
            frame,
            catalog,
            options.gold_database,
            options.gold_bucket,
            table,
            logger,
        )

    statistics.unpersist()
    logger.info(
        f"Gold build completed from {silver_count} deduplicated Silver rows"
    )
    job.commit()


if __name__ == "__main__":
    main()
