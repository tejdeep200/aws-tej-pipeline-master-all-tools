"""Run data-quality checks against the YouTube Silver Glue tables.

Lambda handler when this file is uploaded unchanged: dq_lamda.lambda_handler
If the AWS console file is named lambda_function.py, use:
lambda_function.lambda_handler

The default targets match the tables currently present in this AWS account:
  - tej-pipeline-silver-dev.clean_statistics
  - yt_pipeline_silver_dev.clean_reference_data

The function reads only aggregate results through Athena. It checks the Glue
schema and S3 location, exact row counts, critical-column null percentages,
numeric ranges, duplicate business keys, and freshness.

Required Lambda layer: AWS SDK for pandas (matching Python and architecture).
Required permissions: glue:GetTable, athena:StartQueryExecution,
athena:GetQueryExecution, athena:GetQueryResults, S3 reads for the Silver
bucket, S3 reads/writes for the Athena result prefix, CloudWatch Logs, and
sns:Publish only when SNS_ALERT_TOPIC_ARN is configured.

Optional environment variables:
  S3_BUCKET_SILVER, GLUE_DB_STATISTICS, GLUE_DB_REFERENCE,
  ATHENA_OUTPUT_LOCATION, ATHENA_WORKGROUP, SNS_ALERT_TOPIC_ARN,
  DQ_MIN_ROW_COUNT, DQ_MAX_NULL_PERCENT, DQ_MAX_VIEWS,
  DQ_FRESHNESS_HOURS.

An empty test event, {}, checks both default tables. Step Functions may also
pass {"tables": ["clean_statistics"]} or an explicit "targets" list.
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SILVER_BUCKET = os.environ.get("S3_BUCKET_SILVER", "tej-sliver-data").strip()
STATISTICS_DATABASE = os.environ.get(
    "GLUE_DB_STATISTICS", "tej-pipeline-silver-dev"
).strip()
REFERENCE_DATABASE = os.environ.get(
    "GLUE_DB_REFERENCE", "yt_pipeline_silver_dev"
).strip()
ATHENA_OUTPUT = os.environ.get(
    "ATHENA_OUTPUT_LOCATION",
    "s3://tej-data-pipeline-glue-catalog-result/athena-results/data-quality/",
).strip()
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary").strip()
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "").strip()

MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
MAX_VIEWS = int(os.environ.get("DQ_MAX_VIEWS", "50000000000"))
FRESHNESS_HOURS = int(os.environ.get("DQ_FRESHNESS_HOURS", "48"))

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,255}$")
SNS_TOPIC_PATTERN = re.compile(
    r"^arn:(?:aws|aws-us-gov|aws-cn):sns:[a-z0-9-]+:\d{12}:[A-Za-z0-9_-]{1,256}$"
)

TABLE_RULES = {
    "clean_statistics": {
        "database": STATISTICS_DATABASE,
        "path": f"s3://{SILVER_BUCKET}/youtube/statistics/",
        "critical": {
            "video_id": "string",
            "title": "string",
            "channel_title": "string",
            "views": "bigint",
            "region": "string",
        },
        "numeric": {
            "category_id": None,
            "views": MAX_VIEWS,
            "likes": None,
            "dislikes": None,
            "comment_count": None,
        },
        "duplicate_key": ["video_id", "region", "trending_date_parsed"],
        "timestamps": ["_observed_at", "_processed_at", "_ingestion_timestamp"],
    },
    "clean_reference_data": {
        "database": REFERENCE_DATABASE,
        "path": f"s3://{SILVER_BUCKET}/youtube/reference_data/",
        "critical": {"id": "string", "snippet_title": "string", "region": "string"},
        "numeric": {},
        "duplicate_key": ["id", "region"],
        "timestamps": ["_ingestion_timestamp", "_processed_at"],
    },
}

_boto3 = None
_wr = None


def load_aws_dependencies():
    """Load Lambda-layer libraries only when the AWS handler runs."""
    global _boto3, _wr
    if _boto3 is None:
        import boto3

        _boto3 = boto3
    if _wr is None:
        import awswrangler

        _wr = awswrangler
    return _boto3, _wr


def quoted_identifier(value):
    """Validate user-controlled catalog names before adding them to SQL."""
    if not isinstance(value, str) or not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid Athena identifier: {value!r}")
    return f'"{value}"'


def resolve_targets(event):
    """Resolve supported tables while allowing separate catalog databases."""
    if not isinstance(event, dict):
        raise ValueError("The Lambda event must be a JSON object")
    if event.get("layer", "silver") != "silver":
        raise ValueError("This function only checks the silver layer")

    if "targets" in event:
        requested = event["targets"]
        if not isinstance(requested, list) or not requested:
            raise ValueError("targets must be a nonempty list")
        targets = []
        for target in requested:
            if not isinstance(target, dict):
                raise ValueError("Every target must contain database and table")
            targets.append((target.get("database"), target.get("table")))
    else:
        table_names = event.get("tables", list(TABLE_RULES))
        if not isinstance(table_names, list) or not table_names:
            raise ValueError("tables must be a nonempty list")
        database_override = event.get("database")
        targets = [
            (database_override or TABLE_RULES.get(table, {}).get("database"), table)
            for table in table_names
        ]

    resolved = []
    for database, table in targets:
        if table not in TABLE_RULES:
            raise ValueError(f"Unsupported data-quality table: {table!r}")
        quoted_identifier(database)
        quoted_identifier(table)
        pair = (database, table)
        if pair not in resolved:
            resolved.append(pair)
    return resolved


def catalog_details(glue_client, database, table):
    response = glue_client.get_table(DatabaseName=database, Name=table)
    catalog_table = response["Table"]
    storage = catalog_table.get("StorageDescriptor", {})
    columns = storage.get("Columns", []) + catalog_table.get("PartitionKeys", [])
    types = {column["Name"]: column["Type"].lower() for column in columns}
    return types, storage.get("Location", "")


def check_schema(database, table, actual_types):
    expected = TABLE_RULES[table]["critical"]
    missing = sorted(set(expected) - set(actual_types))
    mismatches = [
        {"column": column, "expected": expected_type, "actual": actual_types[column]}
        for column, expected_type in expected.items()
        if column in actual_types and actual_types[column] != expected_type
    ]
    passed = not missing and not mismatches
    return {
        "check": "schema",
        "database": database,
        "table": table,
        "missing_columns": missing,
        "type_mismatches": mismatches,
        "passed": passed,
        "message": "Schema matches required columns" if passed else "Required schema does not match",
    }


def check_location(database, table, location):
    expected = TABLE_RULES[table]["path"].rstrip("/")
    actual = str(location).rstrip("/")
    passed = actual == expected
    return {
        "check": "catalog_location",
        "database": database,
        "table": table,
        "value": actual,
        "expected": expected,
        "passed": passed,
        "message": "Catalog points to the expected Silver path" if passed else "Catalog points to an unexpected S3 path",
    }


def null_condition(column):
    name = quoted_identifier(column)
    return f"{name} IS NULL OR TRIM(CAST({name} AS VARCHAR)) = ''"


def build_metrics_query(database, table, actual_types):
    relation = f"{quoted_identifier(database)}.{quoted_identifier(table)}"
    rules = TABLE_RULES[table]
    expressions = ["COUNT(*) AS row_count"]

    for column in rules["critical"]:
        if column in actual_types:
            expressions.append(
                f"SUM(CASE WHEN {null_condition(column)} THEN 1 ELSE 0 END) AS null_{column}"
            )

    for column, maximum in rules["numeric"].items():
        if column not in actual_types:
            continue
        name = quoted_identifier(column)
        invalid = f"{name} < 0"
        if maximum is not None:
            invalid += f" OR {name} > {int(maximum)}"
        expressions.append(
            f"SUM(CASE WHEN {invalid} THEN 1 ELSE 0 END) AS invalid_{column}"
        )

    timestamp_column = next(
        (column for column in rules["timestamps"] if column in actual_types), None
    )
    if timestamp_column:
        name = quoted_identifier(timestamp_column)
        if actual_types[timestamp_column] == "string":
            timestamp_expression = f"TRY(from_iso8601_timestamp(CAST({name} AS VARCHAR)))"
        else:
            timestamp_expression = name
        expressions.append(
            f"CAST(MAX({timestamp_expression}) AS VARCHAR) AS latest_timestamp"
        )

    return f"SELECT {', '.join(expressions)} FROM {relation}", timestamp_column


def build_duplicate_query(database, table, actual_types):
    keys = TABLE_RULES[table]["duplicate_key"]
    if any(key not in actual_types for key in keys):
        return None
    relation = f"{quoted_identifier(database)}.{quoted_identifier(table)}"
    grouping = ", ".join(quoted_identifier(key) for key in keys)
    return (
        "SELECT COALESCE(SUM(duplicate_rows), 0) AS duplicate_count "
        "FROM (SELECT COUNT(*) - 1 AS duplicate_rows "
        f"FROM {relation} GROUP BY {grouping} HAVING COUNT(*) > 1)"
    )


def normalized_scalar(value):
    if value is None:
        return None
    try:
        if bool(value != value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, AttributeError):
            pass
    return value


def run_athena_query(wr_module, database, sql):
    options = {
        "sql": sql,
        "database": database,
        "ctas_approach": False,
        "s3_output": ATHENA_OUTPUT,
    }
    if ATHENA_WORKGROUP:
        options["workgroup"] = ATHENA_WORKGROUP
    frame = wr_module.athena.read_sql_query(**options)
    if frame.empty:
        raise RuntimeError("Athena returned no aggregate result")
    return {column: normalized_scalar(frame.iloc[0][column]) for column in frame.columns}


def parse_timestamp(value):
    if value is None or value == "":
        return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, str):
        try:
            text = value.strip()
            if text.endswith(" UTC"):
                text = f"{text[:-4]}+00:00"
            value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def evaluate_metrics(database, table, metrics, timestamp_column, checked_at=None):
    checked_at = checked_at or datetime.now(timezone.utc)
    row_count = int(metrics.get("row_count") or 0)
    results = [{
        "check": "row_count",
        "database": database,
        "table": table,
        "value": row_count,
        "threshold": MIN_ROW_COUNT,
        "passed": row_count >= MIN_ROW_COUNT,
        "message": f"Row count {row_count}; minimum {MIN_ROW_COUNT}",
    }]

    for column in TABLE_RULES[table]["critical"]:
        alias = f"null_{column}"
        if alias not in metrics:
            continue
        null_count = int(metrics.get(alias) or 0)
        null_pct = 100.0 if row_count == 0 else null_count * 100.0 / row_count
        results.append({
            "check": "null_percentage",
            "database": database,
            "table": table,
            "column": column,
            "value": round(null_pct, 4),
            "threshold": MAX_NULL_PCT,
            "passed": null_pct <= MAX_NULL_PCT,
            "message": f"{column} null/blank percentage {null_pct:.4f}%; maximum {MAX_NULL_PCT}%",
        })

    for column in TABLE_RULES[table]["numeric"]:
        alias = f"invalid_{column}"
        if alias not in metrics:
            continue
        invalid_count = int(metrics.get(alias) or 0)
        results.append({
            "check": "value_range",
            "database": database,
            "table": table,
            "column": column,
            "invalid_count": invalid_count,
            "passed": invalid_count == 0,
            "message": f"{column} has {invalid_count} values outside the allowed range",
        })

    latest = parse_timestamp(metrics.get("latest_timestamp"))
    cutoff = checked_at.astimezone(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)
    freshness_passed = latest is not None and latest >= cutoff
    results.append({
        "check": "freshness",
        "database": database,
        "table": table,
        "column": timestamp_column,
        "latest_record": latest.isoformat() if latest else None,
        "cutoff": cutoff.isoformat(),
        "passed": freshness_passed,
        "message": (
            f"Latest timestamp {latest.isoformat()}; cutoff {cutoff.isoformat()}"
            if latest else "No valid freshness timestamp was found"
        ),
    })
    return results


def validate_configuration():
    if not SILVER_BUCKET:
        raise ValueError("S3_BUCKET_SILVER must not be empty")
    if MIN_ROW_COUNT < 1:
        raise ValueError("DQ_MIN_ROW_COUNT must be at least 1")
    if not 0 <= MAX_NULL_PCT <= 100:
        raise ValueError("DQ_MAX_NULL_PERCENT must be between 0 and 100")
    if MAX_VIEWS < 1 or FRESHNESS_HOURS < 1:
        raise ValueError("DQ_MAX_VIEWS and DQ_FRESHNESS_HOURS must be positive")
    if not ATHENA_OUTPUT.startswith("s3://"):
        raise ValueError("ATHENA_OUTPUT_LOCATION must be an S3 URI")
    if SNS_TOPIC and not SNS_TOPIC_PATTERN.fullmatch(SNS_TOPIC):
        raise ValueError(
            "SNS_ALERT_TOPIC_ARN must be a topic ARN without a subscription UUID"
        )


def lambda_handler(event, context):
    """Run checks and return quality_passed for a Step Functions Choice state."""
    validate_configuration()
    targets = resolve_targets(event)
    boto3_module, wr_module = load_aws_dependencies()
    glue_client = boto3_module.client("glue")
    checked_at = datetime.now(timezone.utc)
    results = []

    for database, table in targets:
        logger.info("Checking %s.%s", database, table)
        try:
            actual_types, location = catalog_details(glue_client, database, table)
            results.append(check_schema(database, table, actual_types))
            results.append(check_location(database, table, location))

            metrics_sql, timestamp_column = build_metrics_query(
                database, table, actual_types
            )
            metrics = run_athena_query(wr_module, database, metrics_sql)
            results.extend(
                evaluate_metrics(
                    database, table, metrics, timestamp_column, checked_at=checked_at
                )
            )

            duplicate_sql = build_duplicate_query(database, table, actual_types)
            if duplicate_sql:
                duplicate_metrics = run_athena_query(
                    wr_module, database, duplicate_sql
                )
                duplicate_count = int(duplicate_metrics.get("duplicate_count") or 0)
                results.append({
                    "check": "duplicates",
                    "database": database,
                    "table": table,
                    "key": TABLE_RULES[table]["duplicate_key"],
                    "duplicate_count": duplicate_count,
                    "passed": duplicate_count == 0,
                    "message": f"Found {duplicate_count} duplicate rows by business key",
                })
        except Exception as exc:
            logger.exception("Unable to check %s.%s", database, table)
            results.append({
                "check": "read_table",
                "database": database,
                "table": table,
                "passed": False,
                "message": str(exc),
            })

    failed = [result for result in results if not result["passed"]]
    alert_sent = False
    alert_error = None
    if failed and SNS_TOPIC:
        try:
            boto3_module.client("sns").publish(
                TopicArn=SNS_TOPIC,
                Subject="[YT Pipeline] Silver data quality checks failed",
                Message=json.dumps(failed, ensure_ascii=False, default=str),
            )
            alert_sent = True
        except Exception as exc:
            alert_error = str(exc)
            logger.exception("Unable to publish the optional SNS alert")

    passed_count = len(results) - len(failed)
    logger.info(
        "Data quality result: %s/%s checks passed",
        passed_count,
        len(results),
    )
    return {
        "quality_passed": not failed,
        "checks_passed": passed_count,
        "checks_total": len(results),
        "checked_at": checked_at.isoformat(),
        "targets": [
            {"database": database, "table": table} for database, table in targets
        ],
        "alert_sent": alert_sent,
        "alert_error": alert_error,
        "details": results,
    }
