Bronze Bucket  Name-tej-data-1
silver bucket Name-tej-sliver-data
gold bucket Name-gold-bucket-name

script bucket-tej-script-bucket
SNS-
470223e0-ef38-48a2-a78f-8f120ae52205

## AWS Glue databases

Region: `us-east-2` (Ohio). Catalog ID: `592505727819`.

| Layer | Database | Created (as provided) |
| --- | --- | --- |
| Bronze | [tej-data-bronze-dev](https://592505727819-ghx4nk3g.us-east-2.console.aws.amazon.com/glue/home?region=us-east-2#/v2/data-catalog/databases/view/tej-data-bronze-dev?catalogId=592505727819) | October 7, 2026 at 02:53:28 |
| Silver | [tej-pipeline-silver-dev](https://592505727819-ghx4nk3g.us-east-2.console.aws.amazon.com/glue/home?region=us-east-2#/v2/data-catalog/databases/view/tej-pipeline-silver-dev?catalogId=592505727819) | Not provided |
| Gold | [tej-data-gold-dev](https://592505727819-ghx4nk3g.us-east-2.console.aws.amazon.com/glue/home?region=us-east-2#/v2/data-catalog/databases/view/tej-data-gold-dev?catalogId=592505727819) | October 7, 2026 at 02:54:18 |

## Bronze-to-Silver Glue job parameters

| Key | Value |
| --- | --- |
| `--bronze_database` | `tej-data-bronze-dev` |
| `--bronze_table` | `raw_statistics` |
| `--silver_bucket` | `tej-sliver-data` |
| `--silver_database` | `tej-pipeline-silver-dev` |
| `--silver_table` | `clean_statistics` |

## Silver-to-Gold Glue job parameters

Use these as separate entries under **AWS Glue > Job details > Job parameters**.
AWS Glue supplies `--JOB_NAME` automatically.

| Key | Value |
| --- | --- |
| `--silver_database` | `tej-pipeline-silver-dev` |
| `--silver_bucket` | `tej-sliver-data` |
| `--statistics_table` | `clean_statistics` |
| `--reference_database` | `yt_pipeline_silver_dev` |
| `--reference_table` | `clean_reference_data` |
| `--gold_bucket` | `gold-bucket-name` |
| `--gold_database` | `tej-data-gold-dev` |
| `--job-bookmark-option` | `job-bookmark-disable` |

Glue job settings:

- Maximum concurrent runs: `1`
- Worker type: `G.1X`
- Number of workers: `2`

Gold outputs:

| Glue table | S3 location | Partitions |
| --- | --- | --- |
| `trending_analytics` | `s3://gold-bucket-name/youtube/trending_analytics/` | `region`, `date` |
| `channel_analytics` | `s3://gold-bucket-name/youtube/channel_analytics/` | `region` |
| `category_analytics` | `s3://gold-bucket-name/youtube/category_analytics/` | `region`, `date` |
