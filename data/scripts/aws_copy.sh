#!/usr/bin/env bash
set -euo pipefail

# Run from the project root, even when launched from another directory.
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

aws_cli="aws"
if ! command -v aws >/dev/null 2>&1; then
    if [[ -n "${LOCALAPPDATA:-}" ]]; then
        local_app_data="${LOCALAPPDATA//\\//}"
        aws_cli="$local_app_data/Programs/Amazon/AWSCLIV2/aws.exe"
    fi
    if [[ ! -f "$aws_cli" ]]; then
        echo "AWS CLI was not found. Add it to PATH before running this script." >&2
        exit 1
    fi
fi

for file in data/*videos.csv; do
    [[ -f "$file" ]] || continue
    name="${file##*/}"
    region="${name:0:2}"
    "$aws_cli" s3 cp "$file" "s3://tej-data-1/youtube/raw_statistics/region=${region,,}/" --region us-east-2 --no-progress
done

for file in data/*_category_id.json; do
    [[ -f "$file" ]] || continue
    name="${file##*/}"
    region="${name:0:2}"
    "$aws_cli" s3 cp "$file" "s3://tej-data-1/youtube/raw_statistics_reference_data/region=${region,,}/" --region us-east-2 --no-progress
done
