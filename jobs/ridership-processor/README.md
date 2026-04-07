# ridership-processor

Fetches the [MTA Daily Ridership and Traffic](https://data.ny.gov/Transportation/MTA-Daily-Ridership-and-Traffic-Beginning-2020/sayj-mze2/about_data)
CSV, aggregates it into weekly buckets (Monday–Sunday) with DuckDB, and writes
rows to a DynamoDB table.

Runs daily at 6:00 AM UTC (`0 6 * * *`) as a Kubernetes CronJob.

---

## What it does

1. Downloads the MTA Daily Ridership CSV from the NY.gov data API.
2. Uses DuckDB to aggregate daily counts into weekly buckets (`DATE_TRUNC('week', date)` — each date is the Monday of that week).
3. Normalises mode names to line IDs (e.g., "Subway" → `line-subway`).
4. Writes each weekly total to the **Ridership** DynamoDB table (partition key: `lineId`, sort key: `date` as the week's Monday).
5. Removes local temporary files.

---

## Environment variables

| Variable               | Required | Description                                 |
|------------------------|----------|---------------------------------------------|
| `AWS_ACCESS_KEY_ID`    | Yes      | AWS access key for DynamoDB                 |
| `AWS_SECRET_ACCESS_KEY`| Yes      | AWS secret key for DynamoDB                 |
| `AWS_REGION`           | No       | AWS region (default: `us-east-1`)           |
| `DYNAMODB_TABLE_NAME`  | No       | DynamoDB table name (default: `Ridership`)  |
| `DATA_URL`             | No       | Override the MTA CSV download URL           |

AWS variables are loaded from the `aws-credentials` Kubernetes Secret
(see `../../base/aws-secrets.yaml.example`).

---

## Dependencies

- `boto3` — AWS SDK (DynamoDB writes)
- `duckdb` — CSV-to-Parquet conversion
- `pandas` / `pyarrow` — DataFrame and Parquet support
- `requests` — HTTP downloads

---

## Build and deploy

Pushing changes to `jobs/ridership-processor/` on `main` triggers the CI workflow,
which builds and pushes the image to `ghcr.io/<owner>/ridership-processor`.
Argo CD then syncs the updated `cronjob.yaml` to the cluster.

---

## Manually trigger a run

```bash
kubectl create job ridership-processor-manual \
  --from=cronjob/ridership-processor \
  -n data-jobs
```

Watch the logs:

```bash
kubectl logs -n data-jobs -l job-name=ridership-processor-manual -f
```

---

## Inspect the CronJob

```bash
# Show recent job history
kubectl get jobs -n data-jobs

# Describe the CronJob
kubectl describe cronjob ridership-processor -n data-jobs
```
