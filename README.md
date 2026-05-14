# ingest_aws_s3

Ingest pipeline — **AWS S3 → GCS Data Lake · Cloud Cost (AWS CUR)**

Pulls AWS Cost and Usage Reports (CUR) from S3 and writes them to GCS Data Lake as Parquet files
Supports both copy-then-ingest (2-flow pipeline) and direct ingest from S3

**Stack:** Python · boto3 · pandas · gcsfs · GCP Secret Manager · Prefect v3 · Docker · Jenkins

---

## TL;DR

| | |
|---|---|
| **Source** | AWS S3 — AWS CUR files (ZIP → CSV) split by Linked Account |
| **Sink** | GCS Data Lake · Parquet · partitioned by `linkedaccountid/calendar_year/month_no/day_of_month` |
| **Flows** | 3 Prefect v3 deployments |
| **Pattern** | 2-step pipeline: `awscost_1copy` (S3→GCS raw) → `awscost_2ingest` (GCS→Parquet), or `awscost` (direct S3→Parquet) |
| **Auth** | AWS credentials + GCP SA key — both fetched from GCP Secret Manager at runtime |

---

## Architecture Overview

```
AWS S3 ({your-s3-bucket})
  └── {linked_account_id}/{report_prefix}-cur-with-tags-YYYY-MM.zip
  │
  │  boto3 S3 client (credentials from GCP Secret Manager)
  │
  ├── [Flow 1: awscost_1copy] ─────────────────────────────────────────────
  │   downloads ZIP from S3 → uploads to GCS raw bucket
  │   gs://{bucket}/gcp-storage-aws-costs/{linked_account_id}/...zip
  │
  └── [Flow 2: awscost_2ingest] ──────────────────────────────────────────
      reads ZIPs from GCS → extracts CSV → transforms → writes Parquet
      │
      ▼
  [Flow 3: awscost] ← direct path: S3 → download → extract → transform → write Parquet
      │
      ▼
  gs://{bucket}/gcp-storage-parquet/cloudcost/AWS/
      linkedaccountid={id}/calendar_year=YYYY/month_no=M/day_of_month=D/
      └── {uuid}.parquet
```

**Credentials flow:**
```
GCP Secret Manager
  ├── your-secret-sa-key        → GCP Service Account JSON → GCSFS
  ├── your-secret-aws-key-id    → AWS_ACCESS_KEY_ID
  ├── your-secret-aws-secret-key → AWS_SECRET_ACCESS_KEY
  ├── your-secret-gitlab-token  → GitLab OAuth token (fn_Git_Clone)
  └── your-secret-gitlab-url    → GitLab repo URL
```
Both GCP and AWS credentials are fetched from a single GCP Secret Manager — the worker authenticates using ADC

---

## Design Decisions

### 1. 2-step pipeline (copy → ingest) over direct ingest

**Problem:** CUR files are large (multiple GB per account per month) and are frequently reprocessed
when AWS issues a revised report

**Decision:** separate copy and ingest into distinct steps:
- `awscost_1copy` — copies raw ZIPs to GCS first (idempotent, safe to re-run)
- `awscost_2ingest` — reads from GCS → avoids re-downloading from S3 on reprocessing

**Trade-off:** one additional flow, but reduces S3 egress cost and reprocessing time

`awscost` (flow 3) is the direct path for backfilling old months using `JOB_BACKDATE` filtering

---

### 2. S3 pagination for buckets with >1,000 objects

AWS S3 `list_objects_v2` returns at most 1,000 keys per request — without pagination
objects are missed as the bucket grows:

```python
# Avoided — returns only the first 1,000 objects
response = s3.list_objects_v2(Bucket=S3_BUCKET)

# Used — paginator handles the continuation token automatically
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=S3_BUCKET)
for page in pages:
    for obj in page.get('Contents', []):
        ...
```

---

### 3. Partition key includes `linkedaccountid` in addition to date

CUR combines costs from all linked accounts in the organization into a single file
Partitioning by `linkedaccountid` lets downstream ETL query only the required account directly
without scanning all partitions:

```
cloudcost/AWS/
  linkedaccountid=<AWS_ACCOUNT_ID_1>/calendar_year=2024/month_no=3/day_of_month=15/
  linkedaccountid=<AWS_ACCOUNT_ID_2>/calendar_year=2024/month_no=3/day_of_month=15/
```

---

### 4. Temp file cleanup with try/finally per file

ZIP files can be several GB — if processing fails midway without cleanup
the worker pod's disk fills up:

```python
try:
    blob.download_to_filename(temp_file)
    # ... process ...
finally:
    if os.path.exists(temp_file):
        os.remove(temp_file)
    shutil.rmtree(temp_folder, ignore_errors=True)
```

cleanup runs every time regardless of success or exception

---

### 5. `fill_null_with_last_day` — handles null dates in CUR records

Some CUR rows have a null `UsageStartDate` or `UsageEndDate` (e.g. monthly charges)
Rather than dropping those rows, the last day of the month is inferred from `filedate` (the ZIP filename):

```python
@staticmethod
def fill_null_with_last_day(row):
    if pd.notnull(row['date']):
        return row['date']
    ref = pd.Timestamp(row['filedate'])
    last_day = calendar.monthrange(ref.year, ref.month)[1]
    return pd.Timestamp(ref.year, ref.month, last_day)
```

This ensures monthly charge rows are loaded into the correct partition instead of being lost

---

## Project Structure

```
ingest_aws_s3_livingos/
├── flows/
│   ├── ingest_cloudcost_awscost.py         # Flow 3: direct S3 → GCS Parquet
│   └── ingest_cloudcost_awscost_2ingest.py # Flow 2: GCS raw ZIP → GCS Parquet
│
├── tasks/
│   ├── tasks_gcp.py           # Prefect @task: copy_file, extract, extract_aws, transform, load
│   └── main_components_gcp.py # Core: CopySourceData, ExtractSourceData, TransformData, LoadSourceData
│
├── config/
│   ├── __init__.py            # env dispatcher + Secret Manager (GCP SA + AWS credentials + GitLab)
│   ├── production.py          # production constants
│   └── development.py         # development constants
│
├── config_flows/
│   └── __init__.py            # APPLICATION_NAME, cron, job date params
│
├── scripts/
│   └── command.sh
│
├── deploy.py                  # Register Prefect deployments
├── prefect.yaml               # Prefect deployment definitions (3 deployments)
├── sonar-project.properties   # SonarQube config
├── Dockerfile
└── requirements.txt
```

---

## Registered Flows

| Flow | Pattern | Schedule |
|---|---|---|
| `ingest_cloudcost_awscost_1copy` | S3 → GCS raw (copy ZIP) | daily 01:00 ICT |
| `ingest_cloudcost_awscost_2ingest` | GCS raw → Parquet | daily 01:00 ICT (after 1copy) |
| `ingest_cloudcost_awscost` | S3 → Parquet (direct) | manual / backfill |

---

## GCS Output Path

```
gs://{bucket}/gcp-storage-parquet/cloudcost/AWS/
  linkedaccountid={account_id}/
  calendar_year=YYYY/month_no=M/day_of_month=D/
  {uuid}.parquet
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CI_COMMIT_BRANCH` | `develop` | `main` → production config |
| `IMAGE_TAG` | `ingest_aws_s3_livingos:local` | Docker image tag |
| `JOB_BACKDATE` | `0` | days to look back (`0` = current month for the direct awscost flow) |
| `JOB_STARTDATE` | `2026-03-01` | start date (used when JOB_BACKDATE = -1) |
| `JOB_ENDDATE` | `2026-03-31` | end date |

### GCP Secret Manager Keys

| Secret key | Used for |
|---|---|
| `your-secret-sa-key` | GCP Service Account JSON (GCS access) |
| `your-secret-aws-key-id` | AWS Access Key ID |
| `your-secret-aws-secret-key` | AWS Secret Access Key |
| `your-secret-gitlab-token` | GitLab OAuth token (fn_Git_Clone) |
| `your-secret-gitlab-url` | GitLab repository URL |

---

## Local Development

```bash
pip install -r requirements.txt

export CI_COMMIT_BRANCH=develop

# Step 1: Copy ZIPs from S3 → GCS
python -m flows.ingest_cloudcost_awscost_1copy

# Repair a specific month
python -m flows.ingest_cloudcost_awscost_1copy --year 2024 --month 03

# Step 2: Ingest ZIPs from GCS → Parquet
python -m flows.ingest_cloudcost_awscost_2ingest --year 2024 --month 03

# Direct path (backfill)
python -m flows.ingest_cloudcost_awscost --year 2024 --month 03
```