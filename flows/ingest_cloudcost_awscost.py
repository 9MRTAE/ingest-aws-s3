"""
Flow: ingest_cloudcost_awscost_2ingest
========================================
Step 2 of 2 — อ่าน CSV จาก ZIP ที่ copy ไว้ใน GCS → Transform → Load to GCS (Parquet)
ต้องรัน flow awscost_1copy ก่อนเพื่อให้ไฟล์อยู่ใน GCS แล้ว

AWS Linked Account IDs ที่ active:
    <AWS_ACCOUNT_ID_1> | <AWS_ACCOUNT_ID_2> | <AWS_ACCOUNT_ID_3>  # [SCRUBBED]
    <AWS_ACCOUNT_ID_4> | <AWS_ACCOUNT_ID_5>  # [SCRUBBED]
    <AWS_ACCOUNT_ID_6> → Cancelled  # [SCRUBBED]

Prefect v3: @flow + @task  |  ไม่มี Flow context manager / run_config / storage

Usage (local repair):
    python -m flows.ingest_cloudcost_awscost_2ingest                      # เดือนปัจจุบัน
    python -m flows.ingest_cloudcost_awscost_2ingest --year 2026 --month 02   # ซ่อมเดือนเก่า
"""

import sys
import argparse
from datetime import datetime
from typing import Optional

from prefect import flow  # Prefect v3

sys.path.append('./')
from config_flows import APPLICATION_NAME
from config import GCP_BUCKET_AWS
from tasks.tasks_gcp import extract, transform, load

v_flow_name = f"{APPLICATION_NAME}_awscost_2ingest"


@flow(name=v_flow_name, log_prints=True)
def ingest_cloudcost_awscost_2ingest(
    year: Optional[str] = None,
    month: Optional[str] = None,
) -> None:
    # ── build file key ───────────────────────────────────────────────────────
    now = datetime.now()
    y = year  or str(now.year)
    m = month or f"{now.month:02d}"
    aws_file_key = f"-cur-with-tags-{y}-{m}.zip"
    print(f"📦 AWS file key: {aws_file_key}")

    # ── ETL: read from GCS ───────────────────────────────────────────────────
    df_extracted   = extract(aws_file_key=aws_file_key)
    df_transformed = transform(df_extracted)
    load(GCP_BUCKET_AWS, df_transformed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest AWS Cost from GCS")
    parser.add_argument("--year",  type=str, default=None, help="ปี เช่น 2026 (default: ปีปัจจุบัน)")
    parser.add_argument("--month", type=str, default=None, help="เดือน เช่น 02 (default: เดือนปัจจุบัน)")
    args = parser.parse_args()
    ingest_cloudcost_awscost_2ingest(year=args.year, month=args.month)