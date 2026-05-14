"""
Flow: ingest_cloudcost_awscost_1copy
=====================================
Step 1 of 2 — Copy raw ZIP files จาก AWS S3 → GCS (raw bucket)
ใช้คู่กับ flow awscost_2ingest ซึ่งจะอ่าน CSV จาก GCS ในขั้นตอนถัดไป

Prefect v3: @flow + @task  |  ไม่มี Flow context manager / run_config / storage

Usage (local repair):
    python -m flows.ingest_cloudcost_awscost_1copy                      # เดือนปัจจุบัน
    python -m flows.ingest_cloudcost_awscost_1copy --year 2026 --month 02   # ซ่อมเดือนเก่า
"""

import sys
import argparse
from datetime import datetime
from typing import Optional

from prefect import flow  # Prefect v3

sys.path.append('./')
from config_flows import APPLICATION_NAME
from tasks.tasks_gcp import copy_file

v_flow_name = f"{APPLICATION_NAME}_awscost_1copy"


@flow(name=v_flow_name, log_prints=True)
def ingest_cloudcost_awscost_1copy(
    year: Optional[str] = None,
    month: Optional[str] = None,
) -> None:
    # ── build file key ───────────────────────────────────────────────────────
    now = datetime.now()
    y = year  or str(now.year)
    m = month or f"{now.month:02d}"
    aws_file_key = f"-cur-with-tags-{y}-{m}.zip"
    print(f"📦 AWS file key: {aws_file_key}")

    # ── Step 1: Copy S3 → GCS ────────────────────────────────────────────────
    copy_file(aws_file_key=aws_file_key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy AWS Cost ZIP to GCS")
    parser.add_argument("--year",  type=str, default=None, help="ปี เช่น 2026 (default: ปีปัจจุบัน)")
    parser.add_argument("--month", type=str, default=None, help="เดือน เช่น 02 (default: เดือนปัจจุบัน)")
    args = parser.parse_args()
    ingest_cloudcost_awscost_1copy(year=args.year, month=args.month)