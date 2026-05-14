"""
deploy.py — Register Prefect v3 deployments.

Usage:
    PREFECT_DEPLOY_MODE=1 python deploy.py

CHANGE POINT (v1 → v3):
  Old: flow.register(project_name="main")  called inside each flow file.
  New: deploy.py is the single registration entry point.
       PREFECT_DEPLOY_MODE=1 tells config/__init__.py to skip Secret Manager
       (no DB/GCS access needed during deploy).
"""

from __future__ import annotations

from config_flows import (
    PREFECT_IMAGE,
    PREFECT_WORK_POOL,
    PREFECT_WORK_QUEUE,
    CRON_SCHEDULER_INGEST,
)

# ── Register flows here ───────────────────────────────────────────────────────
from flows.ingest_cloudcost_awscost import ingest_cloudcost_awscost
from flows.ingest_cloudcost_awscost_1copy import ingest_cloudcost_awscost_1copy
from flows.ingest_cloudcost_awscost_2ingest import ingest_cloudcost_awscost_2ingest

FLOW_OBJECTS = [
    ingest_cloudcost_awscost,
    ingest_cloudcost_awscost_1copy,
    ingest_cloudcost_awscost_2ingest,
    # add new flows here
]
# ──────────────────────────────────────────────────────────────────────────────


if __name__ == '__main__':
    import asyncio
    from prefect.deployments import deploy

    deployments = [
        flow_obj.to_deployment(
            name=flow_obj.name,
            work_pool_name=PREFECT_WORK_POOL,
            work_queue_name=PREFECT_WORK_QUEUE,
            image=PREFECT_IMAGE,
            cron=CRON_SCHEDULER_INGEST,
            timezone='Asia/Bangkok',
        )
        for flow_obj in FLOW_OBJECTS
    ]

    asyncio.run(deploy(*deployments))
    print(f"Deployed {len(deployments)} flow(s) successfully.")