"""
config_flows/__init__.py
Prefect v3 deployment constants.

CHANGE POINT (v1 → v3):
  Removed: KubernetesRun, Schedule, CronClock — infrastructure is now declared
  in prefect.yaml per-deployment. This file only holds the constants that
  prefect.yaml and deploy.py need to reference.
"""

import os

APP_ENV   = os.getenv('CI_COMMIT_BRANCH', 'develop')
IMAGE_TAG = os.getenv('IMAGE_TAG', 'ingest_aws_s3_livingos:local')

PREFECT_FLOW_DIRECTORY = '/app/flows/'
PREFECT_IMAGE          = f"{IMAGE_TAG}"

PREFECT_WORK_POOL  = 'kubernetes-agent'
PREFECT_WORK_QUEUE = 'default'

# Cron schedule (set in prefect.yaml → schedules)
CRON_SCHEDULER_INGEST = '0 18 * * *'   # 01:00 ICT daily

APPLICATION_NAME = "ingest_cloudcost"

# Job parameters — read from env so Jenkins can override per run
JOB_BACKDATE  = os.getenv('JOB_BACKDATE',  '0')
JOB_STARTDATE = os.getenv('JOB_STARTDATE', '2026-03-01')
JOB_ENDDATE   = os.getenv('JOB_ENDDATE',   '2026-03-31')