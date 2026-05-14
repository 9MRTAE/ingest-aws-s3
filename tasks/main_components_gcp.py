import os
import shutil
import zipfile
import uuid
import calendar
import sys

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import sqlalchemy as SA
from datetime import timedelta, datetime
from git import Repo
from google.cloud import storage

from prefect.logging import get_run_logger  # Prefect v3

sys.path.append('./')
from config import *
from config_flows import *


# ---------------------------------------------------------------------------
# CopySourceData — copy ZIP files from S3 → GCS (raw bucket)
# ---------------------------------------------------------------------------
class CopySourceData:
    def __init__(self):
        pass

    def fn_Copy_To_Datalake_GCP(self, aws_file_key: str) -> pd.DataFrame:
        logger = get_run_logger()
        logger.info('Start: fn_Copy_To_Datalake_GCP')

        df_final = pd.DataFrame()
        try:
            # ✅ ข้อ 2: AWS_ACCESS_KEY → AWS_ACCESS_KEY_ID, AWS_SECRET_KEY → AWS_SECRET_ACCESS_KEY
            s3 = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            )

            # ✅ ข้อ 5: เพิ่ม pagination สำหรับ S3 (รองรับ >1,000 objects)
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=S3_BUCKET)

            gcp_client = storage.Client(project=GCP_PROJECT, credentials=CREDENTIAL)
            bucket     = gcp_client.bucket(GCP_BUCKET)

            for page in pages:
                if 'Contents' not in page:
                    continue

                for obj in page['Contents']:
                    file_key = obj['Key']
                    if not file_key.endswith(aws_file_key):
                        continue

                    logger.info(file_key)
                    local_file_path = os.path.join('/tmp', os.path.basename(file_key))
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    # ✅ ข้อ 3: GCP_BUCKET → S3_BUCKET
                    s3.download_file(S3_BUCKET, file_key, local_file_path)

                    # Upload to GCS
                    gcp_blob_name = f'{GCP_BUCKET_AWS_COST}/{file_key}'
                    blob = bucket.blob(gcp_blob_name)
                    blob.upload_from_filename(local_file_path)

                    # ✅ ข้อ 8: cleanup temp file
                    if os.path.exists(local_file_path):
                        os.remove(local_file_path)

        except Exception as e:
            logger.error(f'Error : {e}')
            raise  # ✅ ข้อ 7: re-raise ให้ Prefect จับ failure ได้

        return df_final


# ---------------------------------------------------------------------------
# ConnectorDB — PostgreSQL helper (unused in AWS→GCS flow, kept for compat)
# ---------------------------------------------------------------------------
class ConnectorDB:
    def __init__(self):
        pass

    def fn_ConnectPostgresql(self, p_database: str):
        logger = get_run_logger()
        connection_string = (
            f"{DB_TYPE}+{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}"
            f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        engine = SA.create_engine(connection_string)
        return engine.connect()


# ---------------------------------------------------------------------------
# ExtractSourceData — read CSV from GCS or directly from S3
# ---------------------------------------------------------------------------
class ExtractSourceData:
    def __init__(self):
        self.connectdb = ConnectorDB()

    # ── static helpers ──────────────────────────────────────────────────────
    @staticmethod
    def safe_bignumeric(val):
        try:
            from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
            d = Decimal(val)
            return str(d.quantize(
                Decimal('1.00000000000000000000000000000000000000'),
                rounding=ROUND_HALF_UP,
            ))
        except (InvalidOperation, TypeError, ValueError):
            return None

    # ✅ ข้อ 4: แก้จาก backup_date (ไม่เคยมี) → ใช้ filedate ที่สร้างไว้แล้ว
    @staticmethod
    def fill_null_with_last_day(row):
        if pd.notnull(row['date']):
            return row['date']
        ref = row.get('filedate')
        if ref is None or pd.isnull(ref):
            return np.nan
        ref = pd.Timestamp(ref)
        last_day = calendar.monthrange(ref.year, ref.month)[1]
        return pd.Timestamp(ref.year, ref.month, last_day)

    # ── internal shared column builder ──────────────────────────────────────
    @staticmethod
    def _build_derived_columns(df: pd.DataFrame, zipname: str, csv_path: str) -> pd.DataFrame:
        df['zipname']    = zipname
        df['filename']   = os.path.basename(csv_path)
        df['filedate']   = df['filename'].str.extract(r'(\d{4}-\d{2})\.csv')
        df['filedate']   = pd.to_datetime(df['filedate'], format='%Y-%m').dt.date

        df['date'] = df['UsageStartDate'].combine_first(df['UsageEndDate'])
        df['date'] = (
            df['date'].astype('string').str[0:4]
            + df['date'].str[5:7].str.zfill(2)
            + df['date'].str[8:10].str.zfill(2)
        )
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['date'] = df.apply(ExtractSourceData.fill_null_with_last_day, axis=1)

        df['calendar_year']  = pd.to_datetime(df['date']).dt.year
        df['month_no']       = pd.to_datetime(df['date']).dt.month
        df['day_of_month']   = pd.to_datetime(df['date']).dt.day
        df['UsageQuantity']  = pd.to_numeric(df['UsageQuantity'], errors='coerce').astype(float)
        df['UnblendedRate']  = pd.to_numeric(df['UnblendedRate'], errors='coerce').astype(float)
        df['UnBlendedCost']  = pd.to_numeric(df['UnBlendedCost'], errors='coerce').astype(float)
        return df

    # ── read ZIPs already copied to GCS ─────────────────────────────────────
    def fn_Ingest_CSV(self, aws_file_key: str) -> pd.DataFrame:
        logger = get_run_logger()
        logger.info('Start: fn_Ingest_CSV')

        df_final = pd.DataFrame()
        try:
            gcp_client = storage.Client(project=GCP_PROJECT, credentials=CREDENTIAL)
            bucket     = gcp_client.bucket(GCP_BUCKET)
            blobs      = bucket.list_blobs(prefix=f'{GCP_BUCKET_AWS_COST}/', delimiter=None)
            zip_blobs  = [b for b in blobs if b.name.endswith(aws_file_key)]

            for blob in zip_blobs:
                file_key     = blob.name
                zip_filename = os.path.basename(file_key)
                temp_file    = f'/tmp/{zip_filename}'
                temp_folder  = f'/tmp/unzipped/{zip_filename}'

                try:  # ✅ ข้อ 8: try/finally per file
                    logger.info(f'Processing file: {file_key}')
                    blob.download_to_filename(temp_file)

                    os.makedirs(temp_folder, exist_ok=True)
                    with zipfile.ZipFile(temp_file, 'r') as zf:
                        zf.extractall(temp_folder)

                    csv_files = [f for f in os.listdir(temp_folder) if f.endswith('.csv')]
                    if not csv_files:
                        logger.warning(f'No CSV found in {file_key}')
                        continue

                    csv_path = os.path.join(temp_folder, csv_files[0])
                    df = pd.read_csv(csv_path)
                    df = ExtractSourceData._build_derived_columns(df, zip_filename, csv_path)

                    if not df.empty:
                        logger.info(f'Rows : {len(df)} , Columns : {df.shape[1]}')

                    df_final = pd.concat([df_final, df], ignore_index=True)

                finally:
                    # ✅ ข้อ 8: cleanup ทั้ง zip และ extracted folder
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                    shutil.rmtree(temp_folder, ignore_errors=True)

        except Exception as e:
            logger.error(f'Error: {e}')
            raise  # ✅ ข้อ 7

        return df_final

    # ── download directly from S3, process in-place ─────────────────────────
    def fn_Ingest_AWS(self, aws_file_key: str) -> pd.DataFrame:
        logger = get_run_logger()
        logger.info('Start: fn_Ingest_AWS')

        df_final = pd.DataFrame()
        try:
            # ✅ ข้อ 2: แก้ชื่อ variable
            s3 = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            )

            # ✅ ข้อ 5: เพิ่ม pagination
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=S3_BUCKET)

            for page in pages:
                if 'Contents' not in page:
                    continue

                for obj in page['Contents']:
                    file_key = obj['Key']
                    if not file_key.endswith(aws_file_key):
                        continue

                    logger.info(f'===== {file_key} ==========')
                    zip_filename    = os.path.basename(file_key)
                    temp_file       = f'/tmp/{zip_filename}'
                    temp_folder     = f'/tmp/unzipped/{zip_filename}'
                    local_file_path = os.path.join('/tmp', file_key)

                    # Clean up stale tmp artifacts
                    if os.path.isdir(temp_file):
                        shutil.rmtree(temp_file)
                    elif os.path.isfile(temp_file):
                        os.remove(temp_file)

                    try:  # ✅ ข้อ 8: try/finally per file
                        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                        # ✅ ข้อ 3: GCP_BUCKET → S3_BUCKET
                        s3.download_file(S3_BUCKET, file_key, local_file_path)

                        os.makedirs(temp_folder, exist_ok=True)
                        with zipfile.ZipFile(local_file_path, 'r') as zf:
                            zf.extractall(temp_folder)

                        csv_files = [f for f in os.listdir(temp_folder) if f.endswith('.csv')]
                        if not csv_files:
                            logger.warning(f'No CSV found in {file_key}')
                            continue

                        csv_path = os.path.join(temp_folder, csv_files[0])
                        df = pd.read_csv(csv_path)

                        if not df.empty:
                            logger.info(f'CSV Rows : {len(df)} , Columns : {df.shape[1]}')

                        df = ExtractSourceData._build_derived_columns(
                            df,
                            zipname=file_key.strip(),
                            csv_path=os.path.basename(csv_path),
                        )
                        df['date'] = pd.to_datetime(df['date'])

                        # Filter by JOB_BACKDATE
                        cutoff = (datetime.now() + timedelta(days=int(JOB_BACKDATE))).date()
                        df = df[df['date'].dt.date >= cutoff]

                        if not df.empty:
                            logger.info(f'Filtered Rows : {len(df)} , Columns : {df.shape[1]}')

                        df_final = pd.concat([df_final, df], ignore_index=True)

                    finally:
                        # ✅ ข้อ 8: cleanup ทั้ง zip และ extracted folder
                        if os.path.exists(local_file_path):
                            os.remove(local_file_path)
                        shutil.rmtree(temp_folder, ignore_errors=True)

            # Summary log
            if not df_final.empty:
                summary = (
                    df_final[['zipname', 'date']].drop_duplicates()
                    .groupby('zipname')
                    .agg(min_date=('date', 'min'), max_date=('date', 'max'))
                    .reset_index()
                )
                logger.info(summary)

        except Exception as e:
            logger.error(f'Error : {e}')
            raise  # ✅ ข้อ 7

        return df_final

    def fn_Git_Clone(self):
        # ✅ ข้อ 1: แก้ใน PR แล้ว — คงไว้ตาม main ปัจจุบัน
        git_url  = f'https://oauth2:{GITLAB_TOKEN}@{GITLAB_URL}'
        repo_dir = './datasource'
        Repo.clone_from(git_url, repo_dir)


# ---------------------------------------------------------------------------
# TransformData — cast dtypes, normalise nulls, lowercase columns
# ---------------------------------------------------------------------------
class TransformData:
    def __init__(self):
        pass

    def fn_Transform_To_String(self, p_dataframe: pd.DataFrame) -> pd.DataFrame:
        logger = get_run_logger()
        logger.info('Start: fn_Transform_To_String')

        # ✅ ถ้า DataFrame ว่าง ไม่ต้อง transform
        if p_dataframe.empty:
            logger.warning('Input DataFrame is empty — skipping transform.')
            return p_dataframe

        df = p_dataframe.astype({
            col: str
            for col in p_dataframe.columns.difference(['calendar_year', 'month_no', 'day_of_month'])
        })

        null_values = ['NaT', 'None', 'NaN', 'nan', '<NA>', '']
        for v in null_values:
            df = df.replace(v, np.nan)

        df.columns = df.columns.str.lower()
        return df


# ---------------------------------------------------------------------------
# LoadSourceData — write partitioned Parquet to GCS
# ---------------------------------------------------------------------------
class LoadSourceData:
    def __init__(self):
        pass

    @staticmethod
    def _clean_partition_path(gcsfs_instance, path: str) -> None:
        if gcsfs_instance.exists(path):
            files = gcsfs_instance.ls(path, detail=False)
            for f in files:
                gcsfs_instance.rm(f, recursive=True)
            print(f'✅ Deleted: {path}')
        else:
            print(f'⏭️  Path not found: {path}')

    def fn_Load_To_Datalake_GCP(self, p_source_type: str, p_dataframe: pd.DataFrame):
        logger = get_run_logger()
        logger.info('Start: fn_Load_To_Datalake_GCP')

        # ✅ ถ้า DataFrame ว่าง ไม่ต้อง load
        if p_dataframe.empty:
            logger.warning('Input DataFrame is empty — skipping load.')
            return None

        target_path = (
            f"gs://{GCP_BUCKET}/{GCP_BUCKET_PARQUET}"
            f"/{GCP_BUCKET_APPLICATION}/{p_source_type}"
        )
        logger.info(f'Load to : {target_path}')

        try:
            group_keys = ['linkedaccountid', 'calendar_year', 'month_no', 'day_of_month']
            for keys, _ in p_dataframe.groupby(group_keys):
                linkedaccountid, year, month, day = keys
                partition_path = (
                    f"{target_path}/linkedaccountid={linkedaccountid}"
                    f"/calendar_year={year}/month_no={month}/day_of_month={day}"
                )
                LoadSourceData._clean_partition_path(GCSFS, partition_path)

            ds.write_dataset(
                pa.Table.from_pandas(p_dataframe),
                target_path,
                format='parquet',
                filesystem=GCSFS,
                partitioning_flavor='hive',
                basename_template=str(uuid.uuid4()) + '{i}.parquet',
                partitioning=['linkedaccountid', 'calendar_year', 'month_no', 'day_of_month'],
                existing_data_behavior='overwrite_or_ignore',
                max_partitions=100_000,
            )
        except Exception as e:
            logger.error(f'Error : {e}')
            raise 


# ---------------------------------------------------------------------------
# DataLake — helper (kept for compatibility)
# ---------------------------------------------------------------------------
class DataLake:
    def __init__(self):
        pass