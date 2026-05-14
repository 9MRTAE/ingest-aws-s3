import sys
import pandas as pd
from datetime import datetime

from prefect import task                    # Prefect v3: @task decorator
from prefect.logging import get_run_logger  # Prefect v3: structured logger

sys.path.append('./')
from tasks.main_components_gcp import (
    CopySourceData,
    ExtractSourceData,
    TransformData,
    LoadSourceData,
)

datetime_start = str(datetime.today())[0:23]


# ---------------------------------------------------------------------------
# CopyFile  — copy raw ZIP from S3 → GCS
# ---------------------------------------------------------------------------
@task(name="copy_file")
def copy_file(aws_file_key: str) -> pd.DataFrame:
    logger = get_run_logger()
    copy   = CopySourceData()
    df     = pd.DataFrame()
    try:
        df = copy.fn_Copy_To_Datalake_GCP(aws_file_key)
        if not df.empty:
            logger.info('Rows : ' + str(len(df)) + ' , Columns : ' + str(df.shape[1]))
            logger.info(pd.concat([df.iloc[:1], df.tail(1)]))
    except Exception as e:
        logger.error('Error : ' + str(e))
    return df


# ---------------------------------------------------------------------------
# Extract  — read CSV from ZIPs already in GCS
# ---------------------------------------------------------------------------
@task(name="extract")
def extract(aws_file_key: str) -> pd.DataFrame:
    logger  = get_run_logger()
    ext     = ExtractSourceData()
    df      = pd.DataFrame()
    try:
        df = ext.fn_Ingest_CSV(aws_file_key)
        if not df.empty:
            logger.info('Rows : ' + str(len(df)) + ' , Columns : ' + str(df.shape[1]))
            logger.info(pd.concat([df.iloc[:1], df.tail(1)]))
    except Exception as e:
        logger.error('Error : ' + str(e))
    return df


# ---------------------------------------------------------------------------
# ExtractAWS  — download directly from S3 then process
# ---------------------------------------------------------------------------
@task(name="extract_aws")
def extract_aws(aws_file_key: str) -> pd.DataFrame:
    logger  = get_run_logger()
    ext     = ExtractSourceData()
    df      = pd.DataFrame()
    try:
        df = ext.fn_Ingest_AWS(aws_file_key)
        if not df.empty:
            logger.info('Rows : ' + str(len(df)) + ' , Columns : ' + str(df.shape[1]))
            logger.info(pd.concat([df.iloc[:1], df.tail(1)]))
    except Exception as e:
        logger.error('Error : ' + str(e))
    return df


# ---------------------------------------------------------------------------
# Transform  — cast types, normalise nulls, lowercase columns
# ---------------------------------------------------------------------------
@task(name="transform")
def transform(p_dataframe: pd.DataFrame = pd.DataFrame()) -> pd.DataFrame:
    logger      = get_run_logger()
    transformed = TransformData()
    df          = pd.DataFrame()
    try:
        logger.info('Transform data')
        df = transformed.fn_Transform_To_String(p_dataframe=p_dataframe)
        logger.info('Transform data source database')
        if not df.empty:
            logger.info('Rows : ' + str(len(df)) + ' , Columns : ' + str(df.shape[1]))
            if len(df) > 1:
                logger.info(pd.concat([df.iloc[:1], df.tail(1)]))
            else:
                logger.info(df)
    except Exception as e:
        logger.error('Error : ' + str(e))
    return df


# ---------------------------------------------------------------------------
# Load  — write partitioned Parquet to GCS datalake
# ---------------------------------------------------------------------------
@task(name="load")
def load(p_source_type: str, p_dataframe: pd.DataFrame):
    logger  = get_run_logger()
    loaded  = LoadSourceData()
    message = None
    try:
        logger.info('Load data to bucket ' + p_source_type)
        if not p_dataframe.empty:
            logger.info('Rows : ' + str(len(p_dataframe)) + ' , Columns : ' + str(p_dataframe.shape[1]))
            logger.info(pd.concat([p_dataframe.iloc[:1], p_dataframe.tail(1)]))
        message = loaded.fn_Load_To_Datalake_GCP(
            p_source_type=p_source_type,
            p_dataframe=p_dataframe,
        )
        datetime_end = str(datetime.today())[0:23]  # noqa: F841 (kept for future logging)
    except Exception as e:
        logger.error('Error : ' + str(e))
    return message


# ---------------------------------------------------------------------------
# GitClone  — clone contract-api repo (kept for compatibility)
# ---------------------------------------------------------------------------
@task(name="git_clone")
def git_clone():
    logger    = get_run_logger()
    extracted = ExtractSourceData()
    result    = extracted.fn_Git_Clone()
    logger.info(result)
    return result
