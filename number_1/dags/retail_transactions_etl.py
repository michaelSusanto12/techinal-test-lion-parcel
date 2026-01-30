"""
Airflow DAG - ETL Retail Transactions
Handles hourly sync from source DB to data warehouse
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import pytz
import logging

logger = logging.getLogger(__name__)

SOURCE_CONN_ID = 'source_db'
DWH_CONN_ID = 'data_warehouse'

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def extract(**context):
    """Pull latest changes from source database"""
    execution_date = context['execution_date']
    last_hour = execution_date - timedelta(hours=1)
    
    logger.info(f"Fetching records updated since {last_hour}")
    
    source_hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
    
    query = """
        SELECT id, customer_id, last_status, pos_origin, pos_destination,
               created_at, updated_at, deleted_at
        FROM retail_transactions
        WHERE updated_at >= %s OR (deleted_at IS NOT NULL AND deleted_at >= %s)
    """
    
    df = source_hook.get_pandas_df(query, parameters=[last_hour, last_hour])
    logger.info(f"Got {len(df)} records")
    
    context['ti'].xcom_push(key='extracted_data', value=df.to_json(date_format='iso'))
    return len(df)


def transform(**context):
    """Add ETL metadata and mark deleted records"""
    ti = context['ti']
    data_json = ti.xcom_pull(task_ids='extract', key='extracted_data')
    
    if not data_json:
        logger.info("Nothing to transform")
        return 0
    
    df = pd.read_json(data_json)
    
    if df.empty:
        logger.info("Empty dataset, skipping")
        return 0
    
    logger.info(f"Processing {len(df)} records")
    
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    df['etl_loaded_at'] = datetime.now(jakarta_tz)
    df['is_deleted'] = df['deleted_at'].notna()
    
    deleted_count = df['is_deleted'].sum()
    logger.info(f"Found {deleted_count} soft-deleted records")
    
    context['ti'].xcom_push(key='transformed_data', value=df.to_json(date_format='iso'))
    return len(df)


def safe_timestamp(value):
    """Handle pandas NaT values for postgres"""
    if pd.isna(value):
        return None
    return value


def load(**context):
    """Upsert records into data warehouse"""
    ti = context['ti']
    data_json = ti.xcom_pull(task_ids='transform', key='transformed_data')
    
    if not data_json:
        logger.info("No data to load")
        return 0
    
    df = pd.read_json(data_json)
    
    if df.empty:
        logger.info("Empty dataset, skipping")
        return 0
    
    logger.info(f"Upserting {len(df)} records to DWH")
    
    dwh_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    conn = dwh_hook.get_conn()
    cursor = conn.cursor()
    
    upsert_query = """
        INSERT INTO dwh_retail_transactions 
        (id, customer_id, last_status, pos_origin, pos_destination,
         created_at, updated_at, deleted_at, is_deleted, etl_loaded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            last_status = EXCLUDED.last_status,
            pos_origin = EXCLUDED.pos_origin,
            pos_destination = EXCLUDED.pos_destination,
            updated_at = EXCLUDED.updated_at,
            deleted_at = EXCLUDED.deleted_at,
            is_deleted = EXCLUDED.is_deleted,
            etl_loaded_at = EXCLUDED.etl_loaded_at
    """
    
    inserted = 0
    errors = 0
    
    for _, row in df.iterrows():
        try:
            cursor.execute(upsert_query, (
                row['id'],
                row['customer_id'],
                row.get('last_status'),
                row.get('pos_origin'),
                row.get('pos_destination'),
                safe_timestamp(row['created_at']),
                safe_timestamp(row['updated_at']),
                safe_timestamp(row.get('deleted_at')),
                bool(row['is_deleted']),
                safe_timestamp(row['etl_loaded_at'])
            ))
            inserted += 1
        except Exception as e:
            errors += 1
            logger.error(f"Failed to insert {row['id']}: {e}")
            conn.rollback()
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"Done: {inserted} inserted, {errors} failed")
    return inserted


with DAG(
    dag_id='retail_transactions_etl',
    default_args=default_args,
    description='Sync retail transactions to data warehouse',
    schedule_interval='0 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'retail', 'lionparcel']
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
