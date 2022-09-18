
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

POSTGRES_CONNECTION_ID="local-postgres"
POSTGRES_SCHEMA_NAME = "data"
POSTGRES_TABLE_NAME = "subscriptions"

FILE_FORMAT="csv"
GCS_OBJECT_PATH="purchase"
GCS_BUCKET="adv-node-blog"
GCS_FILENAME=f'{GCS_OBJECT_PATH}/{POSTGRES_TABLE_NAME}.{FILE_FORMAT}'

BQ_PROJECT="archisdi" # move to env
BQ_DS="purchase" # move to config
BQ_DESTINATION='.'.join([BQ_PROJECT, BQ_DS, POSTGRES_TABLE_NAME])

with DAG(
    'postgres-to-bigquery',
    description='Postgres to BigQuery',
    catchup=False,
    tags=['data-engineering'],
    start_date=datetime(2022, 9, 8),
) as dag:

    postgres_to_gcs_task = PostgresToGCSOperator(
        task_id=f'postgres-to-bigquery',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql=f'SELECT * FROM {POSTGRES_SCHEMA_NAME}.{POSTGRES_TABLE_NAME};',
        bucket=GCS_BUCKET,
        filename=GCS_FILENAME,
        export_format=FILE_FORMAT,
        gzip=False,
        use_server_side_cursor=False,
    )

    gcs_to_bq_task = GCSToBigQueryOperator(
        task_id=f'gcs_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[GCS_FILENAME],
        destination_project_dataset_table=BQ_DESTINATION,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    cleanup_task = GCSDeleteObjectsOperator(
        task_id='cleanup',
        bucket_name=GCS_BUCKET,
        objects=[GCS_FILENAME],
    )

    postgres_to_gcs_task >> gcs_to_bq_task >> cleanup_task