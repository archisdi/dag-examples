
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@daily',
}

SERVICE_NAME="purchase"
POSTGRES_CONNECTION_ID="local-postgres"
FILE_FORMAT="csv"

GCS_BUCKET_NAME=Variable.get("gcs_bucket")
BQ_PROJECT=Variable.get("bq_project")
SLACK_CHANNEL=Variable.get("slack_notif_channel")

DATA_SOURCE=["data.subscriptions", "data.subscription_groups", "data.subscription_quotations"]

with DAG(
    f'{SERVICE_NAME}-to-bigquery',
    default_args=args,
    description='Postgres to BigQuery',
    catchup=False,
    tags=['data-engineering'],
    start_date=datetime(2022, 9, 8),
) as dag:

    notify_success_task = SlackWebhookOperator(
        task_id="notif-success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        message=f"airflow {SERVICE_NAME}-to-bigquery success",
        channel=SLACK_CHANNEL,
        http_conn_id='slack_connection'
    )

    notify_fails_task = SlackWebhookOperator(
        task_id="notif-fails",
        trigger_rule=TriggerRule.ONE_FAILED,
        message=f"airflow {SERVICE_NAME}-to-bigquery failed",
        channel=SLACK_CHANNEL,
        http_conn_id='slack_connection'
    )

    for table in DATA_SOURCE:
        [POSTGRES_SCHEMA_NAME, POSTGRES_TABLE_NAME] = table.split(".")

        FILENAME=f'{SERVICE_NAME}/{POSTGRES_TABLE_NAME}.{FILE_FORMAT}'
        BQ_DESTINATION='.'.join([BQ_PROJECT, SERVICE_NAME, POSTGRES_TABLE_NAME])

        postgres_to_gcs_task = PostgresToGCSOperator(
            task_id=f'{POSTGRES_TABLE_NAME}-pg-to-gcs',
            postgres_conn_id=POSTGRES_CONNECTION_ID,
            sql=f'SELECT * FROM {POSTGRES_SCHEMA_NAME}.{POSTGRES_TABLE_NAME};',
            bucket=GCS_BUCKET_NAME,
            filename=FILENAME,
            export_format=FILE_FORMAT,
            gzip=False,
            use_server_side_cursor=False,
        )

        gcs_to_bq_task = GCSToBigQueryOperator(
            task_id=f'{POSTGRES_TABLE_NAME}-gcs-to-bq',
            bucket=GCS_BUCKET_NAME,
            source_objects=[FILENAME],
            destination_project_dataset_table=BQ_DESTINATION,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            skip_leading_rows=1,
            allow_quoted_newlines=True
        )

        cleanup_task = GCSDeleteObjectsOperator(
            task_id=f'{POSTGRES_TABLE_NAME}-gcs-cleanup',
            trigger_rule=TriggerRule.ALL_DONE,
            bucket_name=GCS_BUCKET_NAME,
            objects=[FILENAME],
        )

        postgres_to_gcs_task >> gcs_to_bq_task >> cleanup_task >> [notify_success_task, notify_fails_task]