from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteDatasetOperator, BigQueryCreateEmptyDatasetOperator

BQ_DATASET="primary"

with DAG(
    'flush-bq-table',
    description='Flush BigQuery Dataset',
    catchup=False,
    tags=['data-engineering'],
    start_date=datetime(2022, 9, 8),
):
    deleteDataSet = BigQueryDeleteDatasetOperator(
        task_id="rm-bq-ds",
        dataset_id=BQ_DATASET
    )

    createDataSet = BigQueryCreateEmptyDatasetOperator(
        task_id="cr-bq-ds",
        dataset_id=BQ_DATASET
    )

    deleteDataSet >> createDataSet

