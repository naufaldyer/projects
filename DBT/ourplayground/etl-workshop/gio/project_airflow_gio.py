from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "gio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

##########
# DAG config
##########
dag = DAG(
    "data_engineering_project_gio",
    default_args=default_args,
    description="A simple DAG to demonstrate ELT from raw MySQL data to ready-to-use BigQuery table",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 4, 7),
    catchup=False,
)

##########
# Arguments & variables
##########

# Query to extract data from MySQL
# Improvements to be made:
# 1. use index for faster performance
# 2. filter the data to return data within certain period, i.e. D-1 data only
L1_QUERY="""
select  
	*
from
    db_flight.flight_tickets
"""

# BigQuery L1 schema
# Improvements to be made:
# 1. define the schema in Terraform, then retrieve the schema using `bq show --schema` command
BQ_SCHEMA=[
        {"name": "flightId", "type": "STRING", "mode": "REQUIRED"},
        {"name": "searchTerms", "type": "STRING", "mode": "NULLABLE"},
        {"name": "rank", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        {"name": "snippet", "type": "STRING", "mode": "NULLABLE"},
        {"name": "displayLink", "type": "STRING", "mode": "NULLABLE"},
        {"name": "link", "type": "STRING", "mode": "NULLABLE"},
        {"name": "queryTime", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "totalResults", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "cacheId", "type": "STRING", "mode": "NULLABLE"},
        {"name": "formattedUrl", "type": "STRING", "mode": "NULLABLE"},
        {"name": "htmlFormattedUrl", "type": "STRING", "mode": "NULLABLE"},
        {"name": "htmlSnippet", "type": "STRING", "mode": "NULLABLE"},
        {"name": "htmlTitle", "type": "STRING", "mode": "NULLABLE"},
        {"name": "kind", "type": "STRING", "mode": "NULLABLE"},
        {"name": "pagemap", "type": "STRING", "mode": "NULLABLE"},
        {"name": "cseName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "startIndex", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "inputEncoding", "type": "STRING", "mode": "NULLABLE"},
        {"name": "outputEncoding", "type": "STRING", "mode": "NULLABLE"},
        {"name": "safe", "type": "STRING", "mode": "NULLABLE"},
        {"name": "cx", "type": "STRING", "mode": "NULLABLE"},
        {"name": "gl", "type": "STRING", "mode": "NULLABLE"},
        {"name": "searchTime", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "formattedSearchTime", "type": "STRING", "mode": "NULLABLE"},
        {"name": "formattedTotalResults", "type": "STRING", "mode": "NULLABLE"},
    ]
DBT_DIR="/home/tsabitghazian/airflow_demo/ws2dbt/dbt_partnatech"

def DbtRunOperator(dbt_dir=DBT_DIR,model=str):
    dbt_dir="/home/tsabitghazian/airflow_demo/ws2dbt/dbt_partnatech"
    dbt_verb="run"
    model={model}
    return BashOperator(
        task_id=f"l2_dbt_transform_{model}",
        bash_command=f"cd {dbt_dir} && dbt {dbt_verb} -s {model}",
        dag=dag
    )

##########
# Tasks definition
##########
extract_mysql_to_gcs = MySQLToGCSOperator(
    task_id="extract_mysql_to_gcs",
    sql=L1_QUERY,
    bucket="etl-workshop-bucket",
    filename="flight_tickets.json",
    mysql_conn_id="mysql_default",
    gcp_conn_id="google_cloud_gcs",
    dag=dag,
)

l1_gcs_to_bq = GCSToBigQueryOperator(
    task_id="load_to_bq",
    bucket="etl-workshop-bucket",
    source_objects=["flight_tickets.json"],
    destination_project_dataset_table="etl_workshop_gio.flight_tickets_raw",
    schema_fields=BQ_SCHEMA,
    write_disposition="WRITE_TRUNCATE",
    source_format="NEWLINE_DELIMITED_JSON",
    dag=dag,
)