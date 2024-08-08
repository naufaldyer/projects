from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "naufaldy",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "data_engineering_project_naufaldy",
    default_args=default_args,
    description="End to End data engineer project",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
)

# Query to extract the data from mysql
SQL_QUERY = """
select  
	*
from db_flight.flight_tickets ft
"""

# Task for
extract_mysql = MySQLToGCSOperator(
    task_id="extract_mysql_to_gcs",
    sql=SQL_QUERY,
    bucket="etl-workshop-bucket",
    filename="flight_tickets.json",
    mysql_conn_id="mysql_default",
    gcp_conn_id="google_cloud_gcs",
    dag=dag,
)

# Task for load to BigQuery (L1 layer)
l1_layers = GCSToBigQueryOperator(
    task_id="load_to_bq",
    bucket="etl-workshop-bucket",
    source_objects=["flight_tickets.json"],
    destination_project_dataset_table="etl_workshop_naufaldy.flight_tickets_raw",
    schema_fields=[
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
    ],
    write_disposition="WRITE_TRUNCATE",
    source_format="NEWLINE_DELIMITED_JSON",
    dag=dag,
)

l2_transform = BashOperator(
    task_id='l2_transform_naufaldy',
    bash_command='cd /home/tsabitghazian/airflow_demo/ws2dbt/dbt_partnatech && dbt run -s l2_transform_naufaldy',
    dag=dag,
)

l3_transform = BashOperator(
    task_id='l3_transform_naufaldy',
    bash_command='cd /home/tsabitghazian/airflow_demo/ws2dbt/dbt_partnatech && dbt run -s l3_transform_naufaldy',
    dag=dag,
)

l4_transform = BashOperator(
    task_id='l4_transform_naufaldy',
    bash_command='cd /home/tsabitghazian/airflow_demo/ws2dbt/dbt_partnatech && dbt run -s l4_transform_naufaldy',
    dag=dag,
)



# Task workflow
extract_mysql >> l1_layers >> l2_transform >> l3_transform >> l4_transform
