from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_dag',
    default_args=default_args,
    description='An Airflow DAG to run DBT jobs',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
)

run_dbt = BashOperator(
    task_id='run_dbt',
    bash_command='cd /home/tsabitghazian/airflow_demo/ws2dbt/dbt_partnatech && dbt run',
    dag=dag,
)