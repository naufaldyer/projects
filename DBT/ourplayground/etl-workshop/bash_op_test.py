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
    'bash_op_test',
    default_args=default_args,
    description='An Airflow DAG to run Bash jobs',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
)

run_dbt = BashOperator(
    task_id='run_bash',
    bash_command='echo "Hello, World!"',
    dag=dag,
)