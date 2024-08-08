from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def greet():
    print("Hello, Airflow!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_greet_dag',
    default_args=default_args,
    description='A simple DAG to greet',
    schedule_interval=timedelta(days=1),
)

greet_task = PythonOperator(
    task_id='greet_task',
    python_callable=greet,
    dag=dag,
)