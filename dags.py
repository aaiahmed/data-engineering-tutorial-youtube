from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from extract import extract_tables_from_mysql, extract_tables_from_sqlserver
from extract_from_api import extract_from_api
from transform import transform_employee

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
        dag_id='etl_tutorial_dag',
        default_args=default_args,
        description='This is a demo dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 2, 18),
        catchup=False,
        tags=['demo']
) as dag:
    t1 = PythonOperator(
        task_id='sql_server_extract',
        python_callable=extract_tables_from_sqlserver
    )

    t2 = PythonOperator(
        task_id='mysql_extract',
        python_callable=extract_tables_from_mysql
    )

    t3 = PythonOperator(
        task_id='endpoint_extract',
        python_callable=extract_from_api
    )

    t4 = PythonOperator(
        task_id='transform_employee',
        python_callable=transform_employee
    )

[t1, t2, t3] >> t4