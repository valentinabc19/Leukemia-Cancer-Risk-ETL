from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append("/home/ubuntu/Escritorio/Leukemia-Cancer-Risk-ETL/airflow/fuctions")
from etl import extract_data

default_args={
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'leukemia_extract_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False

) as dag:

    def extract_task():
        df = extract_data()
        df.to_csv('/tmp/leukemia_raw_data.csv', index=False)
        return '/tmp/leukemia_raw_data.csv'

    extract_operation = PythonOperator(
        task_id='extract_leukemia_data',
        python_callable=extract_task,
    )