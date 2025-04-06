from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl import extract_data

with DAG(
    'leukemia_extract_dag',
    start_date=datetime(2025, 4, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    def extract_task():
        df = extract_data()
        df.to_csv('/tmp/leukemia_raw_data.csv', index=False)
        return '/tmp/leukemia_raw_data.csv'

    extract_operation = PythonOperator(
        task_id='extract_leukemia_data',
        python_callable=extract_task,
    )