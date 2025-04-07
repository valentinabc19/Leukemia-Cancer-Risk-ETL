from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import pandas as pd
import sys


sys.path.append("/home/ubuntu/Escritorio/Leukemia-Cancer-Risk-ETL/airflow/fuctions")
from etl import export_to_postgres, load_db_credentials

default_args={
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'leukemia_load_dag',
    schedule_interval=None,
    catchup=False,

) as dag:

    def load_task():
        CREDS_FILE = "/home/ubuntu/Escritorio/Leukemia-Cancer-Risk-ETL/credentialsdb.json"
        creds = load_db_credentials(CREDS_FILE)

        dimensions_dict = {
            'Dim_MedicalHistory': pd.read_csv('/tmp/Dim_MedicalHistory.csv'),
            'Dim_Region': pd.read_csv('/tmp/Dim_Region.csv'),
            'Dim_PatientInfo': pd.read_csv('/tmp/Dim_PatientInfo.csv'),
            'Fact_Leukemia': pd.read_csv('/tmp/Fact_Leukemia.csv')
        }
        export_to_postgres(dimensions_dict, creds)

    load_operation = PythonOperator(
        task_id='load_leukemia_data',
        python_callable=load_task,
    )