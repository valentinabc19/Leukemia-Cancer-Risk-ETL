from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import pandas as pd
from etl import export_to_postgres, load_db_credentials

with DAG(
    'leukemia_load_dag',
    start_date=datetime(2025, 4, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    wait_for_transform = ExternalTaskSensor(
        task_id='wait_for_transform',
        external_dag_id='leukemia_transform_dag',
        external_task_id='extract_leukemia_facts',
        timeout=600,
        poke_interval=60,
    )

    def load_task():
        CREDS_FILE = "/home/user/leucemia/Leukemia-Cancer-Risk-ETL/credentialsdb.json"
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

    wait_for_transform >> load_operation