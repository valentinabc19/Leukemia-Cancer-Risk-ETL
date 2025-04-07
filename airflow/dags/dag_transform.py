from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import pandas as pd
import sys


sys.path.append("/home/ubuntu/Escritorio/Leukemia-Cancer-Risk-ETL/airflow/fuctions")
from etl import process_dimensions

default_args={
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'leukemia_transform_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    def transform_task(): 
        df = pd.read_csv('/tmp/leukemia_raw_data.csv')
        dimensions = process_dimensions(df)
        for dim_name, dim_df in dimensions.items():
            dim_df.to_csv(f'/tmp/{dim_name}.csv', index=False)
        return list(dimensions.keys())  

    transform_operation = PythonOperator(
        task_id='process_leukemia_dimensions',
        python_callable=transform_task,
    )