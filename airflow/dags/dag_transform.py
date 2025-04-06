from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import pandas as pd
from etl import process_dimensions


with DAG(
    'leukemia_transform_dag',
    start_date=datetime(2025, 4, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    wait_for_extract = ExternalTaskSensor(
        task_id='wait_for_extract',
        external_dag_id='leukemia_extract_dag',
        external_task_id='extract_leukemia_data',
        timeout=600,
        poke_interval=60,
    )

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

    wait_for_extract >> transform_operation