from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../"))
print(ROOT_DIR)
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from functions.leukemia_extract import extract_data
from functions.api_extraction import api_data_extraction
from functions.api_transformations import process_world_bank_data
from functions.merge import merge_dataframes
from functions.dimensional_model_transform import process_dimensions
from functions.gx_validations import validation_results
from functions.load_data import export_to_postgres
from kafka.kafka_producer import run_kafka_producer

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='leukemia_etl_dag',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    def extract_leukemia_task():
        df = extract_data()
        path = '/tmp/leukemia_df.csv'
        df.to_csv(path, index=False)
        return path

    def extract_api_task():
        df = api_data_extraction()
        return df

    def process_api_task(**context):
        df = context['ti'].xcom_pull(task_ids='extract_api_data')
        processed = process_world_bank_data(df)
        path = '/tmp/api_processed.csv'
        processed.to_csv(path, index=False)
        return path

    def merge_task(**context):
        leukemia_path = context['ti'].xcom_pull(task_ids='extract_leukemia_data')
        api_path = context['ti'].xcom_pull(task_ids='process_api_data')
        df1 = pd.read_csv(leukemia_path)
        df2 = pd.read_csv(api_path)
        merged = merge_dataframes(df1, df2)
        path = '/tmp/merged_df.csv'
        merged.to_csv(path, index=False)
        return path

    def dimension_task(**context):
        merged_path = context['ti'].xcom_pull(task_ids='merge_data')
        df = pd.read_csv(merged_path)
        dim_dict = process_dimensions(df)

        for name, dim_df in dim_dict.items():
            dim_df.to_csv(f'/tmp/{name}.csv', index = False)
        
        return list(dim_dict.keys())

    def validation_task():
        dimensions_dict = {
            'Fact_Leukemia': pd.read_csv('/tmp/Fact_Leukemia.csv'),
            'Dim_PatientInfo': pd.read_csv('/tmp/Dim_PatientInfo.csv'),
            'Dim_MedicalHistory': pd.read_csv('/tmp/Dim_MedicalHistory.csv'),
            'Dim_Region': pd.read_csv('/tmp/Dim_Region.csv')
        }
        result = validation_results(dimensions_dict)
        return result

    def load_task(**context):
        validation_success = context['ti'].xcom_pull(task_ids='validate_data')

        dimensions_dict = {
            'Fact_Leukemia': pd.read_csv('/tmp/Fact_Leukemia.csv'),
            'Dim_PatientInfo': pd.read_csv('/tmp/Dim_PatientInfo.csv'),
            'Dim_MedicalHistory': pd.read_csv('/tmp/Dim_MedicalHistory.csv'),
            'Dim_Region': pd.read_csv('/tmp/Dim_Region.csv')
        }

        export_to_postgres(dimensions_dict, validation_success)

    def kafka_producer_task():
        run_kafka_producer()

    extract_leukemia_op = PythonOperator(
        task_id='extract_leukemia_data',
        python_callable=extract_leukemia_task
    )

    extract_api_op = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_api_task
    )

    process_api_op = PythonOperator(
        task_id='process_api_data',
        python_callable=process_api_task
    )

    merge_op = PythonOperator(
        task_id='merge_data',
        python_callable=merge_task
    )

    transform_op = PythonOperator(
        task_id='transform_dimensions',
        python_callable=dimension_task
    )

    validate_op = PythonOperator(
        task_id='validate_data',
        python_callable=validation_task
    )

    load_op = PythonOperator(
        task_id='load_data',
        python_callable=load_task
    )

    kafka_op = PythonOperator(
        task_id='kafka_producer',
        python_callable=kafka_producer_task
    )

    extract_api_op >> process_api_op
    [extract_leukemia_op, process_api_op] >> merge_op
    merge_op >> transform_op >> validate_op >> load_op >> kafka_op
