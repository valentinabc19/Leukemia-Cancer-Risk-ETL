from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sys

ROOT_DIR = root_dir = os.path.abspath(os.path.join(__file__, "../../../"))

if root_dir not in sys.path:
	sys.path.append(root_dir)      
sys.path.append("/home/ubuntu/Escritorio/Leukemia-Cancer-Risk-ETL/airflow/functions")

from etl import extract_data, process_dimensions, export_to_postgres, load_db_credentials

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='leukemia_etl_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    
    def extract_task():
        df=extract_data()
        raw_path='/tmp/leukemia_raw_data.csv'
        df.to_csv(raw_path, index=False)
        return raw_path
    
    extract_operation=PythonOperator(
        task_id='extract_leukemia_data',
        python_callable=extract_task
    )

    def transform_task(**context):
        """
        Transforms data from the CSV file into dimensional tables (MedicalHistory, Region, PatientInfo)
        and fact tables (FAct_Leukemia) using the process_dimensions() function.
        
        The transformed tables are saved as temporary CSV files in /tmp.
        
        Args:
            context_ Airflow exexution context (used to retrieve XComs).
        
        Returns:
            list: List of processed table names"""

        raw_path=context['ti'].xcom_pull(task_ids='extract_leukemia_data')
        df=pd.read_csv(raw_path)
        dimensions=process_dimensions(df)

        for name, dim_df in dimensions.items():
            dim_df.to_csv(f'/tmp/{name}.csv', index = False)
        
        return list(dimensions.keys())
    
    transform_operation=PythonOperator(
        task_id='transform_leukemia_data',
        python_callable=transform_task,
        provide_context=True
    )

    def load_task():

        creds_path="/home/ubuntu/Escritorio/Leukemia-Cancer-Risk-ETL/credentialsdb.json"
        creds = load_db_credentials(creds_path)

        dimensions_dict = {
            'Fact_Leukemia': pd.read_csv('/tmp/Fact_Leukemia.csv'),
            'Dim_PatientInfo': pd.read_csv('/tmp/Dim_PatientInfo.csv'),
            'Dim_MedicalHistory': pd.read_csv('/tmp/Dim_MedicalHistory.csv'),
            'Dim_Region': pd.read_csv('/tmp/Dim_Region.csv')
        }

        export_to_postgres(dimensions_dict, creds)

    load_operation = PythonOperator(
        task_id='load_leukemia_data',
        python_callable=load_task,
    )


    extract_operation >> transform_operation >> load_operation