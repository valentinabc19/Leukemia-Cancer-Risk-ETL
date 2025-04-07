from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id = 'trigger_dag',
    start_date=datetime(2025, 4, 6),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    trigger_extract = TriggerDagRunOperator(
        task_id='trigger_extract_dag',
        trigger_dag_id='leukemia_extract_dag'
    )

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform_dag',
        trigger_dag_id='leukemia_transform_dag'
    )

    trigger_load = TriggerDagRunOperator(
        task_id='trigger_load_dag',
        trigger_dag_id='leukemia_load_dag'
    )

    trigger_extract >> trigger_transform >> trigger_load