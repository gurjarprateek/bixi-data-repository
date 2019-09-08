from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from scripts.task_incremental_stations import ingest_stations
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup':False,
    'start_date': datetime(2019, 9, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'Stations_Incremental', 
    default_args=default_args, 
    description= 'Pulls station status and information every 5 minutes', 
    schedule_interval='*/5 * * * *')

dummy = DummyOperator(
    task_id=f'Ingestion_Initializer_Stations', 
    dag=dag)

station_status = PythonOperator(
    task_id=f'ingest_station_status', 
    python_callable=ingest_stations, 
    provide_context=True, 
    op_kwargs = {'endpoint':'station_status', 'bucket_name':'bixi.qc.staged'}, 
    dag=dag)

station_information = PythonOperator(
    task_id=f'ingest_station_information', 
    python_callable=ingest_stations, 
    provide_context=True, 
    op_kwargs = {'endpoint':'station_information', 'bucket_name':'bixi.qc.staged'}, 
    dag=dag)

dummy >> station_status
dummy >> station_information
