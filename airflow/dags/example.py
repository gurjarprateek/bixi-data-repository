from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from lib.helpers import load_historical

year_list = [2014, 2015, 2016, 2017, 2018, 2019]

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
    'Historical', default_args=default_args, description= 'desc', schedule_interval='@once')

start = DummyOperator(task_id='Start', dag=dag)

dummy_tasks = []

ingest_trips_tasks = []

ingest_station_tasks = []

for year in year_list:

    #if year==2014:

    dummy = DummyOperator(task_id=f'Ingestion_Initializer_{year}', dag=dag)

    dummy_tasks.append(dummy)

    url = f'https://montreal.bixi.com/c/bixi/file_db/data_all.file/BixiMontrealRentals{year}.zip'

    t = PythonOperator(task_id=f'Ingest_Trips_{year}', python_callable=load_historical, provide_context=True, op_kwargs = {'url':url, 'bucket_name':'bixi.qc.raw', 'load':'trips'}, dag=dag)

    ingest_trips_tasks.append(t)

    s = PythonOperator(task_id=f'Ingest_Stations_{year}', python_callable=load_historical, provide_context=True, op_kwargs = {'url':url, 'bucket_name':'bixi.qc.raw', 'load':'stations'}, dag=dag)

    ingest_station_tasks.append(s)


for i in range(0, len(dummy_tasks)):
    start >> dummy_tasks[i] >>ingest_trips_tasks[i]
    start >> dummy_tasks[i] >>ingest_station_tasks[i]
