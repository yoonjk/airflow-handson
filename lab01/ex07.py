from airflow.operators.dummy import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor 
from airflow import DAG 
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta  


default_args = {
  'start_date' : days_ago(1), 
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'catchup': False
}

with DAG ('file_sensor_ex01',
  default_args = default_args, 
  schedule_interval='@daily'
) as dag:
  start = DummyOperator(task_id='start')
  end = DummyOperator(task_id='end')
  
  sensor_task = FileSensor(
    task_id='file_sensor_task', 
    poke_interval=10,
    fs_conn_id='file_conn',
    filepath='test1.csv'
  )
  
  start >> sensor_task >> end 
  