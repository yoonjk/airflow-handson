from airflow import DAG  
from airflow.operators.python import PythonOperator 

from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 

import requests, json 

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
    'tags': 'training',
    'catchup': False
}

ENDPOINT = 'https://gorest.co.in/public/v2/posts'
def _extract(): 
  jsonData = requests.get(ENDPOINT).json()
  print(jsonData)
  
  return jsonData

def _transform(**context): 
  jsonData = context['task_instance'].xcom_pull(task_ids='extract')
  print('transform:',jsonData)
  
  return {'no_records': len(jsonData)}

def _load(**context):
  no_records = context['task_instance'].xcom_pull(task_ids='transform')
  print(f'No. of records fetched by {ENDPOINT} : {no_records}')
  print('no_records:', no_records)

with DAG(
  dag_id = 'extract-transform-load',
  default_args = default_args
):
  extract = PythonOperator(
    task_id = 'extract',
    python_callable=_extract
  )
  
  transform = PythonOperator(
    task_id = 'transform',
    python_callable=_transform,
    provide_context=True
  )

  load = PythonOperator(
    task_id = 'load',
    python_callable=_load,
    provide_context=True
  )
  
  extract >> transform >> load 