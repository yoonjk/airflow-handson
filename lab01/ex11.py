from airflow import DAG 
from airflow.operators.python import PythonOperator 

# utils
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 

import logging 
import requests

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'catchup': False
}

dag = DAG(
  dag_id = 'data_pipeline_xcon',
  default_args = default_args 
)

def extract(**context):
  url = context['params']['url']
  logging.info(url) 

  f = requests.get(url)

  return f.text 

def transform(**context):
  text = context['task_instance'].xcom_pull(task_ids='exec_extract')
  lines = text.split('\n') 

  return lines 

exec_extract = PythonOperator(
  task_id = 'exec_extract',
  python_callable=extract,
  params={'url': 'https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv'},
  provide_context=True, 
  dag=dag
)

exec_transform=PythonOperator(
  task_id='exec_transform',
  python_callable = transform,
  provide_context=True,
  dag=dag
)

exec_extract >> exec_transform