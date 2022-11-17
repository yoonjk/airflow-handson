from airflow import DAG 
from airflow.operators.python import PythonOperator 

# Utils 
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 
import logging 
import requests 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'catchup': False,
  'schedule_interval': '@dialy'
}

dag = DAG(
  dag_id = 'data_pipeline_func_args',
  default_args = default_args
)

# csv 파일을 str로 저장 
def extract(**context):
  url = context['params']['url']
  logging.info(url)

  f = requests.get(url)

  return f.text

t1 = PythonOperator(
  task_id = 'extract',
  python_callable=extract,
  params={'url': 'https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv'},
  provide_context=True,
  dag=dag
)
