from airflow import DAG 
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator 

from datetime import timedelta 
from airflow.utils.dates import days_ago 
import requests

url = 'https://gorest.co.in/public/v2/posts'
def make_request():
  response = requests.get(url)
  print('response:', response.json())
  
  if response.status_code == 200:
    return 'conn_success'
  elif response.status_code == 404:
    return 'not_reachable'
  else:
    print('Unable to connect API or retrieve data.')

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

with DAG(
  dag_id = 'branch-op',
  default_args = default_args,
  tags = ['training']
) as dag:
  start = EmptyOperator(task_id='start') 
  end = EmptyOperator(task_id='end', trigger_rule = 'one_success') 
  response = EmptyOperator(task_id='conn_success')
  notresponse = EmptyOperator(task_id='not_reachable')
  branch_task = BranchPythonOperator(
    task_id = 'make_request',
    python_callable = make_request
  )

  start >> branch_task >> [response, notresponse] >> end 

