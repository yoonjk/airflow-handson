from airflow import DAG 
from airflow.operators.python import  PythonOperator 
from airflow.operators.empty import EmptyOperator

from airflow.utils.dates import days_ago 
from datetime import timedelta 

import time 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'tags': ['training'],
  'catchup': False
}

def _sleep_func(sleep_time):
  print('Start sleep {sleep_time} seconds'.format(sleep_time = sleep_time))
  time.sleep(sleep_time) 

with DAG(
  dag_id = 'python-op',
  default_args=default_args,
  tags = ['training']
) as dag :
  start = EmptyOperator(task_id = 'start_task')
  end = EmptyOperator(task_id = 'end_task')
  
  task1 = PythonOperator(
    task_id = 'python_callable_task1',
    python_callable = _sleep_func,
    op_kwargs = {"sleep_time" : 10}
  )
  
  start >> task1 >> end  
  
  