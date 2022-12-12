from airflow import DAG 
from airflow.operators.python import PythonOperator 
# Utils
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

def _xcom_push(**context):
  xcom_val = 'val1'
  context['task_instance'].xcom_push(key = 'key1', value = xcom_val)
  return 'xcom_return_val'

def _xcom_pull(**context):
  xcom_return = context['task_instance'].xcom_pull(task_ids = 'xcom_push_task')
  xcom_push_val = context['ti'].xcom_pull(key = 'key1')
  xcom_return_val = context['ti'].xcom_pull(task_ids = 'xcom_push_task', key='key1')
  
  print('xcom_return :{}'.format(xcom_return))
  print('xcom_push_val : {}'.format(xcom_push_val))
  print('xcom_push_return_val : {}'.format(xcom_return_val))

with DAG (
  dag_id = 'xcom-test',
  default_args = default_args,
  tags = ['training']
): 
  xcom_push_task = PythonOperator(
    task_id = 'xcom_push_task',
    python_callable=_xcom_push
  )
  
  xcom_pull_task = PythonOperator(
    task_id = 'xcom_pull_task',
    python_callable=_xcom_pull
  )
  
  xcom_push_task >> xcom_pull_task 
  
  