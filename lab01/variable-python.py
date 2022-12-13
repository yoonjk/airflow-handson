from airflow import DAG 
from airflow.operators.python import PythonOperator 

# Utils
from airflow.utils.dates import days_ago 
from datetime import timedelta 

default_args = {
  'start_date': days_ago(1),
  'retries': 0,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

def _my_func():
  pass

with DAG(
  dag_id = 'variable-python-op',
  default_args = default_args,
  tags = ['training']
) as dag: 
  task1 = PythonOperator(
    task_id = 'process_task',
    python_callable = _my_func
  )
  
  task1