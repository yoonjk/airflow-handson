from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago
from datetime import datetime, timedelta 

def print_execution_date(ds):
    print(ds)
    
default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

ds = ''
with DAG(
  dag_id='external-task', 
  default_args=default_args,
  tags = ['training']
) as dag:
  task_1 = EmptyOperator(
      dag=dag,
      task_id='Task_1'
  )
  task_2 = PythonOperator(
      dag=dag,
      task_id='Task_2',
      python_callable=print_execution_date,
      op_kwargs={'ds': ds},
  )
  task_1 >> task_2