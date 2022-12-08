from airflow import DAG  
from airflow.operators.empty import EmptyOperator 
from airflow.operators.bash import BashOperator 
# Utils
from airflow.utils.dates import days_ago 
from datetime import timedelta 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

with DAG(
  dag_id = 'empty-op',
  default_args = default_args,
  tags = ['training']
) as dag: 
  start = EmptyOperator(
    task_id = 'start_task'
  )
  
  end = EmptyOperator(
    task_id = 'end_task'
  )
  
  t1 = BashOperator(
    task_id = 'task1',
    bash_command = 'echo task1'
  )
  
  start >> t1 >> end

   