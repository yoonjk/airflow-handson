from airflow import DAG 
from airflow.operators.bash import BashOperator 
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# Utils
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 
import random

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False,
  'description': 'BranchPythonOperator test',
  'tags':['training']
}

dag = DAG(
  dag_id='branchOperater',
  default_args = default_args
)

start = EmptyOperator(
  task_id='start',
  dag=dag
)

end = EmptyOperator(
  task_id='end',
  dag=dag
)

def check_condition():
  num = random.randint(0,10)
  print(num) 
  
  if num > 6:
    return 'print_greater'
  else: 
    return 'print_smaller'
  
python_check = BranchPythonOperator(
  task_id='branch_task',
  python_callable=check_condition,
  dag=dag
)

print_greater = BashOperator(
  task_id='print_greater',
  bash_command='echo value is greater than 6',
  dag=dag
)

print_samller = BashOperator(
  task_id='print_smaller',
  bash_command='echo value is smaller than 6',
  dag=dag
)

start >> python_check >> [print_greater , print_samller] >> end


