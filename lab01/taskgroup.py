from airflow import DAG  
from airflow.operators.empty import EmptyOperator 
from airflow.operators.bash import BashOperator 
from airflow.utils.task_group import TaskGroup

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

with DAG (
  dag_id = 'taskgroup',
  default_args = default_args, 
  tags = ['training']
) as dag: 
  start = EmptyOperator(
    task_id = 'start'
  )
  
  end = EmptyOperator(
    task_id = 'end'
  )
  
  with TaskGroup(
    group_id = 'taskGroup1',
    tooltip = 'tasks fro group1'
  ) as group1:
    task1 = EmptyOperator(task_id = 'task1')
    task2 = BashOperator( task_id = 'task2',
      bash_command = 'echo test_group'
    )
    task3 = EmptyOperator(task_id = 'task3')
    
    task1 >> task2 >> task3 

  start >> group1 >> end 