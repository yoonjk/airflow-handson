from airflow import DAG  
from airflow.operators.empty import EmptyOperator 
from airflow.operators.bash import BashOperator 
from airflow.utils.task_group import TaskGroup 

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
  dag_id = 'two-taskgroup',
  default_args = default_args,
  tags = ['training']
) as dag: 
  start = EmptyOperator(task_id = 'start')
  end = EmptyOperator(task_id = 'end') 
  
  with TaskGroup(group_id = 'group1', tooltip = 'Tasks for group1') as group1: 
    task1 = EmptyOperator(task_id = 'task1')
    task2 = BashOperator(task_id = 'task2', bash_command = "echo task2 of group1")
    task3 = EmptyOperator(task_id = 'task3')
    
    task1 >> [task2, task3]
  with TaskGroup(group_id = 'group2', tooltip='Tasks for group2') as group2:
    task2_1 = BashOperator(task_id = 'task2_1', bash_command = "echo task2_1 of group2")
    task2_5 = BashOperator(task_id = 'task2_5', bash_command = "echo task2_5 of group2")
    with TaskGroup(group_id = 'inner_group2', tooltip='Tasks for inner group of group2') as inner_group2:
      task2_2 = EmptyOperator(task_id = 'task2_2')
      task2_3 = BashOperator(task_id = 'task2_3', bash_command = "echo task2_2 of group2")
      task2_4 = EmptyOperator(task_id = 'task2_4') 

      [task2_2, task2_3] >> task2_4
    task2_1 >> inner_group2 >> task2_5 
  start >> group1 >> group2 >> end 
