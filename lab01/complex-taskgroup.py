from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from datetime import timedelta 
from airflow.utils.dates import days_ago 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

with DAG(
  dag_id = 'complex-taskgroup',
  default_args = default_args,
  description = "Task Group demo",
) as dag:

  start = DummyOperator(task_id="start")
  with TaskGroup(group_id='group1') as taskGroup1:
    task1_1 = DummyOperator(task_id="task1_1")
    task1_2 = DummyOperator(task_id="task1_2")
    with TaskGroup(group_id='group1_3') as taskGroup1_3:
      task_1_3_1 = DummyOperator(task_id="task1_3_1")
      task_1_3_2 = DummyOperator(task_id="task1_3_2")
      task_1_3_3 = DummyOperator(task_id="task1_3_3")
      task_1_3_4 = DummyOperator(task_id="task1_3_4")
      [task_1_3_1, task_1_3_2, task_1_3_3] >> task_1_3_4
    task1_1 >> task1_2 >> taskGroup1_3
  with TaskGroup(group_id='group2') as taskGroup2:
    task2_1 = DummyOperator(task_id="task2_1")
    with TaskGroup(group_id='group2_2') as group_2_2:
      task2_2_1 = DummyOperator(task_id="task2_2_1")
      task2_2_2 = DummyOperator(task_id="task2_2_2")
      
      task2_2_1 >> task2_2_2
    with TaskGroup(group_id='group2_3') as group_2_3:
      task2_3_1 = DummyOperator(task_id="task2_3_1")
      task2_3_2 = DummyOperator(task_id="task2_3_2")
      task2_3_3 = DummyOperator(task_id="task2_3_3")
      [task2_3_1, task2_3_2, task2_3_3]
    with TaskGroup(group_id='group2_4') as group_2_4:
      task2_4_1 = DummyOperator(task_id="task2_4_1")
      task2_4_2 = DummyOperator(task_id="task2_4_2")
      task2_4_3 = DummyOperator(task_id="task2_4_3")
      [task2_4_1, task2_4_2] >> task2_4_3
    with TaskGroup(group_id='group2_5') as group_2_5:
      task_2_5_1 = DummyOperator(task_id="task2_5_1")
      task_2_5_2 = DummyOperator(task_id="task2_5_2")
      
      [task_2_5_1, task_2_5_2]
    task2_1 >> group_2_2 >> group_2_3 >> group_2_4 >> group_2_5
  with TaskGroup(group_id='group3') as taskGroup3:
    task3_1 = DummyOperator(task_id='task3_1')
    task3_2 = DummyOperator(task_id='task3_2')
    task3_3 = DummyOperator(task_id='task3_3')
    task3_4 = DummyOperator(task_id='task3_4')
    [task3_1, task3_2, task3_3, task3_4]
  with TaskGroup(group_id='group4') as taskGroup4:
    task4_1 = DummyOperator(task_id='task4_1')
    task4_2 = DummyOperator(task_id='task4_2')
    task4_1 >> task4_2
  end = DummyOperator(task_id="end")
  start >> [taskGroup1, taskGroup2] >> taskGroup3 >> taskGroup4 >> end 