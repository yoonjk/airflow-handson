from airflow import DAG 
from airflow.decorators import task_group, task
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

with DAG (
  dag_id = 'taskgroup3',
  default_args=default_args
) as dag: 
  @task
  def start():
    pass
  
  @task
  def end():
    pass

  @task_group(group_id = 'group1')
  def func_group():
    @task
    def task1():
      pass
  
    @task
    def task2():
      pass 
    task1() >> task2()
  group1 = func_group()  
  start() >> group1 >> end()
  

  