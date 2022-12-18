from airflow import DAG 
from airflow.operators.empty import EmptyOperator
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
  dag_id = 'taskgroup-decorator2',
  default_args=default_args
) as dag: 
  @task
  def start():
    pass
  
  @task
  def end():
    pass

  @task_group(group_id = 'section_1')
  def func_group1():
    @task
    def task_1():
      pass
  
    @task
    def task_2():
      pass 

    @task
    def task_3():
      pass 
    task_1() >> [task_2(), task_3()]
    
  @task_group(group_id = 'section_2')
  def func_group2():
    @task
    def task_1():
      print('task1 of group2')

    @task_group(group_id = 'inner_section_2')
    def func_inner_group2():
      
      task2 = EmptyOperator(task_id = 'task_2')
      task3 = EmptyOperator(task_id = 'task_3')
      task4 = EmptyOperator(task_id = 'task_4')
      
      return [task2,task3] >> task4

    inner_section_2 = func_inner_group2()
    task_1() >> inner_section_2
    
  section_1 = func_group1()  
  section_2 = func_group2() 
  start() >> section_1 >> section_2 >> end()
  

  