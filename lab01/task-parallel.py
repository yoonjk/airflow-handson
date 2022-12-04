from airflow import DAG 
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator

# Utils
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 
from textwrap import dedent 

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
    'tags': 'training',
    'catchup': False
}

dag = DAG(
  'task-parallel',
  default_args = default_args,
  description='Hello world'
)

def hello():
  print('Hello!')
  
def dump() -> None:
  print('Goodbye')  

t1 =  BashOperator(
    task_id='echo_hello',
    bash_command='echo "Hi from bash operator" {{ dag_run.conf.ds | d(ds) }} ',
    dag=dag
)

t2 = python_task = PythonOperator(
    task_id="python_task",
    python_callable=hello,
    dag=dag
)

t3 = BashOperator(
  task_id='templated',
  bash_command='echo "{{ params.my_param }}"',
  params={'my_param': 'Parameter I passed in'},
  dag=dag
)

t4 = PythonOperator(
  task_id="end", 
  python_callable=dump,
  dag=dag
)

t1 >> [t2, t3] >> t4
# t1.set_downstream(t2) 
# t3.set_upstream(t2)

