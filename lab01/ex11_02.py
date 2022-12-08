from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Utils 
from datetime import datetime,timedelta 
from airflow.utils.dates import days_ago 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'tags': 'training',
  'catchup': False
} 

dag = DAG(
  dag_id='xcom_dag', 
  default_args = default_args 
) 

def xcom_push(**context):
  context['task_instance'].xcom_push(
    key='pushed_value',
    value='xcom_push_test_message!')

def pull_func(**context):
  value=context['ti'].xcom_pull(
    key='pushed_value', 
    task_ids='push_by_xcom'
  )
  print(value)

push_by_xcom = PythonOperator(
  task_id='push_by_xcom',
  python_callable=xcom_push, 
  dag=dag
)

pull_task1 = PythonOperator(
  task_id='pull_example1',
  python_callable=pull_func, 
  dag=dag
)

pull_task2 = BashOperator(
  task_id='pull_example2',
  bash_command='echo "{{ ti.xcom_pull(key="pushed_value") }}"', 
  dag=dag,
)

push_by_xcom >> pull_task1 >> pull_task2
