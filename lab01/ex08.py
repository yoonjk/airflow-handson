from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# Date Utils
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta 

default_args = {
  'start_date': days_ago(1),
  'catchup': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5)
}

dag = DAG(
  dag_id="data_pipeline_ex08",
  default_args=default_args
)

def task2_func(t):
  print('var:', t, type(t))

task1 = BashOperator(
  task_id='task1',
  bash_command='echo "{{ var.value.key1 }}"',
  dag=dag
)

task2 = python_task = PythonOperator(
    task_id="python_task",
    python_callable=task2_func,
    op_args=['{{ var.value.key1 }}'],
    dag=dag 
)

task1 >> task2
