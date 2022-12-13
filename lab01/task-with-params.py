# template 을 이용하여 paramter를 갖는 task
from airflow import DAG 
from airflow.operators.empty import EmptyOperator 
from airflow.operators.bash import BashOperator 
# utils
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
  dag_id='task-with-params', 
  default_args = default_args,
  tags = ['training']
) as dag:

  start = EmptyOperator(task_id="start")
  end = EmptyOperator(task_id="end")
  
  bash_task = BashOperator(
      task_id="test_bash",
      bash_command = "echo 'This is the ds: \'$msg\''",
      env = { "msg": '{{ dag_run.conf.ds | d(ds) }}'}
  )
  
  start >> bash_task >> end 