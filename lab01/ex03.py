from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.empty import EmptyOperator 
from airflow.operators.bash import BashOperator 
from airflow.utils.dates import days_ago

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'tags': ['training'],
  'catchup': False
}

dag = DAG(dag_id="task-with-params", 
  start_date=days_ago(1),
  default_args = default_args 
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

bash_task = BashOperator(
    task_id="test_bash",
    bash_command = "echo 'This is the ds: \'$msg\''",
    env = { "msg": '{{ dag_run.conf.ds | d(ds) }}'}
)

start >> bash_task >> end 