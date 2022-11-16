from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.dummy import DummyOperator 
from airflow.operators.bash import BashOperator 
from airflow.utils.dates import days_ago

dag = DAG(dag_id="data_pipeline_ex03", 
  start_date=days_ago(1),
  schedule_interval='@daily',
  catchup=False
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

test_bash = BashOperator(
    task_id="test_bash",
    bash_command = "echo 'This is the ds: \'$msg\''",
    env = { "msg": '{{ dag_run.conf.ds | d("ds not found!") }}'}
)

start >> test_bash >> end 