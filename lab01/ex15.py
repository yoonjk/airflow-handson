# dag 파라메터
from airflow import DAG  
from airflow.operators.bash import BashOperator 


from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta  

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '*/5 * * * *',
  'catchup': False,
  'description': '샘플 dag'
}
dag = DAG(
  dag_id='data_pipeline_dag_params',
  default_args = default_args,
  params={"param1": "first_param"},
)



bash_task1 = BashOperator(
  task_id='bash_task',
  bash_command='echo bash_task: {{params.param1}}',
  dag = dag
)


