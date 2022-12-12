from airflow import DAG
from airflow.operators.bash import BashOperator
# Utils
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models.variable import Variable

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'tags': ['training'],
  'catchup': False
}

common_user = Variable.get("common_user")
apikey = Variable.get("apikey")

with DAG(
  dag_id = 'varialbes-test',
  default_args=default_args,
  tags = ['training']
) as dag:
  t1 = BashOperator(
    task_id='variable_get',
    bash_command='echo {a_user} , {apikey}'.format(a_user=common_user, apikey = apikey),
  )

  t2 = BashOperator(
    task_id='variable_jinja',
    bash_command='echo {{ var.value.common_user }} / {{ var.value.apikey }}',
  )

  t1 >> t2