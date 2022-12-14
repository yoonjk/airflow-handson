from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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

def _my_func(path, filename):
  print(f'{path}, {filename}')

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

  t3 = PythonOperator(
    task_id = 'func_args',
    python_callable = _my_func,
    op_kwargs = {
      'path' : '{{ var.value.path }}',
      'filename': '{{ var.value.filename }}'
    }
  )

  t4 = PythonOperator(
    task_id = 'deserialize',
    python_callable = _my_func,
    op_kwargs = Variable.get('myFile', deserialize_json = True)
  )
  
  t1 >> t2 >> t3 >> t4