from airflow import DAG 
from airflow.operators.python import PythonOperator 

# Date utils
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'tags': 'training',
  'catchup': False
}
dag = DAG(
  dag_id='data_pipeline_func_multi_args',
  default_args = default_args
)

def test_func(arg1, arg2, execution_date):
  print(arg1)
  print(arg2)
  print('execution_date=>', execution_date)

test_task = PythonOperator(
  task_id=f'test_task',
  python_callable=test_func, 
  op_kwargs={
    'arg1': 'hello',
    'arg2': 'airflow!',
    'execution_date': '{{ds_nodash}}'
  },
  provide_context=True,
  dag=dag
)


