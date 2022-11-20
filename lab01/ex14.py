from airflow import DAG  
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Date utils
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 


default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay' : timedelta(minutes=5),
  'schedule_interval': 'None',
  'catchup': False
}

dag=DAG(
  dag_id='callee_dag',
  default_args = default_args 
)

start=EmptyOperator(
  task_id='start',
  dag=dag
)

end=EmptyOperator(
  task_id='end',
  dag=dag
)

def hello(**kwargs):
  task_params=kwargs['dag_run'].conf['message']
  print('Hello world a with {}'.format(task_params))
  
t1 = PythonOperator(
  task_id = 'hello_task',
  python_callable=hello,
  provide_context=True,
  dag=dag
)

start >> t1 >> end 