from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago

def print_execution_date(ds):
    print(ds)
ds = ''
start_date = datetime(2020, 11, 30)
default_args = {
  'start_date': start_date,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}
with DAG(
    dag_id='external-task-sensor', 
    default_args=default_args
) as dag:
    sensor = ExternalTaskSensor(
        task_id='wait_for_task_2',
        external_dag_id='external-task',
        external_task_id='Task_2',
        start_date=start_date,
        execution_date_fn=lambda x: x,
        mode='reschedule',
        timeout=3600,
    )
    task_3 = PythonOperator(
        dag=dag,
        task_id='Task_3',
        python_callable=print_execution_date,
        op_kwargs={'ds': ds},
    )
    sensor >> task_3