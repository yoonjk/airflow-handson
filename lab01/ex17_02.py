from airflow import DAG 
from airflow.operators.python import PythonOperator 
from airflow.sensors.external_task import ExternalTaskSensor

# Utils
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta  
import logging 

logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

default_args = {
  'start_date': days_ago(1), 
  'retries': 1,
  'schedule_interval': '*/5 * * * *',
  'catchup': False
}

dag = DAG(
  dag_id = 'seconddag',
  default_args = default_args,
  tags = ['training']
)
def hello():
  print('Second Dependency Task')
  
external_task = ExternalTaskSensor(
  task_id = 'exteranl_task',
  external_dag_id = 'firstdag',
  external_task_id = 'first_task',
  execution_delta=timedelta(minutes=5),
  mode="reschedule",
  timeout=600, 
  dag = dag
)

external_task 