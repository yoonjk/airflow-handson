from airflow import DAG 
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago 
from datetime import datetime,timedelta 
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
  dag_id = 'firstdag',
  default_args = default_args,
  tags = ['training']
)

def hello():
  print('First Primary Taask')
  
first_task = PythonOperator(
  task_id = 'first_task', 
  python_callable=hello, 
  dag = dag
)



    