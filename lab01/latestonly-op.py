from airflow import DAG 
from airflow.operators.empty import EmptyOperator 
from airflow.operators.latest_only import LatestOnlyOperator 
from airflow.utils.trigger_rule import TriggerRule 

from datetime import timedelta 
from airflow.utils.dates import days_ago 

default_args = {
  'start_date' : days_ago(1), 
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'catchup': False
}

with DAG(
  dag_id = 'latestonly-op',
  default_args = default_args,
  tags = ['training']
) as dag: 
  latest_only = LatestOnlyOperator(task_id = 'latest_only')
  task1 = EmptyOperator(task_id = 'task1')
  task2 = EmptyOperator(task_id = 'task2')
  task3 = EmptyOperator(task_id = 'task3')
  task4 = EmptyOperator(task_id = 'task4') 
  
  latest_only >> task1 >> [task3, task4]
  task2 >> [task3, task4]
  
  