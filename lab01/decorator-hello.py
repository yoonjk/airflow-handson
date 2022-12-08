from airflow.decorators import task 
from airflow import DAG  

# Utils
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
with DAG (
  dag_id = 'Hello',
  default_args = default_args
) as dag: 
  @task() 
  def get_name():
    return {
      'firstName' : 'kildong',
      'lastName' : 'Hong'
    }
  @task()
  def intro(param) -> None:
    print (param)  
    
  intro(get_name())