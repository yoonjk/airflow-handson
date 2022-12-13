from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.operators.email import EmailOperator
# Utils 
from airflow.utils.dates import days_ago 
from datetime import timedelta 
from airflow.utils.helpers import cross_downstream 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

def start_task():
    print("task started")
      
with DAG(
  dag_id = 'email-op',
  default_args=default_args,
  tags = ['training'],
) as dag: 
  start_task = PythonOperator(
    task_id='start_task',
    python_callable=start_task,
  )
  
  sending_email = EmailOperator(
    task_id='sending_email',
    to='nexweb@naver.com',
    subject='Airflow Alert !!!',
    html_content="""<h1>Testing Email using Airflow date:{{ ds }} </h1>""",
    files=['/opt/airflow/data/customer.csv']
  )

  start_task >> sending_email 


