from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook 

from airflow.utils.dates import days_ago 
from datetime import timedelta 

import logging 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

POSTGRES_CONN_ID ='postgres-conn'

def export_db_to_csv(sql):
  pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
  logging.info('Exporting query to file:{}'.format(sql))
  pg_hook.copy_expert(sql, filename='/opt/airflow/data/customer.csv')
  
with DAG(
  dag_id = 'postgres-hook',
  default_args = default_args,
  tags=['training']
) as dag: 
  t1 = PythonOperator(
    task_id = 'extract-task',
    python_callable=export_db_to_csv,
    op_kwargs = {
      'sql': "COPY (SELECT * FROM CUSTOMER WHERE first_name = 'john' ) TO STDOUT WITH CSV HEADER"
    }
  )
  
  t1 