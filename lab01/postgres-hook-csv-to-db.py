from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook 
# utils
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

def load_csv_to_db(sql):
  pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)

  logging.info('Importing file to db:{}'.format(sql))
  pg_hook.copy_expert(sql, filename='/opt/airflow/data/customer.csv')

with DAG(
  dag_id = 'postgres-hook-csv-to-db',
  default_args = default_args,
  tags=['training']
) as dag: 

  start = EmptyOperator(task_id='start')
  end = EmptyOperator(task_id='end') 
  load_task = PythonOperator(
    task_id = 'load-task',
    python_callable=load_csv_to_db,
    op_kwargs = {
      'sql': "COPY customer from STDIN WITH CSV HEADER"
    }
  )
  
  start >> load_task >> end 

  