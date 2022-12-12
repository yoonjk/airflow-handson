from airflow import DAG  
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule 
# Utils
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 
import uuid

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

create_table ='''
  CREATE TABLE users (
    customer_id INT NOT NULL, 
    timestamp TIMESTAMP NOT NULL, 
    user_id VARCHAR(50) NOT NULL
  )
'''

insert_row = 'insert into users (customer_id, timestamp, user_id) values(%s, %s, %s)'

with DAG(
  dag_id = 'postgres-op',
  default_args = default_args,
  tags = ['training']
) as dag: 
  create_table = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id = 'postgres-conn',
    sql = create_table 
  )

  insert_row = PostgresOperator(
    task_id = 'insert_row',
    postgres_conn_id = 'postgres-conn',
    sql = insert_row,
    trigger_rule=TriggerRule.ALL_DONE,
    parameters=(uuid.uuid4().int % 123456789, datetime.now(), uuid.uuid4().hex[:10])
  )
  
  create_table >> insert_row 