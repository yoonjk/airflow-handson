from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.operators.mysql import MySqlHook

from airflow.utils.dates import days_ago 
from datetime import timedelta 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

def insert_db_hook():
  ## MySqlHook으로 Conncetion 정보 Get
  db = MySqlHook(mysql_conn_id='mysql-conn')

  ## Cursor 사용을 위한 Conn 생성
  conn = db.get_conn()
    
  ## Cursor 선언 - Cursor를 통해서 Python 반복문을 통한 DB 대량 작업이 가능
  ## 아래 부분은 동일
  cur = conn.cursor()

  sql="""
      INSERT INTO customer (first_name, last_name, email)
      VALUES ("kildong", "Hong", "test1@gmail.com");
      """

  cur.execute(sql)
  conn.commit()

with DAG(
  dag_id = 'mysql-hook',
  default_args = default_args,
  tags=['training']
) as dag: 
  start = EmptyOperator(task_id='start')
  end = EmptyOperator(task_id='finish')
  # Create Mysql Table
  creating_table = MySqlOperator(
      task_id='creating_table',
      mysql_conn_id='mysql-conn',
      sql = '''
          CREATE TABLE IF NOT EXISTS customer (
              id serial,
              first_name VARCHAR(50),
              last_name VARCHAR(50),
              email VARCHAR(50)
          );
          '''
  )
  
  # Insert DB Record
  insert_db_python = PythonOperator(
      task_id='insert_db_python',
      python_callable=insert_db_hook
  )

  start >> creating_table >> insert_db_python >> end