from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago

from datetime import timedelta 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'catchup': False
}

dag = DAG(
    dag_id = 'mysql-op',
    default_args=default_args,
    tags=['example'],
)

# [START howto_operator_mysql]
create_sql_query = """ CREATE TABLE employee(empid int, empname VARCHAR(25), salary int); """

create_table = MySqlOperator(
  task_id="CreateTable", 
  mysql_conn_id="mysql-conn", 
  sql=create_sql_query, 
  dag=dag
)

insert_sql_query = """ 
  INSERT INTO employee(empid, empname, salary) 
  VALUES(1,'VAMSHI',30000),(2,'chandu',50000); 
"""


# [END howto_operator_mysql]

# [START howto_operator_mysql_external_file]

insert_data = MySqlOperator(
  sql=insert_sql_query, 
  task_id="InsertData", 
  mysql_conn_id="mysql-conn", 
  dag=dag
)
# [END howto_operator_mysql_external_file]

create_table >> insert_data