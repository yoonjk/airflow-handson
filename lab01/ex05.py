from airflow.decorators import dag, task 
from datetime import datetime, timedelta 
from airflow.utils.dates import days_ago 

default_args = {
    "start_date": days_ago(1),
    "catchup": False,
}

@dag (
    dag_id='data_pipeline_ex05',
       default_args = default_args,
       schedule_interval='@daily'
) 
def hello_dag():
    @task
    def hello_task1():
        print('Hello World')

    hello_task1()

# DAG 호출
dag = hello_dag()