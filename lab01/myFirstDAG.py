
from airflow import DAG 
from datetime import datetime, timedelta 
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago 
import pendulum

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
    'tags': 'training',
    'catchup': False
}

def hello_airflow():
    print("Hello airflow")
    
dag = DAG(
    dag_id = "myFirstDag",
    default_args=default_args,
    schedule_interval="@daily"
)

t1 = BashOperator(
    task_id="bash",
    bash_command="echo Hello airflow",
    dag=dag
)

t2 = PythonOperator(
    task_id="python",
    python_callable=hello_airflow,
    dag=dag
)

t1 >> t2

