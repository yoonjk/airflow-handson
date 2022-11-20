
from airflow import DAG 
from datetime import datetime, timedelta 
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago 
import pendulum

# timezone 한국시간으로 변경
kst = pendulum.timezone("Asia/Seoul")

default_args = {
    "start_date": days_ago(1),
    "catchup": False,
}

dag = DAG(
    "myFirstDAG",
    default_args=default_args,
    schedule_interval="@daily"
)

def hello_airflow():
    print("Hello airflow")

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