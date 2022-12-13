# Trigger DAG를 사용하는 경우 Optional하게 원하는 데이터를 JSON 형식으로 전달할 수 있는데,
# 사용 방법은 이전 포스팅에서 설명한 Jinja template 방식
# Trigger DAG에는 아래와 같이 입력하도록 한다.
# { "ds": "today" }
#

from airflow import DAG 
from airflow.operators.bash import BashOperator 
# Utils 
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 
import pendulum 

# timezone 한국시간으로 변경
kst = pendulum.timezone("Asia/Seoul")

default_args = {
  'start_date': datetime(2022,12,2, tzinfo=kst) ,
  'retries': 1,
  'retry_delay': timedelta(minutes = 5),
  'scheduler_interval': '@daily',
  'catchup': False
}

dag = DAG(
  dag_id = 'dag-run',
  default_args = default_args,
  tags =  ['training']
)

templated_command = """
  echo "ds: {{ dag_run.conf.ds | d(ds) }}"
"""
t1 = BashOperator(
  task_id = 'dagrun_conf_task',
  bash_command = templated_command,
  dag = dag
)

t1