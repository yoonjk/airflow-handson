from airflow.models import DAG 
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2022, 1, 2, tzinfo = kst),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
    'tags': 'training',
    'catchup': False
}

with DAG("with-dag", 
  start_date=datetime(2022,11,17),
  schedule_interval="@daily",
  default_args = default_args,
  catchup=False
) as dag:
  is_api_available = HttpSensor(
    task_id='is_api_available',
    http_conn_id='user_api',
    endpoint='api/'
  )

  is_api_available

  