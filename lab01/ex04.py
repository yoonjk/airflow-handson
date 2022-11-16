from airflow.models import DAG 
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("date_pipeline_ex02", 
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

  