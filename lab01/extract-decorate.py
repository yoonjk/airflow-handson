from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import requests 

ENDPOINT = 'https://gorest.co.in/public/v2/posts'

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': '@daily',
  'tags': ['training'],
  'catchup': False
}

@dag(
    dag_id = 'extract-decorate',
    default_args=default_args
)
def example_dag():
    @task
    def extract():
        return requests.get(ENDPOINT).json()
    @task
    def transform(data):
        return {'no_records': len(data)}
    @task
    def load(data):
        print(
            f'No. of records fetched by {ENDPOINT}: {data["no_records"]}'
        )
    load(transform(extract()))
dag = example_dag()