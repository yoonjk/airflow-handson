import json
from airflow.decorators import dag, task 
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 

default_args = {
    "start_date": days_ago(1),
    "catchup": False
}

@dag(
    dag_id = 'data_pipeline_ex06',
    default_args = default_args
)
def example_etl():

    @task 
    def extract():
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string) 

        return order_data_dict 

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        total_order_value=0
        
        for value in order_data_dict.values():
            total_order_value += value 
        
        return {"total_order_value": total_order_value}

    @task
    def load(total_order_value: float):
        print(f'Total order value is: {total_order_value: .2f}')

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary['total_order_value'])

etl_dag = example_etl()