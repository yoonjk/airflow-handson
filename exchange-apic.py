
import json
from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 

import requests 
import pandas as pd #한국 수출입은행 환율 API 발급 
import json 

default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'catchup': False,
  'schedule_interval': '@dialy'
}

baseDate = '20221117'

params = {
  'id': baseDate
}

with DAG (       
    dag_id = 'exchange-apic-to-http',
    default_args = default_args
) as dag:

    def extract():
        url ='https://apic-gw-gateway-practicum.apps.labs.ihost.com/org1/staging/daily-exchange-rate/exchangerate/{id}'.format(**params)
        res = requests.get(url, verify=False)

        if res.status_code == 200:
          json_data = res.json()
          #print(json_data)

          df = pd.json_normalize(json_data)

          records = []
  
          for record in json_data['response']['body']['items']['item']:
             row = {"baseDt" : record['aplyBgnDt']['$'],
                    "curUnit": record['currSgn']['$'],
                    "dealBasR": record['fxrt']['$'],
                    "curNm": record['cntySgn']['$'],
                    "tts": " ",
                    "ttb": record['fxrt']['$'],
                   }
             records.append(row)
        
          df  = pd.DataFrame(records)
          jsonData = df.to_json(orient = 'records')

          return jsonData

    
    def transform(**context):
        jsonData = context['task_instance'].xcom_pull(task_ids='extract')
        total_order_value=0
        print('transform1')
        # print(data_dict)
        print(jsonData)
  
        print('2. transform')
        param = {
          'baseDate': baseDate
        }
        headers = {'Content-Type': 'application/json'}
        url = 'http://exchange-practicum.apps.labs.ihost.com/exchange/bulkload/{baseDate}'.format(**param)
          
        res = requests.post(url, params = params, data =jsonData, headers = headers)
        print (res)
    
    extract = PythonOperator(
      task_id = 'extract',
      python_callable = extract
    )

    transform = PythonOperator(
      task_id = 'transform',
      python_callable = transform,
      provide_context=True
    )
    
    extract >> transform
