
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

baseDate = '20221118'
params = {
  'authkey': 'API-Key',
  'searchdate': baseDate,
  'data': 'AP01'
}
with DAG (       
    dag_id = 'exchange-daily-http',
    default_args = default_args
) as dag:

    def extract():
        url ='https://www.koreaexim.go.kr/site/program/financial/exchangeJSON'
        res = requests.get(url, params)

        if res.status_code == 200:
          json_data = res.json()
          #print(json_data)
          df = pd.json_normalize(json_data)
        
          df.drop(columns = ['result', 'bkpr', 'yy_efee_r', 'ten_dd_efee_r', 'kftc_bkpr','kftc_deal_bas_r', 'cur_nm'] , axis=1, inplace=True)
          df['base_dt'] = baseDate
          df['cur_nm'] = ' ' 
        
          print(df)
        
          df.rename(columns = {'deal_bas_r' : 'dealBasR', 'cur_nm': 'curNm', 'cur_unit': 'curUnit', 'base_dt': 'baseDt'}, inplace = True)
          jsonData = df.to_json(orient = 'records')
          print(jsonData)

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
