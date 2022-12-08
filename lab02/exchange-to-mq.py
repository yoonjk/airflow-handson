
import json
from airflow.decorators import dag, task 
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 

import requests 
import pandas as pd #한국 수출입은행 환율 API 발급 
import json 


default_args = {
    "start_date": days_ago(1),
    "catchup": False
}

params = {
  'authkey': 'API-Key',
  'searchdate': '20220810',
  'data': 'AP01'
}

queue_manager = pymqi.connect('PRACTICUM', 'PRACTICUM.SVRCONN', '10.100.1.20(31414)')



url ='https://www.koreaexim.go.kr/site/program/financial/exchangeJSON'

@dag(
    dag_id = 'exchange-daily',
    default_args = default_args
)
def example_etl():

    @task 
    def extract():
        res = requests.get(url, params)

        if res.status_code == 200:
          json_data = res.json()
        #print(json_data)
          df = pd.json_normalize(json_data)

        df.drop(columns = ['result', 'bkpr', 'yy_efee_r', 'ten_dd_efee_r', 'kftc_bkpr','kftc_deal_bas_r'] , axis=1, inplace=True)
        df['base_dt'] = '20221130'

        print(df)
        jsonData = df.to_json(orient = 'records')
        print(jsonData)


        #return order_data_dict 
        return jsonData

    @task
    def transform(data_dict: dict):
        total_order_value=0
        print('transform')
        # print(data_dict)

        q = pymqi.Queue(queue_manager, 'PRACTICUM.LQ')
        for record in data_dict.items():
            print (record)
        #q.put('airflow test message')
        
    

    order_data = extract()
    transform(order_data)
    # load(order_summary['total_order_value'])

etl_dag = example_etl()
