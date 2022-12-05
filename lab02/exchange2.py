
import json
from airflow.decorators import dag, task 
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta 

import requests 
import pandas as pd #한국 수출입은행 환율 API 발급 
import json 


default_args = {
  'start_date': days_ago(1),
  'retries': 1,
  'retry_delay' : timedelta(minutes=5),
  'catchup': False
}

params = {
  'authkey': 'API-Key',
  'searchdate': '20220810',
  'data': 'AP01'
}


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
        df.rename(columns = {'deal_bas_r' : 'dealBasR', 'cur_nm': 'curNm', 'cur_unit': 'curUnit', 'base_dt': 'baseDt'}, inplace = True)
 
        print(df)
        jsonData = df.to_json(orient = 'records')
        print(jsonData)


        #return order_data_dict 
        return jsonData

    @task
    def transform(data_dict: dict):
      params = dict()
      params["baseDate"] = '20221130'

      headers = {'content-type': 'application/json'}
      url = 'http://192.168.219.111:8090/exchange/bulkload/{{baseDate}}'
      jsonData = json.dumps(data_dict)
      res = requests.post(url,  params = params, data = jsonData, headers = headers)
      print('transform')
  

    order_data = extract()
    transform(order_data)
    # load(order_summary['total_order_value'])

etl_dag = example_etl()
