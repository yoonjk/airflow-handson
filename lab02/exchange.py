
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
  'retry_delay': timedelta(minutes=5),
  'catchup': False,
  'schedule_interval': '@dialy'
}


@dag(
    dag_id = 'exchange-daily',
    default_args = default_args
)
def example_etl():

    @task 
    def extract():
        params = {
          'authkey': 'API-Key',
          'searchdate': '20220810',
          'data': 'AP01'
        }
        
        url ='https://www.koreaexim.go.kr/site/program/financial/exchangeJSON'
        res = requests.get(url, params)


        if res.status_code == 200:
          json_data = res.json()
          #print(json_data)
          df = pd.json_normalize(json_data)
        
          df.drop(columns = ['result', 'bkpr', 'yy_efee_r', 'ten_dd_efee_r', 'kftc_bkpr','kftc_deal_bas_r', 'cur_nm'] , axis=1, inplace=True)
          df['base_dt'] = '20221130'
          df['cur_nm'] = ' ' 
        
          print(df)
        
          df.rename(columns = {'deal_bas_r' : 'dealBasR', 'cur_nm': 'curNm', 'cur_unit': 'curUnit', 'base_dt': 'baseDt'}, inplace = True)
          jsonData = df.to_json(orient = 'records')
          print(jsonData)

          return jsonData

    @task
    def transform(data_dict: dict):
        total_order_value=0
        print('transform')
        # print(data_dict)
        params = dict()
        params["baseDate"] = '20221130'
        
        reqData = json.dumps(data_dict,ensure_ascii=False).encode('utf8')
 
        headers = {'Content-Type': 'application/json'}
        url = 'http://9.194.103.219:8090/exchange/bulkload/{{baseDate}}'
        # url = 'http://9.194.103.219:8090/exchange/bulkload'
        print(url, reqData)    
        res = requests.post(url, params = params, data =reqData, headers = headers)
        print (res)
    

    order_data = extract()
    transform(order_data)
    # load(order_summary['total_order_value'])

etl_dag = example_etl()
