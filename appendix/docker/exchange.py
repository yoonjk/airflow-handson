import requests 
import pandas as pd #한국 수출입은행 환율 API 발급 
import json 
#인증키 발급하기 
#https://www.koreaexim.go.kr/ir/HPHKIR020M01?apino=2&viewtype=C#tab1 

params = {
  'authkey': 'xGTRDgpwNc1hmKTp2KLB5mTpnNg9Ibil',
  'searchdate': '20220810',
  'data': 'AP01'
}

url ='https://www.koreaexim.go.kr/site/program/financial/exchangeJSON'
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
