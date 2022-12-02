import requests 
import pandas as pd #한국 수출입은행 환율 API 발급 
import json 
#인증키 발급하기 
#https://www.koreaexim.go.kr/ir/HPHKIR020M01?apino=2&viewtype=C#tab1 

baseDate = '20221201'

params = {
  'authkey': 'xGTRDgpwNc1hmKTp2KLB5mTpnNg9Ibil',
  'searchdate': baseDate,
  'data': 'AP01'
}

url ='https://www.koreaexim.go.kr/site/program/financial/exchangeJSON'
res = requests.get(url, params)

if res.status_code == 200:
  json_data = res.json()
  #print(json_data)
  df = pd.json_normalize(json_data)

  df.drop(columns = ['result', 'bkpr', 'yy_efee_r', 'ten_dd_efee_r', 'kftc_bkpr','kftc_deal_bas_r'] , axis=1, inplace=True)
  df['base_dt'] = baseDate


  print(df)

  df.rename(columns = {'deal_bas_r' : 'dealBasR', 'cur_nm': 'curNm', 'cur_unit': 'curUnit', 'base_dt': 'baseDt'}, inplace = True)
  jsonData = df.to_json(orient = 'records')
  print(jsonData)


params["baseDate"] = baseDate
headers = {'content-type': 'application/json'};
url = 'http://exchange-practicum.apps.labs.ihost.com/exchange/bulkload/{{baseDate}}'
res = requests.post(url,  params = params, data =jsonData, headers = headers)
  
print(res)

  


# exchange_rate_summary = pd.DataFrame(json_data)   
# print (exchange_rate_summary)
#컬럼정보 
#result             조회 결과
#cur_unit           통화코드 
#ttb                전신환(송금)받으실때 
#tts                전신환(송금)보내실때 
#deal_bas_r         매매 기준율 
#bkpr               장부가격 
#yy_efee_r          년환가료율 
#ten_dd_efee_r      10일환가료율 
#kftc_bkpr          서울외국환중개 매매기준율 
#kftc_deal_bas_r    서울외국환중개 장부가격 
#cur_nm             국가/통화명