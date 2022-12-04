import requests 
import pandas as pd #한국 수출입은행 환율 API 발급 
import json 


#인증키 발급하기 
#https://www.koreaexim.go.kr/ir/HPHKIR020M01?apino=2&viewtype=C#tab1 
baseDate = '20221203'
params = {
  'id': baseDate
}



url ='https://apic-gw-gateway-practicum.apps.labs.ihost.com/org1/staging/daily-exchange-rate/exchangerate/{id}'.format(**params)
res = requests.get(url, verify=False)

if res.status_code == 200:
  json_data = res.json()
  #print(json_data)
  df = pd.json_normalize(json_data)

  #df.drop(columns = ['result', 'bkpr', 'yy_efee_r', 'ten_dd_efee_r', 'kftc_bkpr','kftc_deal_bas_r'] , axis=1, inplace=True)
  #df['base_dt'] = '20221130'

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

  print(jsonData)
  params["baseDate"] = baseDate
  headers = {'content-type': 'application/json'};
  url = 'http://exchange-practicum.apps.labs.ihost.com/exchange/bulkload/{{baseDate}}'
  res = requests.post(url,  params = params, data =jsonData, headers = headers)
  
  print(res)
 










  