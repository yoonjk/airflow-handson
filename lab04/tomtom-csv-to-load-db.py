import argparse 
import os 
import csv 

from datetime import timedelta, datetime
from model import Connection, Tomtom 
import config 

def get_yeserday_date(fetch_date):
  return datetime.strptime(fetch_date, '%Y-%m-%d').date() - timedelta(1) 

def get_file_path(fetch_date):
  yesterday = get_yeserday_date(fetch_date) 
  filename = "tomtom_{}.csv".format(yesterday) 
  
  return os.path.join(config.CSV_FILE_DIR, filename)

def main(fetch_date, db_connection):
  yesterday = get_yeserday_date(fetch_date)
  filename = get_file_path(fetch_date) 
  data_insert = []
  
  with open(filename, encoding='utf-8',) as csvf: 
    csv_reader = csv.DictReader(csvf) 
    for row in csv_reader:
      tomtom_data = Tomtom(timestamp=row['timestamp'],
                              data_time=row['data_time'],
                              traffic_index=row['traffic_index'],
                              jams_count=row['jams_count'],
                              jams_length = row['jams_length'],
                              jams_delay = row['jams_delay'],
                              traffic_index_weekago = row['traffic_index_weekago'],
                              weekday=row['weekday']          
                    )
      data_insert.append(tomtom_data) 
      
  connection = Connection(db_connection) 
  session = connection.get_session()
  session.execute("delete from tomtom where date_time >= timestamp '{} 00:00:00' and datetime < timestamp'{} 00:00:00'".format(yesterday, fetch_date))
  session.bulk_save_objects(data_insert)
  session.commit()
  session.close()
  
if __name__ == '__main__':
  parser = argparse.ArgumentParser() 
  parser.add_argument("--date", required=True, type=str) 
  parser.add_argument("--connection", required=True, type=str)
  args = parser.parse_args() 
  main(args.date, args.connection)
  
  