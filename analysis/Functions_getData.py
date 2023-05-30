import os,sys
sys.path.insert(0,'..')

import requests
import json
from datetime import datetime, timezone, timedelta, date
import os
from time import time
from backend.constants.constants import ACCESS_TOKEN
from time import sleep
import pytz


def getData():
  PRODUCTION = True
  t1 = time()

  if PRODUCTION:
      print('PRODUCTION ENVIRONMENT')
      ENDPOINT = 'https://algocrypto.eu'
      DAYS=0.3
      path_dir = '/home/alberto/Docker/Trading/analysis/json'
  else:
      print('DEVELOPMENT ENVIRONMENT')
      path_dir = '/home/alberto/Docker/Trading/analysis/json_test'
      ENDPOINT = 'http://localhost'
      DAYS=0.4
      
      
  new_datetime_start = datetime.now(timezone.utc)
  print('Script started at: ', new_datetime_start)

  method = '/analysis/get-data/'
  method_most_traded_coins = '/analysis/get-mosttradedcoins'
  datetime_start = datetime(2023,5,7)
  datetime_start = pytz.utc.localize(datetime_start)

  datetime_start_iso = datetime_start.isoformat()


  headers = {
          "accept": "application/json",
          "Authorization": f"Bearer {ACCESS_TOKEN}", 
          "Content-Type": "application/json"
      }

  # get MOST_TRADED_COINS.json froms server
  url_mosttradedcoins = ENDPOINT + method_most_traded_coins
  response = requests.get(url_mosttradedcoins)
  print(f'StatusCode for getting most_traded_coins: {response.status_code}')
  most_traded_coins = response.json()
  path_json_mostradedcoins = "/home/alberto/Docker/Trading/tracker/json/most_traded_coins.json"

  # save MOST_TRADED_COINS.json
  with open(path_json_mostradedcoins, 'w') as outfile:
      outfile.write(most_traded_coins)
      print(f'new Json saved in {path_json_mostradedcoins}')

  # START GATHERING ALL TRACKER DATA

  list_json = os.listdir(path_dir)
  # if data.json already exists, get saved data
  if len(list_json) != 0:
      
      # get all full paths in json directory
      full_path = [path_dir + "/{0}".format(x) for x in list_json]
      print(full_path)
      # get the most recent json
      most_recent_file = max(full_path, key=os.path.getctime)
      print(f'Most recent file is {most_recent_file}')

      # get datetime from name json file
      print(most_recent_file)

      f = open (most_recent_file, "r")
      data = json.loads(f.read())
      datetime_start_iso = data['datetime_creation']
      path_json = most_recent_file

      datetime_start = datetime.fromisoformat(datetime_start_iso)
      datetime_end_iso = (datetime_start + timedelta(days=DAYS)).isoformat()

      
  #if data.json does not exists, initialize data variable
  else:
      t = datetime.now()
      year = str(t.year)
      month = str(t.month)
      day = str(t.day)
      second = str(t.second)
      minute = str(t.minute)
      hour = str(t.hour)
      path_json = f'{path_dir}/data-{day}-{month}-{year}-{hour}{minute}{second}.json'
      date_split = datetime_start_iso.split('T')
      date = date_split[0]
      hour = date_split[1].split('.')[0]
      datetime_end_iso = (datetime_start + timedelta(days=DAYS)).isoformat()
      data = {'datetime_creation': datetime_start_iso, 'data': {}}
      

  print(f'This is the path_json variable: {path_json}')

  # START RETRIEVING DATA FROM SERVER
  while (new_datetime_start - datetime_start).days >= 0:

      while datetime.now().second < 0 and datetime.now().second > 10:
          sleep(0.9)
      
      # GET DATETIME FOR LOGGING
      days_timedelta_iteration = (new_datetime_start - datetime_start).days
      date_split = datetime_start_iso.split('T')
      date = date_split[0]
      hour = date_split[1].split('.')[0]

      
      # PREPARE AND MAKE NEW REQUEST TO SERVER
      params = {'datetime_start': datetime_start_iso, 'datetime_end': datetime_end_iso}
      url = ENDPOINT + method
      now_iso = datetime.now(timezone.utc).isoformat().split('.')[0]
      print(f'{now_iso} Making the request to {url}')
      print(f'Starting to query from: {date} {hour}')
      response = requests.get(url, headers=headers, params=params)
      #response = requests.get(url, params=params)
      new_instrument_data=0
      

      # START UPDATING "DATA" (I.E. ADD ALL THE NEW OBSERVATIONS)
      if response.status_code == 200:
          new_data = response.json()

          # UPDATE DATA
          for instrument_name in new_data:
              if instrument_name in data['data']:
                  for trade in new_data[instrument_name]:
                      data['data'][instrument_name].append(trade)
              else:
                  print(f'new instrument_name: {instrument_name}')
                  new_instrument_data += 1
                  data['data'][instrument_name] = []
                  for trade in new_data[instrument_name]:
                      data['data'][instrument_name].append(trade)

          # UPDATE DATETIME_CREATION
          if days_timedelta_iteration == 0:
              datetime_creation = (datetime.fromisoformat(data['data']['BTCUSDT'][-1]['_id']) + timedelta(seconds=10))
              datetime_creation = (pytz.utc.localize(datetime_creation)).isoformat()
          else:
              datetime_creation = datetime_end_iso
          data['datetime_creation'] = datetime_creation
          
          # PRINT SOME INFO
          print(f'Iterationg through new data is completed.')
          if new_instrument_data != 0:
              print(f'{new_instrument_data} of the instrument fetched are NEW')
          else:
              print("No New Instrument was fetched from the request")
          btc_obs = len(new_data['BTCUSDT'])
          count_coins = sum([1 for coin in list(new_data.keys()) if len(new_data[coin]) != 0])
          print(f'{btc_obs} new observations for {count_coins} coins')
          

          # SAVE UPDATED DATA TO PATH
          json_string = json.dumps(data)
          with open(path_json, 'w') as outfile:
              outfile.write(json_string)
          print(f'new Json saved in {path_json}')

          # check size of file, if it is too great than reinitialize path_json and data
          if os.path.getsize(path_json) > 1400000000:
              t = datetime.now()
              year = str(t.year)
              month = str(t.month)
              day = str(t.day)
              second = str(t.second)
              minute = str(t.minute)
              hour = str(t.hour)
              path_json = f'{path_dir}/data-{day}-{month}-{year}-{hour}{minute}{second}.json'
              print(F'NEW JSON INITIALIZED WITH PATH {path_json}')
              data = {'datetime_creation': datetime_start_iso, 'data': {}}




          # UPDATE PARAMETERS FOR MAKING THE NEXT REQUEST
          datetime_start = datetime_start + timedelta(days=DAYS)
          datetime_end = datetime_start + timedelta(days=DAYS)
          datetime_start_iso = datetime_start.isoformat()
          datetime_end_iso = datetime_end.isoformat()
          new_datetime_start = datetime.now(timezone.utc)
          print('')

      else:
          print('SOMETHING WRONG HAPPENED: ', response.status_code)
          print()
          break

      
  t2 = time()
  t = t2-t1
  print(f'Time Spent: {t} ')


  # Clear the global namespace
  for var in list(globals()):
      if var.startswith('__') and var.endswith('__'):
          continue  # Skip built-in variables
      del globals()[var]