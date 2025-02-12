import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from matplotlib import collections as matcoll
from datetime import datetime, timedelta
import requests
import os, sys
from time import sleep
import pytz
#from scipy.stats import pearsonr
import pandas as pd
from time import time
from multiprocessing import Pool, Manager
from copy import copy
from random import randint
import shutil
import re
from operator import itemgetter
from typing import Literal
from functools import reduce


ROOT_PATH = os.getcwd()
TargetVariable1 = Literal["mean", "max", "min"]


def round_(number, decimal):
    return float(format(number, f".{decimal}f"))

def create_event_keys(event_keys_path, list_minutes, analysis_name):

    with open(event_keys_path, 'r') as file:
        # Retrieve shared memory for JSON data and "start_interval"
        X = json.load(file)
    event_keys = []

    for xi in X:
        lvl = X[xi]['lvl']
        for time_interval in X[xi]['info']:
            for buy_vol in X[xi]['info'][time_interval]['buy_vol']:
                for vol in X[xi]['info'][time_interval]['vol']:
                    if lvl == None:
                        event_key = f'buy_vol_{time_interval}:{buy_vol}/vol_{time_interval}:{vol}/timeframe:{list_minutes}'
                    else:
                        event_key = f'buy_vol_{time_interval}:{buy_vol}/vol_{time_interval}:{vol}/timeframe:{list_minutes}/lvl:{lvl}'
                    event_keys.append(event_key)
    
    riskmanagement_json = {}
    for event_key in event_keys:
        riskmanagement_json[event_key] = []
    
    with open(f'{ROOT_PATH}/riskmanagement_json/{analysis_name}.json', 'w') as outfile:
        json.dump(riskmanagement_json, outfile)
    
    return event_keys

def has_structure(data):
  """
  Checks if the given data has the structure: 
  {key1: {key2: [elem1, elem2, ...]}} 

  Args:
    data: The data to be checked.

  Returns:
    True if the data has the specified structure, False otherwise.
  """
  try:
    # Check if data is a dictionary
    if not isinstance(data, dict):
      return False

    # Check if all values in the top-level dictionary are also dictionaries
    for key1, value1 in data.items():
      if not isinstance(value1, dict):
        return False

    # Check if all values in the nested dictionaries are lists
    for key1, value1 in data.items():
      for key2, value2 in value1.items():
        if not isinstance(value2, list):
          return False

    return True 

  except (IndexError, KeyError):
    return False

def get_order_book(initial_request):

    # check initial_request data structure
    is_initial_request_ok = has_structure(initial_request)
    # GET METADATA
    if is_initial_request_ok:
        path_order_book_metadata = f'{ROOT_PATH}/order_book/metadata.json'
        if os.path.exists(path_order_book_metadata):
            f = open(path_order_book_metadata, "r")
            metadata_order_book = json.loads(f.read())
        else:
            metadata_order_book = {}

        # Check if the order_book_event has been already downloaded and create request
        orderbook_events = 0
        request = {}
        to_update = False
        for event_key in initial_request:
            if event_key not in metadata_order_book:
                metadata_order_book[event_key] = {}
            for coin in initial_request[event_key]:
                if coin not in metadata_order_book[event_key]:
                    metadata_order_book[event_key][coin] = []
                for _id in initial_request[event_key][coin]:
                    # in case the order_book event has never been downloaded, insert in request
                    if _id not in metadata_order_book[event_key][coin] and datetime.fromisoformat(_id) > datetime(2025,1,10) and datetime.fromisoformat(_id) < datetime.now() - timedelta(days=1):
                        print
                        orderbook_events += 1
                        to_update = True
                        if event_key not in request:
                            request[event_key] = {}
                        if coin not in request[event_key]:
                            request[event_key][coin] = [_id]
                        else:
                            request[event_key][coin].append(_id)

        # make the request to the server
        if to_update:
            print(f'{orderbook_events} events for orderbook are going to be extracted in algocrypto.eu')
            url = "https://algocrypto.eu/analysis/get-order-book"
            #url = "http://localhost/analysis/get-order-book"
            response = requests.post(url=url, json = request)
            status_code = response.status_code

            # get the response, and update data_order_book_event_key and metadata
            if status_code == 200:
                response = json.loads(response.text)
                for event_key in response:
                    event_key_path = event_key.replace(":", "_").replace("/", "_")
                    path_order_book_event_key = f'{ROOT_PATH}/order_book/{event_key_path}.json'
                    if os.path.exists(path_order_book_event_key):
                        f = open(path_order_book_event_key, "r")
                        data_order_book_event_key = json.loads(f.read())
                    else:
                        data_order_book_event_key = {}
                    for coin in response[event_key]:
                        if coin not in data_order_book_event_key:
                            data_order_book_event_key[coin] = {}
                        for _id in response[event_key][coin]:
                            metadata_order_book[event_key][coin].append(_id)
                            data_order_book_event_key[coin][_id] = response[event_key][coin][_id]
                    # Save the data_order_book_event_key
                    with open(path_order_book_event_key, 'w') as outfile_data:
                        json.dump(data_order_book_event_key, outfile_data)

                # Save the metadata
                with open(path_order_book_metadata, 'w') as outfile_metadata:
                        json.dump(metadata_order_book, outfile_metadata, indent=4)
            else:
                print(f'Status Code: {status_code}, error received from server')
        else:
            print('Order Book Metadata is up to date')
        
        return metadata_order_book
    else:
        print(" data structure of the initial request is not correct ")
    
    

    
    







def get_volatility(dynamic_benchmark_info_coin, full_timestamp):
    '''
    This function outputs the volatility of the coin (timeframe: last 30 days) in a specific point in time
    '''
    # get timestamp of the previous day, since benchmark are updated each midnight
    correct_full_datetime = datetime.fromisoformat(full_timestamp) - timedelta(days=1)
    correct_full_timestamp = correct_full_datetime.isoformat()
    short_timestamp = correct_full_timestamp.split('T')[0]
    while short_timestamp not in dynamic_benchmark_info_coin:
        short_timestamp = ((correct_full_datetime - timedelta(days=1)).isoformat()).split('T')[0]
        correct_full_datetime = correct_full_datetime - timedelta(days=1)
        #print(short_timestamp)

    volatility =  int(dynamic_benchmark_info_coin[short_timestamp])
    return volatility

def get_volume_standings(volume_standings_full, full_timestamp, coin):
    '''
    This function outputs the volume standings of the coin (timeframe: last 30 days) in a specific point in time
    '''
    # get timestamp of the previous day, since benchmark are updated each midnight
    date = datetime.fromisoformat(full_timestamp).strftime("%Y-%m-%d")
    return volume_standings_full[date][coin]
    

def data_preparation(data, n_processes = 8):
    '''
    This function prepares the input for the function "wrap_analyze_events_multiprocessing", input "data".
    In particular, this function outputs a list of sliced_data that will be fed for multiprocessing analysis
    '''
    # CONSTANTS
    data_arguments = []
    coins_list = list(data.keys())
    coins_slices = []
    n_coins = len(data)
    step = n_coins // n_processes
    remainder = n_coins % n_processes
    remainder_coins = coins_list[-remainder:]
    slice_start = 0
    slice_end = slice_start + step

    for i in range(n_processes):
        data_i = {}
        for coin in coins_list[slice_start:slice_end]:
            data_i[coin] = data[coin]

        data_arguments.append(data_i)
        del data_i
        slice_start = slice_end
        slice_end += step
    
    # add the remaining coins to the data_arguments
    if remainder != 0:
        for coin, index in zip(remainder_coins, range(len(remainder_coins))):
            data_arguments[index][coin] = data[coin]
                           
    for data_argument, index in zip(data_arguments, range(len(data_arguments))):
        len_data_argument = len(data_argument)
        #print(f'Length of data argument {index}: {len_data_argument}')

    
    total_coins = 0
    if 'BTCUSDT' in data:
        start_interval = data['BTCUSDT'][0]['_id']
        end_interval = data['BTCUSDT'][-1]['_id']
    else:
        random_coin = list(data.keys())[0]
        start_interval = data[random_coin][0]['_id']
        end_interval = data[random_coin][-1]['_id']


    for slice_coins in data_arguments:
        total_coins += len(slice_coins)


    print(f'Data for {total_coins} coins are loaded from {start_interval} to {end_interval}')

    for var in list(locals()):
        if var.startswith('__') and var.endswith('__'):
            continue  # Skip built-in variables
        if var == "data_arguments":
            continue
        del locals()[var]
    return data_arguments

def retrieve_datetime_for_load_data(path, list_paths, index):
    '''
    this function is used from "load_data" to retrieve the datetimes between two close json in analysis/json
    these two datetimes represent the first timestamps in the list of observations
    '''
    # retrieve first datetime of list_json
    path_split = path.split('-')
    day = int(path_split[3])
    month = int(path_split[2])
    year = int(path_split[1])
    hour = int(path_split[4])
    minute = int(path_split[5].split('.')[0])
    if minute== 60:
        minute=59
    datetime1 = datetime(year=year, month=month, day=day, hour=hour, minute=minute)


    # retrieve first datetime of the next list_json
    next_path = list_paths[index + 1]
    next_path_split = next_path.split('-')
    day = int(next_path_split[3])
    month = int(next_path_split[2])
    year = int(next_path_split[1])
    hour = int(next_path_split[4])
    minute = int(next_path_split[5].split('.')[0])
    if minute== 60:
        minute=59
    datetime2 = datetime(year=year, month=month, day=day, hour=hour, minute=minute)

    return datetime1, datetime2

def updateData_for_load_data(data, path, most_traded_coin_list, start_interval, end_interval):
    print(f'Retrieving data from {path}')
    f = open(path, "r")
    
    temp_data_dict = json.loads(f.read())
    
    for coin in temp_data_dict['data']:
        if coin in most_traded_coin_list:
            if coin not in data:
                data[coin] = []
            for obs in temp_data_dict['data'][coin]:
                if datetime.fromisoformat(obs['_id']) >= start_interval and datetime.fromisoformat(obs['_id']) <= end_interval:
                    new_obs = {}
                    for field in list(obs.keys()):
                        if 'std' not in field and 'trd' not in field and '%' not in field:
                            new_obs[field] = obs[field]
                    data[coin].append(new_obs)
    
    for var in list(locals()):
        if var.startswith('__') and var.endswith('__'):
            continue  # Skip built-in variables
        if var == "data":
            continue
        del locals()[var]

    return data

def get_date_key(path):
    # Define a regular expression pattern to match the date part of the filename
    date_pattern = r"data-(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{1,2})"

    match = re.search(date_pattern, path)
    if match:
        year, month, day, hour, minute = map(int, match.groups())
        # Create a tuple to use for sorting (year, month, day)
        return (year, month, day, hour, minute)
    else:
        # Handle cases where the filename format doesn't match
        return ()
    
def load_data(start_interval=datetime(2023,5,7, tzinfo=pytz.UTC), end_interval=datetime.now(tz=pytz.UTC), filter_position=(0,500), coin=None):
    '''
    This functions loads all the data from "start_interval" until "end_interval".
    The data should be already download through "getData.ipynb" and stored in "analysis/json" path.
    '''
    
    # get the most traded coins from function "get_benchmark_info"
    start_coin = filter_position[0]
    end_coin = filter_position[1]
    benchmark_info, df_benchmark, volatility = get_benchmark_info()
    volume_info = []
    for coin in benchmark_info:
        volume_info.append({'coin': coin, 'volume_30': benchmark_info[coin]['volume_30_avg']})
    # sort the list by the volume_average of the last 30 days
    volume_info.sort(key=lambda x: x['volume_30'], reverse=True)
    most_traded_coin_list = [info['coin'] for info in volume_info[start_coin:end_coin]]

    del volume_info, df_benchmark

    # get all the json paths in "analysis/json"
    #path_dir = "/Volumes/PortableSSD/Alberto/Trading/Json/json_tracker_update/" #IN TRACKER_UPDATE YOU WILL FIND ORDER CONCENTRATION
    path_dir = "/Volumes/PortableSSD/Alberto/Trading/Json/json_tracker/"
    list_json = os.listdir(path_dir)
    #print(list_json)
    list_files = sorted(list_json, key=get_date_key)
    #print(list_files)
    list_paths_full = [file for file in list_files if file.startswith('data')]
    list_paths = [path_dir + file for file in list_paths_full]
    # for path_print in list_paths:
    #     print(path_print)

    STOP_LOADING = False

    data= {}
    for path, index in zip(list_paths, range(len(list_paths))):
        if STOP_LOADING:
            break
        # check if this is not the last json saved
        if list_paths.index(path) + 1 != len(list_paths):
            # retrieve first datetime of path and first datetime of the next path
            datetime1, datetime2 = retrieve_datetime_for_load_data(path, list_paths, index)
            # if start_interval is between these 2 datetimes, retrieve json
            if start_interval > datetime1 and start_interval < datetime2:

                print(path)
                data = updateData_for_load_data(data, path, most_traded_coin_list, start_interval, end_interval)
                # if end_interval terminates before datetime2, then loading is completed. break the loop
                if end_interval < datetime2:
                    break
                else:
                    # let's determine the new list_paths
                    list_paths = list_paths[index+1:]
                    # iterate through the new "list_paths" for loading the other json
                    for path, index2 in zip(list_paths, range(len(list_paths))):
        
                        data = updateData_for_load_data(data, path, most_traded_coin_list, start_interval, end_interval)

                        # if this is the last json than break the loop, the loading is completed
                        if list_paths.index(path) + 1 == len(list_paths):
                            STOP_LOADING = True
                            break
                        else:

                            datetime1, datetime2 = retrieve_datetime_for_load_data(path, list_paths, index2)
                            if end_interval > datetime1 and end_interval < datetime2:
                                STOP_LOADING = True
                                break
                        
            # go to the next path
            else:
                #print(f'Nothing to retrieve from {path}')
                continue

        
        else:
            data = updateData_for_load_data(data, path, most_traded_coin_list, start_interval, end_interval)

    for var in list(locals()):
        if var.startswith('__') and var.endswith('__'):
            continue  # Skip built-in variables
        if var == "data":
            continue
        del locals()[var]
    
    return data

def get_benchmark_info():
    '''
    this function queries the benchmark info from all the coins from the db on server
    from Analysis2023
    '''
    now = datetime.now(tz=pytz.UTC) - timedelta(days=1)
    
    year = now.year
    month = now.month
    day = now.day
    file = 'benchmark-' + str(day) + '-' + str(month) + '-' + str(year)
    full_path = ROOT_PATH + '/benchmark_json/' + file

    if os.path.exists(full_path):
        print(f'{full_path} exists. Loading the file...')
        f = open(full_path)
        benchmark_info = json.load(f)
    else:
        print(f'{full_path} does not exist. Making the request to the server..')
        ENDPOINT = 'https://algocrypto.eu'
        METHOD = '/analysis/get-benchmarkinfo'

        url_mosttradedcoins = ENDPOINT + METHOD
        response = requests.get(url_mosttradedcoins)
        print(f'StatusCode for getting get-benchmarkinfo: {response.status_code}')
        benchmark_info = response.json()
        with open(full_path, 'w') as outfile:
            json.dump(benchmark_info, outfile)

    # check if there is any 0 in "volume_30_avg"
    # Also let's count how volatility is distributed across all the coins
    volatility = {}
    for coin in benchmark_info:
        if benchmark_info[coin]['volume_30_avg'] == 0:
            benchmark_info[coin]['volume_30_avg'] = 1
            benchmark_info[coin]['volume_30_std'] = 1
        else:
            coin_volatility = str(int(benchmark_info[coin]['volume_30_std'] / benchmark_info[coin]['volume_30_avg']))
            if coin_volatility not in volatility:
                volatility[coin_volatility] = 1
            else:
                volatility[coin_volatility] += 1
            
    #benchmark_info = json.loads(benchmark_info)
    df = pd.DataFrame(benchmark_info).transpose()
    df.drop('volume_series', inplace=True, axis=1)

    # Modify DF
    st_dev_ON_mean_30 = df['volume_30_std'] / df['volume_30_avg']
    df.insert (2, "st_dev_ON_mean_30", st_dev_ON_mean_30)
    df = df.sort_values(by=['volume_30_avg'], ascending=False)

    for var in list(locals()):
        if var.startswith('__') and var.endswith('__'):
            continue  # Skip built-in variables
        if var == 'benchmark_info' and var == 'df':
            continue
        del locals()[var]

    return benchmark_info, df, volatility

def get_benchmark_info_deprecated():
    '''
    this function queries the benchmark info from all the coins from the db on server
    '''

    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    full_path = f"{ROOT_PATH}/benchmark_json/benchmark-30-12-2024"

    f = open(full_path)
    benchmark_info = json.load(f)

    # check if there is any 0 in "volume_30_avg"
    # Also let's count how volatility is distributed across all the coins
    volatility = {}
    for coin in benchmark_info:
        if benchmark_info[coin]['volume_30_avg'] == 0:
            benchmark_info[coin]['volume_30_avg'] = 1
            benchmark_info[coin]['volume_30_std'] = 1
        else:
            coin_volatility = str(int(benchmark_info[coin]['volume_30_std'] / benchmark_info[coin]['volume_30_avg']))
            if coin_volatility not in volatility:
                volatility[coin_volatility] = 1
            else:
                volatility[coin_volatility] += 1
            
    #benchmark_info = json.loads(benchmark_info)
    df = pd.DataFrame(benchmark_info).transpose()
    df.drop('volume_series', inplace=True, axis=1)

    # Modify DF
    st_dev_ON_mean_30 = df['volume_30_std'] / df['volume_30_avg']
    df.insert (2, "st_dev_ON_mean_30", st_dev_ON_mean_30)
    df = df.sort_values(by=['volume_30_avg'], ascending=False)

    for var in list(locals()):
        if var.startswith('__') and var.endswith('__'):
            continue  # Skip built-in variables
        if var == 'benchmark_info' and var == 'df':
            continue
        del locals()[var]

    return benchmark_info, df, volatility


def get_dynamic_volatility(benchmark_info):
    '''
    This functions processes from the output of def "get_benchmark_info" and delivers a timeseries (daily) for each coin of this equation: (volume_30_std / volume_30_avg)
    In other terms, this function computes dynamically the average of the last 30 days for each date available in "volume_series" from db_benchmark
    '''
    dynamic_volatility = {}
    days = 30

    for coin in benchmark_info:
        # get "volume series" for a coin, as per db_benchmark
        
        volume_series = benchmark_info[coin]['volume_series']

        # turn volume_series from an object of objects to a list of tuples. See the example below
        list_info = list(volume_series.items())



        # iterate through each tuple. 1st element is date, 2nd element is a list: [volume, std]
        for info_1day in list_info:
            volume_avg_list = []
            volume_std_list = []
            date = info_1day[0]
            position = list_info.index(info_1day)

            if position < days:
                # if there are "lte" (lower or equal than) "days" days available, let's get the last "position"s.
                # if position == 15 --> (0,15) observations
                range_ = range(position+1)
            else:
                # otherwise let's get the last "days" observations from position
                # if position == 45 --> (15,45) observations
                range_ = range(position + 1 - days, position + 1)


            for i in range_:
                '''
                example of list_info: LIST OF TUPLES:
                FOR EACH TUPLE: 
                FIRST ELEMENT IS "date"

                [('2023-05-17', [91.58, 463.58]),
                ('2023-05-18', [104.09, 517.24]),
                ('2023-05-19', [56.22, 270.68]),
                ('2023-05-20', [70.49, 431.15]),
                ('2023-05-21', [90.07, 552.42]),
                ('2023-05-22', [65.84, 309.63]),
                ('2023-05-23', [129.86, 782.03]),
                ('2023-05-24', [86.24, 326.74]),
                ('2023-05-25', [131.69, 1158.56])]
                '''

                # print(position)
                volume_avg_i = list_info[i][1][0]
                volume_std_i = list_info[i][1][1]
                volume_avg_list.append(volume_avg_i)
                volume_std_list.append(volume_std_i)
            
            # get the mean average for a specific date (mean of all the observations until "date")
            mean_one_date = round_(np.mean(volume_avg_list),2)
            std_one_date = round_(np.mean(volume_std_list),2)
            
            if coin not in dynamic_volatility:
                dynamic_volatility[coin] = {}
            
            #dynamic_volatility[coin][date] = {'vol_avg': mean_one_date, 'vol_std': std_one_date}
            
            # get percentage std over mean
            if mean_one_date != 0:
                dynamic_volatility[coin][date] = std_one_date / mean_one_date
            else:
                dynamic_volatility[coin][date] = 1


    for var in list(locals()):
        if var.startswith('__') and var.endswith('__'):
            continue  # Skip built-in variables
        if var == "dynamic_volatility":
            continue
        del locals()[var]


    return dynamic_volatility

def get_substring_between(original_string, start_substring, end_substring):
    start_index = original_string.find(start_substring)
    end_index = original_string.find(end_substring, start_index + len(start_substring))

    if start_index == -1 or end_index == -1:
        return None

    return original_string[start_index + len(start_substring):end_index]

def pooled_standard_deviation(stds, sample_size):

    pooled_std = np.sqrt(sum((sample_size-1) * std**2 for std in stds) / (len(stds) * (sample_size - 1)))
    return pooled_std

def load_analysis_json_info(analysis_json_path, analysis_timeframe = 7, INTEGRATION=False):
    
    if os.path.exists(analysis_json_path):
        with open(analysis_json_path, 'r') as file:
            print(f'Loading analysis_json in {analysis_json_path}')
            # Retrieve shared memory for JSON data and "start_interval"
            analysis_json = json.load(file)
            start_interval = analysis_json['start_next_analysis']
            del analysis_json
    else:
        # Create shared memory for JSON data and initialize "start_interval"
        start_interval = datetime(2023,5,11).isoformat()

    end_interval = min(datetime.now(), datetime.fromisoformat(start_interval) + timedelta(days=analysis_timeframe))

    print(f'start_interval at {start_interval}')
    print(f'end_interval at {end_interval}')
    return start_interval, end_interval

def updateAnalysisJson(shared_data_value, file_path, start_next_analysis, slice_i=None, start_next_analysis_str=None, INTEGRATION=False):

    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            # Retrieve shared memory for JSON data and "start_interval"
            analysis_json = json.load(file)
            print(f'analysis_json is loaded with file_path: {file_path}')
    else:
        analysis_json = {'data': {}}

    new_data = json.loads(shared_data_value)

    print('new data was jsonized')
    del shared_data_value

    for key in list(new_data.keys()):

        if key not in analysis_json['data']:
            analysis_json['data'][key] = {}

        if 'info' in analysis_json['data'][key]:

            # update complete "info"
            for coin in new_data[key]['info']:
                if coin not in analysis_json['data'][key]['info']:
                    analysis_json['data'][key]['info'][coin] = []
                for event in new_data[key]['info'][coin]:
                    analysis_json['data'][key]['info'][coin].append(event)
            
            del new_data[key]


        else:
            analysis_json['data'][key]['info'] = new_data[key]['info']

    del new_data
    
    if slice_i != None:
        if slice_i == 1:
            start_next_analysis_str_2 = 'start_next_analysis_2'
            if start_next_analysis_str_2 in analysis_json:
                json_to_save = {start_next_analysis_str_2: analysis_json[start_next_analysis_str_2], start_next_analysis_str : start_next_analysis, 'data': analysis_json['data']}
            else:
                json_to_save = {start_next_analysis_str_2: datetime(2023,6,7).isoformat(), start_next_analysis_str : start_next_analysis, 'data': analysis_json['data']}

        else:
            start_next_analysis_str_1 = 'start_next_analysis_1'
            json_to_save = {start_next_analysis_str_1: analysis_json[start_next_analysis_str_1], start_next_analysis_str : start_next_analysis, 'data': analysis_json['data']}
    else:
        if INTEGRATION:
            json_to_save = {'start_next_analysis' : analysis_json['start_next_analysis'], 'data': analysis_json['data'], 'start_next_analysis_integration': start_next_analysis}
        else:
            if "start_next_analysis_integration" in analysis_json:
                json_to_save = {'start_next_analysis' : start_next_analysis, 'data': analysis_json['data'], 'start_next_analysis_integration': analysis_json['start_next_analysis_integration']}
            else:
                json_to_save = {'start_next_analysis' : start_next_analysis, 'data': analysis_json['data']}


    with open(file_path, 'w') as file:
        json.dump(json_to_save, file)
        

def getsubstring_fromkey(text):
    '''
    This simple function returns the substrings for volume, buy_volume and timeframe from "key". 
    "key" is the label that defines an event. "key" is created in the function "wrap_analyze_events_multiprocessing"

    For example:
    from buy_vol_5m:0.65/vol_24h:8/timeframe:1440/vlty:1
    it returns:
    timeframe -> 1440
    buy_vol --> buy_vol_5m
    vol --> vol_24h
    buy_vol_value --> 0.65
    vol_value --> 8
    lvl --> if exists
    '''
    match = re.search(r'vol_(\d+m):(\d+(?:\.\d+)?)/vol_(\d+m):(\d+(?:\.\d+)?)/timeframe:(\d+)', text)
    if match:
        if 'lvl' in text:
            lvl = int(text.split('lvl:')[-1])
        else:
            lvl = None

        buy_vol = 'buy_vol_' + match.group(1)
        buy_vol_value = float(match.group(2))
        vol = 'vol_' + match.group(3)
        vol_value = int(match.group(4))
        timeframe = int(match.group(5))
    
    return vol, vol_value, buy_vol, buy_vol_value, timeframe, lvl


def sort_files_in_json_dir():
    # Directory containing the files
    directory_path = "/Users/albertorainieri/Projects/Personal/analysis/json_tracker_link/"

    # Define a regex pattern to extract the date
    date_pattern = r"data-(\d{2})-(\d{2})-(\d{4})-(\d{2})-(\d{2}).json"

    # List all files in the specified directory
    file_list = os.listdir(directory_path)

    # Create a list of tuples containing the file name and the extracted date
    file_date_tuples = []
    for file in file_list:
        match = re.search(date_pattern, file)
        if match:
            day, month, year, hour, minute = match.groups()
            file_date = (file, datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minute)))
            file_date_tuples.append(file_date)

    # Sort the list of tuples based on the date
    sorted_file_date_tuples = sorted(file_date_tuples, key=lambda x: x[1], reverse=True)

    # Extract the sorted file names
    sorted_file_name_tuple = [file_date[0] for file_date in sorted_file_date_tuples]

    return sorted_file_name_tuple

def update_optimized_results(optimized_results_path):

    f = open(optimized_results_path, "r")
    optimized_results_obj = json.loads(f.read())

    request = {}

    for event_key in optimized_results_obj:
        #initialize risk
        if event_key not in request:
            request[event_key] = {}
        # iterate trough list of key "events" and take position "i" for getting "coin"
        for event, i in zip(optimized_results_obj[event_key]['events'], range(len(optimized_results_obj[event_key]['events']))):
            coin = optimized_results_obj[event_key]['coin'][i]
            # initialize coin
            if coin not in request[event_key]:
                request[event_key][coin] = []
            # append start_timestamp
            request[event_key][coin].append(event)
    

    url = "https://algocrypto.eu/analysis/get-pricechanges"
    response = requests.post(url=url, json = request)
    response = json.loads(response.text)
    pricechanges = response['data']
    msg = response['msg']
    print(pricechanges)
    
    # msg shows all nan values replaced with different timeframes
    msg = response['msg']
    if len(msg) > 0:
        text = '\n'.join(msg)
        # Specify the file path where you want to save the text
        now_isoformat = datetime.now().isoformat().split('.')[0]
        file_path = ROOT_PATH + "/logs/" + now_isoformat + '-nanreplaced.txt'
        print('NaN values detected, check this path ', file_path)

        # Open the file for writing
        with open(file_path, "w") as file:
            # Write the text to the file
            file.write(text)

    # get info keys defined in "list_timeframe" in the route get-pricechanges
    random_event_key = list(pricechanges.keys())[0]
    random_coin = list(pricechanges[random_event_key].keys())[0]
    random_start_timestamp = list(pricechanges[random_event_key][random_coin].keys())[0]
    info_keys = list(pricechanges[random_event_key][random_coin][random_start_timestamp].keys())

    for event_key in optimized_results_obj:
        #initialize all info keys (price, vol and buy)

        for info_key in info_keys:
            timeframe = info_key.split('_')[-1] # example: "1h" or "3h" or ... "7d"
            price_key = 'price_%_' + timeframe
            vol_key = 'vol_' + timeframe
            buy_vol_key = 'buy_' + timeframe

            optimized_results_obj[event_key][price_key] = [None] * len(optimized_results_obj[event_key]['events'])
            optimized_results_obj[event_key][vol_key] = [None] * len(optimized_results_obj[event_key]['events'])
            optimized_results_obj[event_key][buy_vol_key] = [None] * len(optimized_results_obj[event_key]['events'])

        # iterate through each event, take position and coin        
        for event in optimized_results_obj[event_key]['events']:
            position = optimized_results_obj[event_key]['events'].index(event)
            coin = optimized_results_obj[event_key]['coin'][position]

            # finally fill the value for each info_key of each event
            for info_key in info_keys:
                timeframe = info_key.split('_')[-1] # example: "1h" or "3h" or ... "7d"
                price_key = 'price_%_' + timeframe
                vol_key = 'vol_' + timeframe
                buy_vol_key = 'buy_' + timeframe

                optimized_results_obj[event_key][price_key][position] = pricechanges[event_key][coin][event][info_key][0]
                optimized_results_obj[event_key][vol_key][position] = pricechanges[event_key][coin][event][info_key][1]
                optimized_results_obj[event_key][buy_vol_key][position] = pricechanges[event_key][coin][event][info_key][2]

    
    with open(optimized_results_path, 'w') as outfile:
            json.dump(optimized_results_obj, outfile)

    return optimized_results_obj

def getTimeseriesPaths(event_key_path):
    path = ROOT_PATH + "/timeseries_json/"
    timeseries_list = os.listdir(path)
    timeseries_paths = []

    if 'vlty' not in event_key_path:
        VOLATILITY_GROUP = True
    else:
        VOLATILITY_GROUP = False

    for timeseries_path in timeseries_list:
        if VOLATILITY_GROUP and 'vlty' in timeseries_path:
            continue
        
        if event_key_path in timeseries_path:
            timeseries_paths.append(path + timeseries_path)

    return timeseries_paths

def getnNewInfoForVolatilityGrouped(event_key, info):
    new_info = {}
    for key in info:
        if event_key in key:
            if event_key not in new_info:
                new_info[event_key] = {'info': info[key]['info']}

            else:
                
                for coin in info[key]['info']:
                    if coin not in new_info[event_key]['info']:
                        new_info[event_key]['info'][coin] = []
                    for event in info[key]['info'][coin]:
                        new_info[event_key]['info'][coin].append(event)
    
    return new_info





def load_timeseries(event_key_path):
    # FIND THE THE TIMESERIES json. some Timeseries json might be divided in PART<n> if the file is too big
    timeseries_paths = getTimeseriesPaths(event_key_path)
    timeseries_json = {}
    
    if len(timeseries_paths) == 1:
        #print(f'There is only one JSON associated with {event_key_path}')
        timeseries_json_path = timeseries_paths[0]
        with open(timeseries_json_path, 'r') as file:
            #print(f'Loading {timeseries_json_path}')
            timeseries_json = json.load(file)
    elif len(timeseries_paths) > 1:
        len_timeseries_json = len(timeseries_paths)
        #print(f'There are {len_timeseries_json} JSON associated with {event_key_path}')
        # Order the list based on PART numbers in ascending order
        ordered_files = sorted(timeseries_paths, key=lambda x: int(re.search(r'PART(\d+)', x).group(1)) if re.search(r'PART(\d+)', x) else float('inf'))
        i = 0
        for timeseries_path_PART in ordered_files:
            with open(timeseries_path_PART, 'r') as file:
                tmp_timeseries = json.load(file)
            
            for coin in tmp_timeseries:
                if coin not in timeseries_json:
                    timeseries_json[coin] = {}
                for start_timestamp in tmp_timeseries[coin]:
                    i+=1
                    timeseries_json[coin][start_timestamp] = tmp_timeseries[coin][start_timestamp]
            
            del tmp_timeseries
        #print(f'{i} events have been loaded')
    else:
        #print('Timeseries Json does not exist. Add code in this section for downloading the timeseries from local server or Set DISCOVER to True')
        return None
    
    #print('Timeseries has been downloaded')
    return timeseries_json

def extract_part_number(filename):
    match = re.search(r'PART(\d+)', filename)
    if match:
        return int(match.group(1))
    return -1

def sort_filenames(filenames):
    return sorted(filenames, key=extract_part_number)

def extract_date_time(iso_string):
  """
  Extracts 'yyyy-MM-ddTHH:MM' from an ISO 8601 formatted string.

  Args:
    iso_string: The ISO 8601 formatted string (e.g., "2025-01-14T09:36:08").

  Returns:
    A string in the format 'yyyy-MM-ddTHH:MM'.
  """
  try:
    dt = datetime.fromisoformat(iso_string)
    return dt.strftime('%Y-%m-%dT%H:%M')
  except ValueError:
    print(f"Invalid ISO 8601 format: {iso_string}")
    return None

def filter_request_with_orderbook_available(complete_info, metadata_order_book, event_key):
    request = {'info': {}}
    n_events = 0
    for coin in complete_info['info']:
        if coin not in metadata_order_book[event_key]:
            continue
        else:
            request['info'][coin] = []
        for event in complete_info['info'][coin]:
            timestamp_start = event['event']
            if timestamp_start in metadata_order_book[event_key][coin]:
                n_events += 1
                request['info'][coin].append(event)
    
    #print(f'{n_events} timeseries will be downloaded')
    if n_events != 0:
        return request
    else:
        return None
    
def get_timeseries_from_server(initial_request, check_past, check_future):
    # check initial_request data structure
    is_initial_request_ok = has_structure(initial_request)
    # GET METADATA
    if is_initial_request_ok:
        path_timeseries_metadata = f'{ROOT_PATH}/timeseries_json/metadata.json'
        if os.path.exists(path_timeseries_metadata):
            f = open(path_timeseries_metadata, "r")
            metadata_timeseries = json.loads(f.read())
        else:
            metadata_timeseries = {}
        
        # Check if the order_book_event has been already downloaded and create request
        orderbook_events = 0
        request_event_keys = {}
        to_update = False
        for event_key in initial_request:
            if event_key not in metadata_timeseries:
                metadata_timeseries[event_key] = {}
            for coin in initial_request[event_key]:
                if coin not in metadata_timeseries[event_key]:
                    metadata_timeseries[event_key][coin] = []
                for _id in initial_request[event_key][coin]:
                    # in case the timeseries event has never been downloaded, insert in request
                    if _id not in metadata_timeseries[event_key][coin] and datetime.fromisoformat(_id) > datetime(2025,1,10) and datetime.fromisoformat(_id) < datetime.now() - timedelta(days=1):
                        print
                        orderbook_events += 1
                        to_update = True
                        if event_key not in request_event_keys:
                            request_event_keys[event_key] = {}
                        if coin not in request_event_keys[event_key]:
                            request_event_keys[event_key][coin] = [_id]
                        else:
                            request_event_keys[event_key][coin].append(_id)
        
        # make the request to the server
        if to_update:
            for event_key in request_event_keys:
                print(event_key)
                request = {'info': {event_key: request_event_keys[event_key] }}
                request['check_past'] = check_past
                request['check_future'] = check_future
                vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = getsubstring_fromkey(event_key)
                request['timeframe'] = timeframe
                print(f'{orderbook_events} events for timeseries are going to be extracted in localhost')
                url = "http://localhost/analysis/get-timeseries"
                response = requests.post(url=url, json = request)
                status_code = response.status_code

                # get the response, and update data_timeseries_event_key and metadata
                if status_code == 200:
                    response = json.loads(response.text)
                    event_key_path = event_key.replace(":", "_").replace("/", "_")
                    path_timeseries_event_key = f'{ROOT_PATH}/timeseries_json/{event_key_path}.json'
                    if os.path.exists(path_timeseries_event_key):
                        f = open(path_timeseries_event_key, "r")
                        data_timeseries_event_key = json.loads(f.read())
                    else:
                        data_timeseries_event_key = {}
                    for coin in response['data']:
                        if coin not in data_timeseries_event_key:
                            data_timeseries_event_key[coin] = {}
                        for _id in response['data'][coin]:
                            metadata_timeseries[event_key][coin].append(_id)
                            data_timeseries_event_key[coin][_id] = response['data'][coin][_id]
                    # Save the data_timeseries_event_key
                    with open(path_timeseries_event_key, 'w') as outfile_data:
                        json.dump(data_timeseries_event_key, outfile_data)

                    # Save the metadata
                    with open(path_timeseries_metadata, 'w') as outfile_metadata:
                            json.dump(metadata_timeseries, outfile_metadata, indent=4)
                else:
                    print(f'Status Code: {status_code}, error received from server')
        else:
            print('Timeseries is up to date')

def get_full_timeseries(event_key, metadata_order_book):
    '''
    this function mixes orderbook and timeseries
    '''
    vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = getsubstring_fromkey(event_key)
    file_name = event_key.replace(':', '_').replace('/', '_')
    path_full_timeseries = ROOT_PATH + '/full_timeseries/' + file_name + '.json'
    path_timeseries = ROOT_PATH + '/timeseries_json/' + file_name + '.json'
    path_order_book = ROOT_PATH + '/order_book/' + file_name + '.json'
    path_full_timeseries_metadata = ROOT_PATH + '/full_timeseries/metadata.json'

    # check if order_book and full_timeseries metadata coincide
    if os.path.exists(path_full_timeseries_metadata):
        f = open(path_full_timeseries_metadata, "r")
        metadata_full_timeseries = json.loads(f.read())
    else:
        metadata_full_timeseries = {}
    
    # define which events are missing in full_timeseries
    create_full_timeseries_summary = {event_key: {}}
    if event_key not in metadata_full_timeseries:
        metadata_full_timeseries[event_key] = {}
    for coin in metadata_order_book[event_key]:
        create_full_timeseries_summary[event_key][coin] = []
        if coin not in metadata_full_timeseries[event_key]:
            metadata_full_timeseries[event_key][coin] = []
        for timestamp_start in metadata_order_book[event_key][coin]:
            if timestamp_start not in metadata_full_timeseries[event_key][coin]:
                metadata_full_timeseries[event_key][coin].append(timestamp_start)
                create_full_timeseries_summary[event_key][coin].append(timestamp_start)

    data_uploaded = False
    
    full_timeseries = {}
    
    for coin in create_full_timeseries_summary[event_key]:
        # check if there are full_timeseries to be created
        if len(create_full_timeseries_summary[event_key][coin]) != 0:
            if not data_uploaded:
                print(f'Starts updating Full Timeseries')
                f = open(path_timeseries, "r")
                timeseries_json = json.loads(f.read())
                f = open(path_order_book, "r")
                order_book_json = json.loads(f.read())
                if os.path.exists(path_full_timeseries):
                    f = open(path_full_timeseries, "r")
                    full_timeseries = json.loads(f.read())
                else:
                    full_timeseries = {}
                data_uploaded = True
            if coin not in full_timeseries:
                full_timeseries[coin] = {}

            for start_timestamp in create_full_timeseries_summary[event_key][coin]:
                if coin in order_book_json and len(order_book_json[coin][start_timestamp]) != 0:
                    timeseries = timeseries_json[coin][start_timestamp]['data']
                    order_book = order_book_json[coin][start_timestamp][0]['data']
                    ranking = order_book_json[coin][start_timestamp][0]['ranking']
                    full_timeseries[coin][start_timestamp] = {'data':{}, 'ranking': ranking}
                    for obs_timeseries in timeseries:
                        ts = extract_date_time(obs_timeseries['_id'])
                        data = [obs_timeseries['price'], obs_timeseries[vol_field], obs_timeseries[buy_vol_field], None, None, None, None]
                        full_timeseries[coin][start_timestamp]['data'][ts] = data
                    for obs_timestamp in order_book:
                        ts = extract_date_time(obs_timestamp)
                        price = order_book[obs_timestamp][0]
                        bid_volume = order_book[obs_timestamp][1]
                        ask_volume = order_book[obs_timestamp][2]
                        bid_orders = order_book[obs_timestamp][3]
                        ask_orders = order_book[obs_timestamp][4]

                        if ts not in full_timeseries[coin][start_timestamp]['data']:
                            full_timeseries[coin][start_timestamp]['data'][ts] = [None, None, None, None, None, None, None]

                        full_timeseries[coin][start_timestamp]['data'][ts][0] = price
                        full_timeseries[coin][start_timestamp]['data'][ts][3] = bid_volume
                        full_timeseries[coin][start_timestamp]['data'][ts][4] = ask_volume
                        full_timeseries[coin][start_timestamp]['data'][ts][5] = bid_orders
                        full_timeseries[coin][start_timestamp]['data'][ts][6] = ask_orders
        
    if not data_uploaded:
        print(f'Full Timeseries is up to date ')

    if len(full_timeseries) != 0:
        with open(path_full_timeseries, 'w') as file:
            json.dump(full_timeseries, file)
        with open(path_full_timeseries_metadata, 'w') as file:
            json.dump(metadata_full_timeseries, file)

def hit_jump_price_levels_range(current_price, dt, bid_price_levels, neighborhood_of_price_jump = 0.005, distance_jump_to_current_price=0.01):
    '''
    This function defines all the historical level whereas a jump price change was existent
    Since it can happen that price_jump_level are not always in the same point (price) I check if the jump price is in the neighboorhood of the historical jump price (average with np.mean)

    # THIS IS THE INPUT OF BID_PRICE_LEVELS
    Structure of bid price_levels: IT IS A LIST OF LISTS
    - bid_price_levels: e.g. [[13.978], [13.958], [13.958], [13.978], [13.949, 12.942], [13.97], [13.939, 12.933], [14.053]]
      EACH SUBLIST containes the jump prices at dt

    # THIS THE STRUCTURE OF SUMMARY_JUMP_PRICE
    [LIST [TUPLES]]
        [ (average price jump level, list_of_jump_price_levels )]
    '''



    summary_jump_price_level = {}
    # iterate throgh 
    for bid_price_levels_dt_info in bid_price_levels:
        dt = bid_price_levels_dt_info[1]
        bid_price_levels_dt = bid_price_levels_dt_info[0]

        for abs_price in bid_price_levels_dt:
            if abs_price == None:
                continue
            if len(summary_jump_price_level) != 0:
                IS_NEW_X = True
                for x in summary_jump_price_level:
                    historical_price_level = summary_jump_price_level[x][0]
                    historical_price_level_list = summary_jump_price_level[x][1]
                    #print(historical_price_level, abs_price)
                    if abs_price <= historical_price_level * (1 + neighborhood_of_price_jump) and abs_price >= historical_price_level * (1 - neighborhood_of_price_jump):
                        historical_price_level_list.append(abs_price)
                        historical_price_level = np.mean(historical_price_level_list)
                        summary_jump_price_level[x] = (historical_price_level, historical_price_level_list)
                        IS_NEW_X = False
                        break
                if IS_NEW_X:
                    list_x = [int(i) for i in list(summary_jump_price_level.keys())]
                    new_x = str(max(list_x) + 1)
                    summary_jump_price_level[new_x] = (abs_price, [abs_price])
            else:
                #print(abs_price)
                summary_jump_price_level['1'] = (abs_price, [abs_price])
            
    #print(summary_jump_price_level)
    for x in summary_jump_price_level:
        if abs((current_price - summary_jump_price_level[x][0] ) / current_price ) <= distance_jump_to_current_price:
            return True
            #print(current_price, dt)
    return False






def simulate_entry_position(price_list, list_datetime, start_datetime,
                             bid_price_levels, ask_order_distribution, price_change_jump,
                               price_drop_limit=0.05, distance_jump_to_current_price = 0.03, max_ask_order_distribution_level = 0.2):
    
    '''
    INPUT DESCRIPTION
    - bid_price_levels: e.g. [[13.978], [13.958], [13.958], [13.978], [13.949, 12.942], [13.97], [13.939, 12.933], [14.053]]
    - price_drop_limit: (PHASE 1) THRESHOLD OF price drop. if current price drop is higher than go to PHASE 2
    - distance_from_jump_levels: (PHASE 2) It is the DISTANCE between current price and historical jump levels
    - max_ask_order_distribution_level: (PHASE 3) LIMIT of the cumulative volume. if the nearest price ranges (0-2.5% - 2.5-5%) have a lower cumulative volume than here is the opportunity
    '''
    
    # get price_list, datetime_list from start_datetime
    #return
    dt = list_datetime[-1]
    ts = dt.isoformat()
    price = price_list[-1]
    position_start_datetime = list_datetime.index(start_datetime)
    price_list = price_list[position_start_datetime:]
    list_datetime = list_datetime[position_start_datetime:]

    # PHASE 1
    # DEFINE PRICE DROP (price drop from initial price or from max price)
    max_price = max(price_list)
    max_datetime = list_datetime[price_list.index(max_price)]
    current_timedelta_from_max = dt - max_datetime

    current_price_drop = abs( (price - max_price) / max_price )
    if current_price_drop >= price_drop_limit:
        #print(f'Price Drop at {ts} of {round_(current_price_drop,4)*100}%')
        
        # PHASE 2
        # DEFINE HOW CLOSE THE PRICE IS TO HISTORICAL JUMP LEVELS
        is_jump_price_level = hit_jump_price_levels_range(current_price=price, dt=dt, bid_price_levels=bid_price_levels,
                                                           distance_jump_to_current_price=distance_jump_to_current_price)

        if is_jump_price_level:
            if ask_order_distribution[str(price_change_jump)] < max_ask_order_distribution_level:
                buy_price = price
                dt_price = dt
                info_buy = (price, dt)
                print(price, dt, ask_order_distribution[str(price_change_jump)], ask_order_distribution[str(price_change_jump+price_change_jump)])
                return info_buy
    return None


    # DETERMINE THE CURRENT_ORDER_DISTRIBUTION ON THE ASK-LEVEL

    pass

def plot_timeseries(event_key, check_past, check_future, jump, limit, price_change_jump=0.025):
    '''
    INPUT DESCRITION
    "jump" is used to see there are jumps in the order book. E.g. from -5% and -9% (price change) there are not orders (THIS IS A JUMP)
    "limit" is the price window range, where order books are checked. if limit = 0.4, then only the order books within the 40% price range are analyzed
    "price_change_jump" is used for the orde
    '''

    vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = getsubstring_fromkey(event_key)
    file_name = event_key.replace(':', '_').replace('/', '_')
    path_full_timeseries = ROOT_PATH + '/full_timeseries/' + file_name + '.json'
    f = open(path_full_timeseries, "r")
    full_timeseries = json.loads(f.read())
    Colors = [['#0400ff', '#FF0000'],
          ['#09ff00', '#ff8c00']]
    
    dark_violet = "#9400D3"
    violet = '#EE82EE'
    red = '#FF0000' 
    light_red = '#FFB6C1'
    light_yellow = '#FFFFE0'
    white = '#FFFFFF'
    dark_orange_hex = "#E67A00"

    order_colors = [white, light_yellow, light_red, violet, dark_orange_hex, red, dark_violet]
    price_range_colors = [(0,0.025), (0.025, 0.05), (0.05, 0.1), (0.1, 0.15), (0.15, 0.2), (0.2, 0.3), (0.3, 1)]
    assert len(order_colors) == len(price_range_colors)
    order_distribution_color_legend = {}

    for color, price_range in zip(order_colors,price_range_colors):
        order_distribution_color_legend[price_range] = color

    for coin in full_timeseries:
        for start_timestamp in full_timeseries[coin]:
            start_datetime = datetime.fromisoformat(start_timestamp)
            start_datetime = start_datetime.replace(second=0).replace(microsecond=0)
            end_datetime = start_datetime + timedelta(minutes=int(timeframe))
            prestart_datetime = start_datetime - timedelta(minutes=check_past)
            postend_datetime = end_datetime + timedelta(minutes=check_future)

            data = full_timeseries[coin][start_timestamp]['data']
            list_ts = list(data.keys())
            price_list = []
            max_min_price_list = []
            vol_list = []
            buy_vol_list = []
            bid_volume_list = []
            ask_volume_list = []
            bid_price_levels = []
            ask_price_levels = []
            bid_ask_volume_list = []
            list_datetime = []
            bid_order_distribution_list = []
            ask_order_distribution_list = []
            bid_color_list = []
            ask_color_list = []
            bid_volume_wrt_total = []
            ask_volume_wrt_total = []
            previous_dt = datetime.fromisoformat(list_ts[0])
            
            fig, ax = plt.subplots(5, 1, sharex=True, figsize=(20, 10))
            info_buy = None
            SELL = False
            for ts in list_ts:
                dt = datetime.fromisoformat(ts)

                if dt < previous_dt:
                    continue
                else:
                    previous_dt = dt

                if dt < prestart_datetime or dt > postend_datetime:
                    continue

                list_datetime.append(dt)
                price = data[ts][0]
                price_list.append(price)
                vol_list.append(data[ts][1])
                buy_vol_list.append(data[ts][2])

                if start_datetime == dt.replace(second=0):
                    initial_price = price
                
                if dt <= end_datetime and dt >= start_datetime:
                    max_min_price_list.append(price)
                    max_price = max(max_min_price_list)
                    min_price = min(max_min_price_list)

                bid_orders = data[ts][5]
                ask_orders = data[ts][6]
                if bid_orders != None:
                    bid_price_level, bid_order_distribution, bid_cumulative_level = get_price_levels(price, bid_orders, jump, limit, price_change_jump)
                    ask_price_level, ask_order_distribution, ask_cumulative_level = get_price_levels(price, ask_orders, jump, limit, price_change_jump)
                    #print(bid_order_distribution)

                    # bid/ask_actual_jump can be a number or False, it checks if there a was a jump, otherwise just the get the cumulative volume at the limit price change
                    bid_actual_jump = bid_price_level[0][3]
                    ask_actual_jump = ask_price_level[0][3]

                    # for each jump level, just retrieve the absolute price
                    bid_price_levels_dt = [] #all bid price levels at dt
                    ask_price_levels_dt = [] #all ask price levels at dt
                    for lvl in bid_price_level:
                        bid_price_levels_dt.append(lvl[0])
                    for lvl in ask_price_level:
                        ask_price_levels_dt.append(lvl[0])
                    bid_price_levels.append((bid_price_levels_dt, dt)) #this is a list of lists. each sublist contains all prices where a jump has occurred 
                    ask_price_levels.append((ask_price_levels_dt, dt))

                    # plot the jump prices
                    if bid_actual_jump:
                        for lvl_bid in bid_price_levels_dt:
                            ax[0].plot(dt, lvl_bid, 'go', markersize=1)
                            ax[1].plot(dt, lvl_bid, 'go', markersize=1)
                    if ask_actual_jump:
                        for lvl_ask in ask_price_levels_dt:
                            ax[0].plot(dt, lvl_ask, 'ro', markersize=1)
                            ax[1].plot(dt, lvl_ask, 'go', markersize=1)

                    # plot the order book distribution
                    if len(bid_order_distribution) != 0:
                        for lvl_bid in bid_order_distribution:
                            for lvl_col in order_distribution_color_legend:
                                if bid_order_distribution[lvl_bid] >= lvl_col[0] and bid_order_distribution[lvl_bid] < lvl_col[1]:
                                    order_distribution_dt = [(mdates.date2num(dt), price * (1-float(lvl_bid)+price_change_jump)), (mdates.date2num(dt), price * (1-float(lvl_bid)))]
                                    bid_order_distribution_list.append(order_distribution_dt)
                                    bid_color_list.append(order_distribution_color_legend[lvl_col])
                                    break
                        for lvl_ask in ask_order_distribution:
                            for lvl_col in order_distribution_color_legend:
                                if ask_order_distribution[lvl_ask] >= lvl_col[0] and ask_order_distribution[lvl_ask] < lvl_col[1]:
                                    order_distribution_dt = [(mdates.date2num(dt), price * (1+float(lvl_ask)-price_change_jump)), (mdates.date2num(dt), price * (1+float(lvl_ask)))]
                                    ask_order_distribution_list.append(order_distribution_dt)
                                    ask_color_list.append(order_distribution_color_legend[lvl_col])
                                    break

                    # this is the cumulative level (from 0 to 100) based on $limit
                    # Basically I want to get the total bid/volume with respect to the limit
                    
                    bid = data[ts][3] * bid_cumulative_level
                    ask = data[ts][4] * ask_cumulative_level
                    bid_volume_list.append(bid)
                    ask_volume_list.append(ask)
                    bid_volume_wrt_total.append(bid_cumulative_level)
                    ask_volume_wrt_total.append(ask_cumulative_level)
                    bid_ask_volume_list.append(bid + ask)
                else:
                    bid_volume_list.append(0)
                    ask_volume_list.append(0)
                    bid_ask_volume_list.append(0)
                    bid_volume_wrt_total.append(0)
                    ask_volume_wrt_total.append(0)
                
                if dt > start_datetime and dt < end_datetime and info_buy == None:
                    info_buy = simulate_entry_position(price_list, list_datetime, start_datetime, bid_price_levels, ask_order_distribution, price_change_jump)
                elif info_buy != None and dt >= end_datetime and not SELL:
                    SELL = True
                    sell_price = price
                    buy_price = info_buy[0]
                    gain = round_((( sell_price - buy_price ) / buy_price ) * 100,2)
                    print(f'Gain: {gain} - buy-price = {buy_price} - sell-price = {sell_price}')
            
            #print(bid_price_levels)
            mean_price = np.mean(price_list)
            max_change = round_((max_price - initial_price)*100 / initial_price, 2)
            min_change = round_((min_price - initial_price)*100 / initial_price, 2)

            dt_max_price = list_datetime[price_list.index(max_price)]
            dt_min_price = list_datetime[price_list.index(min_price)]


            ax[0].plot(list_datetime, price_list, linewidth=0.5, color='black')
            ax[0].axvline(x=start_datetime, color='blue', linestyle='--')
            ax[0].axvline(x=end_datetime, color='blue', linestyle='--')
            ax[0].set_ylabel('Price')
            ax[0].grid(True)
            ax[0].annotate(f'+{max_change}%', xy=(dt_max_price, max_price),
                                xytext=(dt_max_price, max_price*(1-((max_change/100)))),
                                textcoords='data', ha='center', va='top',arrowprops=dict(arrowstyle='->'))
            ax[0].annotate(f'-{min_change}%', xy=(dt_min_price, min_price),
                                xytext=(dt_min_price, min_price*(1-((max_change/100)))),
                                textcoords='data', ha='center', va='top',arrowprops=dict(arrowstyle='->'))

            title = f'{coin} -- {start_timestamp} -- Event_key: {event_key} -- Initial Price: {initial_price} - Max: {max_change}% - Min: {min_change}%'
            print(title)
            ax[0].set_title(title)
            y0_min, y0_max = ax[0].get_ylim()

            if len(bid_order_distribution_list) > 0:
                #create legend
                for color, price_range_color in zip(order_colors, price_range_colors):
                    label = '> ' + str(price_range_color[0]*100) + '%'
                    ax[1].plot([], [], color=color, label=label)
                
                ax[1].legend(loc='upper right')

                for lvl in np.arange(price_change_jump,limit+price_change_jump,price_change_jump*2):
                    ax[1].annotate(f'-{round_(lvl*100,1)}%', xy=(start_datetime, initial_price*(1-lvl)),
                                    xytext=(start_datetime- timedelta(hours=1), initial_price*(1-lvl)),
                                    textcoords='data', ha='center', va='top',arrowprops=dict(arrowstyle='->'))
                    ax[1].annotate(f'+{round_(lvl*100,1)}%', xy=(start_datetime, initial_price*(1+lvl)),
                                    xytext=(start_datetime- timedelta(hours=1), initial_price*(1+lvl)),
                                    textcoords='data', ha='center', va='top',arrowprops=dict(arrowstyle='->'))
                bid_linecoll = matcoll.LineCollection(bid_order_distribution_list, colors=bid_color_list, zorder=0)
                ask_linecoll = matcoll.LineCollection(ask_order_distribution_list, colors=ask_color_list, zorder=0)
                ax[1].add_collection(bid_linecoll)
                ax[1].add_collection(ask_linecoll)
                ax[1].plot(list_datetime, price_list, linewidth=1.5, color='grey')
                ax[1].axvline(x=start_datetime, color='blue', linestyle='--')
                ax[1].axvline(x=end_datetime, color='blue', linestyle='--')
                ax[1].set_ylabel('Price')
                ax[1].grid(True, zorder=2)
                ax[1].set_ylim(y0_min, y0_max)


            ax[2].plot(list_datetime, bid_ask_volume_list, color='red', linewidth=0.5, alpha=0.8)
            ax[2].fill_between(list_datetime, bid_ask_volume_list, bid_volume_list, alpha=0.3, color='red', label='Area Between')
            ax[2].plot(list_datetime, bid_volume_list, color='green', linewidth=0.5, alpha=0.8)
            ax[2].fill_between(list_datetime, bid_volume_list, 0, alpha=0.3, color='green', label='Area Under 2') # Fill to zero
            ax[2].axvline(x=start_datetime, color='blue', linestyle='--')
            ax[2].axvline(x=end_datetime, color='blue', linestyle='--')
            ax[2].set_ylabel(f'Bid-Ask Abs Vol ({limit*100}%')
            ax[2].grid(True)

            # consider the cumulative price different from zero
            list_mean_bid_volume_wrt_total = []
            list_mean_ask_volume_wrt_total = []
            for dt,bid,ask in zip(list_datetime, bid_volume_wrt_total, ask_volume_wrt_total):
                if bid != 0:
                    ax[3].plot(dt, bid, 'go', markersize=1)
                    list_mean_bid_volume_wrt_total.append(bid)
                if ask != 0:
                    ax[3].plot(dt, ask, 'ro', markersize=1)
                    list_mean_ask_volume_wrt_total.append(ask)
            
            mean_bid_volume_wrt_total = round_(np.mean(list_mean_bid_volume_wrt_total)*100,2)
            mean_ask_volume_wrt_total = round_(np.mean(list_mean_ask_volume_wrt_total)*100,2)

            ax[3].axvline(x=start_datetime, color='blue', linestyle='--')
            ax[3].axvline(x=end_datetime, color='blue', linestyle='--')
            ax[3].set_ylabel(f'Bid-Ask Rel {limit*100}%')
            ax[3].grid(True)
            ax[3].annotate(f'avg {mean_bid_volume_wrt_total}%', xy=(end_datetime, mean_bid_volume_wrt_total/100),
                    xytext=(end_datetime+timedelta(hours=2), mean_bid_volume_wrt_total/100),
                    textcoords='data', ha='center', va='top',arrowprops=dict(arrowstyle='->'))
            ax[3].annotate(f'avg {mean_ask_volume_wrt_total}%', xy=(end_datetime, mean_ask_volume_wrt_total/100),
                    xytext=(end_datetime+timedelta(hours=2), mean_ask_volume_wrt_total/100),
                    textcoords='data', ha='center', va='top',arrowprops=dict(arrowstyle='->'))


            ax[4].plot(list_datetime, vol_list, linewidth=0.5)
            ax[4].axvline(x=datetime.fromisoformat(start_timestamp), color='blue', linestyle='--')
            ax[4].axvline(x=end_datetime, color='blue', linestyle='--')
            ax[4].set_ylabel('Volume')
            ax[4].grid(True)

            plt.show()
            plt.close()
            
    



            

def get_timeseries(info, check_past=1440, check_future=1440, jump=0.03, limit=0.15, event_keys_filter = [], plot=False):
    '''
    check_past: minutes before event trigger
    check_future: minutes after the end of event (usually after 1 days from event trigger)
    jump: jump from price levels in terms of cumulative volume order (from 0 to 1)
    limit: get the window of price change (from 0 to 1) (e.g. 0.15 check only the orders whose level is within 15% price change from current price)
    '''

    n_event_keys = len(info)
    for event_key, event_key_i in zip(info, range(1,n_event_keys+1)):
        if event_keys_filter != [] and event_key not in event_keys_filter:
            continue
        print('')
        print('')
        print('')
        print('#####################################################################')
        print(f'{event_key_i}/{n_event_keys} Event Key: {event_key}')
        print('#####################################################################')
        
        request_order_book = {event_key: {}}
        for coin in info[event_key]['info']:
            request_order_book[event_key][coin] = []
            for event in info[event_key]['info'][coin]:
                request_order_book[event_key][coin].append(event['event'])

        # get full_timeseries
        # metadata_full_timeseries, full_timeseries = get_full_timeseries(request_order_book, plot)        

        # Download all data (order_book and timeseries)
        metadata_order_book = get_order_book(request_order_book)
        # get all timeseries (even when orderbook no available)
        get_timeseries_from_server(request_order_book, check_past, check_future)
        # get fulltimeseries (mix of orderbook and timeseries)
        get_full_timeseries(event_key, metadata_order_book)

        if plot:
            plot_timeseries(event_key, check_past, check_future, jump, limit)
            
    



def getTimeseries(info, check_past=False, look_for_newdata=False, plot=False):
    '''
    This function retrieves the timeseries based on "info" and "key"
    "info" is the output of function "download_show_output" and key is the name of event list (e.g. "buy_vol_5m:0.65/vol_24h:8/timeframe:1440/vlty:1")
    It downloads the data from server if not exists. otherwise the data is downloaded from "/timeseries_json"

    if "check_past" is not False, it is an Integer. It is used to retrieve all the observations occurred before the Event. It is expressed in minutes.
    "look_for_newdata" is a boolean. if True, it looks for NEW timeseries (triggered by an event) in the db server.
    '''
    n_event_keys = len(info)
    for key, key_i in zip(info, range(1,n_event_keys+1)):
        print(f'{key_i}/{n_event_keys} Event Key: {key}')
        msg = f'{key_i}/{n_event_keys} - {key}'
        request_order_book = {key: {}}
        for coin in info[key]['info']:
            request_order_book[key][coin] = []
            for event in info[key]['info'][coin]:
                request_order_book[key][coin].append(event['event'])
                
        metadata_order_book = get_order_book(request_order_book)
        with open(ROOT_PATH + "/timeseries_json/ + metadata.json", 'r') as file:
            metadata_timeseries = json.load(file)

        # load from local or from server
        key_json = key.replace(':', '_')
        key_json = key_json.replace('/', '_')
        url = 'http://localhost/analysis/get-timeseries'

        path = ROOT_PATH + "/timeseries_json/"
        timeseries_list = os.listdir(path)
        timeseries_key = []

        for timeseries_path in timeseries_list:
            if key_json in timeseries_path:
                timeseries_key.append(path + timeseries_path)
        
        if len(timeseries_key) == 1:
            file_path = timeseries_key[0]
            PATH_EXISTS = True
        elif len(timeseries_key) == 0:
            file_path = path + key_json + '.json'
            PATH_EXISTS = False

        vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = getsubstring_fromkey(key)
        fields = [vol_field, buy_vol_field, timeframe, buy_vol_value, vol_value, lvl]

        if PATH_EXISTS:
            timeseries = load_timeseries(key_json)

            if look_for_newdata:
                request = info[key]
                request = filter_request_with_orderbook_available(request, metadata_order_book,  key)
                if request == None:
                    continue
                request['timeframe'] = int(timeframe)
                if not check_past:
                    pass
                else:
                    request['check_past'] = check_past
                
                request['last_timestamp'] = {}
                # define for each from which timestamp new events should be discovered
                for coin in timeseries:
                    #get most recent timestamp
                    timestamp_list = list(timeseries[coin].keys())
                    # Convert the ISO format timestamps to datetime objects
                    datetime_list = [datetime.fromisoformat(timestamp) for timestamp in timestamp_list]
                    # Find the most recent timestamp using the min function
                    most_recent_timestamp = max(datetime_list).isoformat()
                    request['last_timestamp'][coin] = most_recent_timestamp


                # send request
                print('Sending the request for get-timeseries')
                response = requests.post(url, json = request)
                print('Status Code is : ', response.status_code)
                response = json.loads(response.text)
                new_timeseries = response['data']
                msg = response['msg']
                retry = response['retry']
                print(msg)

                # if file size is greater than 800MB, lets create a new one
                if os.path.getsize(file_path) > 800000000:
                    # check if the current file part has already PART substring. 
                    # In this case thre is not, rename the path with PART substring
                    if '_PART' not in file_path:
                        file_path1 = path + key_json + '_PART1' '.json'
                        os.rename(file_path, file_path1)
                        max_part_number = 1
                    else:
                        file_path_split = file_path.split('PART')
                        max_part_number = int(file_path_split[1][0])

                    # DEFINE THE NEW PATH JSON
                    new_part_number = max_part_number + 1
                    part_string = '_PART' + str(new_part_number)
                    new_file_path = path + key_json + part_string + '.json'

                    n_events = 0

                    for coin in new_timeseries:
                        for timestamp_start in list(new_timeseries[coin].keys()):
                            n_events += 1

                    print(f'{n_events} new events for {key}')
                    print(f'Creating {file_path}')
                    with open(new_file_path, 'w') as file:
                        json.dump(new_timeseries, file)
                else:

                    # I re load timeseries, since I want to fetch not all events, but only those saved in the last PART
                    if len(timeseries_key) > 1:
                        del timeseries
                        with open(file_path, 'r') as file:
                            timeseries = json.load(file)

                    n_events = 0
                    # let's update "timeseries" with the new events occurred in "new_timeseries"
                    for coin in new_timeseries:
                        # iterate through NEW each event of the coin
                        for timestamp_start in list(new_timeseries[coin].keys()):
                            # if coin does not exist in timeseries (an event has never occurred before). let's create this key in "timeseries"
                            n_events += 1
                            if coin not in timeseries:
                                timeseries[coin] = {}
                            if timestamp_start not in timeseries[coin]:
                                timeseries[coin][timestamp_start] = new_timeseries[coin][timestamp_start]
                            else:
                                print(f'WARNING: {timestamp_start} is already present for {coin}. {key_json}. Should not be happening')
                            
                    print(f'{n_events} new events for {key}')

                    print(f'Overwriting {file_path}')
                    with open(file_path, 'w') as file:
                        json.dump(timeseries, file)

                del new_timeseries
                

                    
        else:
            print('File does not exist, Download from server...')

            # build the request body
            request = info[key]
            request = filter_request_with_orderbook_available(request, metadata_order_book, key)
            if request == None:
                continue
            request['timeframe'] = int(timeframe)

            if not check_past:
                pass
            else:
                request['check_past'] = check_past

            response = requests.post(url, json = request)
            print('Status Code is : ', response.status_code)
            response = json.loads(response.text)
            timeseries = response['data']

            n_events = 0

            for coin in timeseries:
                for timestamp_start in list(timeseries[coin].keys()):
                    n_events += 1
            print(f'{n_events} new events for {key} at the first download')

            msg = response['msg']
            retry = response['msg']
            print(msg)

            with open(file_path, 'w') as file:
                json.dump(timeseries, file)
        
        plotTimeseries(timeseries, key, fields, check_past, plot)


def plotTimeseries(timeseries, event_key, fields, check_past, plot, filter_start=False, filter_best=False):

    vol_field = fields[0]
    buy_vol_field = fields[1]
    timeframe = fields[2]
    buy_vol_value = fields[3]
    vol_value = fields[4]

    event_key_path = event_key.replace(":", "_").replace("/", "_")
    path_order_book_event_key = f'{ROOT_PATH}/order_book/{event_key_path}.json'
    if os.path.exists(path_order_book_event_key):
        f = open(path_order_book_event_key, "r")
        data_order_book_event_key = json.loads(f.read())
    else:
        print(f"no json at {path_order_book_event_key} was found")
        return

    # iterate through each coin
    timeseries_skipped = 0
    for coin in timeseries:
        #print(coin)
        # iterate through each event of the coin
        for timestamp_start in list(timeseries[coin].keys()):
            
            # skip if you want to plot only live timeseries
            if filter_start and datetime.fromisoformat(timestamp_start) < filter_start:
                continue
            
            if coin not in data_order_book_event_key:
                #print(f"Skipping {event_key} - {coin} - {timestamp_start}")
                timeseries_skipped += 1
                continue

            if timestamp_start not in data_order_book_event_key[coin]:
                #print(f"Skipping {event_key} - {coin} - {timestamp_start}")
                timeseries_skipped += 1
                continue

            if len(data_order_book_event_key[coin][timestamp_start]) == 0:
                #print(f"Skipping {event_key} - {coin} - {timestamp_start}")
                timeseries_skipped += 1
                continue
                
            order_book_event = data_order_book_event_key[coin][timestamp_start]

            #print(timestamp_start)
            timeframe = timeseries[coin][timestamp_start]['statistics']['timeframe']
            #print('statistic: ', timeseries[coin][timestamp_start]['statistics'])
            timestamp_end = datetime.fromisoformat(timestamp_start) + timedelta(minutes=timeframe)
            # get mean and std of event
            mean = timeseries[coin][timestamp_start]['statistics']['mean']
            std = timeseries[coin][timestamp_start]['statistics']['std']

            datetime_list = []
            price_list = []
            vol_list = []
            buy_vol_list = []
            # number of observation per event
            

            # label x-axis every "interval" minutes
            interval = int(timeframe / 6)
            current_price, volume_event, buy_volume_event = get_event_info(timeseries[coin][timestamp_start]['data'], timestamp_start, vol_field, buy_vol_field)

            volume_event = (datetime.fromisoformat(timestamp_start), volume_event)
            buy_volume_event = (datetime.fromisoformat(timestamp_start), buy_volume_event)
            if plot:
                print(f'Event occurred at {timestamp_start}')
                print(f'Purchase Price: {current_price} - {buy_vol_field}: {buy_volume_event[1]} - {vol_field}: {volume_event[1]} ')

            # get max price and min price
            max_price = (datetime.fromisoformat(timestamp_start), current_price)
            min_price = (datetime.fromisoformat(timestamp_start), current_price)

            if check_past != False:
                skip_timeseries = False
                #print('1')
                start_price = current_price
                one_day_before_price = timeseries[coin][timestamp_start]['data'][0]['price']
                six_hour_before_price_flag = True
                three_hour_before_price_flag = True
                one_hour_before_price_flag = True
                final_price_price_flag = True


                ante_performance_one_day = round_((start_price - one_day_before_price ) / one_day_before_price,2)
                #print(start_price)

                max_change = 0
                min_change = 0
                #print('2')
                
                iterator = 0
                for obs in timeseries[coin][timestamp_start]['data']:
                    if six_hour_before_price_flag and datetime.fromisoformat(timestamp_start) - datetime.fromisoformat(obs['_id']) < timedelta(hours=6):
                        ante_performance_six_hour = round_((start_price - obs['price'] ) / obs['price'],2)
                        six_hour_before_price_flag = False
                    
                    if three_hour_before_price_flag and datetime.fromisoformat(timestamp_start) - datetime.fromisoformat(obs['_id']) < timedelta(hours=3):
                        ante_performance_three_hour = round_((start_price - obs['price'] ) / obs['price'],2)
                        three_hour_before_price_flag = False
                    
                    if one_hour_before_price_flag and datetime.fromisoformat(timestamp_start) - datetime.fromisoformat(obs['_id']) < timedelta(hours=1):
                        ante_performance_one_hour = round_((start_price - obs['price'] ) / obs['price'],2)
                        one_hour_before_price_flag = False

                    if final_price_price_flag and datetime.fromisoformat(obs['_id']) - datetime.fromisoformat(timestamp_start) > timedelta(minutes=timeframe):
                        final_performance_timeseries = round_((obs['price'] - start_price) / start_price,2)
                        # skip timeseries if you want to filter only the best performance
                        if filter_best and max_change < filter_best:
                            skip_timeseries = True
                        final_price_price_flag = False
                                            
                    
                    #print('3')
                    datetime_list.append(datetime.fromisoformat(obs['_id']))
                    price_list.append(obs['price'])
                    vol_list.append(obs[vol_field])
                    buy_vol_list.append(obs[buy_vol_field])

                    if datetime.fromisoformat(obs['_id']) > datetime.fromisoformat(timestamp_start) and datetime.fromisoformat(obs['_id']) < timestamp_end:
                            
                        if obs['price'] > max_price[1]:
                            max_price = (datetime.fromisoformat(obs['_id']), obs['price'])
                            max_change = round_(((max_price[1] - start_price) / start_price)*100,2)
                        elif obs['price'] < min_price[1]:
                            min_price = (datetime.fromisoformat(obs['_id']), obs['price'])
                            min_change = round_(((min_price[1] - start_price) / start_price)*100,2)
            else:
                start_price = timeseries[coin][timestamp_start]['data'][0]['price']
                max_change = 0
                min_change = 0

                for obs in timeseries[coin][timestamp_start]['data']:
                    datetime_list.append(datetime.fromisoformat(obs['_id']))
                    price_list.append(obs['price'])
                    vol_list.append(obs[vol_field])
                    buy_vol_list.append(obs[buy_vol_field])
                    
                    if obs['price'] > max_price[1]:
                        max_price = (datetime.fromisoformat(obs['_id']), obs['price'])
                        max_change = round_(((max_price[1] - start_price) / start_price)*100,2)
                    if obs['price'] < min_price[1]:
                        min_price = (datetime.fromisoformat(obs['_id']), obs['price'])
                        min_change = round_(((min_price[1] - start_price) / start_price)*100,2)
            
            if plot:
                print(f'Max price occurred at {max_price[0]}: {max_price[1]} ({max_change})')
                print(f'Min price occurred at {min_price[0]}: {min_price[1]} ({min_change})')
                print(f'Performance 1 day at the triggering event {ante_performance_one_day}')
                print(f'Performance 6 hours at the triggering event {ante_performance_six_hour}')
                print(f'Performance 3 hours at the triggering event {ante_performance_three_hour}')
                print(f'Performance 1 hour at the triggering event {ante_performance_one_hour}')
                print(f'Performance at the end of timeseries {final_performance_timeseries}')

                

                #print('ok')
                fig, ax = plt.subplots(3, 1, sharex=True, figsize=(20, 10))

                # Plotting the first time series
                ax[0].plot(datetime_list, price_list)
                ax[0].set_ylabel('Price')
                
                ax[0].set_title(f'{coin} -- {timestamp_start} -- Mean: {mean}, Std: {std}')
                #print('title')
                ax[0].annotate(f'Max Change: {max_change}%', xy=(max_price[0], max_price[1]),
                                xytext=(max_price[0], max_price[1]*(1-((max_change/100)/2))),
                                textcoords='data', ha='center', va='top',arrowprops=dict(arrowstyle='->'))
                ax[0].annotate(f'Min Change: {min_change}%', xy=(min_price[0], min_price[1]),
                                xytext=(min_price[0], min_price[1]*(1+((min_change/100)/2))),
                                textcoords='data', ha='center', va='bottom',arrowprops=dict(arrowstyle='->'))
                ax[0].axvline(x=datetime.fromisoformat(timestamp_start), color='blue', linestyle='--')
                ax[0].axvline(x=timestamp_end, color='blue', linestyle='--')
                ax[0].xaxis.set_major_locator(mdates.MinuteLocator(interval=interval))
                ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                ax[0].grid(True)


                # Plotting the second time series
                ax[1].plot(datetime_list, vol_list)
                ax[1].set_ylabel(f'{vol_field}:{vol_value}')
                ax[1].axvline(x=datetime.fromisoformat(timestamp_start), color='blue', linestyle='--')
                ax[1].axvline(x=timestamp_end, color='blue', linestyle='--')
                ax[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=interval))
                ax[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                ax[1].annotate(f'Volume event: {volume_event[1]}', xy=(volume_event[0], volume_event[1]),
                                xytext=(volume_event[0], volume_event[1]),
                                textcoords='data', ha='center', va='bottom',arrowprops=dict(arrowstyle='->'))
                ax[1].grid(True)

                # Plotting the third time series
                ax[2].plot(datetime_list, buy_vol_list)
                ax[2].set_ylabel(f'{buy_vol_field}:{buy_vol_value}')
                ax[2].axhline(y=0.5, color='red', linestyle='--')
                ax[2].axvline(x=datetime.fromisoformat(timestamp_start), color='blue', linestyle='--')
                ax[2].axvline(x=timestamp_end, color='blue', linestyle='--')
                ax[2].xaxis.set_major_locator(mdates.MinuteLocator(interval=interval))
                ax[2].xaxis.set_major_formatter(mdates.DateFormatter(' %H:%M'))
                ax[2].annotate(f'Volume event: {buy_volume_event[1]}', xy=(buy_volume_event[0], buy_volume_event[1]),
                                xytext=(buy_volume_event[0], buy_volume_event[1]),
                                textcoords='data', ha='center', va='bottom',arrowprops=dict(arrowstyle='->'))
                ax[2].grid(True)

                # Display the graph
                plt.show()
                plt.close()
            
            print(f'{timeseries_skipped} timeseries have not been shown because of lack of data of orderbooks')

def get_event_info(observations, timestamp_start, vol_field, buy_vol_field):
    '''
    this returns the price, vol_value and buy_value registered at the event triggering
    '''
    position = 0
    for obj in observations:
        position += 1
        if obj.get('_id') == timestamp_start:
            return obj['price'], obj[vol_field], obj[buy_vol_field] 
    
    print('something went wrong "get_position_from_list_of_objects"')
    return None

def riskmanagement_data_preparation(data, n_processes, delete_X_perc=False, key=None, analysis_json_path='/analysis_json_v3/'):
    # CONSTANTS
    data_arguments = []
    coins_list = list(data.keys())
    n_events = sum([1 for coin in data for _ in data[coin]])
    coins_slices = []
    n_coins = len(data)
    step = n_coins // n_processes
    remainder = n_coins % n_processes
    remainder_coins = coins_list[-remainder:]
    slice_start = 0
    slice_end = slice_start + step

    for i in range(n_processes):
        data_i = {}
        for coin in coins_list[slice_start:slice_end]:
            data_i[coin] = data[coin]

        data_arguments.append(data_i)
        del data_i
        slice_start = slice_end
        slice_end += step
    
    # add the remaining coins to the data_arguments
    if remainder != 0:
        for coin, index in zip(remainder_coins, range(len(remainder_coins))):
            data_arguments[index][coin] = data[coin]

    events_discarded = 0
    if delete_X_perc:
        new_data_arguments = []
        file_path = ROOT_PATH + analysis_json_path + 'analysis.json'
        with open(file_path, 'r') as file:
            print(f'Downloading {file_path}')
            # Retrieve shared memory for JSON data and "start_interval"
            analysis_json = json.load(file)
            analysis_json = analysis_json['data']

        max_list = [] # list of max value for each event
        
        for coin in analysis_json[key]['info']:
            for event in analysis_json[key]['info'][coin]:
                max_list.append(event['max'])

        perc99_max = np.percentile(max_list, delete_X_perc)
        events_99 = []

        # get events in 99 perc
        for coin in analysis_json[key]['info']:
            for event in analysis_json[key]['info'][coin]:
                event['coin'] = coin
                if event['max'] >= perc99_max:
                    events_99.append((coin, event['event']))
                    
        for index in range(len(data_arguments)):
            data_i = {}
            for coin in data_arguments[index]:
                for start_timestamp in data_arguments[index][coin]:
                    if (coin, start_timestamp) not in events_99:
                        if coin not in data_i:
                            data_i[coin] = {}
                        data_i[coin][start_timestamp] = data[coin][start_timestamp]
                    else:
                        print(f'{coin} - {start_timestamp} outlier discarded')
                        events_discarded += 1
            new_data_arguments.append(data_i)

        data_arguments = new_data_arguments
    
    print(f'{events_discarded} events have been discarded for {delete_X_perc} percentile')
    n_events = n_events - events_discarded
    n_events_divided = sum([1 for data_i in range(len(data_arguments)) for coin in data_arguments[data_i] for _ in data_arguments[data_i][coin]]) 

    if n_events != n_events_divided:
        print(f'WARNING: Mismatch in data_preparation: expected: {n_events}, current: {n_events_divided}')

    return data_arguments

def get_volume_standings_file(path_volume_standings, benchmark_json=None):
    '''
    This function delivers the standings of volumes for each coin for each day
    { BTCUSDT --> { "2024-09-26" : 1 } ... } , { "2024-09-27" : 1 } }
    '''
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day


    if os.path.exists(path_volume_standings):
        print(f'volume standings is up to date')
    else:
        print(f'{path_volume_standings} does not exist')
        if benchmark_json == None:
            path_benchmark_new = f'/Users/albertorainieri/Personal/analysis/benchmark_json/benchmark-2024-12-30.json'
            if os.path.exists(path_benchmark_new):
                with open(path_benchmark_new, 'r') as file:
                    # Retrieve shared memory for JSON data and "start_interval"
                    benchmark_json = json.load(file)

        list_dates = list(benchmark_json["BTCUSDT"]["volume_series"].keys())

        #orig_list.sort(key=lambda x: x.count, reverse=True)
        summary = {}
        for coin in benchmark_json:
            for date in list_dates:
                if date not in summary:
                    summary[date] = []
                if date not in benchmark_json[coin]["volume_series"]:
                    continue
                total_volume_30_days = [benchmark_json[coin]["volume_series"][date][0]]
                current_datetime = datetime.strptime(date, "%Y-%m-%d")
                for i in range(1,31):
                    datetime_past = current_datetime - timedelta(days=i)
                    previous_date = datetime_past.strftime("%Y-%m-%d")
                    if previous_date in benchmark_json[coin]["volume_series"]:
                        total_volume_30_days.append(benchmark_json[coin]["volume_series"][previous_date][0])

                summary[date].append({"coin":coin,"volume_avg": round_(np.mean(total_volume_30_days),2)})
        
        standings = {}
        #print(summary)
        for date in summary:
            if date not in standings:
                standings[date] = []
            
            list_volumes = summary[date]
            standings[date] = sorted(list_volumes, key=itemgetter('volume_avg'), reverse=True)
        
        final_standings = {}
        for date in standings:
            final_standings[date] = {}
            position = 1
            for obj in standings[date]:
                coin = obj['coin']
                final_standings[date][coin] = position
                position += 1
        
        
        with open(path_volume_standings, 'w') as f:
            json.dump(final_standings, f, indent=4)
    
    return standings

def load_volume_standings():
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    path_volume_standings = ROOT_PATH + f'/benchmark_json/volume_standings_{year}-{month}-{day}.json'
    if os.path.exists(path_volume_standings):
        print(f'svolume standings is up to date, loading then..')
        with open(path_volume_standings, 'r') as file:
            volume_standings = json.load(file)
    else:
        benchmark_json, df_benchmark, volatility = get_benchmark_info()
        volume_standings = get_volume_standings_file(path_volume_standings, benchmark_json)
    
    return volume_standings

def get_currency_coin():
    return ['GBUSDT', 'FDUSDUSDT', 'EURUSDT']

def frequency_events_analysis(complete_info):

    

    summary = {}
    for event_key in complete_info:
        for coin in complete_info[event_key]['info']:
            for event in complete_info[event_key]['info'][coin]:
                year_month_datetime = datetime.fromisoformat(event['event'])
                year_month = f"{year_month_datetime.year}-{year_month_datetime.month:02d}"
                year_month = year_month[2:]
                if year_month not in summary:
                    summary[year_month] = 1
                else:
                    summary[year_month] += 1

    cols = 2
    rows = int(len(summary) / cols + 1)
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(15, 15))
    # y = i % cols
    # x = i // cols
    sorted_data = dict(sorted(summary.items()))
    months = list(sorted_data.keys())
    values = list(sorted_data.values())
    #plt.figure(figsize=(10, 6))
    axes.bar(months, values, color='skyblue')
    axes.set_xticks(months[::2]) 
    axes.set_title(event_key)
    # Adjust spacing between subplots
    plt.tight_layout()
    # Show the plot
    plt.show()




def filter_xth_percentile(obj, filter_field, xth_percentile):
    """
    Filters keys in an object based on the 99th percentile of their 'val1' values.

    Args:
        obj: An object (e.g., dictionary) where values are lists of 
            tuples (or similar) with at least one element named 'val1'.

    Returns:
        A new object containing only the keys whose 'val1' values 
        are below the 99th percentile of all 'val1' values across all keys.
    """

    filter_mapping = {'mean': 0, 'std': 1, 'max':2, 'min':3}
    position_to_filter = filter_mapping[filter_field]

    # Extract 'val1' values from all keys
    
    all_valx = [values[position_to_filter] for key, values in obj.items()] 
    events_discarded = []

    # Calculate the 99th percentile of 'val1' values
    if len(all_valx) > 0:
        val1_threshold = np.percentile(all_valx, xth_percentile) 

        # Filter keys based on the threshold
        filtered_obj = {}
        for key, values in obj.items():
            if values[0] <= val1_threshold:
                filtered_obj[key] = values
            else:
                events_discarded.append(key)

        return filtered_obj, events_discarded
    else:
        return {}, None
    
def create_rismanagement_from_complete_info(complete_info):
    sum_events = 0
    riskmanagement_json = {}
    for event_key in complete_info:
        if '30m' in event_key or '60m' in event_key:
            continue
        riskmanagement_json[event_key] = complete_info[event_key]['frequency/month']
        sum_events += complete_info[event_key]['frequency/month']
    n_event_keys = len(riskmanagement_json)
    print(f'{n_event_keys} event_keys')
    print(riskmanagement_json)
    print(sum_events/30)
    print('At this moment no riskmanagement json is saved, change this line of code in Helpers.py')
    # with open(f'/Users/albertorainieri/Personal/backend/riskmanagement/riskmanagement.json', 'w') as outfile:
    #     json.dump(riskmanagement_json, outfile, indent=4)
    return riskmanagement_json

def filter_complete_info_by_current_eventkeys(output, complete_info):
    '''
    This function filters complete_info with the event_keys that are used in production
    '''
    path_riskmanagement = '/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json_production/riskmanagement.json'
    new_complete_info = {}
    new_output = {}
    with open(path_riskmanagement, 'r') as f:
        riskmanagement = json.load(f)
        riskmanagement_event_keys = list(riskmanagement.keys())
    filtered_event_keys=0
    for event_key in complete_info:
        if event_key in riskmanagement_event_keys:
            new_complete_info[event_key] = complete_info[event_key]
            new_output[event_key] = output[event_key]
        else:
            filtered_event_keys += 1
    print(f'Filtered {filtered_event_keys} event_keys')
    
    return new_output, new_complete_info


def get_price_levels(price, bid_orders, cumulative_volume_jump=0.03, price_change_limit=0.4, price_change_jump=0.025):
    '''
    this function outputs the pricelevels (tuple: 4 elements), order_distribution, cumulative_level
    price_levels (TUPLE):
        [1] : absolute price_level
        [2] : relative price_level (percentage from current price)
        [3] : cumulative_level (from 0 to 100, which percentage of bid/ask total volume this level corresponds to) (NOT USED)
        [4] : this is the JUMP. distance from previous level (from 0 to 100, gt > "$jump"). Only if greater than "cumulative_volume_jump"
    order_distribution (OBJ): (KEYS: price_change; VALUES: density of this price level
      (i.e. 5% of the bid/ask volume is concentrated in the first 2.5% price range, considering ONLY the "price_change_limit" (e.g. 40% price range from current price) ))
        {0.025: 0.05,
         0.05:  0.02,
         ...
         }
    previous_cumulative_level: it is the percentage of the bid/ask volume considering only the "price_change_limit" (e.g. 40% price range from current price)
                                with respect to the total bid/ask volume
    '''
    previous_level = 0
    price_levels = []
    n_decimals = count_decimals(price)
    cumulative_level_without_jump = 0
    price_change_level = price_change_jump
    order_distribution = {}
    for i in np.arange(price_change_jump,price_change_limit+price_change_jump,price_change_jump):
        order_distribution[str(round_(i,3))] = 0
    previous_cumulative_level = 0

    for level in bid_orders:
        cumulative_level = level[1]
        price_change = level[0]

        # in this "if condition", I see how orders are distributed along the (0,price_change_limit) range,
        #  the chunks of orders are divided relative to "price_change_jump" (i.e. if var=0.025, I check in the [0,2.5%] price change window how deep is the order book and so on)
        # if price_change is below next threshold, keep updating the cumulative volume level for that price level
        if abs(price_change) <= price_change_level:
            order_distribution[str(price_change_level)] = cumulative_level
        
        # in case is above next threshold, update price_change_level and initialize next price_change_level
        else:
            # before moving to the next price level, update the relative level
            order_distribution[str(price_change_level)] = round_(order_distribution[str(price_change_level)] - previous_cumulative_level,2)

            # now update the next price change level
            previous_cumulative_level += order_distribution[str(price_change_level)]
            price_change_level = round_(price_change_level + price_change_jump,3)

            # in case some price level is empty, skip to the next level
            while abs(price_change) > price_change_level:
                price_change_level = round_(price_change_level + price_change_jump,3)

            # next chunk is below next thrshold
            if abs(price_change) <= price_change_level and abs(price_change) <= price_change_limit:
                order_distribution[str(price_change_level)] = cumulative_level

        # here, I discover the jumps, the info is stored in "price_levels"
        if cumulative_level - previous_level >= cumulative_volume_jump and abs(price_change) <= price_change_limit and abs(price_change) >= 0.01:
            actual_jump = round_(cumulative_level - previous_level,3)
            price_level = price * (1+price_change)
            info = (round_(price_level,n_decimals), price_change, cumulative_level, actual_jump)
            #print(info, cumulative_level, price_change)
            price_levels.append(info)
        elif abs(price_change) <= price_change_limit:
            cumulative_level_without_jump = cumulative_level

        if abs(price_change) > price_change_limit:
            break
        previous_level = cumulative_level
    
    # scale order_distribution to [0,100] range
    for lvl in order_distribution:
        order_distribution[lvl] = round_(order_distribution[lvl] / previous_cumulative_level,3)    
    
    # if there are not jumps, at least I want to get the cumulative volume at the limit price level
    if len(price_levels) == 0:
        info = (None, None, cumulative_level_without_jump, False)
        price_levels.append(info)
    
    if cumulative_level_without_jump == 0:
        print(bid_orders)
    
    return price_levels, order_distribution, round_(previous_cumulative_level,3)

def count_decimals(num):
  """
  Determines the number of decimal places in a given number.

  Args:
    num: The number to check.

  Returns:
    The number of decimal places, or 0 if the number is an integer.
  """
  try:
    str_num = str(num)
    decimal_index = str_num.index('.')
    return len(str_num) - decimal_index
  except ValueError:
    # If no decimal point is found, it's an integer
    return 1

def get_analysis():
  import sys
  sys.path.insert(0,'..')
  from Functions import download_show_output
  from Helpers import filter_complete_info_by_current_eventkeys
  import pandas as pd
  from datetime import datetime
  pd.set_option('display.max_rows', None)

  minimum_event_number = 1
  minimum_event_number_list = [minimum_event_number]
  mean_threshold = -10
  frequency_threshold = 0
  std_multiplier = 10
  early_validation = False
  # file_paths = ["/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json/analysis-buy-50-150-450.json",
  #              "/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json/analysis-sell-50-150-450.json",
  #              "/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json/analysis-buy-sell-10-250-highfrequency.json"]

  file_paths = ["/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json_production/analysis.json"]
  start_analysis= datetime(2025,1,1)
  early_validation = datetime(2026,1,1)
  xth_percentile=100
  filter_field='mean' #mean, std, max, min
  output, complete_info = download_show_output(minimum_event_number=minimum_event_number,mean_threshold=mean_threshold, frequency_threshold=frequency_threshold,
                                                early_validation=early_validation, std_multiplier=std_multiplier, file_paths=file_paths,
                                                  start_analysis=start_analysis, DELETE_99_PERCENTILE=True, filter_field=filter_field, xth_percentile=xth_percentile)


  output, complete_info = filter_complete_info_by_current_eventkeys(output, complete_info)

  df = pd.DataFrame(output).transpose()
  n_event_keys = len(df['mean'])
  print(f'Number of event_keys: {n_event_keys}')
  daily_frequency_all_events = int(sum(df['frequency/month']) / 30)
  print(f'Daily frequency of events: {daily_frequency_all_events}')

  return output, complete_info