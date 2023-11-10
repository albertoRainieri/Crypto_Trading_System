import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime, timedelta
import requests
import os, sys
from time import sleep
import pytz
#from scipy.stats import pearsonr
import pandas as pd
from time import time
from Functions_getData import getData
from multiprocessing import Pool, Manager
from copy import copy
from random import randint
import shutil
import re
import xgboost as xgb
from xgboost import XGBClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from typing import Literal
from sklearn.preprocessing import StandardScaler
from functools import reduce



ROOT_PATH = os.getcwd()
TargetVariable1 = Literal["mean", "max", "min"]


def round_(number, decimal):
    return float(format(number, f".{decimal}f"))

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
        print(f'Length of data argument {index}: {len_data_argument}')

    
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
    day = int(path_split[1])
    month = int(path_split[2])
    year = int(path_split[3])
    hour = int(path_split[4])
    minute = int(path_split[5].split('.')[0])
    datetime1 = datetime(year=year, month=month, day=day, hour=hour, minute=minute)

    # retrieve first datetime of the next list_json
    next_path = list_paths[index + 1]
    next_path_split = next_path.split('-')
    day = int(next_path_split[1])
    month = int(next_path_split[2])
    year = int(next_path_split[3])
    hour = int(next_path_split[4])
    minute = int(next_path_split[5].split('.')[0])
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
    date_pattern = r"data-(\d{2})-(\d{2})-(\d{4})"

    match = re.search(date_pattern, path)
    if match:
        day, month, year = map(int, match.groups())
        # Create a tuple to use for sorting (year, month, day)
        return (year, month, day)
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
    path_dir = ROOT_PATH + "/json/"
    list_json = os.listdir(path_dir)
    list_files = sorted(list_json, key=get_date_key)
    list_paths = [path_dir + file for file in list_files]
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
                print(f'Nothing to retrieve from {path}')
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

def get_dynamic_volume_avg(benchmark_info):
    '''
    This functions processes from the output of def "get_benchmark_info" and delivers a timeseries (daily) for each coin of this equation: (volume_30_std / volume_30_avg)
    In other terms, this function computes dynamically the average of the last 30 days for each date available in "volume_series" from db_benchmark
    '''
    dynamic_benchmark_volume = {}
    days = 30 #days

    for coin in benchmark_info[0]:
        # get "volume series" for a coin, as per db_benchmark
        volume_series = benchmark_info[0][coin]['volume_series']

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
            
            if coin not in dynamic_benchmark_volume:
                dynamic_benchmark_volume[coin] = {}
            
            #dynamic_benchmark_volume[coin][date] = {'vol_avg': mean_one_date, 'vol_std': std_one_date}
            
            # get percentage std over mean
            if mean_one_date != 0:
                dynamic_benchmark_volume[coin][date] = std_one_date / mean_one_date
            else:
                dynamic_benchmark_volume[coin][date] = 1


    for var in list(locals()):
        if var.startswith('__') and var.endswith('__'):
            continue  # Skip built-in variables
        if var == "dynamic_benchmark_volume":
            continue
        del locals()[var]


    return dynamic_benchmark_volume

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
            # Retrieve shared memory for JSON data and "start_interval"
            analysis_json = json.load(file)

            # INTEGRATION HAS BEEN ADDED LATER TO ANALYZE NEW COMBINATIONS AND MERGE THE RESULTS INTO THE EXISTING analysis_json
            if INTEGRATION:
                if 'start_next_analysis_integration' not in analysis_json:
                    start_interval = datetime(2023,5,11).isoformat()
                else:
                    start_interval = analysis_json['start_next_analysis_integration']
                # get start_next_analysis of primary analysis
                start_interval_primary = analysis_json['start_next_analysis']
            else:
                start_interval = analysis_json['start_next_analysis']
            del analysis_json
    else:
        # Create shared memory for JSON data and initialize "start_interval"
        start_interval = datetime(2023,5,11).isoformat()

        # define "end_interval" and "filter_position"
    if INTEGRATION:
        end_interval = min(datetime.now(), datetime.fromisoformat(start_interval) + timedelta(days=analysis_timeframe))
        # start_interval_primary is the timestamp that determines the last moment in which data have been analyzed in the last analysis
        # end interval is the timestamp that determines the timestamp that data must be loaded for the next analysis
        # end interval is 3 days ahead then end_interval_analysis, that's why I am adding 3 days to start_interval_primary in the following line
        end_interval = min( datetime.fromisoformat(start_interval_primary) + timedelta(days=3), end_interval)
    else:
        end_interval = min(datetime.now(), datetime.fromisoformat(start_interval) + timedelta(days=analysis_timeframe))

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

def getsubstring_fromkey(key):
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
    '''
    # split the key
    key_split = key.split(':')

    # get buy_vol
    buy_vol = key_split[0]

    #get vol
    vol = key_split[1][key_split[1].index('/')+1:]

    # get buy_vol_value
    buy_vol_value = key.split(buy_vol + ':')[-1].split('/vol')[0]

    # get vol_value
    vol_value = key.split(vol + ':')[-1].split('/timeframe')[0]

    # get timeframe value
    # initializing substrings
    sub1 = "timeframe"
    sub2 = "/vlty"
    # getting index of substrings
    idx1 = key.index(sub1)

    # in case keys are grouped by volatility
    try:
        idx2 = key.index(sub2)
        timeframe = ''
        # getting elements in between
        for idx in range(idx1 + len(sub1) + 1, idx2):
            timeframe = timeframe + key[idx]
    # in case keys are NOT grouped by volatility
    except:
        timeframe = key.split('timeframe:')[-1]
        timeframe = timeframe[:-1]

    return vol, vol_value, buy_vol, buy_vol_value, timeframe


def sort_files_in_json_dir():
    # Directory containing the files
    directory_path = "/Users/albertorainieri/Projects/Personal/analysis/json/"

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

def load_data_for_supervised_analysis(complete_info=None, complete_info_path=None, search_parameters=None, target_variable: TargetVariable1 = 'mean'):
    '''
    This function analyzes with a supervised algorithm the output of "download_show_output". In particular the variable "info" or "complete_info" is taken as input.
    The decision variables will be taken from the route /get-pricechanges which provides all info about (pricechanges, volumes and buy_volumes)
    The target variables can be one of the following (mean, max, min) which are already present in "info"
    the function will iterate through each event and get necessary information for building the matrix (decision variables + target) and start to anaylyze
    '''
    
    if complete_info == None and complete_info_path == None:
        msg = 'Provide at least one of the following: complete_info or complete_info_path'
        return msg
    if complete_info and complete_info_path:
        msg = 'Both complete_info and complete_info path are not NULL. Only one can be NULL'
        return msg
    
    
    # DOWNLOAD DATA FOR SUPERVISED ANALYSIS
    if complete_info:
        assert search_parameters != None
        print('New complete_info provided, grabbing all decision variables from server. You have 5 seconds to abort this action')
        sleep(5)
        request = {}
        # First we need to prepare the request for retrieving the decision variables
        for event_key in complete_info:
            if event_key not in request:
                request[event_key] = {}
            # get info of 1 key
            for coin in complete_info[event_key]['info']:
                if coin not in request:
                    request[event_key][coin] = []
                for event in complete_info[event_key]['info'][coin]:
                    request[event_key][coin].append(event['event'])
    
        url = "https://algocrypto.eu/analysis/get-pricechanges"
        
        response = requests.post(url=url, json = request)
        print(response.status_code)
        response = json.loads(response.text)
        decision_variables = response['data']
        msg = response['msg']

        # msg shows all nan values replaced with different timeframes
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
        random_event_key = list(decision_variables.keys())[0]
        random_coin = list(decision_variables[random_event_key].keys())[0]
        random_start_timestamp = list(decision_variables[random_event_key][random_coin].keys())[0]
        decision_variable_keys = list(decision_variables[random_event_key][random_coin][random_start_timestamp].keys())


        info = {}

        for event_key in complete_info:
            # INITIALIZE KEYS FOR EACH EVENT
            if event_key not in info:
                # add the keys relative to target variables
                info[event_key] = {'event': [], 'coin': [], 'mean': [], 'std': [],
                                    'max': [], 'min': [], 'volatility': []}
                # add the keys relative to decision variables
                for decision_variable_key in decision_variable_keys:
                    timeframe = decision_variable_key.split('_')[-1] # example: "1h" or "3h" or ... "7d"
                    price_key = 'price_%_' + timeframe
                    vol_key = 'vol_' + timeframe
                    buy_vol_key = 'buy_' + timeframe

                    info[event_key][price_key] = []
                    info[event_key][vol_key] = []
                    info[event_key][buy_vol_key] = []
            
            for coin in complete_info[event_key]['info']:
                # FILL VALUES FOR EACH EVENT
                for event in complete_info[event_key]['info'][coin]:
                    start_timestamp = event['event']
                    for key in ['mean', 'std', 'max', 'min', 'volatility', 'event']:
                        # append for each event the following: mean, std, max, min, volatility
                        if key == 'max' or key == 'min':
                            info[event_key][key].append(round_(event[key],4))
                        else:
                            info[event_key][key].append(event[key])
                        
                    # append coin
                    info[event_key]['coin'].append(coin)
                    
                    for decision_variable_key in decision_variable_keys:
                        # append for each event all the decision variables
                        timeframe = decision_variable_key.split('_')[-1] # example: "1h" or "3h" or ... "7d"
                        price_key = 'price_%_' + timeframe
                        vol_key = 'vol_' + timeframe
                        buy_vol_key = 'buy_' + timeframe

                        info[event_key][price_key].append(decision_variables[event_key][coin][start_timestamp][decision_variable_key][0])
                        info[event_key][vol_key].append(decision_variables[event_key][coin][start_timestamp][decision_variable_key][1])
                        info[event_key][buy_vol_key].append(decision_variables[event_key][coin][start_timestamp][decision_variable_key][2])
        
        now = datetime.now()
        year = str(now.year)
        month = str(now.month)
        day = str(now.day)
        random_number = str(randint(0,1000))

        # define the file path in which info will be saved
        file_path = ROOT_PATH + f'/complete_info/info-{year}-{month}-{day}-'
        for params in search_parameters:
            valueParam = str(search_parameters[params])
            file_path += f'{params}:{valueParam}-'
        file_path += f'{random_number}.json'

        with open(file_path, 'w') as file:
            json.dump(info, file)

    else:
        print('complete_info_path PROVIDED. Loading data locally')
        f = open(complete_info_path, "r")
        info = json.loads(f.read())

    
    return info


def train_model_xgb(X_train, X_test, y_train, y_test, classifier_strings, event_key):

    # Encode Labels
    le = LabelEncoder()
    y_train = le.fit_transform(y_train)

    #Initialize accuracy_score
    best_acc_score = -10**9
    params = {
        'eta_list' : [0, 0.5, 1],
        'gamma_list' : [0,1],
        'max_depth_list' : [6],
        'min_child_weight_list' : [1],
        'max_delta_step_list' : [0],
        'subsample_list' : [0.5, 1],
        'sampling_method_list' : ['uniform'],
        'lambda_list' : [0,1],
        'alpha_list' : [0,1],
        'tree_method_list' : ['auto', 'approx', 'hist'],
        'n_estimators_list': [10,50],
        'max_leaves_list': [8,16,32],
        'eval_metric_list': ['logloss']
    }


    total_number_of_combinations = reduce(lambda x, y: x * y, [len(params[i]) for i in params])
    print(f'Total Number of combinations: {total_number_of_combinations}')
    i = 0
    SKIP_10=False

    for n_estimators in params['n_estimators_list']:
        for eta in params['eta_list']:
            for gamma in params['gamma_list']:
                for max_leaves in params['max_leaves_list']:
                    for max_depth in params['max_depth_list']:
                        for min_child_weight in params['min_child_weight_list']:
                            for max_delta_step in params['max_delta_step_list']:
                                for subsample in params['subsample_list']:
                                    for sampling_method in params['sampling_method_list']:
                                        for lambda_ in params['lambda_list']:
                                            for alpha in params['alpha_list']:
                                                for eval_metric in params['eval_metric_list']:
                                                    for tree_method in params['tree_method_list']:
                                                        acc_score = 0
                                                        i+=1
                                                        if not SKIP_10:
                                                            if i >= 0.1*total_number_of_combinations:
                                                                print('10 percent of Data covered')
                                                                SKIP_10 = True
                                                        
                                                        model = XGBClassifier(
                                                                    n_estimators=n_estimators,
                                                                    gamma=gamma,
                                                                    max_depth=max_depth,
                                                                    max_leaves=max_leaves,
                                                                    min_child_weight=min_child_weight,
                                                                    max_delta_step=max_delta_step,
                                                                    eta=eta,
                                                                    subsample=subsample,
                                                                    reg_lambda=lambda_,
                                                                    sampling_method=sampling_method,
                                                                    tree_method=tree_method,
                                                                    eval_metric=eval_metric,
                                                                    use_label_encoder=False,
                                                                    random_state=1000,
                                                                    alpha=alpha,
                                                                    n_jobs=-1)

                                                        model.fit(X_train,y_train)

                                                        # Predict
                                                        y_pred = model.predict(X_test)
                                                        y_pred = le.inverse_transform(y_pred)
                                                        
                                                        for a,b in zip(y_pred, y_test):
                                                            if b == 0 and a == 0:
                                                                acc_score += 2
                                                            elif b == 0 and a == 2:
                                                                acc_score -= 2

                                                        if acc_score > best_acc_score:
                                                            print('Found new Acc Score: ', acc_score)
                                                            best_acc_score = acc_score
                                                            cm = confusion_matrix(y_test, y_pred)
                                                            df_cm = pd.DataFrame(cm, index = [i for i in classifier_strings], columns = [i for i in classifier_strings])

    now = datetime.now()
    year = str(now.year)
    month = str(now.month)
    day = str(now.day)
    random_number = str(randint(0,1000))
    event_key = event_key.replace(':', '_')
    event_key = event_key.replace('/', '_')
    file_path = f'{ROOT_PATH}/scores/{event_key}-{year}{month}{day}-{random_number}.json'
    doc = {'score': int(best_acc_score), 'n_estimators': n_estimators, 'gamma': gamma, 'max_depth': max_depth,
           'max_leaves': max_leaves, 'min_child_weight': min_child_weight, 'max_delta_step': max_delta_step,
           'eta': eta, 'subsample': subsample, 'reg_lambda': lambda_, 'sampling_method': sampling_method,
           'tree_method': tree_method, 'eval_metric': eval_metric, 'alpha': alpha}

    with open(file_path, 'w') as outfile:
            json.dump(doc, outfile)
    print('BEST_ACC_SCORE: ', best_acc_score)
    return df_cm

def scale_filter_select_features(df, target_variable):
    # Identify Decision and Output variables
    all_columns = list(df.columns)
    #selected_columns = [column for column in all_columns if 'price_%' in column or 'vol' in column or 'buy' in column]
    selected_columns = [column for column in all_columns if 'price_%' in column or 'vol_' in column or 'buy_' in column]

    decision_variables = df[selected_columns]
    # Initialize the StandardScaler
    scaler = StandardScaler()

    # Fit the scaler on your data and transform the DataFrame
    decision_variables = pd.DataFrame(scaler.fit_transform(decision_variables), columns=decision_variables.columns)

    output_variable = np.array(df[target_variable])

    # Filter out NaN values
    X_withnan = np.array(decision_variables.to_numpy())
    nan_mask = np.isnan(X_withnan)
    nan_mask_y = []
    for row in nan_mask:
        if True in row:
            nan_mask_y.append(True)
        else:
            nan_mask_y.append(False)


    # Select X and y
    X = X_withnan[~np.array(nan_mask_y)]
    y = output_variable[~np.array(nan_mask_y)]

    return X,y