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
from Helpers import round_, get_volatility, data_preparation, load_data, get_benchmark_info, get_dynamic_volume_avg
from Helpers import get_substring_between, load_analysis_json_info, updateAnalysisJson, pooled_standard_deviation, getsubstring_fromkey
from Helpers import load_data_for_supervised_analysis, train_model_xgb, scale_filter_select_features
from sklearn.svm import SVR  # Support Vector Regression
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import xgboost as xgb
from xgboost import XGBClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
import seaborn as sn
from typing import Literal


'''
Functions.py define a set of functions that are used to analyze the strategy based on combinations of volumes and % Buyers/Sellers
'''

ROOT_PATH = os.getcwd()
TargetVariable = Literal["mean_event", "max_price", "min_price"]
TargetVariable1 = Literal["mean", "max", "min"]

def analyze_events(data, buy_vol_field, vol_field, minutes_price_windows, event_buy_volume, event_volume, dynamic_benchmark_volume, end_interval_analysis):
    '''
    This function analyzes what happens in terms of price changes after ONE specific event.
    This function is used by "wrap_analyze_events_multiprocessing" and "wrap_analyze_events"

    data: it is the dataset
    buy_vol_field: it is the key that determines which buy_vol in terms of timeframe (5m, 15m, 30m, 60m, ..., 1d) --> STRING (e.g. buy_vol_5m)
    vol_field: it is the key that determines which vol in terms of timeframe (5m, 15m, 30m, 60m, ..., 1d) --> STRING (e.g. vol_5m)
    minutes_price_windows: time window in terms of minutes. how many minutes into the future I want the check the price changes? --> INTEGER (e.g. 60)
    event_buy_volume: it is the value of "buy_vol_field". --> FLOAT (e.g. 0.6) MUST BE BETWEEN 0 and 1
    event_volume: it is the value of "buy_vol_field". --> FLOAT (e.g. 2.0)
    '''

    complete_info = {}
    nan = {}
    #print(data)
    # analyze for each coin
    for coin in list(data.keys()):
        
        # initialize limit_window
        limit_window = datetime(2000,1,1)
        
        # check through each observation of the coin
        for obs, index in zip(data[coin], range(len(data[coin]))):

            # this datetime_obs is needed to not trigger too many events. For example two closed events will overlap each other, this is not ideal
            datetime_obs = datetime.fromisoformat(obs['_id'])
            # if index == 0:
            #     print(f'{coin}: {datetime_obs}')

            # the observation must not be younger than "end_interval_analysis". The reason is explained in function "total_function_multiprocessing"
            if datetime_obs < end_interval_analysis:
                if buy_vol_field in obs and buy_vol_field in obs and obs[buy_vol_field] != None and obs[vol_field] != None:
                    # if buy_vol is greater than limit and
                    # if vol is greater than limit and
                    # if datetime_obs does not fall in a previous analysis window. (i.e. datetime_obs is greater than the limit_window set)
                    if obs[buy_vol_field] >= event_buy_volume and obs[vol_field] > event_volume and datetime_obs > limit_window:
                        if vol_field == 'vol_5m' and event_volume == 6:
                            continue
                        #EVENT TRIGGERED

                        # get initial price of the coin at the triggered event
                        initial_price = obs['price']

                        # get initial timestamp of the coin at the triggered event
                        timestamp_event_triggered = obs['_id']

                        # initialize current price_changes
                        event_price_changes = []

                        # get dynamic volatility
                        volatility = str(get_volatility(dynamic_benchmark_volume[coin], obs['_id']))

                        # initialize "price_changes" and "events" if first time
                        if volatility not in complete_info:
                            complete_info[volatility] = {}

                        if coin not in complete_info[volatility]:
                            complete_info[volatility][coin] = []

                        max_price_change = 0
                        min_price_change = 0

                        # initialize "price_changes" to fetch all changes for THIS event
                        price_changes = []

                        limit_window  = datetime_obs + timedelta(minutes=minutes_price_windows)
                        # get all the price changes in the "minutes_price_windows"
                        for obs_event, obs_i in zip(data[coin][index:index+minutes_price_windows], range(minutes_price_windows)):
                            # if actual observation has occurred in the last minute and 10 seconds from last observation, let's add the price "change" in "price_changes":
                            actual_datetime = datetime.fromisoformat(data[coin][index+obs_i]['_id'])
                            if actual_datetime - datetime.fromisoformat(data[coin][index+obs_i-1]['_id']) <= timedelta(minutes=10,seconds=10):
                                
                                change = (obs_event['price'] - initial_price)/initial_price
                                if not np.isnan(change):
                                    max_price_change = max(max_price_change, change)
                                    min_price_change = min(min_price_change, change)

                                    price_changes.append(change)
                                    event_price_changes.append(change)
                                else:
                                    if coin not in nan:
                                        nan[coin] = []
                                    nan[coin].append(timestamp_event_triggered)

                        complete_info[volatility][coin].append({'event': timestamp_event_triggered, 'mean': round_(np.mean(event_price_changes),4), 'std': round(np.std(event_price_changes, ddof=1),4),
                                                                'max': max_price_change, 'min': min_price_change})
            else:
                # in case an observations of a coin goes beyond "end_interval_analysis", then I skip the whole analysis of the coin,
                # since the obsevations are ordered chronologically
                # print(f'Analysis for {coin} is completed: Last timestamp: {datetime_obs.isoformat()}; {index}')
                break

    return complete_info, nan

def show_output(shared_data):
    '''
    This function takes as input the shared_data from "wrap_analyze_events_multiprocessing" ans return the output available for pandas DATAFRAME
    '''
    shared_data = json.loads(shared_data.value)
    df_output = {}
    coins = {}
    for key in list(shared_data.keys()):
        if key != 'coins' or key != 'events':
            if shared_data[key]['events'] > 0:
                shared_data[key]['price_changes'] = np.array(shared_data[key]['price_changes'])

                isfinite = np.isfinite(shared_data[key]['price_changes'])
                shared_data[key]['price_changes'] = shared_data[key]['price_changes'][isfinite]

                mean_weighted = np.mean(shared_data[key]['price_changes'])*100
                std_weighted = np.std(shared_data[key]['price_changes'])*100

                df_output[key] = {'mean': mean_weighted, 'std': std_weighted, 'lower_bound': mean_weighted - std_weighted, 'n_coins': len(shared_data[key]['coins']), 'n_events': shared_data[key]['events']}
                coins[key] = {'coins': shared_data[key]['coins'], 'events': shared_data[key]['events']}
            else:
                df_output[key] = {'mean': None, 'std': None, 'lower_bound': None, 'n_coins': 0, 'n_events': 0}

    return df_output, coins

def wrap_analyze_events_multiprocessing(data, data_i, list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, dynamic_benchmark_volume, end_interval_analysis, lock, shared_data):
    '''
    this function summarizes the events for every field of vol_x, buy_vol_x and a list of timeframes.
    list_buy_vol --> LIST: ['buy_vol_5m, ..., 'buy_vol_1d']
    buy_vol --> LIST: ['vol_5m, ..., 'vol_1d']
    list_minutes --> LIST: [5,10, ..., 60, ..., 60*24]
    list_event_buy_volume --> LIST: [0.55, 0.6, 0.7, 0.8, 0.9]
    list_event_volume --> LIST: [2, 3, 4, 5, 6]
    it's been tested and it provides the same result of wrap_analyze_events but much faster

    FOR MULTIPROCESSING
    '''

    if data_i == 1:
        total_combinations = len(list_buy_vol) * len(list_vol) * len(list_minutes) * len(list_event_volume) * len(list_event_buy_volume)
        PERC_10 = True
        THRESHOLD_10 = total_combinations * 0.1
        PERC_25 = True
        THRESHOLD_25 = total_combinations * 0.25
        PERC_50 = True
        THRESHOLD_50 = total_combinations * 0.5
        PERC_75 = True
        THRESHOLD_75 = total_combinations * 0.75
        iteration = 0

    print(f'Slice {data_i} has started')
    temp = {}
    for buy_vol_field in list_buy_vol:
        for vol_field in list_vol:
            for minutes_price_windows in list_minutes:
                for event_buy_volume in  list_event_buy_volume:
                    for event_volume in list_event_volume:
                        if data_i == 1:
                            iteration += 1
                            if PERC_10:
                                if iteration > THRESHOLD_10:
                                    PERC_10 = False
                                    print('1/10 of data analyzed')
                            if PERC_25:
                                if iteration > THRESHOLD_25:
                                    PERC_25 = False
                                    print('1/4 of data analyzed')
                            elif PERC_50:
                                if iteration > THRESHOLD_50:
                                    PERC_50 = False
                                    print('1/2 of data analyzed')
                            elif PERC_75:
                                if iteration > THRESHOLD_75:
                                    PERC_75 = False
                                    print('3/4 of data analyzed')
                                
                        
                        complete_info, nan = analyze_events(data, buy_vol_field, vol_field, minutes_price_windows, event_buy_volume, event_volume, dynamic_benchmark_volume, end_interval_analysis)
                        
                        # price changes is a dict with keys regarding the volatility: {'1': [...], '2': [...], ..., '5': [...], ...}
                        # the value of the dict[volatility] is a list of all price_changes of a particular event
                        for volatility in complete_info:
                            # let's define the key: all event info + volatility coin
                            key = str(buy_vol_field) + ':' + str(event_buy_volume) + '/' + str(vol_field) + ':' + str(event_volume) + '/' + 'timeframe:' + str(minutes_price_windows) + '/' + 'vlty:' + volatility
                            temp[key] = {}
                            temp[key]['info'] = complete_info[volatility]
                            # temp[key]['nan'] = nan


    #del total_means, total_stds, events, coins, data, complete_info
    del complete_info
    #print(f'Slice {data_i} has finished')


    # Lock Shared Variable
    # this lock is essential in multiprocessing which permits to work on shared resources
    with lock:
        resp = json.loads(shared_data.value)

        for key in list(temp.keys()):
            
            if key not in resp:
                resp[key] = {}

            if 'info' in resp[key]:
                # update complete "info"
                for coin in temp[key]['info']:
                    if coin not in resp[key]['info']:
                        resp[key]['info'][coin] = []
                    for event in temp[key]['info'][coin]:
                        resp[key]['info'][coin].append(event)
                
            else:
                # initialize the keys of resp[key]
                resp[key]['info'] = temp[key]['info']

        del temp

        shared_data.value = json.dumps(resp)
        size_shared_data_value = int(sys.getsizeof(shared_data.value)) / 10**6

        n_events = 0
        for key in resp:
            for coin in resp[key]['info']:
                n_events += len(resp[key]['info'][coin])

    #print(f'Data saved fot slice {data_i}')
    print(f'size of shared_data_value before completion of {data_i}: {size_shared_data_value} Mb; Number of events saved: {n_events}')
    #print(f'Events recorded from slice {data_i}: ')

    # for var in list(locals()):
    #     if var.startswith('__') and var.endswith('__'):
    #         continue  # Skip built-in variables
    #     #print(f'deleting variable {var} in "wrap_analyze_events_multiprocessing"')
    #     del locals()[var]

def start_wrap_analyze_events_multiprocessing(data, list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, start_interval, end_interval, n_processes):
    '''
    this function starts "wrap_analyze_events_multiprocessing" function
    '''
    # get dynamic benchmark volume. This will be used for each observation for each coin, to see what was the current volatility (std_dev / mean) of the last 30 days.
    benchmark_info = get_benchmark_info()
    dynamic_benchmark_volume = get_dynamic_volume_avg(benchmark_info)


    t1 = time()
    data_arguments = data_preparation(data, n_processes = n_processes)
    total_combinations = len(list_buy_vol) * len(list_vol) * len(list_minutes) * len(list_event_volume) * len(list_event_buy_volume)
    print('total_combinantions', ': ', total_combinations)
    path = ROOT_PATH + "/analysis_json/"
    file_path = path + 'analysis' + '.json'

    manager = Manager()
    # Create shared memory for JSON data
    
    shared_data = manager.Value(str, json.dumps({}))
    lock = Manager().Lock()


    # Create a multiprocessing Pool
    pool = Pool()

    # Execute the function in parallel
    pool.starmap(wrap_analyze_events_multiprocessing, [(data_arguments, arg, arg_i, list_buy_vol, list_vol, list_minutes,
                                        list_event_buy_volume, list_event_volume, dynamic_benchmark_volume, lock, shared_data) for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])

    # Close the pool
    pool.close()
    pool.join()
    t2 = time()
    print(t2-t1, ' seconds')

    return shared_data

def log_wrap_analize(total_combinations, i, task_25, task_50, task_75):

    if task_25:
        if i/total_combinations > 0.25:
            print('25% Completed')
            task_25 = False
            task_50 = True
    elif task_50:
        if i/total_combinations > 0.5:
            print('50% Completed')
            task_50 = False
            task_75 = True
    elif task_75:
        if i/total_combinations > 0.75:
            print('75% Completed')
            task_75 = False
    
    return task_25, task_50, task_75
    
def show_output_nomultiprocessing(resp):
    '''
    This function shows the output of the function "wrap_analyze_events". NO MULTIPROCESSING
    '''

    df = pd.DataFrame(resp).transpose()
    lb = []
    for index, row in df.iterrows():
        mean = row['mean']
        std = row['std']
        lb.append(mean - std)
        
    df['lb'] = lb
        
    df = df.sort_values(by=['lb'], ascending=False)

    return df

def wrap_analyze_events(data, list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, start_interval, end_interval):
    '''
    this function summarizes the events for every field of vol_x, buy_vol_x and a list of timeframes.
    list_buy_vol --> LIST: ['buy_vol_5m, ..., 'buy_vol_1d']
    buy_vol --> LIST: ['vol_5m, ..., 'vol_1d']
    list_minutes --> LIST: [5,10, ..., 60, ..., 60*24]
    list_event_buy_volume --> LIST: [0.55, 0.6, 0.7, 0.8, 0.9]
    list_event_volume --> LIST: [2, 3, 4, 5, 6]
    THIS FUNCTION DOES NOT USE MULTIPROCESSING
    '''
    total_combinations = len(list_buy_vol) * len(list_vol) * len(list_minutes) * len(list_event_volume) * len(list_event_buy_volume)
    print(f'{total_combinations} total combinations')
    # get dynamic benchmark volume. This will be used for each observation for each coin, to see what was the current volatility (std_dev / mean).
    benchmark_info = get_benchmark_info()
    dynamic_benchmark_volume = get_dynamic_volume_avg(benchmark_info)

    resp = {}
    t1 = time()
    i = 0
    # global task_25
    # global task_50
    # global task_75

    task_25 = True
    task_50 = False
    task_75 = False

    for buy_vol_field in list_buy_vol:
        for vol_field in list_vol:
            for minutes_price_windows in list_minutes:
                for event_buy_volume in  list_event_buy_volume:
                    for event_volume in list_event_volume:
                        i += 1
                        task_25, task_50, task_75 = log_wrap_analize(total_combinations, i, task_25, task_50, task_75)
                        total_changes, events, coins = analyze_events(data, buy_vol_field, vol_field, minutes_price_windows, event_buy_volume, event_volume, dynamic_benchmark_volume)

                        for volatility in total_changes:
                            key = str(buy_vol_field) + ':' + str(event_buy_volume) + '/' + str(vol_field) + ':' + str(event_volume) + '/' + 'timeframe:' + str(minutes_price_windows) + '/' + 'vlty:' + volatility
                            mean_total_changes = np.mean(total_changes[volatility])*100
                            std_total_changes = np.std(total_changes[volatility])*100
                            resp[key] = {'mean': round_(mean_total_changes,2), 'std': round_(std_total_changes,2), 'coins': len(coins[volatility]), 'events': events[volatility]}
    
    print('100% Completed')
    t2 = time()
    time_spent = t2 - t1
    if  time_spent > 60:
        minutes = int(time_spent / 60)
        seconds = time_spent % 60
        print(f'{minutes} minutes and {seconds} seconds spent to run wrap_analyze_events')
    else:
        print(f'{time_spent} seconds spent to run wrap_analyze_events')
    return resp

def total_function_multiprocessing(list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, n_processes, LOAD_DATA, analysis_timeframe, INTEGRATION=False):
    '''
    this function loads only the data not analyzed and starts "wrap_analyze_events_multiprocessing" function. Finally it saves the output a path dedicated

    '''
    t1 = time()
    
    # getData from server. This will be stored in json/
    if LOAD_DATA:
        getData()

    # get dynamic benchmark volume. This will be used for each observation for each coin, to see what was the current volatility (std_dev / mean) of the last 30 days.
    benchmark_info = get_benchmark_info()
    dynamic_benchmark_volume = get_dynamic_volume_avg(benchmark_info)
    del benchmark_info

    # get json_analysis path. Check if there is already some data analyzed in json_analysis/ path. The new data will be appended
    total_combinations = len(list_buy_vol) * len(list_vol) * len(list_minutes) * len(list_event_volume) * len(list_event_buy_volume)
    print('total_combinations', ': ', total_combinations)

    #load analysis json info
    analysis_json_path = ROOT_PATH + '/analysis_json/analysis.json'
    start_interval, end_interval = load_analysis_json_info(analysis_json_path, analysis_timeframe=analysis_timeframe)

    # load data from local (json/) and store in "data" variable
    data = load_data(start_interval=datetime.fromisoformat(start_interval), end_interval=end_interval)

    # get data slices for multiprocessing. .e.g divide the data in batches for allowing multiprocessing
    data_arguments = data_preparation(data, n_processes = n_processes)

    # initialize Manager for "shared_data". Used for multiprocessing
    manager = Manager()
    
    # initialize lock for processing shared variable between multiple processes
    lock = Manager().Lock()

    # Create a multiprocessing Pool
    pool = Pool()

    #initialize shared_data
    shared_data = manager.Value(str, json.dumps({}))

    # define "end_interval_analysis". this is variable says "do not analyze events that are older than this date".
    # Since "minutes_price_windows" looks for N minutes observations after a specific event, I might get smaller time windows than expected (the most recent ones). This check should avoid this problem.
    end_interval_analysis = datetime.fromisoformat(data['BTCUSDT'][-1]['_id']) - timedelta(days=3) #last datetime from btcusdt - 3 days
    del data
    # This is going to be also the starting time for next analysis
    start_next_analysis = end_interval_analysis.isoformat()
    print(f'Events from {start_interval} to {start_next_analysis} will be analyzed')

    # Execute the function "wrap_analyze_events_multiprocessing" in parallel
    pool.starmap(wrap_analyze_events_multiprocessing, [(arg, arg_i, list_buy_vol, list_vol, list_minutes,
                                        list_event_buy_volume, list_event_volume, dynamic_benchmark_volume, end_interval_analysis, lock, shared_data) for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])

    # Close the pool
    pool.close()
    print('pool closed')
    pool.join()
    print('pool joined')
    del data_arguments, dynamic_benchmark_volume

    data = json.loads(shared_data.value)
    # save new updated analysis to json_analysis/


    # update the analysis json for storing performances
    #updateAnalysisJson(shared_data.value, file_path, start_next_analysis, INTEGRATION)
    updateAnalysisJson(shared_data.value, analysis_json_path, start_next_analysis, slice_i=None, start_next_analysis_str=None, INTEGRATION=INTEGRATION)

    t2 = time()
    print(t2-t1, ' seconds')

def total_function_multiprocessing_lessRAM(list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, n_processes, LOAD_DATA, slice_i):
    '''
    this function loads only the data not analyzed and starts "wrap_analyze_events_multiprocessing" function. Finally it saves the output to a dedicated path

    '''
    t1 = time()
    
    # getData from server. This will be stored in json/
    if LOAD_DATA:
        getData()

    # get dynamic benchmark volume. This will be used for each observation for each coin, to see what was the current volatility (std_dev / mean) of the last 30 days.
    benchmark_info = get_benchmark_info()
    dynamic_benchmark_volume = get_dynamic_volume_avg(benchmark_info)
    del benchmark_info

    # get json_analysis path. Check if there is already some data analyzed in json_analysis/ path. The new data will be appended
    total_combinations = len(list_buy_vol) * len(list_vol) * len(list_minutes) * len(list_event_volume) * len(list_event_buy_volume)
    print('total_combinantions', ': ', total_combinations)
    path = ROOT_PATH + "/analysis_json2/"
    file_path = path + 'analysis' + '.json'

    # initialize Manager for "shared_data". Used for multiprocessing
    manager = Manager()

    if slice_i == 1:
        analysis_timeframe = 4.7333333
    else:
        analysis_timeframe = 3.266 #days. How many days "total_function_multiprocessing" will analyze data from last saved?

    # Load files form json_analysis if exists otherwise initialize. Finally define "start_interval" and "end_interval" for loading the data to be analyzed
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            # Retrieve shared memory for JSON data and "start_interval"
            analysis_json = json.load(file)
            start_next_analysis_str = 'start_next_analysis_' + str(slice_i)
            start_interval = analysis_json[start_next_analysis_str]
            del analysis_json
    else:
        # Create shared memory for JSON data and initialize "start_interval"
        start_interval = datetime(2023,5,11).isoformat()
    
    # define "end_interval" and "filter_position"
    end_interval = min(datetime.now(), datetime.fromisoformat(start_interval) + timedelta(days=analysis_timeframe))
    if slice_i == 1:
        filter_position = (0,185) # this is enough to load all the coins available
    else:
        filter_position = (185,400)

    shared_data = manager.Value(str, json.dumps({}))

    
    # load data from local (json/) and store in "data" variable
    data = load_data(start_interval=datetime.fromisoformat(start_interval), end_interval=end_interval, filter_position=filter_position)
    

    # get data slices for multiprocessing. .e.g divide the data in batches for allowing multiprocessing
    data_arguments = data_preparation(data, n_processes = n_processes)
    

    # initialize lock for processing shared variable between multiple processes
    lock = Manager().Lock()

    # Create a multiprocessing Pool
    pool = Pool()

    # define "end_interval_analysis". this is variable says "do not analyze events that are older than this date".
    # Since "minutes_price_windows" looks for N minutes observations after a specific event, I might get smaller time windows than expected (the most recent ones). This check should avoid this problem.
    if slice_i == 1:
        end_interval_analysis = datetime.fromisoformat(data['BTCUSDT'][-1]['_id']) - timedelta(days=3) #last datetime from btcusdt - 3 days
    else:
        random_coin_slice2 = list(data_arguments[0].keys())[0]
        end_interval_analysis = datetime.fromisoformat(data[random_coin_slice2][-1]['_id']) - timedelta(days=3) #last datetime from btcusdt - 3 days


    del data
    # This is going to be also the starting time for next analysis
    start_next_analysis = end_interval_analysis.isoformat()
    print(f'Events from {start_interval} to {start_next_analysis} will be analyzed')

    # Execute the function "wrap_analyze_events_multiprocessing" in parallel
    pool.starmap(wrap_analyze_events_multiprocessing, [(arg, arg_i, list_buy_vol, list_vol, list_minutes,
                                        list_event_buy_volume, list_event_volume, dynamic_benchmark_volume, end_interval_analysis, lock, shared_data) for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])

    # Close the pool
    pool.close()
    print('pool closed')
    pool.join()
    print('pool joined')
    del data_arguments, dynamic_benchmark_volume

    # update the analysis json for storing performances
    updateAnalysisJson(shared_data.value, file_path, start_next_analysis, slice_i, start_next_analysis_str)

    t2 = time()
    print(t2-t1, ' seconds')

def earlyValidation(minimum_event_number_list, minimum_coin_number, mean_threshold, lb_threshold, frequency_threshold, optimized_gain_threshold,
                     mean_gain_threshold, group_coins=False, best_coins_volatility=None,
                       early_validation=False, OPTIMIZED=False, DISCOVER=False, riskmanagement_path=False, std_multiplier=3):
    
    if riskmanagement_path == False:
        print('RiskManagement Path is NOT provided')
        early_validation_timestamp = early_validation.isoformat()
        print(f'started data fetching from analysis.json until {early_validation_timestamp}')
        output, info = nested_download_show_output(minimum_event_number_list=minimum_event_number_list, minimum_coin_number=minimum_coin_number,
                                        mean_threshold=mean_threshold, lb_threshold=lb_threshold, frequency_threshold=frequency_threshold,
                                        group_coins=group_coins, best_coins_volatility=best_coins_volatility, early_validation=early_validation, std_multiplier=std_multiplier)

        print(f'started data fetching from analysis.json completed')

        print('Starting RiskManagement Configuration')
        riskmanagement_conf = (minimum_event_number_list, minimum_coin_number, mean_threshold, lb_threshold, frequency_threshold, group_coins, best_coins_volatility)
        none_df, riskmanagement_path = RiskConfiguration(info, riskmanagement_conf, optimized_gain_threshold, mean_gain_threshold, early_validation, std_multiplier, DISCOVER)
        print('RiskManagement Configuration Completed')
        DISCOVER=False
    else:
        print('RiskManagement Path is provided')

    event_investment_amount=100
    df, biggest_drop, biggest_drop_date, positive_outcome, negative_outcome, PERFORMANCE_SCENARIOS = analyzeRiskManagementPerformance(riskmanagement_path, OPTIMIZED, DISCOVER, event_investment_amount)
    
    print(f'Positive events: {positive_outcome}')
    print(f'Negative events: {negative_outcome}')
    print(f'Biggest Drop: {biggest_drop} at {biggest_drop_date}')

    return df

def nested_download_show_output(minimum_event_number_list, minimum_coin_number, mean_threshold, lb_threshold, frequency_threshold, group_coins=False, best_coins_volatility=None, early_validation=False, std_multiplier=3):
    '''
    THIS FUNCTION is an evolution of "download_show_output". It tries to fetch the best keys from a set of minimum_coin_number requirement.
    In this way, it is possible to better diversify the strategies.
    '''
    # DOWNLOAD_FILE
    path = ROOT_PATH + "/analysis_json/"
    file_path = path + 'analysis' + '.json'
    with open(file_path, 'r') as file:
        t1 = time()
        print(f'Downloading {file_path}')
        # Retrieve shared memory for JSON data and "start_interval"
        shared_data = json.load(file)
        t2 = time()
        delta_t_1 = round_(t2 - t1,2)
        print(f'Download completed in {delta_t_1} seconds')

    nested_output = []
    nested_info = []
    list_all_keys = []
    volatility_group = {}


    for minimum_event_number, i in zip(minimum_event_number_list, range(1,len(minimum_event_number_list)+1)):
        
        output, complete_info = download_show_output(minimum_event_number, minimum_coin_number, mean_threshold, lb_threshold, frequency_threshold, group_coins, best_coins_volatility, shared_data, early_validation, std_multiplier)
        for key in output:

            volatility = key.split('vlty:')[1]
            if volatility not in volatility_group:
                volatility_group[volatility] = {}
            if str(minimum_event_number) not in volatility_group[volatility]:
                volatility_group[volatility][str(minimum_event_number)] = []
            
            if key not in list_all_keys:
                dict_key_event = output[key].copy()
                dict_key_event['key'] = key
                volatility_group[volatility][str(minimum_event_number)].append(dict_key_event)
                list_all_keys.append(key)
            
        nested_info.append(complete_info)
        nested_output.append(output)

    winner_keys = []
    for volatility in volatility_group:
        for event_number in volatility_group[volatility]:
            volatility_group[volatility][event_number].sort(key=lambda x: x['mean'], reverse=True)
            if len(volatility_group[volatility][event_number]) > 0:
                best_key = volatility_group[volatility][event_number][0]['key']
                #print(f'best_key: {best_key} for {volatility} and {event_number}')
                winner_keys.append(best_key)
    
    final_nested_info = {}
    final_nested_output = {}
    for info, output in zip(nested_info, nested_output):
        for key in info:
            if key in winner_keys:
                final_nested_info[key] = info[key]
                final_nested_output[key] = output[key]

    return final_nested_output, final_nested_info
      
def download_show_output(minimum_event_number, minimum_coin_number, mean_threshold, lb_threshold, frequency_threshold, group_coins=False, best_coins_volatility=None, shared_data=None, early_validation=False, std_multiplier=3):
    '''
    This function takes as input data stored in analysis_json/ ans return the output available for pandas DATAFRAME. 
    This is useful to have a good visualization about what TRIGGER have a better performance
    The input is taken from "analysis_json" path.
    The input is a dict where each key corresponds of a specific TRIGGER (e.g. "buy_vol_15m:0.75/vol_60m:20/timeframe:1440/vlty:3")
    with this function, you can decide whether;
    - set a minimum number of events per key 
    - set a minimum number of coins per key
    - set a minimum mean threshold for all events for a specific key
    - group all keys which share the same TRIGGER (e.g. "buy_vol_15m:0.75/vol_60m:20/timeframe:1440/") but have a different volatility ("vlty:<x>")
    - represent only the best keys grouped by volatility if "group_coins=False" or the best keys grouped by "timeframe" if "group_coins=True"
    - 
    - early_validation: [datetime] this analyses the keys until a certain point in time. Used to check how analysis behaves in the future
    '''

    # download data if shared_data is None
    if shared_data == None:
        path = ROOT_PATH + "/analysis_json/"
        file_path = path + 'analysis' + '.json'
        with open(file_path, 'r') as file:
            t1 = time()
            print(f'Downloading {file_path}')
            # Retrieve shared memory for JSON data and "start_interval"
            shared_data = json.load(file)
            t2 = time()
            delta_t_1 = round_(t2 - t1,2)
            print(f'Download completed in {delta_t_1} seconds')

    #return shared_data
    shared_data = shared_data['data']
    output = {}
    complete_info = {}
    volatility_list = {}

    # group best keys by volatility == False
    if not group_coins:
        for key in list(shared_data.keys()):
            if key != 'coins' or key != 'events':

                # compute number of events
                n_coins = len(shared_data[key]['info'])
                n_events = 0
                mean_list = []
                std_list = []

                # I want to get earliest and latest event
                # intitialize first_event and last_event
                random_coin = list(shared_data[key]['info'].keys())[0]
                first_event = datetime.fromisoformat(shared_data[key]['info'][random_coin][0]['event'])
                last_event =  datetime(1970,1,1)
                
                for coin in shared_data[key]['info']:
                    for event in shared_data[key]['info'][coin]:
                        if early_validation and datetime.fromisoformat(event['event']) > early_validation: 
                            continue
                        else:
                            n_events += 1
                            mean_list.append(event['mean'])
                            std_list.append(event['std'])
                            first_event = min(datetime.fromisoformat(event['event']), first_event)
                            last_event = max(datetime.fromisoformat(event['event']), last_event)
                
                # get frequency event per month
                time_interval_analysis = (last_event - first_event).days
                if time_interval_analysis != 0:
                    event_frequency_month = round_((n_events / time_interval_analysis) * 30,2)

                
                if n_events >= minimum_event_number and n_coins >= minimum_coin_number and event_frequency_month >= frequency_threshold:

                    vol, vol_value, buy_vol, buy_vol_value, timeframe = getsubstring_fromkey(key)
                    volatility = key.split('vlty:')[1]
                    
                    mean = round_(np.mean(np.array(mean_list))*100,2)
                    std = round_(pooled_standard_deviation(np.array(std_list), sample_size=int(timeframe))*100,2)
                    upper_bound = mean + std
                    lower_bound = mean - std

                    if mean > mean_threshold and lower_bound > lb_threshold and mean * std_multiplier > std:

                        if best_coins_volatility:
                            if volatility not in volatility_list:
                                volatility_list[volatility] = [{'key': key, 'upper_bound': upper_bound}]
                            else:
                                volatility_list[volatility].append({'key': key, 'upper_bound': upper_bound})
                    
                        complete_info[key] = {'info': shared_data[key]['info'], 'coins': n_coins, 'events': n_events, 'frequency/month': event_frequency_month}
                        output[key] = {'mean': mean, 'std': std, 'upper_bound': upper_bound, 'lower_bound': lower_bound,
                                        'n_coins': n_coins, 'n_events': n_events, 'frequency/month': event_frequency_month}
        
        if best_coins_volatility:
            keys_to_keep = {}
            for volatility in volatility_list:
                #print(volatility_list[volatility])
                volatility_list[volatility].sort(key=lambda x: x['upper_bound'], reverse=True)
                volatility_list[volatility] = volatility_list[volatility][:best_coins_volatility]
                keys_to_keep[volatility] = [x['key'] for x in volatility_list[volatility]]

            new_output = {}
            new_info = {}
            for key in output:
                volatility = key.split('vlty:')[1]
                if key in keys_to_keep[volatility]:
                    new_output[key] = output[key]
                    new_info[key] = complete_info[key]

            del output, complete_info
            return new_output, new_info
        
    # Group Event keys by volatility == True         
    else:
        for key in list(shared_data.keys()):
            # print(key)
            # print(shared_data[key].keys())
            if key != 'coins' or key != 'events':
                vol, vol_value, buy_vol, buy_vol_value, timeframe = getsubstring_fromkey(key)
                key_split = key.split('/vlty:')
                key_without_volatility = key_split[0]
                n_events = 0
                mean_by_coin_list = []
                std_by_coin_list = []
                list_coins = list(shared_data[key]['info'].keys())

                for coin in shared_data[key]['info']:
                    n_events += len(shared_data[key]['info'][coin])
                    for event in shared_data[key]['info'][coin]:
                        if early_validation and datetime.fromisoformat(event['event']) > early_validation: 
                            continue
                        else:
                            mean_by_coin_list.append(event['mean'])
                            std_by_coin_list.append(event['std'])

                mean_by_coin = round_(np.mean(np.array(mean_by_coin_list))*100,2)
                std_by_coin = round_(pooled_standard_deviation(np.array(std_by_coin_list), sample_size=int(timeframe))*100,2)

                # Start filling output and complete_info
                if key_without_volatility not in output:
                    # initialize output and complete_info
                    output[key_without_volatility] = {'mean': [(mean_by_coin,len(list_coins))], 'std': [(std_by_coin,len(list_coins))], 'n_coins': list_coins, 'n_events': n_events}
                    #update 'info' by adding "volatility" variable
                    complete_info[key_without_volatility] = {'info': {}, 'n_coins': list_coins, 'events': n_events}
                    for coin in shared_data[key]['info']:
                        if coin not in complete_info[key_without_volatility]['info']:
                            # initialize coin in complete_info
                            complete_info[key_without_volatility]['info'][coin] = []
                        for event in shared_data[key]['info'][coin]:
                            # add volatility
                            volatility = int(key.split('/vlty:')[1])
                            event['volatility'] = volatility
                            complete_info[key_without_volatility]['info'][coin].append(event)

                else:
                    # update output
                    output[key_without_volatility]['mean'].append((mean_by_coin,len(list_coins)))
                    output[key_without_volatility]['std'].append((std_by_coin,len(list_coins)))
                    output[key_without_volatility]['n_coins'] += list_coins
                    output[key_without_volatility]['n_events'] += n_events
                    
                    # update complete info
                    complete_info[key_without_volatility]['n_coins'] += list_coins
                    complete_info[key_without_volatility]['events'] += n_events
                    for coin in shared_data[key]['info']:
                        if coin not in complete_info[key_without_volatility]['info']:
                            complete_info[key_without_volatility]['info'][coin] = []
                        for event in shared_data[key]['info'][coin]:
                            volatility = int(key.split('/vlty:')[1])
                            event['volatility'] = volatility
                            complete_info[key_without_volatility]['info'][coin].append(event)

        delete_keys = []
        # Filter best keys by performance
        for key_without_volatility in output:
            mean = sum([item[0]*item[1] for item in output[key_without_volatility]['mean']]) / sum([item[1] for item in output[key_without_volatility]['mean']] )
            std = sum([item[0]*item[1] for item in output[key_without_volatility]['std']]) / sum([item[1] for item in output[key_without_volatility]['std']] )
            if output[key_without_volatility]['n_events'] >= minimum_event_number and mean > mean_threshold and mean <= std_multiplier * std:
                output[key_without_volatility]['mean'] = mean
                output[key_without_volatility]['std'] = std
                output[key_without_volatility]['upper_bound'] = output[key_without_volatility]['mean'] + output[key_without_volatility]['std']
                output[key_without_volatility]['lower_bound'] = output[key_without_volatility]['mean'] - output[key_without_volatility]['std']
                output[key_without_volatility]['n_coins'] = len(set(output[key_without_volatility]['n_coins']))
                complete_info[key_without_volatility]['n_coins'] = len(set(complete_info[key_without_volatility]['n_coins']))
            else:
                delete_keys.append(key_without_volatility)

        for key_without_volatility in delete_keys:
            output.pop(key_without_volatility)
            complete_info.pop(key_without_volatility)

        # Filter the best that share the same trigger (e.g. buy_vol_6h:0.75/vol_6h:10) but different timeframe
        # in this case "best_coins_volatility" is intended for best coins by timeframe.

        
        if best_coins_volatility:
            # ORDER BY PERFORMANCE
            keys_filtered_by_timeframe = {}
            for key_without_volatility in output:
                key_without_timeframe = key_without_volatility.split('/timeframe')[0]
                if key_without_timeframe not in keys_filtered_by_timeframe:
                    keys_filtered_by_timeframe[key_without_timeframe] = []
                keys_filtered_by_timeframe[key_without_timeframe].append({'key': key_without_volatility, 'mean': output[key_without_volatility]['mean']})
            
            # GET THE BEST KEYS
            keys_to_keep = {}
            for key_without_timeframe in keys_filtered_by_timeframe:
                #print(volatility_list[volatility])
                keys_filtered_by_timeframe[key_without_timeframe].sort(key=lambda x: x['mean'], reverse=True)
                keys_filtered_by_timeframe[key_without_timeframe] = keys_filtered_by_timeframe[key_without_timeframe][:best_coins_volatility]
                keys_to_keep[key_without_timeframe] = [x['key'] for x in keys_filtered_by_timeframe[key_without_timeframe]]

            # UPDATE OUTPUT AND COMPLLETE INFO
            new_output = {}
            new_info = {}
            for key_without_volatility in output:
                key_without_timeframe = key_without_volatility.split('/timeframe')[0]
                if key_without_volatility in keys_to_keep[key_without_timeframe]:
                    new_output[key_without_volatility] = output[key_without_volatility]
                    new_info[key_without_volatility] = complete_info[key_without_volatility]

            del output, complete_info
            return new_output, new_info
            
        

def plot_live_timeseries(risk_management_path, filter_live: bool = False, filter_best: float = False):
    '''
    This function plots the timeseries from a riskmanagement configuration and filters only the events older than the creation of the riskmanagement itself
    '''
    if filter_live:
        risk_management_path_split = risk_management_path.split('-')
        year = int(risk_management_path_split[1])
        month = int(risk_management_path_split[2])
        day = int(risk_management_path_split[3])
        start_plot_datetime = datetime(year=year, month=month, day=day)
    else:
        start_plot_datetime = False

    with open(risk_management_path, 'r') as file:
        # Retrieve timeseries
        risk_management_configuration = json.load(file)
    
    list_of_keys = risk_management_configuration['Dataframe']['keys']

    for key in list_of_keys:
        print("#######################")
        print('Plotting :', key)
        print("#######################")
        print('')

        key_json = key.replace(':', '_')
        key_json = key_json.replace('/', '_')
        path = ROOT_PATH + "/timeseries_json/"
        file_path = path + key_json + '.json'
        vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe = getsubstring_fromkey(key)
        fields = [vol_field, buy_vol_field, timeframe, buy_vol_value, vol_value]

        with open(file_path, 'r') as file:
            # Retrieve timeseries
            timeseries = json.load(file)

        
        plotTimeseries(timeseries, fields, check_past=1440, plot=True, filter_start=start_plot_datetime, filter_best=filter_best)


def getTimeseries(info, key, check_past=False, look_for_newdata=False, plot=False):
    '''
    This function retrieves the timeseries based on "info" and "key"
    "info" is the output of function "download_show_output" and key is the name of event list (e.g. "buy_vol_5m:0.65/vol_24h:8/timeframe:1440/vlty:1")
    It downloads the data from server if not exists. otherwise the data is downloaded from "/timeseries_json"

    if "check_past" is not False, it is an Integer. It is used to retrieve all the observations occurred before the Event. It is expressed in minutes.
    "look_for_newdata" is a boolean. if True, it looks for NEW timeseries (triggered by an event) in the db server.
    '''

    # load from local or from server
    key_json = key.replace(':', '_')
    key_json = key_json.replace('/', '_')

    path = ROOT_PATH + "/timeseries_json/"
    file_path = path + key_json + '.json'

    # get substrings (vol, buy_vol, timeframe) from key
    # vol, vol_value, buy_vol, buy_vol_value, timeframe
    vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe = getsubstring_fromkey(key)

    fields = [vol_field, buy_vol_field, timeframe, buy_vol_value, vol_value]

    if os.path.exists(file_path):
        print(f'File exists, Download from loca: {file_path}')
        with open(file_path, 'r') as file:
            # Retrieve timeseries
            timeseries = json.load(file)

        if look_for_newdata:
            
            # prepare usual request for https://algocrypto.eu/analysis/get-timeseries
            request = info[key]
            request['timeframe'] = int(timeframe)
            if not check_past:
                pass
            else:
                request['check_past'] = check_past
            
            request['last_timestamp'] = {}
            # define for each from which timestamp new events should be discovered
            for coin in timeseries:
                #get most recent timestamp
                most_recent_timestamp = list(timeseries[coin].keys())[-1]

                request['last_timestamp'][coin] = most_recent_timestamp

            url = 'https://algocrypto.eu/analysis/get-timeseries'

            # send request
            response = requests.post(url, json = request)
            print('Status Code is : ', response.status_code)
            response = json.loads(response.text)
            new_timeseries = response['data']
            msg = response['msg']
            print(msg)

            n_events = 0
            # let's update "timeseries" with the new events occurred in "new_timeseries"
            for coin in new_timeseries:
                # iterate through NEW each event of the coin
                for timestamp_start in list(new_timeseries[coin].keys()):
                    # if coin does not exist in timeseries (an event has never occurred before). let's create this key in "timeseries"
                    n_events += 1
                    if coin not in timeseries:
                        timeseries[coin] = {}
                    timeseries[coin][timestamp_start] = new_timeseries[coin][timestamp_start]
                    
            print(f'{n_events} new events for {key}')
            with open(file_path, 'w') as file:
                json.dump(timeseries, file)

            del new_timeseries
            

                
    else:
        print('File does not exist, Download from server...')

        # build the request body
        request = info[key]
        request['timeframe'] = int(timeframe)

        if not check_past:
            pass
        else:
            request['check_past'] = check_past

        #url = 'http://localhost/analysis/get-timeseries'
        url = 'https://algocrypto.eu/analysis/get-timeseries'

        response = requests.post(url, json = request)
        print('Status Code is : ', response.status_code)
        response = json.loads(response.text)
        timeseries = response['data']
        msg = response['msg']
        print(msg)

        with open(file_path, 'w') as file:
            json.dump(timeseries, file)
    
    plotTimeseries(timeseries, fields, check_past, plot)

    # this part is added for "RiskConfiguration" function, in order to retry the request if the following message is received
    if msg == "WARNING: Request too big. Not all data have been downloaded, retry...":
        return True
    else:
        return False

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

def plotTimeseries(timeseries, fields, check_past, plot, filter_start=False, filter_best=False):

    vol_field = fields[0]
    buy_vol_field = fields[1]
    timeframe = fields[2]
    buy_vol_value = fields[3]
    vol_value = fields[4]

    # iterate through each coin
    for coin in timeseries:
        #print(coin)
        # iterate through each event of the coin
        for timestamp_start in list(timeseries[coin].keys()):
            
            # skip if you want to plot only live timeseries
            if filter_start and datetime.fromisoformat(timestamp_start) < filter_start:
                continue

            #print(timestamp_start)
            timeframe = timeseries[coin][timestamp_start]['statistics']['timeframe']
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
            
            if skip_timeseries:
                continue
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
                ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
                ax[0].grid(True)


                # Plotting the second time series
                ax[1].plot(datetime_list, vol_list)
                ax[1].set_ylabel(f'{vol_field}:{vol_value}')
                ax[1].axvline(x=datetime.fromisoformat(timestamp_start), color='blue', linestyle='--')
                ax[1].axvline(x=timestamp_end, color='blue', linestyle='--')
                ax[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=interval))
                ax[1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
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
                ax[2].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
                ax[2].annotate(f'Volume event: {buy_volume_event[1]}', xy=(buy_volume_event[0], buy_volume_event[1]),
                                xytext=(buy_volume_event[0], buy_volume_event[1]),
                                textcoords='data', ha='center', va='bottom',arrowprops=dict(arrowstyle='->'))
                ax[2].grid(True)

                # Display the graph
                plt.show()

def RiskManagement_lowest_level(tmp, timeseries_json, coin, start_timestamp, risk_key,
                                 STEP_original, GOLDEN_ZONE_original, STEP_NOGOLDEN, timeframe, extratimeframe):
    '''
    this is the lowest level in the Riskmanagemnt process
    It is called by "RiskManagement_multiprocessing"
    '''
    
    STEP = copy(STEP_original)
    GOLDEN_ZONE = copy(GOLDEN_ZONE_original)
    #print(f'new event: {coin}; {start_timestamp}')
    #coin_list.append(coin)

    #STEP = 0.05
    GOLDEN_ZONE_BOOL = False
    #GOLDEN_ZONE = 0.3 #
    GOLDEN_ZONE_LB = GOLDEN_ZONE - STEP
    GOLDEN_ZONE_UB = GOLDEN_ZONE + STEP
    LB_THRESHOLD = None
    UB_THRESHOLD = None

    iterator = timeseries_json[coin][start_timestamp]['data']
    #initial_price = iterator[0]['price']
    SKIP = True

    # iterate through each obs
    for obs, obs_i in zip(iterator, range(1, len(iterator) + 1)):
        #print(GOLDEN_ZONE_BOOL, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP)
        if obs['_id'] == start_timestamp:
            SKIP = False
            initial_price = obs['price']
            max_price = obs['price']
            min_price = obs['price']
        
        if SKIP == False:
            current_price = obs['price']
            max_price = max(obs['price'], max_price)
            min_price = min(obs['price'], min_price)
            
            current_change = (current_price - initial_price) / initial_price

            # if I'm already in GOLDEN ZONE, then just manage this scenario
            if GOLDEN_ZONE_BOOL:
                SELL, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP, GOLDEN_ZONE_BOOL = manageGoldenZoneChanges(current_change, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP, GOLDEN_ZONE_BOOL)
                if SELL:
                    tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, True,
                                                        max_price, min_price)                        
                    break

            # check if price went above golden zone
            if current_change > GOLDEN_ZONE:
                SELL, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP, GOLDEN_ZONE_BOOL = manageGoldenZoneChanges(current_change, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP, GOLDEN_ZONE_BOOL)

            # check if minimum time window has passed
            elif datetime.fromisoformat(obs['_id']) > datetime.fromisoformat(start_timestamp) + timedelta(minutes=timeframe):
                SELL, LB_THRESHOLD, UB_THRESHOLD = manageUsualPriceChanges(current_change, LB_THRESHOLD, UB_THRESHOLD, STEP_NOGOLDEN)
                if SELL:
                    tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, True,
                                                        max_price, min_price)
                    break
            
            #print(int(timeframe * extratimeframe))
            extra = int(timeframe * extratimeframe)
            if datetime.fromisoformat(obs['_id']) > datetime.fromisoformat(start_timestamp) + timedelta(minutes=timeframe) + timedelta(minutes= extra):
                tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, False,
                                                    max_price, min_price)
                break
                
            elif obs_i == len(iterator):
                tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, False,
                                                    max_price, min_price)
                
    return tmp
                                        
def RiskManagement_multiprocessing(timeseries_json, arg_i, EXTRATIMEFRAMES, STEPS_NOGOLDEN, STEPS_GOLDEN,
                                        GOLDEN_ZONES, timeframe, lock, early_validation, results):
    '''
    This function is used by def "RiskManagement". The purpose is to analyze the best combinations of variables for handling the risk management
    Similar to "total_function_multiprocessing", a multiprocessing task gets carried out.
    '''
    
    tmp = {}
    for extratimeframe in EXTRATIMEFRAMES:
        for STEP_NOGOLDEN in STEPS_NOGOLDEN:
            for STEP_original in STEPS_GOLDEN:
                for GOLDEN_ZONE_original in GOLDEN_ZONES:

                    risk_key = 'risk_golden_zone:' + str(GOLDEN_ZONE_original) + '_step:' + str(STEP_original) + '_step_no_golden:' + str(STEP_NOGOLDEN) + '_extratime:' + str(round_(extratimeframe,2))
                    tmp[risk_key] = {}

                    # iterate through each coin
                    for coin in timeseries_json:
                        
                        # check if there is at least a time window of "timeframe" between 2 events of the same coin
                        timestamp_list = list(timeseries_json[coin].keys())
                        timestamp_list = sorted(timestamp_list)
                        timestamp_to_analyze = [timestamp_list[0]]

                        if len(timestamp_list) > 1:
                            for timestamp in timestamp_list[1:]:
                                if datetime.fromisoformat(timestamp) - datetime.fromisoformat(timestamp_to_analyze[-1]) > timedelta(minutes=timeframe):
                                    timestamp_to_analyze.append(timestamp)
                                #else:
                                    #print(f'found duplicate for {coin} between {timestamp} and {timestamp_to_analyze[-1]}')


                        # iterate through each event
                        #for start_timestamp in timestamp_to_analyze:
                        for start_timestamp in timestamp_list:
                            if early_validation and datetime.fromisoformat(start_timestamp) > early_validation:
                                continue
                            else:
                                # start the riskmanagement analysis for this event
                                tmp = RiskManagement_lowest_level(tmp, timeseries_json, coin, start_timestamp, risk_key,
                                 STEP_original, GOLDEN_ZONE_original, STEP_NOGOLDEN, timeframe, extratimeframe)
    with lock:
        resp = json.loads(results.value)
        for key in list(tmp.keys()):
            
            if key not in resp:
                resp[key] = {}

            # update complete "info"
            for start_timestamp in tmp[key]:
                resp[key][start_timestamp] = tmp[key][start_timestamp]

        results.value = json.dumps(resp)

def prepareOptimizedConfigurarionResults(results, event_key):
    '''
    This function takes as input the results of the RiskManagement process and prepares the output
    for the optimized results in the "optimized_results_backup" directory
    '''

    # initialize variables
    profit_list = {}
    timestamp_exit_list = {}
    buy_price_list = {}
    exit_price_list = {}
    coin_list = {}
    early_sell = {}
    max_price_list = {}
    min_price_list = {}
    start_timestamp_list = {}

    # risk key is defined as
    # risk_key = 'risk_golden_zone:' + str(GOLDEN_ZONE_original) + '_step:' + str(STEP_original) + '_step_no_golden:' + str(STEP_NOGOLDEN) + '_extratime:' + str(round_(extratimeframe,2))
    # it is not the event_key
    for risk_key in list(results.keys()):
        profit_list[risk_key] = []
        timestamp_exit_list[risk_key] = []
        buy_price_list[risk_key] = []
        exit_price_list[risk_key] = []
        coin_list[risk_key] = []
        early_sell[risk_key] = []
        max_price_list[risk_key] = []
        min_price_list[risk_key] = []
        start_timestamp_list[risk_key] = []

        for start_timestamp in results[risk_key]:
            coin = results[risk_key][start_timestamp][4]
            profit_list[risk_key].append(results[risk_key][start_timestamp][0])
            timestamp_exit_list[risk_key].append(results[risk_key][start_timestamp][1])
            buy_price_list[risk_key].append(results[risk_key][start_timestamp][2])
            exit_price_list[risk_key].append(results[risk_key][start_timestamp][3])
            coin_list[risk_key].append(coin)
            early_sell[risk_key].append(results[risk_key][start_timestamp][5])
            max_price_list[risk_key].append(results[risk_key][start_timestamp][6])
            min_price_list[risk_key].append(results[risk_key][start_timestamp][7])
            
            start_timestamp_list[risk_key].append(start_timestamp)

    # choose the best risk key
    best_risk_key = ''
    best_profit = - 10

    risk_key_df = []
    profit_mean_list_df = []
    profit_std_list_df = []

    risk_configuration_list = []
    for risk_key in profit_list:
        profit = np.mean(profit_list[risk_key])
        std = np.std(profit_list[risk_key])

        risk_key_df.append(risk_key)
        profit_mean_list_df.append(profit)
        profit_std_list_df.append(std)

        n_events = len(profit_list[risk_key])
        profit_print = round_(profit*100,2)

        if profit > best_profit:
            best_profit = profit
            best_std = std
            best_risk_key = risk_key
            best_profit_print = round_(best_profit*100,2)
            best_std_print = round_(best_std*100,2)
    
    #Finally make the request to the server for getting the price changes, volumes, buy_volumes history for each event.
    # Then, define info_obj
    # prepping request for price changes, we need event_key, coin and timestamp
    request = {event_key: {}}
    for coin, start_timestamp in zip(coin_list[best_risk_key], start_timestamp_list[best_risk_key]):
        if coin not in request[event_key]:
            request[event_key][coin] = []
        request[event_key][coin].append(start_timestamp)

    # make request to the server
    response = requests.post(url='https://algocrypto.eu/analysis/get-pricechanges', json=request)
    response = json.loads(response.text)
    pricechanges = response['data']

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

    # assuming the info keys are equal for all events, get list of keys (timeframes) in this way
    random_coin = list(pricechanges[event_key].keys())[0]
    random_start_timestamp = list(pricechanges[event_key][random_coin].keys())[0]
    info_keys = list(pricechanges[event_key][random_coin][random_start_timestamp].keys()) # info key example: [info_1h, info_3h, ..., info_7d]
    
    # initialize info_obj by creating a list of None values for each price_key, vol_key and buyvol_key
    # price_key, vol_key and buyvol_key are extraced from "info_keys"
    info_obj = {}
    for info_key in info_keys:
        timeframe = info_key.split('_')[-1] # example: "1h" or "3h" or ... "7d"
        price_key = 'price_%_' + timeframe
        vol_key = 'vol_' + timeframe
        buy_vol_key = 'buy_' + timeframe
        info_obj[price_key] = [None] * len(start_timestamp_list[best_risk_key])
        info_obj[vol_key] = [None] * len(start_timestamp_list[best_risk_key])
        info_obj[buy_vol_key] = [None] * len(start_timestamp_list[best_risk_key])

    # fill "info_obj" (price changes, vol, buy_vol) according to the position of "start_timestamp_list" of the best resk key
    for coin in pricechanges[event_key]:
        for start_timestamp in pricechanges[event_key][coin]:
            position = start_timestamp_list[best_risk_key].index(start_timestamp)
            for info_key in info_keys:
                timeframe = info_key.split('_')[-1]
                price_key = 'price_%_' + timeframe
                vol_key = 'vol_' + timeframe
                buy_vol_key = 'buy_' + timeframe

                # example of pricechanges[event_key][coin][start_timestamp][info_key] --> (0.02, 1.5, 0.5)
                # the first element is the price change with respect to "timeframe" ago
                # the second element is the volume registered "timeframe" ago
                # the third element is the buy_volume registered "timeframe" ago

                info_obj[price_key][position] = pricechanges[event_key][coin][start_timestamp][info_key][0]
                info_obj[vol_key][position] = pricechanges[event_key][coin][start_timestamp][info_key][1]
                info_obj[buy_vol_key][position] = pricechanges[event_key][coin][start_timestamp][info_key][2]
    
    optimized_riskconfiguration_results = {'events': list(results[best_risk_key].keys()), 'gain': profit_list[best_risk_key], 'buy_price': buy_price_list[best_risk_key],
                         'exit_price': exit_price_list[best_risk_key],  'timestamp_exit': timestamp_exit_list[best_risk_key],
                           'coin': coin_list[best_risk_key], 'early_sell': early_sell[best_risk_key], 'max_price': max_price_list[best_risk_key], 'min_price': min_price_list[best_risk_key]}
    
    # update optimized_riskconfiguration_results with the lists of "info_obj"
    for info_key in info_obj:
        optimized_riskconfiguration_results[info_key] = info_obj[info_key]
    
    return optimized_riskconfiguration_results, (n_events, best_profit, best_risk_key, best_profit_print, risk_key_df, profit_mean_list_df, profit_std_list_df, best_std_print)
    
def RiskManagement(key, early_validation, investment_per_event=100):

    '''
    This function is called by RiskConfiguration.
    This function focuses on one key event and prepares for multiprocessing
    '''
    EXTRATIMEFRAMES = [0]
    STEPS_NOGOLDEN = [0.01]
    STEPS_GOLDEN = [0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.3]
    GOLDEN_ZONES = [0.10, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    
    n_combinations = len(EXTRATIMEFRAMES) * len(STEPS_NOGOLDEN) * len(STEPS_GOLDEN) * len(GOLDEN_ZONES)
    print(f'Number of combinations {n_combinations}')

    # get timeseries
    timeseries_path = ROOT_PATH + "/timeseries_json/"
    timeseries_file = key.replace(':', '_')
    timeseries_file = timeseries_file.replace('/', '_')
    timeseries_full_path = timeseries_path + timeseries_file + '.json'
    if not os.path.exists(timeseries_full_path):
        #response = getTimeseries(info, key, check_past=360, look_for_newdata=True)
        print(f'KEY DOES NOT EXIST. Download timeseries {key} ')
        return False, False, False, False

    f = open(timeseries_full_path, "r")
    timeseries_json = json.loads(f.read())

    # get timeframe
    vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe = getsubstring_fromkey(key)
    timeframe = int(timeframe) - 0.2*int(timeframe)

    # MULTIPROCESSING
    data_arguments = riskmanagement_data_preparation(timeseries_json, n_processes=8)
    manager = Manager()
    results = manager.Value(str, json.dumps({}))
    lock = Manager().Lock()
    pool = Pool()
    pool.starmap(RiskManagement_multiprocessing, [(arg, arg_i, EXTRATIMEFRAMES, STEPS_NOGOLDEN, STEPS_GOLDEN,
                                        GOLDEN_ZONES, timeframe, lock, early_validation, results) for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])

    pool.close()
    pool.join()

    # split data
    results = json.loads(results.value)
    optimized_riskconfiguration_results, other_vars = prepareOptimizedConfigurarionResults(results, key)
    # with open('tmp.json', 'w') as outfile:
    #     json.dump(optimized_riskconfiguration_results, outfile)

    n_events = other_vars[0]
    best_profit = other_vars[1]
    best_risk_key = other_vars[2]
    best_profit_print = other_vars[3]
    risk_key_df = other_vars[4]
    profit_mean_list_df = other_vars[5]
    profit_std_list_df = other_vars[6]
    best_std_print = other_vars[7]
    #n_events, best_profit, best_risk_key, best_profit_print, risk_key_df, profit_mean_list_df, profit_std_list_df  
    

    profit = round_((investment_per_event * n_events) * (best_profit),2)
    print(f'{profit} euro of profit for an investment of {investment_per_event} euro per event (total of {n_events} events). {best_risk_key} with {best_profit_print} %')
    df1 = pd.DataFrame({'risk_key': risk_key_df, 'mean': profit_mean_list_df, 'std': profit_std_list_df})
    df2 = pd.DataFrame(optimized_riskconfiguration_results)
    
    # mean of all events per key
    mean_all_configs_print = round_(np.mean(profit_mean_list_df)*100,2)
    std_all_configs_print = round_(np.mean(profit_std_list_df)*100,2)
    median_all_configs_print = round_(np.median(profit_mean_list_df)*100,2)

    risk_configuration = {'best_risk_key': best_risk_key, 'best_mean_print':  best_profit_print, 'best_std_print': best_std_print,
                           'statistics': {'mean': mean_all_configs_print, 'std': std_all_configs_print, 'median': median_all_configs_print}}
    
    return df1, df2, risk_configuration, optimized_riskconfiguration_results

def riskmanagement_data_preparation(data, n_processes):
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
                           
    # for data_argument, index in zip(data_arguments, range(len(data_arguments))):
    #     len_data_argument = len(data_argument)
    #     print(f'Length of data argument {index}: {len_data_argument}')

    return data_arguments

def manageGoldenZoneChanges(current_change, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP, GOLDEN_ZONE_BOOL):
    GOLDEN_ZONE_BOOL = True
    SELL = False

    #print(GOLDEN_ZONE_UB, GOLDEN_ZONE_LB)

    if current_change > GOLDEN_ZONE_UB:
        #print('UB touched: ', current_change)
        GOLDEN_ZONE_UB += STEP
        GOLDEN_ZONE_LB += STEP
        #STEP += 0.01

    elif current_change <= GOLDEN_ZONE_LB:
        #print('LB touched: ', current_change)
        SELL = True
        return SELL, None, None, None, None
    

    return SELL, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP, GOLDEN_ZONE_BOOL

def manageUsualPriceChanges(current_change, LB_THRESHOLD, UB_THRESHOLD, STEP):
    SELL = False

    if LB_THRESHOLD == None:
        LB_THRESHOLD = current_change - STEP
        UB_THRESHOLD = current_change + STEP
        return SELL, LB_THRESHOLD, UB_THRESHOLD
    
    if current_change > UB_THRESHOLD:
        UB_THRESHOLD += STEP
        LB_THRESHOLD += STEP
    
    elif current_change <= LB_THRESHOLD:
        SELL = True
        return SELL, None, None
    
    return SELL, LB_THRESHOLD, UB_THRESHOLD

def infoTimeseries(info, key):
    '''
    This function plots the performance of a KEY event, without regard of risk management
    '''

    timeseries_info = []

    # sort all the vents chronologically
    for coin in info[key]['info']:
        for event in info[key]['info'][coin]:
            timeseries_info.append((event, coin))

    timeseries_info.sort(key=lambda x: x[0]['event'], reverse=False)

    timestamp_timeseries = []
    datetime_timeseries = []
    mean_timeseries = []
    std_timeseries = []
    mean_list = []
    std_list = []
    coin_list = []

    for event, i in zip(timeseries_info, range(len(timeseries_info))):
        
        mean_list.append(event[0]['mean'])
        std_list.append(event[0]['std'])
        coin_list.append(event[1])

        mean_timeseries.append(np.mean(mean_list))
        std_timeseries.append(np.mean(std_list))
        timestamp_timeseries.append(event[0]['event'])

    upper_bound = np.array(mean_timeseries) + np.array(std_timeseries)
    lower_bound = np.array(mean_timeseries) - np.array(std_timeseries)

    for timestamp in timestamp_timeseries:
        datetime_timeseries.append(datetime.fromisoformat(timestamp))

    df = pd.DataFrame({'event': timestamp_timeseries, 'mean_series': mean_timeseries, 'mean_event': mean_list, 'std_event': std_list, 'coin': coin_list})
    fig, ax = plt.subplots(1, 1, sharex=True, figsize=(10, 6))

    ax.plot(mean_timeseries)
    ax.plot(upper_bound)
    ax.plot(lower_bound)
    ax.axhline(y=0, color='red', linestyle='--')
    return df

def check_investment_amount(info, output, investment_amount = 100, riskmanagement_path=None):
    '''
    This function helps to understand the account balance required for investing based on "investment_amount"
    '''
    investment_list_info = []
    if riskmanagement_path:
        if os.path.exists(riskmanagement_path):
            with open(riskmanagement_path, 'r') as file:
                # Retrieve shared memory for JSON data and "start_interval"
                riskmanagement_dict = json.load(file)
                riskmanagement_integration = riskmanagement_dict['Dataframe']["keys"]
                print(riskmanagement_integration)
        else:
            print(f'{riskmanagement_path} does not exist')
        

    for key in output:
        #print(key)
        if riskmanagement_path:
            if key not in riskmanagement_integration:
                print('KEY NOT PRESENT:', key)
                continue
            # else:
            #     #print('PRESENT:', key)

        vol, vol_value, buy_vol, buy_vol_value, timeframe = getsubstring_fromkey(key)
        for coin in info[key]['info']:
            for event in info[key]['info'][coin]:
                obj1 = {'event': event['event'], 'side': 1}
                exit_timestamp =  (datetime.fromisoformat(event['event']) + timedelta(minutes=int(timeframe))).isoformat()
                obj2 = {'event': exit_timestamp, 'side': -1}

                investment_list_info.append(obj1)
                investment_list_info.append(obj2)
    
    investment_list_info.sort(key=lambda x: x['event'], reverse=False)

    datetime_list = []
    investment_list = []
    total_investment_amount = 0

    for investement_event in investment_list_info:
        datetime_list.append(datetime.fromisoformat(investement_event['event']))
        total_investment_amount += investement_event['side'] * investment_amount
        investment_list.append(total_investment_amount)

    average_investment = np.mean(investment_list)

    plt.figure(figsize=(10, 6))
    plt.axhline(y=average_investment, color='red', linestyle='--')
    plt.plot(datetime_list, investment_list)
    plt.xlabel('Time')
    plt.ylabel('Capital Investement (euro)')
    plt.title('Dynamic Capital Investment')
    plt.grid(True)
    plt.xticks(rotation=45)  # Rotate x-axis labels for better visibility
    plt.tight_layout()  # Adjust layout to prevent overlapping labels
    plt.show()

def RiskConfiguration(info, riskmanagement_conf, optimized_gain_threshold, mean_gain_threshold, early_validation=False, std_multiplier=3, DISCOVER=False):
    '''
    This functions has the objective to define the best risk configuration for a selected number of keys.
    "download_show_output" function will provide the input
    the riskmanagement_configuration will be used during the live trading for optimization /risk management

    this function is called by "earlyValidation"
    '''
    t1 = time()
    keys_list = list(info.keys())
    n_keys = len(keys_list)
    print(f'{n_keys} keys will be analyzed in terms of risk configuration')
    risk_configuration = {}
    total_optimized_riskconfiguration_results = {}

    for key, key_i in zip(keys_list, range(1,len(keys_list)+1)):
        print(key)
        print(f'ITERATION {key_i} has started')

        if 'vlty' in key:
            VOLATILITY_GROUP = True
            volatility = key.split('vlty:')[1]
            # initialize key in risk_configuration
            if volatility not in risk_configuration:
                risk_configuration[volatility] = {}
        else:
            VOLATILITY_GROUP = False
        
        if DISCOVER:
        # get latest timeseries
            retry = True        
            # if response is not complete, retry with a new request
            while retry:
                retry = getTimeseries(info, key, check_past=1440, look_for_newdata=True, plot=False)


        df1, df2, risk, optimized_riskconfiguration_results = RiskManagement(key, early_validation)
        best_risk_key = risk['best_risk_key']
        best_mean_print = risk['best_mean_print']
        best_std_print = risk['best_std_print']
        statistica_all_configs = risk['statistics']
        mean = statistica_all_configs['mean']
        std = statistica_all_configs['std']
        median = statistica_all_configs['median']

        golden_zone_str = get_substring_between(best_risk_key, "risk_golden_zone:", "_step:")
        golden_step_str = get_substring_between(best_risk_key, "_step:", "_step_no_golden:")
        nogolden_step_str = get_substring_between(best_risk_key, "_step_no_golden:", "_extratime:")
        extratime = best_risk_key.split('_extratime:')[1]
        frequency = info[key]["frequency/month"]
        n_events =  info[key]["events"]

        total_optimized_riskconfiguration_results[key] = optimized_riskconfiguration_results

        # FILTER ONLY THE BEST KEY EVENTS
        obj = {
                'riskmanagement_conf': {
                    'golden_zone': golden_zone_str,
                    'step_golden': golden_step_str,
                    'step_nogolden': nogolden_step_str,
                    'extra_timeframe': extratime,
                    'optimized_gain': best_mean_print,
                    'optimized_std': best_std_print,
                    'frequency': frequency,
                    'n_events' : n_events,
                    'mean_gain_all_configs': mean,
                    'median_gain_all_configs': median,
                    'std_gain_all_configs': std,
                }}
        if VOLATILITY_GROUP:
            if best_mean_print >= optimized_gain_threshold and mean >= mean_gain_threshold:
                risk_configuration[volatility][key] = obj
            else:
                risk_configuration[key] = obj
        

    key_list = []
    golden_zone_list = []
    step_golden_list = []
    step_nogolden_list = []
    extra_timeframe_list = []
    optimized_gain_list = []
    optimized_std_list = []
    frequency_list = []
    n_events_list = []
    mean_gain_list = []
    median_gain_list = []
    std_gain_list = []

    # GET INFO FOR KEYS SELECTION (same info of "download_show_output")
    risk_configuration_dict = {
        "minimum_event_number": riskmanagement_conf[0],
        "minimum_coin_number": riskmanagement_conf[1],
        "mean_threshold": riskmanagement_conf[2],
        "lb_threshold": riskmanagement_conf[3],
        "frequency_threshold": riskmanagement_conf[4],
        "group_coins": riskmanagement_conf[5],
        "best_coins_volatility": riskmanagement_conf[6]
    }

     # PREPARE FILES FOR PANDAS AND FOR SAVING
    if VOLATILITY_GROUP:
        for volatility in risk_configuration:
            for key in risk_configuration[volatility]:
                key_list.append(key)
                golden_zone_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['golden_zone'])
                step_golden_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['step_golden'])
                step_nogolden_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['step_nogolden'])
                extra_timeframe_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['extra_timeframe'])
                optimized_gain_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['optimized_gain'])
                optimized_std_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['optimized_std'])
                mean_gain_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['mean_gain_all_configs'])
                median_gain_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['median_gain_all_configs'])
                std_gain_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['std_gain_all_configs'])
                frequency_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['frequency'])
                n_events_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['n_events'])
    else:
        for key in risk_configuration:
            key_list.append(key)
            golden_zone_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['golden_zone'])
            step_golden_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['step_golden'])
            step_nogolden_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['step_nogolden'])
            extra_timeframe_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['extra_timeframe'])
            optimized_gain_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['optimized_gain'])
            optimized_std_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['optimized_std'])
            mean_gain_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['mean_gain_all_configs'])
            median_gain_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['median_gain_all_configs'])
            std_gain_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['std_gain_all_configs'])
            frequency_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['frequency'])
            n_events_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['n_events'])
            

    average_gain_percentage = sum(np.array(optimized_gain_list) * (np.array(frequency_list)/sum(frequency_list))) / 100
    df_dict = {"keys": key_list, "golden_zone": golden_zone_list, 'step_golden': step_golden_list, 'step_nogolden': step_nogolden_list,
                'extra_timeframe': extra_timeframe_list, 'optimized_gain': optimized_gain_list, 'optimized_std': optimized_std_list,
                  'mean_gain': mean_gain_list, 'median_gain': median_gain_list, 'std_gain': std_gain_list,
                  'frequency': frequency_list, 'n_events': n_events_list}
    

    
    risk_management_config_json = {'Timestamp': datetime.now().isoformat(), 'Info': risk_configuration_dict,
                                    'Dataframe': df_dict, 'RiskManagement': risk_configuration, 'Gain': {'average_per_event': round_(average_gain_percentage,2), 'n_events_per_month': sum(frequency_list)}}

    # SAVE FILE
    if early_validation == False:
        file_path_riskmanagement = ROOT_PATH + "/riskmanagement_json/riskmanagement.json"
        with open(file_path_riskmanagement, 'w') as file:
            json.dump(risk_management_config_json, file)

        file_path_optimized_results = ROOT_PATH + "/optimized_results/optimized_results.json"
        with open(file_path_optimized_results, 'w') as file:
            json.dump(total_optimized_riskconfiguration_results, file)

    # BACKUP RISKMANAGEMENT CONFIGURATION
    random_id = str(randint(1,1000))
    now = datetime.now()
    day = str(now.day)
    month = str(now.month)
    year = str(now.year)
    minimum_event_number = str(risk_configuration_dict['minimum_event_number'])
    mean_threshold = str(risk_configuration_dict['mean_threshold'])

    if early_validation == False:
        dst_backup = f"{ROOT_PATH}/riskmanagement_backup/riskmanagement-{year}-{month}-{day}-{minimum_event_number}-{mean_threshold}-{random_id}.json"
        dst_optimized = f"{ROOT_PATH}/optimized_results_backup/optimized_results-{year}-{month}-{day}-{minimum_event_number}-{mean_threshold}-{random_id}.json"
        shutil.copyfile(file_path_riskmanagement, dst_backup)
        shutil.copyfile(file_path_optimized_results, dst_optimized)
    else:
        early_validation = early_validation + timedelta(days=3)
        year = str(early_validation.year)
        month = str(early_validation.month)
        day = str(early_validation.day)
        std_multiplier = str(std_multiplier)

        dst_backup = f"{ROOT_PATH}/riskmanagement_backup/riskmanagement-{year}-{month}-{day}-{minimum_event_number}-{mean_threshold}-earlyvalidation-{std_multiplier}stdmultiplier-{random_id}.json"
        dst_optimized = f"{ROOT_PATH}/optimized_results_backup/optimized_results-{year}-{month}-{day}-{minimum_event_number}-{mean_threshold}-earlyvalidation-{std_multiplier}stdmultiplier-{random_id}.json"
        print(f'Early Validation On: analize the post performance of this path: {dst_backup}')

        with open(dst_backup, 'w') as file:
            json.dump(risk_management_config_json, file)

        with open(dst_optimized, 'w') as file:
            json.dump(total_optimized_riskconfiguration_results, file)

    
    t2 = time()
    time_spent = t2 - t1
    print(f'{time_spent} seconds spent to run wrap_analyze_events')



    # CREATE PANDAS DATAFRAME
    df = pd.DataFrame(df_dict)
    df = df.sort_values("optimized_gain", ascending=False)
    return df, dst_backup

def load_riskconfiguration(another_riskconfiguration=None):
    if another_riskconfiguration == None:
        file_path = ROOT_PATH + "/riskmanagement_json/riskmanagement.json"
    else:
        file_path = another_riskconfiguration
        
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            # Retrieve shared memory for JSON data and "start_interval"
            riskmanagement_dict = json.load(file)

    df_dict = riskmanagement_dict['Dataframe']
    info = riskmanagement_dict['Info']
    timestamp = riskmanagement_dict['Timestamp']
    info['timestamp'] = timestamp
    pretty_json = json.dumps(info, indent=4)

    #Compute Percentage Gain in 1 Month
    optimized_gain_list = df_dict['optimized_gain']
    mean_gain_list = df_dict['mean_gain']
    frequency_list = df_dict['frequency']

    investment_amount = 100
    average_number_events_permonth = int(sum(frequency_list))
    weighted_average_optimized_gain_percentage = sum(np.array(optimized_gain_list) * np.array(frequency_list)) / average_number_events_permonth / 100
    average_optimized_gain_per_event = weighted_average_optimized_gain_percentage * investment_amount
    total_optimized_gain_1month = int(average_optimized_gain_per_event*average_number_events_permonth)

    weighted_average_mean_gain_percentage = sum(np.array(mean_gain_list) * np.array(frequency_list))/ average_number_events_permonth / 100
    average_mean_gain_per_event = weighted_average_mean_gain_percentage * investment_amount
    total_mean_gain_1month = int(average_mean_gain_per_event*average_number_events_permonth)

    print(f'Total events per month on average: {average_number_events_permonth}')
    print(f'Optimized gain each month (percentage) for each event: {weighted_average_optimized_gain_percentage} euro with investment amount {investment_amount} euro per event')
    print(f'Optimized gain each month: {total_optimized_gain_1month} euro with investment amount {investment_amount} euro per event')
    print(f'Mean gain each month (percentage) for each event: {weighted_average_mean_gain_percentage} euro with investment amount {investment_amount} euro per event')
    print(f'Mean gain each month: {total_mean_gain_1month} euro with investment amount {investment_amount} euro per event')


    # Print the pretty JSON
    print(pretty_json)

    df = pd.DataFrame(df_dict)
    df = df.sort_values("optimized_gain", ascending=False)

    return df

def send_riskconfiguration():
    file_path = ROOT_PATH + "/riskmanagement_json/riskmanagement.json"
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            # Retrieve shared memory for JSON data and "start_interval"
            riskmanagement_dict = json.load(file)

    # send request
    request = riskmanagement_dict['RiskManagement']
    #url = 'http://localhost/analysis/riskmanagement-configuration'
    url = 'https://algocrypto.eu/analysis/riskmanagement-configuration'

    response = requests.post(url, json = request)
    print('Status Code is : ', response.status_code)
    response = json.loads(response.text)

def send_userconfiguration(request):

    url = 'https://algocrypto.eu/analysis/user-configuration'
    #url = 'http://localhost/analysis/user-configuration'

    response = requests.post(url, json = request)
    print('Status Code is : ', response.status_code)
    response = json.loads(response.text)
    return response

def analyzeRiskManagementPerformance(riskmanagement_path, OPTIMIZED=True, DISCOVER=False, event_investment_amount=100):
    '''
    This function has the goal to anaylze the post performance of an event. 
    The post performance is referred to 2 types: 
    1) without optimizing with riskmanagement --> OPTIMIZED=False
    2) with riskmanagement optimization --> OPTIMIZED=True

    In the first case analysis.json will be considered as input alonmgside riskmanagement.json
    In the second example optimized_results.json will be considered as input alongside riskmanagement.json

    
    '''

    PERFORMANCE_SCENARIOS = {'-1:-0.6': [[-1,-0.6],0],
                              '-0.6:-0.4': [[-0.6,-0.4],0],
                                '-0.4:-0.2': [[-0.4,-0.2],0],
                                  '-0.2:-0.1': [[-0.2,-0.1],0],
                                    '-0.1:-0.05': [[-0.1, -0.05],0],
                                    '-0.05:-0.02': [[-0.05, -0.02],0],
                                    '-0.02:0': [[-0.02, 0],0],
                                    '0:0.02': [[0, 0.02],0],
                                    '0.02:0.05': [[0.02,0.05],0],
                              '0.05:0.1': [[0.05,0.1],0],
                                '0.1:0.2': [[0.1, 0.2],0],
                                  '0.2:0.4': [[0.2, 0.4],0], 
                                  '0.4:0.6': [[0.4,0.6],0],
                                    '0.6:1': [[0.6,1],0],
                                      '1:10': [[1,10],0]}
    

    #/Users/albertorainieri/Projects/Personal/analysis/riskmanagement_backup/riskmanagement-2023-8-7-15-3-724.json
    riskmanagement_path_split = riskmanagement_path.split('-')
    year = int(riskmanagement_path_split[1])
    month = int(riskmanagement_path_split[2])
    day = int(riskmanagement_path_split[3])

    # The analysis starts from the date the riskmanagemnt file was created - 3 days (4320 minutes)
    start_analysis_datetime = datetime(year=year, month=month, day=day) - timedelta(days=3)

    # download riskmanagement_path
    if os.path.exists(riskmanagement_path):
        with open(riskmanagement_path, 'r') as file:
            riskmanagement_dict = json.load(file)
        #print(f'{riskmanagement_path} is loaded')
    else:
        print(f'{riskmanagement_path} does not exist')
        pass

    riskmanagement = riskmanagement_dict['RiskManagement']

    print('Loading analysis.json for both scenarios: OPTIMIZED or not')
    analysis_json_path = ROOT_PATH + "/analysis_json/analysis.json"
    if os.path.exists(analysis_json_path):
        with open(analysis_json_path, 'r') as file:
            analysis_json_dict = json.load(file)
            analysis_json = analysis_json_dict['data']


    # Start OPTIMIZED ANALYSIS
    if OPTIMIZED:
        print('OPTIMIZED ANALYSIS')
        path_split = riskmanagement_path.split('riskmanagement')
        corresponding_optimized_results_suffix = path_split[-1]
        optimized_results_json_path = ROOT_PATH + "/optimized_results_backup/optimized_results" + corresponding_optimized_results_suffix

        # load optimized results
        if os.path.exists(optimized_results_json_path):
            # retrieve optimized_results_dict
            with open(optimized_results_json_path, 'r') as file:
                optimized_results_dict = json.load(file)
            
            info = {}
            for key in analysis_json:
                if key in optimized_results_dict:
                    info[key] = analysis_json[key]
            del analysis_json

            
            analysis_json = {}

            # Loading Data
            print('Loading data from analysis.json')

            # get keys for price change, vol and buy_vol
            random_key = list(optimized_results_dict.keys())[0]
            price_keys = [key for key in list(optimized_results_dict[random_key].keys()) if 'price_%' in key]
            vol_keys = [key for key in list(optimized_results_dict[random_key].keys()) if 'vol' in key]
            buy_vol_keys = [key for key in list(optimized_results_dict[random_key].keys()) if 'buy' in key]
            price_vol_buy_vol_keys = price_keys + vol_keys + buy_vol_keys

            # iterate through each event key and update if needed
            for event_key in optimized_results_dict:
                new_optimized_results = {event_key: {}}
                vol, vol_value, buy_vol, buy_vol_value, timeframe = getsubstring_fromkey(event_key)
                timeframe = int(timeframe)
                volatility = event_key.split('vlty:')[1]

                if event_key in riskmanagement[volatility]:
                    # retrieve riskMangement_features (GOLDEN_ZONE, STEP, ecc...)

                    
                    GOLDEN_ZONE_original = float(riskmanagement[volatility][event_key]['riskmanagement_conf']['golden_zone'])
                    STEP_original = float(riskmanagement[volatility][event_key]['riskmanagement_conf']['step_golden'])
                    STEP_NOGOLDEN = float(riskmanagement[volatility][event_key]['riskmanagement_conf']['step_nogolden'])
                    extratimeframe = float(riskmanagement[volatility][event_key]['riskmanagement_conf']['extra_timeframe'])

                    # update timeseries.json
                    if DISCOVER:
                        print(f'Downloading timeseries for {event_key} from server')
                        retry = getTimeseries(info, event_key, check_past=1440, look_for_newdata=True, plot=False)
                    # load timeseries.json
                    event_key_path = event_key.replace(':', '_')
                    event_key_path = event_key_path.replace('/', '_')
                    
                    #print(f'Reading timeseries for {event_key} from local')
                    timeseries_json_path = ROOT_PATH + "/timeseries_json/" + event_key_path + '.json'
                    with open(timeseries_json_path, 'r') as file:
                        timeseries_json = json.load(file)

                    #print('Check if there is new events to analyze')
                    # check if new event has occurred
                    for coin in timeseries_json:
                        for start_timestamp in timeseries_json[coin]:
                            # let's check if event of timeseries json is new in optimized_results
                            if start_timestamp not in optimized_results_dict[event_key]['events']:

                                #analyze the event with the current strategy
                                new_optimized_results = RiskManagement_lowest_level(new_optimized_results, timeseries_json, coin, start_timestamp, event_key,
                                    STEP_original, GOLDEN_ZONE_original, STEP_NOGOLDEN, timeframe, extratimeframe)
                    
                    if len(new_optimized_results[event_key]) > 0:
                        print('Updating optimized_results_configuration. def prepareOptimizedConfigurarionResults')
                        print(event_key)
                        new_optimized_riskconfiguration_results, other_vars = prepareOptimizedConfigurarionResults(new_optimized_results, event_key)
                        
                        

                        # update optimized_results_dict
                        for info_key in new_optimized_riskconfiguration_results:
                            optimized_results_dict[event_key][info_key] += new_optimized_riskconfiguration_results[info_key]
                    else:
                        print(f'No need to update optimized_results for {event_key}')

                # let's reproduce analysis_json with the same structure starting from optimized_results.json
                if event_key not in analysis_json:
                    analysis_json[event_key] = {'info': {}}

                for iterator in range(len(optimized_results_dict[event_key]['events'])):
                    doc_to_add = {'event': optimized_results_dict[event_key]['events'][iterator],
                                    'mean': optimized_results_dict[event_key]['gain'][iterator],
                                    'buy_price': optimized_results_dict[event_key]['buy_price'][iterator],
                                    'exit_price': optimized_results_dict[event_key]['exit_price'][iterator],
                                    'max_price': optimized_results_dict[event_key]['max_price'][iterator],
                                    'min_price': optimized_results_dict[event_key]['min_price'][iterator],
                                    'timestamp_exit': optimized_results_dict[event_key]['timestamp_exit'][iterator],
                                    'std': 0}
                    
                    for price_vol_buy_vol_key in price_vol_buy_vol_keys:
                        doc_to_add[price_vol_buy_vol_key] = optimized_results_dict[event_key][price_vol_buy_vol_key][iterator]
                    
                    coin = optimized_results_dict[event_key]['coin'][iterator]
                    if coin not in analysis_json[event_key]['info']:
                        analysis_json[event_key]['info'][coin] = [doc_to_add]
                    else:
                        analysis_json[event_key]['info'][coin].append(doc_to_add)

            # Update optimized_risk.json
            with open(optimized_results_json_path, 'w') as outfile:
                json.dump(optimized_results_dict, outfile)
            print(f'Json File Updated: {optimized_results_json_path}')
                            
        else:
            print(f'{analysis_json_path} does not exist')
            pass
    # load analysis_json
    else:
        print('NOT OPTIMIZED ANALYSIS')

    
    
    event_key_list = []
    mean_list = []
    std_list = []
    # this series comprehends all the events
    #all_timeseries_info = []

    # this dict divides the series of the event by event_key
    timeseries_info = {}
    timeseries_info = {'total': []}
    positive_outcome = 0
    negative_outcome = 0

    for volatility in riskmanagement:
        for event_key in riskmanagement[volatility]:
            if event_key in analysis_json:
                for coin in analysis_json[event_key]['info']:
                    for event in analysis_json[event_key]['info'][coin]:
                        # initialize series for single event_key
                        if event_key not in timeseries_info:
                            timeseries_info[event_key] = []
                        
                        # append event for single event_key
                        event['event_key'] = event_key
                        event['coin'] = coin
                        timeseries_info[event_key].append(event)

                        # append event for "total"
                        timeseries_info['total'].append(event)
                        # event_key_list.append(event['event'])
                        # mean_list.append(event['mean'])
                        # std_list.append(event['std'])

                        
                        for performance_scenario in PERFORMANCE_SCENARIOS:
                            if event['mean'] >= PERFORMANCE_SCENARIOS[performance_scenario][0][0] and event['mean'] < PERFORMANCE_SCENARIOS[performance_scenario][0][1]:
                                PERFORMANCE_SCENARIOS[performance_scenario][1] += 1
                                continue

                        if event['mean'] > 0:
                            positive_outcome += 1
                        else:
                            negative_outcome += 1

    del analysis_json, riskmanagement

    # sort all series
    for event_key in timeseries_info:
        timeseries_info[event_key].sort(key=lambda x: x['event'], reverse=False)
    
    starting_balance_account = 1000
    #event_investment_amount = 65
    biggest_drop = 0

    
    # compute dynamic performance mean for all series
    for event_key in timeseries_info:

        timestamp_timeseries = []
        datetime_timeseries = []
        mean_timeseries = []
        std_timeseries = []
        mean_list = []
        std_list = []
        coin_list = []
        post_mean_list = []
        event_key_list = []
        post_performance_timeseries = []
        balance_account = []
        stop = False
        start_analysis_i = len(timeseries_info[event_key]) - 1

        if OPTIMIZED:
            buy_price_list = []
            exit_price_list = []
            timestamp_exit_list = []
            max_price_list = []
            min_price_list = []
            info_obj = {}

        # compute each timeseries
        for event, i in zip(timeseries_info[event_key], range(len(timeseries_info[event_key]))):
            
            # compute account balance performance if event_key == 'total'
            if event_key == 'total':
                starting_balance_account += round_(event_investment_amount * event['mean'],2)
                balance_account.append(starting_balance_account)

                # find biggest drop in series
                #print(f'{max(balance_account)} - {balance_account[-1]} > {biggest_drop}')
                if max(balance_account) - balance_account[-1] > biggest_drop:
                    biggest_drop = round_(max(balance_account) - balance_account[-1],2)
                    biggest_drop_date = event['event']

                
            
            # compute post mean performance
            if datetime.fromisoformat(event['event']) > start_analysis_datetime:
                if stop == False:
                    start_analysis_i = i - 1
                    stop = True
                post_mean_list.append(event['mean'])
                post_performance_timeseries.append(np.mean(post_mean_list))
            else:
                post_performance_timeseries.append(0)
                

            # compute overall mean performance
            mean_list.append(event['mean'])
            std_list.append(event['std'])
            coin_list.append(event['coin'])
            event_key_list.append(event['event_key'])
            mean_timeseries.append(np.mean(mean_list))
            std_timeseries.append(np.mean(std_list))
            timestamp_timeseries.append(event['event'])

            if OPTIMIZED:
                # add all fields related to price_change, volume and buy_volume
                for info_key in price_vol_buy_vol_keys:
                    if info_key not in info_obj:
                        info_obj[info_key] = []
                    info_obj[info_key].append(event[info_key])
                
                # add the following fields
                buy_price_list.append(event['buy_price'])
                exit_price_list.append(event['exit_price'])
                timestamp_exit_list.append(event['timestamp_exit'])
                max_price_list.append(event['max_price'])
                min_price_list.append(event['min_price'])

        upper_bound = np.array(mean_timeseries) + np.array(std_timeseries)
        lower_bound = np.array(mean_timeseries) - np.array(std_timeseries)

        # 
        # for timestamp in timestamp_timeseries:
        #     datetime_timeseries.append(datetime.fromisoformat(timestamp))
        
        # if event_key == 'buy_vol_3h:0.65/vol_6h:6/timeframe:360/vlty:2':
        #     print(event_key)
        #     print(post_mean_list)
        #     print(mean_list)
        #     print(mean_timeseries)
        
        # plot each timeseries in terms of percentages
        fig, ax = plt.subplots(1, 1, figsize=(7, 4))
        #print(len(mean_timeseries), len(upper_bound), len(lower_bound))
        ax.plot(mean_timeseries)
        ax.plot(post_performance_timeseries)
        ax.plot(upper_bound)
        ax.plot(lower_bound)
        ax.axhline(y=0, color='red', linestyle='--')
        ax.axvline(x=start_analysis_i, color='red', linestyle='--')
        ax.set_title(event_key)
        
        if event_key == 'total':
            doc_df1 = {'event': timestamp_timeseries, 'mean_series': mean_timeseries, 'mean_event': mean_list, 'balance': balance_account, 'coin': coin_list, 'event_key': event_key_list}
            if OPTIMIZED:
                doc_df1['buy_price'] = buy_price_list
                doc_df1['exit_price'] = exit_price_list
                doc_df1['timestamp_exit'] = timestamp_exit_list
                doc_df1['max_price'] = max_price_list
                doc_df1['min_price'] = min_price_list

                # add the fields related to price_changes, volumes and buy_volumes
                for info_key in info_obj:
                    doc_df1[info_key] = info_obj[info_key]
                
            with open('tmp.json', 'w') as outfile:
                json.dump(doc_df1, outfile)

            df1 = pd.DataFrame(doc_df1)
                                
            total_performance = round_(np.mean(mean_list)*100,2)
            n_events_total_performance = len(mean_list)
            post_performance = round_(np.mean(post_mean_list)*100,2)
            n_event_post_performance = len(post_mean_list)

            print(f'the profit of the entire timeseries is {total_performance}% with {n_events_total_performance} events')
            print(f'the post profit is {post_performance}% with {n_event_post_performance} events')
            print(f'Positive events: {positive_outcome}')
            print(f'Negative events: {negative_outcome}')
            print(f'Biggest Drop: {biggest_drop} at {biggest_drop_date}')
        
    keys = list(PERFORMANCE_SCENARIOS.keys())
    values = [entry[1] for entry in PERFORMANCE_SCENARIOS.values()]

    plt.bar(keys, values)
    plt.xlabel('Ranges')
    plt.ylabel('Values')
    plt.xticks(rotation=45)
    plt.title('Histogram')
    plt.tight_layout()
    plt.axvline(x=keys.index('0:0.02'), color='red', linestyle='--', label='Zero Line')

    plt.show()

    return df1, biggest_drop, biggest_drop_date, positive_outcome, negative_outcome, PERFORMANCE_SCENARIOS
    
def PriceVariation_analysis(df, model_type='svc', target_variable: TargetVariable = 'mean_event', test_size=0.2):

    # Identify Decision and Output variables
    all_columns = list(df.columns)
    #selected_columns = [column for column in all_columns if 'price_%' in column or 'vol' in column or 'buy' in column]
    selected_columns = [column for column in all_columns if 'price_%' in column or 'vol' in column or 'buy' in column]
    decision_variables = df[selected_columns]

    # define decision variable, only 'mean_event' is normalized, max_price and min_price need to be normalized
    if target_variable != 'mean_event':
        output_variable = np.array((df[TargetVariable] - df['buy_price']) / df['buy_price'])
    else:
        output_variable = np.array(df[TargetVariable])

    #output_variable = np.array(df[target_variable])
    #print(len(output_variable))

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
    

    # Start Training
    random_state = randint(0,len(y))
    X_train, X_test, y_train_real, y_test_real = train_test_split(X, y, test_size=test_size, random_state=random_state)

    # print(y_train)
    # print(y_test)

    if target_variable == 'mean_event':
        classifier = {'1': (-1,-0.2), '2': (-0.2,0), '3':(0,0.2), '4': (0.2,3)}
        classifier_strings = [str(classifier[cls][0]) + '<x<' + str(classifier[cls][1]) for cls in classifier]
    elif target_variable == 'max_price':
        classifier = {'1': (0,0.05), '2':(0.05,0.2), '3': (0.2,3)}
        classifier_strings = [str(classifier[cls][0]) + '<x<' + str(classifier[cls][1]) for cls in classifier]
    elif target_variable == 'min_price':
        classifier = {'1': (-1,-0.2), '2': (-0.2,-0.05), '3':(-0.05,0.0001)}
        classifier_strings = [str(classifier[cls][0]) + '<x<' + str(classifier[cls][1]) for cls in classifier]
    
    y_train = []
    y_test = []
    
    # encode y_test and y_train
    for y, i in zip(y_train_real, range(len(y_train_real))):
        for cl_key in classifier:
            if y >= classifier[cl_key][0] and y < classifier[cl_key][1]:
                y_train.append(int(cl_key))
                break

    for y, i in zip(y_test_real, range(len(y_test_real))):
        for cl_key in classifier:
            if y >= classifier[cl_key][0] and y < classifier[cl_key][1]:
                y_test.append(int(cl_key))
                break
    
    le = LabelEncoder()
    y_train = le.fit_transform(y_train)


    # Fit Linear Regression
    if model_type == 'linear_regression':
        print('Linear Regression Model')
        model = LinearRegression()
        model.fit(X_train, y_train)
    elif model_type == 'svc':
        print('SVC Model')
        model = SVR(kernel='rbf')  # Puoi scegliere il kernel desiderato
        model.fit(X_train, y_train)
    elif model_type == 'xgboost':
        model = XGBClassifier(n_estimators=50,
                     max_depth=5,
                     max_leaves=64,
                     eta=0.1,
                     reg_lambda=0,
                     tree_method='hist',
                     eval_metric='logloss',
                     use_label_encoder=False,
                     random_state=1000,
                     n_jobs=-1)

        model.fit(X_train,y_train)

    # Predict
    y_pred = model.predict(X_test)
    y_pred = le.inverse_transform(y_pred)

    # x_line = np.linspace(min(y_test), max(y_test), 100)
    # mse = mean_squared_error(y_test, y_pred)
    # print(f"Mean Squared Error: {mse}")
    # plt.scatter(y_test, y_pred)
    # plt.plot(x_line, x_line, 'r--', label="x = y", linewidth=2)  # 'r--' creates a red dashed line
    # plt.xlabel("Valori reali")
    # plt.ylabel("Valori previsti")
    # plt.title("Confronto tra valori reali e previsti")
    # plt.show()

    cm = confusion_matrix(y_test, y_pred)
    df_cm = pd.DataFrame(cm, index = [i for i in classifier_strings], columns = [i for i in classifier_strings])


    ax= plt.subplot()
    sn.heatmap(df_cm, annot=True, fmt='g', ax=ax);  #annot=True to annotate cells, ftm='g' to disable scientific notation
    # labels, title and ticks
    ax.set_xlabel('Predicted labels');ax.set_ylabel('True labels')
    ax.set_title(f'Confusion Matrix for {target_variable}')
    #accuracy_score(y_test, y_pred)

def supervised_analysis(complete_info=None, complete_info_path=None, search_parameters=None, target_variable: TargetVariable1 = 'mean', test_size=0.2):
    '''
    This function analyzes with a supervised algorithm the output of "download_show_output". In particular the variable "info" or "complete_info" is taken as input.
    The decision variables will be taken from the route /get-pricechanges which provides all info about (pricechanges, volumes and buy_volumes)
    The target variables can be one of the following (mean, max, min) which are already present in "info"
    the function will iterate through each event and get necessary information for building the matrix (decision variables + target) and start to anaylyze
    '''
    
    info = load_data_for_supervised_analysis(complete_info=complete_info, complete_info_path=complete_info_path,
                                       search_parameters=search_parameters, target_variable = target_variable)

    #return info
    #################################################################

    # SUPERVISION ANALYSIS START
    for event_key in info:
        df = pd.DataFrame(info[event_key])

        X,y = scale_filter_select_features(df, target_variable)


        X_train, X_test, y_train_real, y_test_real = train_test_split(X, y, test_size=test_size, random_state=0)

        if target_variable == 'mean':
            classifier = {'1': (-1,0), '2':(0,3)}
            classifier_strings = [str(classifier[cls][0]) + '<x<' + str(classifier[cls][1]) for cls in classifier]
        elif target_variable == 'max':
            classifier = {'1': (0,0.05), '2':(0.05,0.2), '3': (0.2,3)}
            classifier_strings = [str(classifier[cls][0]) + '<x<' + str(classifier[cls][1]) for cls in classifier]
        elif target_variable == 'min':
            classifier = {'1': (-1,-0.2), '2': (-0.2,-0.05), '3':(-0.05,0.0001)}
            classifier_strings = [str(classifier[cls][0]) + '<x<' + str(classifier[cls][1]) for cls in classifier]
        
        y_train = []
        y_test = []
        
        # encode y_test and y_train
        for y, i in zip(y_train_real, range(len(y_train_real))):
            for cl_key in classifier:
                if y >= classifier[cl_key][0] and y < classifier[cl_key][1]:
                    y_train.append(int(cl_key))
                    break

        for y, i in zip(y_test_real, range(len(y_test_real))):
            for cl_key in classifier:
                if y >= classifier[cl_key][0] and y < classifier[cl_key][1]:
                    y_test.append(int(cl_key))
                    break
        
        df_cm = train_model_xgb(X_train, X_test, y_train, y_test, classifier_strings, event_key)

        ax= plt.subplot()
        sn.heatmap(df_cm, annot=True, fmt='g', ax=ax);  #annot=True to annotate cells, ftm='g' to disable scientific notation
        ax.set_xlabel('Predicted labels');ax.set_ylabel('True labels')
        ax.set_title(f'Confusion Matrix for {target_variable} - {event_key}')
        plt.show()



            