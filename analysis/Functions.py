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


ROOT_PATH = os.getcwd()

def round_(number, decimal):
    return float(format(number, f".{decimal}f"))

def check_correlation(data, field_volume, field_price, coin = None, limit_volume=3):
    '''
    This function checks the correlation between 2 fields chosen
    '''

    list1 = []
    list2 = []

    # get the volume field based on the timeframe (e.g. 5m, 15m, ..., 6h)
    timeframe = field_volume.split('_')[-1]
    volume_field = 'vol_' + timeframe
    minute_or_hours = timeframe[-1]
    if minute_or_hours == 'm':
        jump = int(timeframe[:-1])
    else:
        jump = int(timeframe[:-1]) * 60
    
    print(jump)

    # analyze one coin if speicified
    if coin != None:
        for obs in data[coin]:
            
            if obs[field_volume] != None and obs[field_price] != None:
                list1.append(obs[field_volume])
                list2.append(obs[field_price])
        
        correlation, p_value = pearsonr(list1, list2)

        print("Correlation:", correlation)
        print("P-value:", p_value)

    # analyze all coins in data
    else:
        correlations = []
        pvalues = []
        n_coins = 0
        for coin in list(data.keys()):
            del list1
            del list2

            list1 = []
            list2 = []

            # for obs_vol,obs_price in zip(data[coin][:-60], data[coin][60:]):
            #     if obs_vol[field_volume] != None and obs_price[field_price] != None and obs_vol[volume_field] >= limit_volume:
            
            #         list1.append(obs_vol[field_volume])
            #         list2.append(obs_price[field_price])

            for obs in data[coin]:
                if obs[field_volume] != None and obs[field_price] != None and obs[volume_field] >= limit_volume:
                    list1.append(obs[field_volume])
                    list2.append(obs[field_price])
                
            if len(list1) > 3:
                n_coins += 1
                correlation, p_value = pearsonr(list1, list2)

                # get correlation and pvalue
                if correlation != None and p_value != None and isinstance(correlation, np.float64):
                    correlations.append(correlation)
                    pvalues.append(p_value)
                else:
                    type_ = type(correlation)
                    print(f'{coin}: pvalue={p_value}, correlation={correlation}, type={type_}')
                    
            # else:
            #     print(coin)
        
        # get the average correlation for all the analyzed coins
        print(len(correlations))
        max_corr = max(correlations)
        min_corr = min(correlations)
        std_dev_corr = np.std(correlations)
        print(f' {n_coins} have been analyzed')
        print(f'Max corr: {max_corr}')
        print(f'Min corr: {min_corr}')
        print(f'Std corr: {std_dev_corr}')

        print("Correlation:", np.mean(correlations))
        print("P-value:", np.mean(pvalues))

        return correlations, pvalues


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
                                    price_changes.append(change)
                                    event_price_changes.append(change)
                                else:
                                    if coin not in nan:
                                        nan[coin] = []
                                    nan[coin].append(timestamp_event_triggered)

                        complete_info[volatility][coin].append({'event': timestamp_event_triggered, 'mean': round_(np.mean(event_price_changes),4), 'std': round(np.std(event_price_changes, ddof=1),4)})
            else:
                # in case an observations of a coin goes beyond "end_interval_analysis", then I skip the whole analysis of the coin,
                # since the obsevations are ordered chronologically
                # print(f'Analysis for {coin} is completed: Last timestamp: {datetime_obs.isoformat()}; {index}')
                break

    return complete_info, nan


def get_volatility(dynamic_benchmark_info_coin, full_timestamp):
    '''
    This function outputs the volatility of the coin (timeframe: last 30 days) in a specific point in time
    '''

    short_timestamp = full_timestamp.split('T')[0]
    volatility =  int(dynamic_benchmark_info_coin[short_timestamp])
    return volatility



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


def data_preparation(data, n_processes = 5):
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

def retrieve_datetime_for_load_data(json_info, list_json_info, index):
    '''
    this function is used from "load_data" to retrieve the datetimes between two close json in analysis/json
    these two datetimes represent the first timestamps in the list of observations
    '''
    # retrieve first datetime of list_json
    path = json_info['path']
    path_split = path.split('-')
    day = int(path_split[1])
    month = int(path_split[2])
    year = int(path_split[3])
    hour = int(path_split[4])
    minute = int(path_split[5].split('.')[0])
    datetime1 = datetime(year=year, month=month, day=day, hour=hour, minute=minute)

    # retrieve first datetime of the next list_json
    next_path = list_json_info[index + 1]['path']
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

def load_data(start_interval=datetime(2023,5,7, tzinfo=pytz.UTC), end_interval=datetime.now(tz=pytz.UTC), filter_position=(0,50), coin=None):
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
    path_dir = ROOT_PATH + "/json"
    list_json = os.listdir(path_dir)
    full_paths = [path_dir + "/{0}".format(x) for x in list_json]
    print(full_paths)

    list_json_info = []
    for full_path in full_paths:
        json_ = {'path': full_path, 'time': os.path.getmtime(full_path)}
        list_json_info.append(json_)

    # this is the list of all the paths in analysis/json/ sorted by time
    list_json_info.sort(key=lambda x: x['time'], reverse=False)
    STOP_LOADING = False

    data= {}
    for json_info, index in zip(list_json_info, range(len(list_json_info))):
        if STOP_LOADING:
            break
        # check if this is not the last json saved
        if list_json_info.index(json_info) + 1 != len(list_json_info):
            # retrieve first datetime of json_info and first datetime of the next json_info
            path = json_info['path']
            datetime1, datetime2 = retrieve_datetime_for_load_data(json_info, list_json_info, index)
            # if start_interval is between these 2 datetimes, retrieve json
            if start_interval > datetime1 and start_interval < datetime2:


                data = updateData_for_load_data(data, path, most_traded_coin_list, start_interval, end_interval)
                # if end_interval terminates before datetime2, then loading is completed. break the loop
                if end_interval < datetime2:
                    break
                else:
                    # let's determine the new list_json_info
                    list_json_info = list_json_info[index+1:]
                    # iterate through the new "list_json_info" for loading the other json
                    for json_info, index2 in zip(list_json_info, range(len(list_json_info))):
                        path = json_info['path']
                        data = updateData_for_load_data(data, path, most_traded_coin_list, start_interval, end_interval)

                        # if this is the last json than break the loop, the loading is completed
                        if list_json_info.index(json_info) + 1 == len(list_json_info):
                            STOP_LOADING = True
                            break
                        else:

                            datetime1, datetime2 = retrieve_datetime_for_load_data(json_info, list_json_info, index2)
                            if end_interval > datetime1 and end_interval < datetime2:
                                STOP_LOADING = True
                                break
                        
            # go to the next path
            else:
                print(f'Nothing to retrieve from {path}')
                continue

        
        else:
            path = json_info['path']
            data = updateData_for_load_data(data, path, most_traded_coin_list, start_interval, end_interval)


    
    # summary = {}
    # keys_data = list(data.keys())
    # for coin in keys_data:
    #     if len(data[coin]) > 0:
    #         # get first obs from data[coin]
    #         start_coin = data[coin][0]['_id']
    #         # get standard dev on mean (std.dev / mean)
    #         std_on_mean = benchmark_info[coin]['volume_30_std']  / benchmark_info[coin]['volume_30_avg']
    #         # update summary
    #         summary[coin] = {'n_observations': len(data[coin]), 'position': most_traded_coin_list.index(coin), 'vol_30_avg': benchmark_info[coin]['volume_30_avg'], 'std_on_mean': std_on_mean, 'first_obs': start_coin}
    #     else:
    #         del data[coin]
    
    # n_coins = len(data)
    # print(f'{n_coins} coins have been loaded')

    # print('Data Loading is completed')
    # df = pd.DataFrame(summary)
    # df = df.transpose()
    # df = df.sort_values(by=['vol_30_avg'], ascending=False)
    #return data, df, summary
    for var in list(locals()):
        if var.startswith('__') and var.endswith('__'):
            continue  # Skip built-in variables
        if var == "data":
            continue
        del locals()[var]
    
    return data

def get_volume_info():
    ENDPOINT = 'https://algocrypto.eu'
    #ENDPOINT = 'http://localhost'

    method_most_traded_coins = '/get-volumeinfo'

    url_mosttradedcoins = ENDPOINT + method_most_traded_coins
    response = requests.get(url_mosttradedcoins)
    print(f'StatusCode for getting get-volumeinfo: {response.status_code}')
    volume_info = response.json()
    volume_info = json.loads(volume_info)
    return volume_info


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

            if position <= days:
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
        


            

def total_function_multiprocessing(list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, n_processes, LOAD_DATA):
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
    print('total_combinantions', ': ', total_combinations)
    path = ROOT_PATH + "/analysis_json/"
    file_path = path + 'analysis' + '.json'

    # initialize Manager for "shared_data". Used for multiprocessing
    manager = Manager()

    
    analysis_timeframe = 7 #days. How many days "total_function_multiprocessing" will analyze data from last saved?
    # Load files form json_analysis if exists otherwise initialize. Finally define "start_interval" and "end_interval" for loading the data to be analyzed
    
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            # Retrieve shared memory for JSON data and "start_interval"
            analysis_json = json.load(file)
            start_interval = analysis_json['start_next_analysis']
            del analysis_json
    else:
        # Create shared memory for JSON data and initialize "start_interval"
        start_interval = datetime(2023,5,11).isoformat()
    
    # define "end_interval" and "filter_position"
    end_interval = min(datetime.now(), datetime.fromisoformat(start_interval) + timedelta(days=analysis_timeframe))
    filter_position = (0,500) # this is enough to load all the coins available

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
    updateAnalysisJson(shared_data.value, file_path, start_next_analysis)

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

def updateAnalysisJson(shared_data_value, file_path, start_next_analysis, slice_i=None, start_next_analysis_str=None):

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
        json_to_save = {'start_next_analysis' : start_next_analysis, 'data': analysis_json['data']}


    with open(file_path, 'w') as file:
        json.dump(json_to_save, file)



            
def download_show_output(minimum_event_number, minimum_coin_number, mean_threshold, lb_threshold, frequency_threshold, group_coins=False, best_coins_volatility=None):
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
    - represent only the best keys grouped by volatility. This is activated only if "group_coins=False". "best_coins_volatility" must be an integer in this case.
    '''

    path = ROOT_PATH + "/analysis_json/"
    file_path = path + 'analysis' + '.json'
    t1 = time()

    with open(file_path, 'r') as file:
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
                last_event =  datetime.fromisoformat(shared_data[key]['info'][random_coin][-1]['event'])
                
                for coin in shared_data[key]['info']:
                    first_event = min(datetime.fromisoformat(shared_data[key]['info'][coin][0]['event']), first_event)
                    last_event = max(datetime.fromisoformat(shared_data[key]['info'][coin][-1]['event']), last_event)
                    n_events += len(shared_data[key]['info'][coin])
                    for event in shared_data[key]['info'][coin]:
                        mean_list.append(event['mean'])
                        std_list.append(event['std'])
                
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

                    if mean > mean_threshold and lower_bound > lb_threshold:

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
                    
    else:
        for key in list(shared_data.keys()):
            # print(key)
            # print(shared_data[key].keys())
            if key != 'coins' or key != 'events':
                vol, vol_value, buy_vol, buy_vol_value, timeframe = getsubstring_fromkey(key)
                key_without_volatility = key.split('vlty:')[0]
                n_events = 0
                mean_list = []
                std_list = []
                n_coins = len(shared_data[key]['info'])

                for coin in shared_data[key]['info']:
                    n_events += len(shared_data[key]['info'][coin])
                    for event in shared_data[key]['info'][coin]:
                        mean_list.append(event['mean'])
                        std_list.append(event['std'])

                mean = round_(np.mean(np.array(mean_list))*100,2)
                std = round_(pooled_standard_deviation(np.array(std_list), sample_size=int(timeframe))*100,2)

                if key_without_volatility not in output:
                    output[key_without_volatility] = {'mean': [mean], 'std': [std], 'n_coins': n_coins, 'n_events': n_events}
                else:
                    output[key_without_volatility]['mean'].append(mean)
                    output[key_without_volatility]['std'].append(std)
                    output[key_without_volatility]['n_coins'] += n_coins
                    output[key_without_volatility]['n_events'] += n_events
        
        delete_keys = []
        for key in output:
            if output[key]['n_events'] >= minimum_event_number:
                output[key]['mean'] = round_(np.mean(output[key]['mean']),2)
                output[key]['std'] = round_(np.mean(output[key]['std']),2)
            
                output[key]['upper_bound'] = output[key]['mean'] + output[key]['std']
                output[key]['lower_bound'] = output[key]['mean'] - output[key]['std']
            else:
                delete_keys.append(key)

        for key in delete_keys:
            output.pop(key)
        complete_info = None

    
    
    t3 = time()
    delta_t_2 = round_(t3 - t2,2)
    print(f'Data Preparation completed in {delta_t_2} seconds')


    return output, complete_info

def pooled_standard_deviation(stds, sample_size):

    pooled_std = np.sqrt(sum((sample_size-1) * std**2 for std in stds) / (len(stds) * (sample_size - 1)))
    return pooled_std

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
    idx2 = key.index(sub2)
    timeframe = ''
    # getting elements in between
    for idx in range(idx1 + len(sub1) + 1, idx2):
        timeframe = timeframe + key[idx]

    return vol, vol_value, buy_vol, buy_vol_value, timeframe



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
        print('File exists, Download from local...')
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
        print('File does not exist, Donwload from server...')

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


def plotTimeseries(timeseries, fields, check_past, plot):

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
                #print('1')
                start_price = current_price
                #print(start_price)

                max_change = 0
                min_change = 0
                #print('2')
                for obs in timeseries[coin][timestamp_start]['data']:
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

def RiskManagement_multiprocessing(timeseries_json, arg_i, EXTRATIMEFRAMES, STEPS_NOGOLDEN, STEPS_GOLDEN,
                                        GOLDEN_ZONES, timeframe, lock, results):
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
                        for start_timestamp in timestamp_to_analyze:
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
                                
                                if SKIP == False:
                                    current_price = obs['price']
                                    current_change = (current_price - initial_price) / initial_price

                                    # if I'm already in GOLDEN ZONE, then just manage this scenario
                                    if GOLDEN_ZONE_BOOL:
                                        SELL, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP, GOLDEN_ZONE_BOOL = manageGoldenZoneChanges(current_change, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP, GOLDEN_ZONE_BOOL)
                                        if SELL:
                                            tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, True)                        
                                            break

                                    # check if price went above golden zone
                                    if current_change > GOLDEN_ZONE:
                                        SELL, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP, GOLDEN_ZONE_BOOL = manageGoldenZoneChanges(current_change, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP, GOLDEN_ZONE_BOOL)

                                    # check if minimum time window has passed
                                    elif datetime.fromisoformat(obs['_id']) > datetime.fromisoformat(start_timestamp) + timedelta(minutes=timeframe):
                                        SELL, LB_THRESHOLD, UB_THRESHOLD = manageUsualPriceChanges(current_change, LB_THRESHOLD, UB_THRESHOLD, STEP_NOGOLDEN)
                                        if SELL:
                                            tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, True)
                                            break
                                    
                                    #print(int(timeframe * extratimeframe))
                                    extra = int(timeframe * extratimeframe)
                                    if datetime.fromisoformat(obs['_id']) > datetime.fromisoformat(start_timestamp) + timedelta(minutes=timeframe) + timedelta(minutes= extra):
                                        tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, False)
                                        break
                                        
                                    elif obs_i == len(iterator):
                                        tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, False)

    with lock:
        resp = json.loads(results.value)
        for key in list(tmp.keys()):
            
            if key not in resp:
                resp[key] = {}

            # update complete "info"
            for start_timestamp in tmp[key]:
                resp[key][start_timestamp] = tmp[key][start_timestamp]

        results.value = json.dumps(resp)

def RiskManagement(key, investment_per_event=100):

    EXTRATIMEFRAMES = [1/3, 1/6, 1/9]
    STEPS_NOGOLDEN = [0.01, 0.03, 0.05]
    STEPS_GOLDEN = [0.05, 0.075, 0.1, 0.15, 0.2, 0.3]
    GOLDEN_ZONES = [0.10, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.8, 1]
    
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
        return False, False, False

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
                                        GOLDEN_ZONES, timeframe, lock, results) for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])

    pool.close()
    pool.join()

    # split data
    results = json.loads(results.value)

    mean_list = {}
    timestamp_exit_list = {}
    buy_price_list = {}
    exit_price_list = {}
    coin_list = {}
    early_sell = {}

    for risk_key in list(results.keys()):
        mean_list[risk_key] = []
        timestamp_exit_list[risk_key] = []
        buy_price_list[risk_key] = []
        exit_price_list[risk_key] = []
        coin_list[risk_key] = []
        early_sell[risk_key] = []

        for start_timestamp in results[risk_key]:
            mean_list[risk_key].append(results[risk_key][start_timestamp][0])
            timestamp_exit_list[risk_key].append(results[risk_key][start_timestamp][1])
            buy_price_list[risk_key].append(results[risk_key][start_timestamp][2])
            exit_price_list[risk_key].append(results[risk_key][start_timestamp][3])
            coin_list[risk_key].append(results[risk_key][start_timestamp][4])
            early_sell[risk_key].append(results[risk_key][start_timestamp][5])

    best_risk_key = ''
    best_mean = - 10

    risk_key_df = []
    mean_list_df = []
    std_list_df = []

    for risk_key in mean_list:
        mean = np.mean(mean_list[risk_key])
        std = np.std(mean_list[risk_key])

        risk_key_df.append(risk_key)
        mean_list_df.append(mean)
        std_list_df.append(std)

        n_events = len(mean_list[risk_key])
        mean_print = round_(mean*100,2)

        #print(f'{risk_key}: Mean is {mean_print} % for {n_events} events')
        if mean > best_mean:
            best_mean = mean
            best_std = std
            best_risk_key = risk_key
            best_mean_print = round_(best_mean*100,2)
            best_std_print = round_(best_std*100,2)
    
    df1 = pd.DataFrame({'risk_key': risk_key_df, 'mean': mean_list_df, 'std': std_list_df})

    #INVESTMENT_PER_EVENT = 200
    profit = round_((investment_per_event * n_events) * (best_mean),2)
    print(f'{profit} euro of profit for an investment of {investment_per_event} euro per event (total of {n_events} events). {best_risk_key} with {best_mean_print} %')
    
    
    df2 = pd.DataFrame({'events': list(results[risk_key].keys()), 'gain': mean_list[risk_key], 'buy_price': buy_price_list[risk_key],
                         'exit_price': exit_price_list[risk_key],  'timestamp_exit': timestamp_exit_list[risk_key],
                           'coin': coin_list[risk_key], 'early_sell': early_sell[risk_key]})
    
    risk_configuration = {'best_risk_key': best_risk_key, 'best_mean_print':  best_mean_print, 'best_std_print': best_std_print}
    return df1, df2, risk_configuration

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


def check_invevestment_amount(info, output, investment_amount = 100):
    '''
    This function helps to understand the account balance required for investing based on "investment_amount"
    '''
    investment_list_info = []

    for key in output:
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

def get_substring_between(original_string, start_substring, end_substring):
    start_index = original_string.find(start_substring)
    end_index = original_string.find(end_substring, start_index + len(start_substring))

    if start_index == -1 or end_index == -1:
        return None

    return original_string[start_index + len(start_substring):end_index]


def RiskConfiguration(info, riskmanagement_conf):
    '''
    This functions has the objective to define the best risk configuration for a selected number of keys.
    "download_show_output" function will provide the input
    the riskmanagement_configuration will be used during the live trading for optimization /risk management
    '''
    t1 = time()
    keys_list = list(info.keys())
    n_keys = len(keys_list)
    print(f'{n_keys} keys will be analyzed in terms of risk configuration')
    risk_configuration = {}

    for key, key_i in zip(keys_list, range(1,len(keys_list)+1)):
        print(f'ITERATION {key_i} has started')
        volatility = key.split('vlty:')[1]
        # initialize key in risk_configuration
        if volatility not in risk_configuration:
            risk_configuration[volatility] = {}
        
        # get latest timeseries
        retry = True        
        # if response is not complete, retry with a new request
        while retry:
            retry = getTimeseries(info, key, check_past=360, look_for_newdata=True, plot=False)

        df1, df2, risk = RiskManagement(key)

        best_risk_key = risk['best_risk_key']
        best_mean_print = risk['best_mean_print']
        best_std_print = risk['best_std_print']

        golden_zone_str = get_substring_between(best_risk_key, "risk_golden_zone:", "_step:")
        golden_step_str = get_substring_between(best_risk_key, "_step:", "_step_no_golden:")
        nogolden_step_str = get_substring_between(best_risk_key, "_step_no_golden:", "_extratime:")
        extratime = best_risk_key.split('_extratime:')[1]
        frequency = info[key]["frequency/month"]

        risk_configuration[volatility][key] = {
            'riskmanagement_conf': {
                'golden_zone': golden_zone_str,
                'step_golden': golden_step_str,
                'step_nogolden': nogolden_step_str,
                'extra_timeframe': extratime,
                'estimated_gain': best_mean_print,
                'estimated_std': best_std_print,
                'frequency': frequency
            }}
        

    key_list = []
    golden_zone_list = []
    step_golden_list = []
    step_nogolden_list = []
    extra_timeframe_list = []
    estimated_gain_list = []
    estimated_std_list = []
    frequency_list = []

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
    for volatility in risk_configuration:
        for key in risk_configuration[volatility]:
            key_list.append(key)
            golden_zone_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['golden_zone'])
            step_golden_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['step_golden'])
            step_nogolden_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['step_nogolden'])
            extra_timeframe_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['extra_timeframe'])
            estimated_gain_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['estimated_gain'])
            estimated_std_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['estimated_std'])
            frequency_list.append(risk_configuration[volatility][key]['riskmanagement_conf']['frequency'])

    average_gain_percentage = sum(np.array(estimated_gain_list) * (np.array(frequency_list)/sum(frequency_list))) / 100
    df_dict = {"keys": key_list, "golden_zone": golden_zone_list, 'step_golden': step_golden_list, 'step_nogolden': step_nogolden_list,
                'extra_timeframe': extra_timeframe_list, 'estimated_gain': estimated_gain_list, 'estimated_std': estimated_std_list, 'frequency': frequency_list}
    

    
    risk_management_config_json = {'Timestamp': datetime.now().isoformat(), 'Info': risk_configuration_dict,
                                    'Dataframe': df_dict, 'RiskManagement': risk_configuration, 'Gain': {'average_per_event': round_(average_gain_percentage,2), 'n_events_per_month': sum(frequency_list)}}

    # SAVE FILE
    file_path = ROOT_PATH + "/riskmanagement_json/riskmanagement.json"
    with open(file_path, 'w') as file:
        json.dump(risk_management_config_json, file)

    # BACKUP RISKMANAGEMENT CONFIGURATION
    random_id = str(randint(1,1000))
    now = datetime.now()
    day = str(now.day)
    month = str(now.month)
    year = str(now.year)
    src = file_path
    minimum_event_number = str(risk_configuration_dict['minimum_event_number'])
    mean_threshold = str(risk_configuration_dict['mean_threshold'])
    dst = f"{ROOT_PATH}/riskmanagement_backup/riskmanagement-{day}-{month}-{year}-{minimum_event_number}-{mean_threshold}-{random_id}.json"
    shutil.copyfile(src, dst)
    t2 = time()
    time_spent = t2 - t1
    print(f'{time_spent} seconds spent to run wrap_analyze_events')



    # CREATE PANDAS DATAFRAME
    df = pd.DataFrame(df_dict)
    df = df.sort_values("estimated_gain", ascending=False)
    return df


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
    estimated_gain_list = df_dict['estimated_gain']
    frequency_list = df_dict['frequency']

    investment_amount = 100
    weighted_average_gain_percentage = sum(np.array(estimated_gain_list) * (np.array(frequency_list)/sum(frequency_list))) / 100
    average_gain_per_event = weighted_average_gain_percentage * investment_amount
    total_estimated_gain_1month = int(average_gain_per_event*sum(frequency_list))

    print(f'Estimated gain each month: {total_estimated_gain_1month} euro with investment amount {investment_amount} euro per event')

    # Print the pretty JSON
    print(pretty_json)

    df = pd.DataFrame(df_dict)
    df = df.sort_values("estimated_gain", ascending=False)

    return df

def send_riskconfiguration():
    file_path = ROOT_PATH + "/riskmanagement_json/riskmanagement.json"
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            # Retrieve shared memory for JSON data and "start_interval"
            riskmanagement_dict = json.load(file)

    # send request
    request = riskmanagement_dict['RiskManagement']
    url = 'http://localhost/analysis/riskmanagement-configuration'
    #url = 'https://algocrypto.eu/analysis/riskmanagement-configuration'

    response = requests.post(url, json = request)
    print('Status Code is : ', response.status_code)
    response = json.loads(response.text)