import json
import os,sys
sys.path.insert(0,'..')

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
from multiprocessing import Pool, Manager
from copy import copy
from random import randint
import shutil
import re
from Analysis2024.Helpers import round_, get_volume_standings, data_preparation, load_data, get_benchmark_info, get_dynamic_volatility, getnNewInfoForVolatilityGrouped, plotTimeseries, riskmanagement_data_preparation
from Analysis2024.Helpers import get_substring_between, load_analysis_json_info, updateAnalysisJson, pooled_standard_deviation, getsubstring_fromkey, load_timeseries, getTimeseries, load_volume_standings
import calendar
import seaborn as sn
from typing import Literal

ROOT_PATH="/Users/albertorainieri/Personal/analysis/Analysis2024"

def total_function_multiprocessing(list_time_interval_field, list_order_concentration, list_minutes, list_event_buy_volume, list_event_volume, n_processes, analysis_timeframe):
    '''
    this function loads only the data not analyzed and starts "wrap_analyze_events_multiprocessing" function. Finally it saves the output a path dedicated

    '''
    INTEGRATION=False
    
    t1 = time()

    # get volume_standings (the standings of all the coins in terms of amount of traded volume).
    volume_standings = load_volume_standings()


    # get json_analysis path. Check if there is already some data analyzed in json_analysis/ path. The new data will be appended
    total_combinations = len(list_time_interval_field) * len(list_order_concentration) * len(list_minutes) * len(list_event_volume) * len(list_event_buy_volume)
    print('total_combinations', ': ', total_combinations)

    #load analysis json info
    analysis_json_path = ROOT_PATH + '/analysis_json/analysis.json'
    start_interval, end_interval = load_analysis_json_info(analysis_json_path, analysis_timeframe=analysis_timeframe, INTEGRATION=INTEGRATION)

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
    end_interval_analysis = datetime.fromisoformat(data['BTCUSDT'][-1]['_id']) - timedelta(days=1) #last datetime from btcusdt - 3 days
    del data
    # This is going to be also the starting time for next analysis
    start_next_analysis = end_interval_analysis.isoformat()
    print(f'Events from {start_interval} to {start_next_analysis} will be analyzed')

    # Execute the function "wrap_analyze_events_multiprocessing" in parallel
    pool.starmap(wrap_analyze_events_multiprocessing, [(arg, arg_i, list_time_interval_field, list_order_concentration, list_minutes,
                                         list_event_buy_volume, list_event_volume, volume_standings, end_interval_analysis, lock, shared_data) for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])

    # Close the pool
    pool.close()
    print('pool closed')
    pool.join()
    print('pool joined')
    del data_arguments

    data = json.loads(shared_data.value)
    # save new updated analysis to json_analysis/


    # update the analysis json for storing performances
    #updateAnalysisJson(shared_data.value, file_path, start_next_analysis, INTEGRATION)
    updateAnalysisJson(shared_data.value, analysis_json_path, start_next_analysis, slice_i=None, start_next_analysis_str=None, INTEGRATION=INTEGRATION)

    t2 = time()
    print(t2-t1, ' seconds')


def wrap_analyze_events_multiprocessing(data, data_i, list_time_interval_field, list_order_concentration, list_minutes,
                                         list_event_buy_volume, list_event_volume, volume_standings, end_interval_analysis, lock, shared_data):
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
        total_combinations = len(list_time_interval_field) * len(list_minutes) * len(list_event_volume) * len(list_event_buy_volume) * len(list_order_concentration)
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
    for time_interval_field in list_time_interval_field:
        buy_vol_field = "buy_vol_" + time_interval_field
        vol_field = "vol_" + time_interval_field
        trades_field = "trades_" + time_interval_field
        for minutes_price_windows in list_minutes:
            for event_buy_volume in  list_event_buy_volume:
                for event_volume in list_event_volume:
                    for order_concentration_level in list_order_concentration:
                        if data_i == 1:
                            iteration += 1
                            if PERC_10:
                                if iteration > THRESHOLD_10:
                                    PERC_10 = False
                                    print('1/10 of data analyzed')
                            elif PERC_25:
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
                            
                    
                        complete_info, nan = analyze_events(data, buy_vol_field, vol_field, trades_field, minutes_price_windows, order_concentration_level, 
                                                            event_buy_volume, event_volume, volume_standings, end_interval_analysis)
                    
                        # price changes is a dict with keys regarding the volatility: {'1': [...], '2': [...], ..., '5': [...], ...}
                        # the value of the dict[volatility] is a list of all price_changes of a particular event
                        for volatility in complete_info:
                            # let's define the key: all event info + volatility coin
                            key = str(buy_vol_field) + ':' + str(event_buy_volume) + '/' + str(vol_field) + ':' + str(event_volume) + '/' + str(trades_field) + ':' + str(order_concentration_level) + '/' + 'timeframe:' + str(minutes_price_windows) + '/' + 'vlty:' + volatility
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


def analyze_events(data, buy_vol_field, vol_field, trades_field, minutes_price_windows, order_concentration_level, event_buy_volume, event_volume, volume_standings_full, end_interval_analysis):
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

        if coin == 'EURUSDT':
            continue
        
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
                if buy_vol_field in obs and obs[buy_vol_field] != None:
                    # if buy_vol is greater than limit and
                    # if vol is greater than limit and
                    # if datetime_obs does not fall in a previous analysis window. (i.e. datetime_obs is greater than the limit_window set)
                    #if obs[buy_vol_field] >= event_buy_volume and obs[vol_field] > event_volume and datetime_obs > limit_window and obs[trades_field] != None:
                    if obs[buy_vol_field] >= event_buy_volume and obs[vol_field] > event_volume and datetime_obs > limit_window:
                        
                        # UNCOMMENT FOR TRADE CONCENTRATIION ANALYSIS
                        # trades_order = obs[trades_field]
                        # order_concentration = trades_order / obs[vol_field]
                        # if order_concentration > order_concentration_level:
                        #     continue
                        
                        #print(f'buy_vol: {buy_vol_field} -> {event_buy_volume} - vol: {vol_field} -> {event_volume} - order_conc: {order_concentration_level}')
                        
                        # if filter_out_events(vol_field, event_volume, buy_vol_field, event_buy_volume):
                        #     continue

                        # get initial price of the coin at the triggered event
                        initial_price = obs['price']

                        # get initial timestamp of the coin at the triggered event
                        timestamp_event_triggered = obs['_id']

                        # initialize current price_changes
                        event_price_changes = []

                        # get dynamic volatility
                        volume_standings = str(get_volume_standings(volume_standings_full, obs['_id'], coin))

                        # initialize "price_changes" and "events" if first time
                        if volume_standings not in complete_info:
                            complete_info[volume_standings] = {}

                        if coin not in complete_info[volume_standings]:
                            complete_info[volume_standings][coin] = []

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

                        complete_info[volume_standings][coin].append({'event': timestamp_event_triggered, 'mean': round_(np.mean(event_price_changes),4), 'std': round(np.std(event_price_changes, ddof=1),4),
                                                                'max': max_price_change, 'min': min_price_change})
            else:
                # in case an observations of a coin goes beyond "end_interval_analysis", then I skip the whole analysis of the coin,
                # since the obsevations are ordered chronologically
                # print(f'Analysis for {coin} is completed: Last timestamp: {datetime_obs.isoformat()}; {index}')
                break

    return complete_info, nan


def download_show_output(minimum_event_number, minimum_coin_number, mean_threshold, lb_threshold, frequency_threshold, group_coins=False, best_coins_volatility=None, early_validation=False, std_multiplier=3, file_path=None):
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
    if file_path == None:
        path = ROOT_PATH + "/analysis_json/"
        file_path = path + 'analysis' + '.json'
        print('Reading from ANALYSIS 2023')
    else:
        print('Reading from ANALYSIS 2024')

    # download data if shared_data is None
    with open(file_path, 'r') as file:
        print(f'Downloading {file_path}')
        # Retrieve shared memory for JSON data and "start_interval"
        shared_data = json.load(file)


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
        first_event = {}
        last_event = {}
        for key in list(shared_data.keys()):
            n_coins = len(shared_data[key]['info'])
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
                # I want to get earliest and latest event
                # intitialize first_event and last_event

                if key_without_volatility not in first_event:
                    random_coin = list(shared_data[key]['info'].keys())[0]
                    first_event[key_without_volatility] = datetime.fromisoformat(shared_data[key]['info'][random_coin][0]['event'])
                    last_event[key_without_volatility] =  datetime(1970,1,1)

                for coin in shared_data[key]['info']:
                    for event in shared_data[key]['info'][coin]:
                        if early_validation and datetime.fromisoformat(event['event']) > early_validation: 
                            continue
                        else:
                            n_events += 1
                            mean_by_coin_list.append(event['mean'])
                            std_by_coin_list.append(event['std'])
                            first_event[key_without_volatility] = min(datetime.fromisoformat(event['event']), first_event[key_without_volatility])
                            last_event[key_without_volatility] = max(datetime.fromisoformat(event['event']), last_event[key_without_volatility])

                if len(mean_by_coin_list) > 0:
                    mean_by_coin = round_(np.mean(np.array(mean_by_coin_list))*100,2)
                    median_by_coin = round_(np.median(np.array(mean_by_coin_list))*100,2)
                    std_by_coin = round_(pooled_standard_deviation(np.array(std_by_coin_list), sample_size=int(timeframe))*100,2)

                    # Start filling output and complete_info
                    if key_without_volatility not in output:
                        # initialize output and complete_info
                        output[key_without_volatility] = {'mean': [(mean_by_coin,len(mean_by_coin_list))], 'std': [(std_by_coin,len(std_by_coin_list))],
                                                           'median': [(median_by_coin,len(mean_by_coin_list))], 'n_coins': list_coins, 'n_events': n_events}
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
                        output[key_without_volatility]['mean'].append((mean_by_coin,len(mean_by_coin_list)))
                        output[key_without_volatility]['std'].append((std_by_coin,len(std_by_coin_list)))
                        output[key_without_volatility]['median'].append((median_by_coin,len(mean_by_coin_list)))
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
                else:
                    continue

        delete_keys = []
        # Filter best keys by performance
        for key_without_volatility in output:
            mean = sum([item[0]*item[1] for item in output[key_without_volatility]['mean']]) / sum([item[1] for item in output[key_without_volatility]['mean']] )
            std = sum([item[0]*item[1] for item in output[key_without_volatility]['std']]) / sum([item[1] for item in output[key_without_volatility]['std']] )
            median = sum([item[0]*item[1] for item in output[key_without_volatility]['median']]) / sum([item[1] for item in output[key_without_volatility]['median']] )

            # get frequency event per month
            time_interval_analysis = (last_event[key_without_volatility] - first_event[key_without_volatility]).days
            n_events = output[key_without_volatility]['n_events']
            if time_interval_analysis != 0:
                event_frequency_month = round_((n_events / time_interval_analysis) * 30,2)

            if output[key_without_volatility]['n_events'] >= minimum_event_number and mean > mean_threshold and mean <= std_multiplier * std and event_frequency_month >= frequency_threshold:
                output[key_without_volatility]['mean'] = mean
                output[key_without_volatility]['median'] = median
                output[key_without_volatility]['std'] = std
                output[key_without_volatility]['upper_bound'] = output[key_without_volatility]['mean'] + output[key_without_volatility]['std']
                output[key_without_volatility]['lower_bound'] = output[key_without_volatility]['mean'] - output[key_without_volatility]['std']
                output[key_without_volatility]['n_coins'] = len(set(output[key_without_volatility]['n_coins']))
                output[key_without_volatility]['frequency/month'] = event_frequency_month
                complete_info[key_without_volatility]['n_coins'] = len(set(complete_info[key_without_volatility]['n_coins']))
                complete_info[key_without_volatility]['frequency/month'] = event_frequency_month
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
            
        