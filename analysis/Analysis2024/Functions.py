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
from Analysis2024.Helpers import round_, get_volume_standings, data_preparation, load_data, get_benchmark_info, get_dynamic_volatility, getnNewInfoForVolatilityGrouped, riskmanagement_data_preparation
from Analysis2024.Helpers import get_substring_between, load_analysis_json_info, updateAnalysisJson, pooled_standard_deviation, getsubstring_fromkey, get_currency_coin, load_volume_standings, filter_xth_percentile
import calendar
import seaborn as sn
from typing import Literal

ROOT_PATH="/Users/albertorainieri/Personal/analysis/Analysis2024"

def total_function_multiprocessing(event_keys, analysis_timeframe, n_processes, KEEP_PRODUCTION_ANALYSIS):
    '''
    this function loads only the data not analyzed and starts "wrap_analyze_events_multiprocessing" function. Finally it saves the output a path dedicated

    '''
    INTEGRATION=False
    
    t1 = time()

    # get volume_standings (the standings of all the coins in terms of amount of traded volume).
    volume_standings_full = load_volume_standings()


    # get json_analysis path. Check if there is already some data analyzed in json_analysis/ path. The new data will be appended
    total_combinations = len(event_keys)
    print('total_combinations', ': ', total_combinations)

    #load analysis json info
    if KEEP_PRODUCTION_ANALYSIS:
        analysis_json_path = ROOT_PATH + '/analysis_json_production/analysis.json'
    else:
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
    pool.starmap(wrap_analyze_events_multiprocessing, [(arg, arg_i, event_keys, volume_standings_full, end_interval_analysis, lock, shared_data) for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])

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


def wrap_analyze_events_multiprocessing(data, data_i, event_keys, volume_standings_full, end_interval_analysis, lock, shared_data):
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
        total_combinations = len(event_keys)
        PERC_10 = True
        THRESHOLD_10 = total_combinations * 0.1
        PERC_25 = True
        THRESHOLD_25 = total_combinations * 0.25
        PERC_50 = True
        THRESHOLD_50 = total_combinations * 0.5
        PERC_75 = True
        THRESHOLD_75 = total_combinations * 0.75
        iteration = 0

    #print(f'Slice {data_i} has started')
    temp = {}
    for event_key in event_keys:
        vol, vol_value, buy_vol, buy_vol_value, timeframe, lvl = getsubstring_fromkey(event_key)
        if data_i == 1:
            iteration += 1
            if PERC_10:
                if iteration > THRESHOLD_10:
                    PERC_10 = False
                    #print('1/10 of data analyzed')
            elif PERC_25:
                if iteration > THRESHOLD_25:
                    PERC_25 = False
                    #print('1/4 of data analyzed')
            elif PERC_50:
                if iteration > THRESHOLD_50:
                    PERC_50 = False
                    print('1/2 of data analyzed')
            elif PERC_75:
                if iteration > THRESHOLD_75:
                    PERC_75 = False
                    #print('3/4 of data analyzed')
            
    
        complete_info = analyze_events(data, vol, vol_value, buy_vol, buy_vol_value, timeframe, lvl, volume_standings_full, end_interval_analysis)
    
        # price changes is a dict with keys regarding the volatility: {'1': [...], '2': [...], ..., '5': [...], ...}
        # the value of the dict[volatility] is a list of all price_changes of a particular event
        # let's define the key: all event info + volatility coin
        # vol, vol_value, buy_vol, buy_vol_value, timeframe, lvl = getsubstring_fromkey(event_key)

        temp[event_key] = {}
        temp[event_key]['info'] = complete_info

    del complete_info


    # Lock Shared Variable
    # this lock is essential in multiprocessing which permits to work on shared resources
    with lock:
        resp = json.loads(shared_data.value)

        for event_key in list(temp.keys()):
            
            if event_key not in resp:
                resp[event_key] = {}

            if 'info' in resp[event_key]:
                # update complete "info"
                for coin in temp[event_key]['info']:
                    if coin not in resp[event_key]['info']:
                        resp[event_key]['info'][coin] = []
                    for event in temp[event_key]['info'][coin]:
                        resp[event_key]['info'][coin].append(event)
                
            else:
                # initialize the event_keys of resp[event_key]
                resp[event_key]['info'] = temp[event_key]['info']

        del temp

        shared_data.value = json.dumps(resp)
        size_shared_data_value = int(sys.getsizeof(shared_data.value)) / 10**6

        n_events = 0
        for event_key in resp:
            for coin in resp[event_key]['info']:
                n_events += len(resp[event_key]['info'][coin])

    #print(f'Data saved fot slice {data_i}')
    print(f'size of shared_data_value before completion of {data_i}: {size_shared_data_value} Mb; Number of events saved: {n_events}')
    #print(f'Events recorded from slice {data_i}: ')


def analyze_events(data, vol, vol_value, buy_vol, buy_vol_value, timeframe, lvl, volume_standings_full, end_interval_analysis):
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
    #print(data)
    # analyze for each coin
    for coin in list(data.keys()):
        if coin in get_currency_coin():
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
                if buy_vol in obs and obs[buy_vol] != None:
                    # if buy_vol is greater than limit and
                    # if vol is greater than limit and
                    # if datetime_obs does not fall in a previous analysis window. (i.e. datetime_obs is greater than the limit_window set)
                    #if obs[buy_vol_field] >= event_buy_volume and obs[vol_field] > event_volume and datetime_obs > limit_window and obs[trades_field] != None:
                    if obs[vol] > vol_value and datetime_obs > limit_window:

                        # if buy_vol is None, it means that buy_vol check is skipped
                        if buy_vol_value != 0:
                            # if buy_vol is greater than 0.5 then look for events above threshold
                            if buy_vol_value > 0.5:
                                if obs[buy_vol] < buy_vol_value:
                                    continue
                            # otherwise look for events below threshold
                            elif obs[buy_vol] > buy_vol_value:
                                    continue


                        # get initial price of the coin at the triggered event
                        initial_price = obs['price']

                        # get initial timestamp of the coin at the triggered event
                        timestamp_event_triggered = obs['_id']

                        # initialize current price_changes
                        event_price_changes = []

                        # get dynamic volume standings
                        volume_standings = int(get_volume_standings(volume_standings_full, obs['_id'], coin))
                        if lvl != None and volume_standings > lvl:
                            continue

                        if coin not in complete_info:
                            complete_info[coin] = []

                        max_price_change = 0
                        min_price_change = 0

                        # initialize "price_changes" to fetch all changes for THIS event
                        price_changes = []

                        limit_window  = datetime_obs + timedelta(minutes=timeframe)
                        # get all the price changes in the "timeframe"
                        for obs_event, obs_i in zip(data[coin][index:index+timeframe], range(timeframe)):
                            # if actual observation has occurred in the last minute and 10 seconds from last observation, let's add the price "change" in "price_changes":
                            actual_datetime = datetime.fromisoformat(data[coin][index+obs_i]['_id'])
                            if actual_datetime - datetime.fromisoformat(data[coin][index+obs_i-1]['_id']) <= timedelta(minutes=10,seconds=10):
                                
                                change = (obs_event['price'] - initial_price)/initial_price
                                max_price_change = max(max_price_change, change)
                                min_price_change = min(min_price_change, change)

                                price_changes.append(change)
                                event_price_changes.append(change)

                        complete_info[coin].append({'event': timestamp_event_triggered, 'mean': round_(np.mean(event_price_changes),4), 'std': round(np.std(event_price_changes, ddof=1),4),
                                                                'max': max_price_change, 'min': min_price_change, 'lvl': volume_standings})
            else:
                # in case an observations of a coin goes beyond "end_interval_analysis", then I skip the whole analysis of the coin,
                # since the obsevations are ordered chronologically
                # print(f'Analysis for {coin} is completed: Last timestamp: {datetime_obs.isoformat()}; {index}')
                break

    return complete_info


def download_show_output(minimum_event_number, mean_threshold, frequency_threshold, early_validation=False, std_multiplier=3,
                          start_analysis=datetime(2023,1,1), file_paths=None, DELETE_99_PERCENTILE=True, filter_field='mean', xth_percentile=99):
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
    aggregate_output = {}
    aggregate_complete_info = {}

    for file_path in file_paths:

        output = {}
        complete_info = {}
        first_event = {}
        last_event = {}
        # download data if shared_data is None
        with open(file_path, 'r') as file:
            print(f'Downloading {file_path}')
            # Retrieve shared memory for JSON data and "start_interval"
            analysis_json = json.load(file)


        #return shared_data
        analysis_json = analysis_json['data']

        for event_key in list(analysis_json.keys()):
            
            n_coins = len(analysis_json[event_key]['info'])
            # print(key)
            # print(analysis_json[key].keys())
            vol, vol_value, buy_vol, buy_vol_value, timeframe, lvl = getsubstring_fromkey(event_key)
            n_events = 0
            mean_by_coin_list = []
            std_by_coin_list = []
            max_by_coin_list = []
            min_by_coin_list = []
            info_by_coin_list = {}
            standings_by_coin_list = []
            list_coins = []
            # I want to get earliest and latest event
            # intitialize first_event and last_event

            if event_key not in first_event and len(analysis_json[event_key]['info']) != 0:
                random_coin = list(analysis_json[event_key]['info'].keys())[0]
                first_event[event_key] = datetime.fromisoformat(analysis_json[event_key]['info'][random_coin][0]['event'])
                last_event[event_key] =  datetime(1970,1,1)

            for coin in analysis_json[event_key]['info']:
                for event in analysis_json[event_key]['info'][coin]:
                    if coin in get_currency_coin():
                        continue
                    if early_validation and datetime.fromisoformat(event['event']) > early_validation: 
                        continue
                    elif start_analysis and datetime.fromisoformat(event['event']) < start_analysis:
                        continue
                    else:
                        if coin not in list_coins:
                            list_coins.append(coin)
                        n_events += 1
                        info_by_coin_list[event['event']] = [event['mean'], event['std'], event['max'], event['min']]
                        # mean_by_coin_list.append(event['mean'])
                        # std_by_coin_list.append(event['std'])
                        # max_by_coin_list.append(event['max'])
                        # min_by_coin_list.append(event['min'])
                        standings_by_coin_list.append(int(event['lvl']))
                        first_event[event_key] = min(datetime.fromisoformat(event['event']), first_event[event_key])
                        last_event[event_key] = max(datetime.fromisoformat(event['event']), last_event[event_key])
            
            if DELETE_99_PERCENTILE:
                filtered_info_by_coin_list, events_discarded = filter_xth_percentile(info_by_coin_list, filter_field, xth_percentile)
            
            for event in filtered_info_by_coin_list:
                mean_by_coin_list.append(filtered_info_by_coin_list[event][0])
                std_by_coin_list.append(filtered_info_by_coin_list[event][1])
                max_by_coin_list.append(filtered_info_by_coin_list[event][2])
                min_by_coin_list.append(filtered_info_by_coin_list[event][3])

            if len(mean_by_coin_list) > 0:
                mean_by_coin = round_(np.mean(np.array(mean_by_coin_list))*100,2)
                median_max_by_coin = round_(np.median(np.array(max_by_coin_list))*100,2)
                median_min_by_coin = round_(np.median(np.array(min_by_coin_list))*100,2)
                max_by_coin = round_(np.mean(np.array(max_by_coin_list))*100,2)
                min_by_coin = round_(np.mean(np.array(min_by_coin_list))*100,2)
                std_by_coin = round_(pooled_standard_deviation(np.array(std_by_coin_list), sample_size=int(timeframe))*100,2)
                avg_standings_by_coin = round_(np.mean(np.array(standings_by_coin_list)),2)

                # Start filling output and complete_info
                if event_key not in output:
                    # initialize output and complete_info
                    output[event_key] = {'mean': [(mean_by_coin,len(mean_by_coin_list))], 
                                        'std': [(std_by_coin,len(std_by_coin_list))],
                                        'max': [(max_by_coin,len(max_by_coin_list))],
                                        'min': [(min_by_coin,len(min_by_coin_list))],
                                        'median_max': [(median_max_by_coin,len(mean_by_coin_list))],
                                        'median_min': [(median_min_by_coin,len(mean_by_coin_list))],
                                        'standings': [(avg_standings_by_coin,len(standings_by_coin_list))],
                                        'n_coins': list_coins, 'n_events': n_events}
                    #update 'info' by adding "volatility" variable
                    complete_info[event_key] = {'info': {}, 'n_coins': list_coins, 'events': n_events}
                    for coin in analysis_json[event_key]['info']:
                        if coin in get_currency_coin():
                            continue
                        if coin not in complete_info[event_key]['info']:
                            # initialize coin in complete_info
                            complete_info[event_key]['info'][coin] = []
                        for event in analysis_json[event_key]['info'][coin]:
                            if datetime.fromisoformat(event['event']) > start_analysis:
                                complete_info[event_key]['info'][coin].append(event)

                else:
                    # update output
                    output[event_key]['mean'].append((mean_by_coin,len(mean_by_coin_list)))
                    output[event_key]['std'].append((std_by_coin,len(std_by_coin_list)))
                    output[event_key]['max'].append((max_by_coin,len(max_by_coin_list)))
                    output[event_key]['min'].append((min_by_coin,len(min_by_coin_list)))
                    output[event_key]['median_max'].append((median_max_by_coin,len(mean_by_coin_list)))
                    output[event_key]['median_min'].append((median_min_by_coin,len(mean_by_coin_list)))
                    output[event_key]['standings'].append((avg_standings_by_coin,len(standings_by_coin_list)))
                    output[event_key]['n_coins'] += list_coins
                    output[event_key]['n_events'] += n_events
                    
                    # update complete info
                    complete_info[event_key]['n_coins'] += list_coins
                    complete_info[event_key]['events'] += n_events
                    for coin in analysis_json[event_key]['info']:
                        if coin in get_currency_coin():
                            continue
                        if coin not in complete_info[event_key]['info']:
                            complete_info[event_key]['info'][coin] = []
                        for event in analysis_json[event_key]['info'][coin]:
                            if datetime.fromisoformat(event['event']) > start_analysis:
                                complete_info[event_key]['info'][coin].append(event)
            else:
                continue

        delete_keys = []
        # Filter best keys by performance
        for event_key in output:
            mean = sum([item[0]*item[1] for item in output[event_key]['mean']]) / sum([item[1] for item in output[event_key]['mean']] )
            std = sum([item[0]*item[1] for item in output[event_key]['std']]) / sum([item[1] for item in output[event_key]['std']] )
            median_max = sum([item[0]*item[1] for item in output[event_key]['median_max']]) / sum([item[1] for item in output[event_key]['median_max']] )
            median_min = sum([item[0]*item[1] for item in output[event_key]['median_min']]) / sum([item[1] for item in output[event_key]['median_min']] )
            max_ = sum([item[0]*item[1] for item in output[event_key]['max']]) / sum([item[1] for item in output[event_key]['max']] )
            min_ = sum([item[0]*item[1] for item in output[event_key]['min']]) / sum([item[1] for item in output[event_key]['min']] )
            avg_standings = sum([item[0]*item[1] for item in output[event_key]['standings']]) / sum([item[1] for item in output[event_key]['standings']] )

            # get frequency event per month
            #time_interval_analysis = (last_event[event_key] - first_event[event_key]).days
            time_interval_analysis = (early_validation - start_analysis).days
            n_events = output[event_key]['n_events']
            if time_interval_analysis != 0:
                event_frequency_month = round_((n_events / time_interval_analysis) * 30,2)

            if output[event_key]['n_events'] >= minimum_event_number and mean > mean_threshold and mean <= std_multiplier * std and event_frequency_month >= frequency_threshold:
                output[event_key]['mean'] = mean
                output[event_key]['median_max'] = median_max
                output[event_key]['median_min'] = median_min
                output[event_key]['std'] = std
                output[event_key]['max'] = max_
                output[event_key]['min'] = min_
                output[event_key]['standings'] = avg_standings
                output[event_key]['upper_bound'] = output[event_key]['mean'] + output[event_key]['std']
                output[event_key]['lower_bound'] = output[event_key]['mean'] - output[event_key]['std']
                output[event_key]['n_coins'] = len(set(output[event_key]['n_coins']))
                output[event_key]['frequency/month'] = event_frequency_month
                complete_info[event_key]['n_coins'] = len(set(complete_info[event_key]['n_coins']))
                complete_info[event_key]['frequency/month'] = event_frequency_month
            else:
                delete_keys.append(event_key)

        for event_key in delete_keys:
            output.pop(event_key)
            complete_info.pop(event_key)
        
        # aggregate output
        for event_key in output:
            aggregate_output[event_key] = output[event_key]
            aggregate_complete_info[event_key] = complete_info[event_key]

    return aggregate_output, aggregate_complete_info
            
