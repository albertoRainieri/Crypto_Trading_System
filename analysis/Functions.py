import json
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta, timezone
import requests
import os
from time import sleep
import pytz
from scipy.stats import pearsonr
import pandas as pd
from time import time
from random import randint
from multiprocessing import Process
from multiprocessing import Lock, Pool, Manager


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
    if coin is not None:
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


def analyze_events(data, buy_vol_field, vol_field, minutes_price_windows, event_buy_volume, event_volume, multiprocessing=True):
    '''
    This function analyzes what happens in terms of price changes after certain events

    data: it is the dataset
    buy_vol_field: it is the key that determines which buy_vol in terms of timeframe (5m, 15m, 30m, 60m, ..., 1d) --> STRING (e.g. buy_vol_5m)
    vol_field: it is the key that determines which vol in terms of timeframe (5m, 15m, 30m, 60m, ..., 1d) --> STRING (e.g. vol_5m)
    minutes_price_windows: time window in terms of minutes. how many minutes into the future I want the check the price changes? --> INTEGER (e.g. 60)
    event_buy_volume: it is the value of "buy_vol_field". --> FLOAT (e.g. 0.6) MUST BE BETWEEN 0 and 1
    event_volume: it is the value of "buy_vol_field". --> FLOAT (e.g. 2.0)
    '''
    price_changes = {}
    events = 0
    #print(data)
    tot_n_coins = len(list(data.keys()))
    # analyze for each coin
    for coin in list(data.keys()):
        # initialize limit_window
        limit_window = datetime(2000,1,1)

        # get initial price of the coin
        initial_price = data[coin][0]['price']
        price_changes[coin] = []

        # check through each observation of the coin
        for obs, index in zip(data[coin], range(len(data[coin]))):

            # this datetime_obs is needed to not trigger too many events. For example two closed events will overlap each other, this is not ideal
            datetime_obs = datetime.fromisoformat(obs['_id'])
            if obs[buy_vol_field] is not None:
                if buy_vol_field in obs and buy_vol_field and obs and obs[buy_vol_field] is not None and obs[vol_field] is not None:

                    # if buy_vol is greater than limit and
                    # if vol is greater than limit and
                    # if datetime_obs does not fall in a previous analysis window. (i.e. datetime_obs is greater than the limit_window set)
                    if obs[buy_vol_field] >= event_buy_volume and obs[vol_field] > event_volume and datetime_obs > limit_window:
                        #EVENT TRIGGERED
                        events += 1
                        limit_window  = datetime_obs + timedelta(minutes=minutes_price_windows)
                        # get all the price changes in the "minutes_price_windows"
                        for obs, obs_i in zip(data[coin][index:index+minutes_price_windows], range(minutes_price_windows)):
                            # if actual observation has occurred in the last minute and 10 seconds from last observation:
                            if datetime.fromisoformat(data[coin][index+obs_i]['_id']) - datetime.fromisoformat(data[coin][index+obs_i-1]['_id']) <= timedelta(minutes=1,seconds=10):
                                change = (obs['price'] - initial_price)/initial_price
                                if not np.isnan(change):
                                    price_changes[coin].append(change)


    total_changes = []
    coins = []
    for coin in price_changes:
        if len(price_changes[coin]) > 0:
            coins.append(coin)
        price_changes[coin] = np.mean(price_changes[coin])
        if not np.isnan(price_changes[coin]):
            total_changes.append(price_changes[coin])
    
    if multiprocessing:
        return total_changes, events, coins

    else:
        mean_total_changes = np.mean(total_changes)*100
        std_total_changes = np.std(total_changes)*100
        return mean_total_changes, std_total_changes, events, coins


#['_id',
#'price', price_%_1d','price_%_6h','price_%_3h','price_%_1h',
#'vol_1m', 'buy_vol_1m', buy_trd_1m
#'vol_5m','vol_5m_std','buy_vol_5m','buy_trd_5m'
#'vol_15m','vol_15m_std','buy_vol_15m','buy_trd_15m',
#'vol_30m','vol_30m_std','buy_vol_30m','buy_trd_30m',
#'vol_60m','vol_60m_std','buy_vol_60m','buy_trd_60m',
#'vol_24h','vol_24h_std','buy_vol_24h','buy_trd_24h',]

def show_output(shared_data):
    '''
    This function takes as input the shared_data from "wrap_analyze_events_multiprocessing" ans return the output available for pandas
    '''
    shared_data = json.loads(shared_data.value)
    output = {}
    for key in list(shared_data.keys()):
        if key is not 'coins' or key is not 'events':
            if shared_data[key]['events'] > 0:
                shared_data[key]['price_changes'] = np.array(shared_data[key]['price_changes'])

                isfinite = np.isfinite(shared_data[key]['price_changes'])
                shared_data[key]['price_changes'] = shared_data[key]['price_changes'][isfinite]

                mean_weighted = np.mean(shared_data[key]['price_changes'])*100
                std_weighted = np.std(shared_data[key]['price_changes'])*100

                output[key] = {'mean': mean_weighted, 'std': std_weighted, 'n_coins': len(shared_data[key]['coins']), 'n_events': shared_data[key]['events']}
            else:
                output[key] = {'mean': None, 'std': None, 'n_coins': 0, 'n_events': 0}

    return output


def wrap_analyze_events_multiprocessing(data, data_i, list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, start_interval, end_interval, file_path, lock, shared_data):
    '''
    this function summarizes the events for every field of vol_x, buy_vol_x and a list of timeframes.
    list_buy_vol --> LIST: ['buy_vol_5m, ..., 'buy_vol_1d']
    buy_vol --> LIST: ['vol_5m, ..., 'vol_1d']
    list_minutes --> LIST: [5,10, ..., 60, ..., 60*24]
    list_event_buy_volume --> LIST: [0.55, 0.6, 0.7, 0.8, 0.9]
    list_event_volume --> LIST: [2, 3, 4, 5, 6]
    it's been tested and it provides the same result of wrap_analyze_events but much faster
    '''

    
    # mean_list = []
    # std_list = []
    # events_tot = 0


    temp = {}
    for buy_vol_field in list_buy_vol:
        for vol_field in list_vol:
            for minutes_price_windows in list_minutes:
                for event_buy_volume in  list_event_buy_volume:
                    for event_volume in list_event_volume:
                        
                        price_changes, events, coins = analyze_events(data, buy_vol_field, vol_field, minutes_price_windows, event_buy_volume, event_volume)
                        
                        # there was at least an event. let's save it into the json
                        key = str(buy_vol_field) + ':' + str(event_buy_volume) + '/' + str(vol_field) + ':' + str(event_volume) + '/' + 'timeframe:' + str(minutes_price_windows)
                        temp[key] = {}

                        temp[key]['price_changes'] = price_changes
                        #temp[key]['std'] = [std_total_changes]
                        temp[key]['coins'] = coins
                        temp[key]['events'] = events


    
    # Lock Shared Variable
    with lock:
        resp = json.loads(shared_data.value)

        for key in list(temp.keys()):
            
            if key not in resp:
                resp[key] = {}

            if 'price_changes' in resp[key]:
                for price_change in temp[key]['price_changes']:        
                    resp[key]['price_changes'].append(price_change)
                resp[key]['events'] += temp[key]['events']
                for coin in temp[key]['coins']:
                    if coin not in resp[key]['coins']:
                        resp[key]['coins'].append(coin)
            else:
                resp[key]['price_changes'] = temp[key]['price_changes']
                resp[key]['coins'] = temp[key]['coins']
                resp[key]['events'] = temp[key]['events']

        shared_data.value = json.dumps(resp)

def start_wrap_analyze_events_multiprocessing(data, list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, start_interval, end_interval, n_processes):
    '''
    this function starts "wrap_analyze_events_multiprocessing" function
    '''
    t1 = time()
    data_arguments = data_preparation(data, n_processes = n_processes)
    total_combinations = len(list_buy_vol) * len(list_vol) * len(list_minutes) * len(list_event_volume) * len(list_event_buy_volume)
    print('total_combinantions', ': ', total_combinations)
    path = "/home/alberto/Docker/Trading/analysis/analysis_json/"

    now = datetime.now()
    now_year = str(now.year)
    now_month = str(now.month)
    now_day = str(now.day)
    now_hour = str(now.hour)
    now_minute = str(now.minute)
    file_path = path + now_month+now_day+ '_' + now_hour+now_minute + '.json'

    manager = Manager()
    # Create shared memory for JSON data
    shared_data = manager.Value(str, json.dumps({}))
    lock = Manager().Lock()


    # Create a multiprocessing Pool
    pool = Pool()

    # Execute the function in parallel
    pool.starmap(wrap_analyze_events_multiprocessing, [(arg, arg_i, list_buy_vol, list_vol, list_minutes,
                                        list_event_buy_volume, list_event_volume, start_interval,
                                        end_interval, file_path, lock, shared_data) for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])

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
    


def wrap_analyze_events(data, list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, start_interval, end_interval, multiprocessing=False):
    '''
    this function summarizes the events for every field of vol_x, buy_vol_x and a list of timeframes.
    list_buy_vol --> LIST: ['buy_vol_5m, ..., 'buy_vol_1d']
    buy_vol --> LIST: ['vol_5m, ..., 'vol_1d']
    list_minutes --> LIST: [5,10, ..., 60, ..., 60*24]
    list_event_buy_volume --> LIST: [0.55, 0.6, 0.7, 0.8, 0.9]
    list_event_volume --> LIST: [2, 3, 4, 5, 6]
    this functions does not use multiprocessing
    '''
    total_combinations = len(list_buy_vol) * len(list_vol) * len(list_minutes) * len(list_event_volume) * len(list_event_buy_volume)
    print(f'{total_combinations} total combinations')
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
                        mean_total_changes, std_total_changes, events, coins = analyze_events(data, buy_vol_field, vol_field, minutes_price_windows, event_buy_volume, event_volume, multiprocessing)
                        if mean_total_changes == None:
                            continue

                        # there was at least an event. let's save it into the json
                        key = str(buy_vol_field) + ':' + str(event_buy_volume) + '/' + str(vol_field) + ':' + str(event_volume) + '/' + 'timeframe:' + str(minutes_price_windows)
                        
                        resp[key] = {'mean': round_(mean_total_changes,2), 'std': round_(std_total_changes,2), 'coins': len(coins), 'events': events}
    
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
    step = int(n_coins / n_processes)
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
    
    if step * n_processes != len(list(data.keys())):
        data_i = {}
        for coin in coins_list[step * n_processes:]:
            data_i[coin] = data[coin]
        data_arguments.append(data_i)

    total_coins = 0
    start_interval = data['BTCUSDT'][0]['_id']
    end_interval = data['BTCUSDT'][-1]['_id']
    for slice_coins in data_arguments:
        total_coins += len(slice_coins)


    print(f'{total_coins} coins will be analyzed from {start_interval} to {end_interval}')

    del data
    return data_arguments



def load_data(start_interval=datetime(2023,5,7, tzinfo=pytz.UTC), end_interval=datetime.now(tz=pytz.UTC), filter_position=(0,50)):
    # get MOST_TRADED_COINS.json froms server
    ENDPOINT = 'https://algocrypto.eu'
    method_most_traded_coins = '/analysis/get-mosttradedcoins'

    url_mosttradedcoins = ENDPOINT + method_most_traded_coins
    response = requests.get(url_mosttradedcoins)
    print(f'StatusCode for getting most_traded_coins: {response.status_code}')
    most_traded_coins = response.json()
    path_most_traded_json = "/home/alberto/Docker/Trading/tracker/json/most_traded_coins.json"
    # save MOST_TRADED_COINS.json
    with open(path_most_traded_json, 'w') as outfile:
        outfile.write(most_traded_coins)
        print(f'new Json saved in {path_most_traded_json}')

    path_most_traded_json = "/home/alberto/Docker/Trading/tracker/json/most_traded_coins.json"
    f = open(path_most_traded_json, "r")
    most_traded_coin = json.loads(f.read())
    start_coin_list = filter_position[0]
    end_coin_list = filter_position[1]

    most_traded_coin_list = most_traded_coin['most_traded_coins'][start_coin_list:end_coin_list]


    path_dir = "/home/alberto/Docker/Trading/analysis/json"
    list_json = os.listdir(path_dir)
    full_paths = [path_dir + "/{0}".format(x) for x in list_json]
    print(full_paths)

    list_json_info = []
    for full_path in full_paths:
        json_ = {'path': full_path, 'time': os.path.getmtime(full_path)}
        list_json_info.append(json_)

    list_json_info.sort(key=lambda x: x['time'], reverse=False)
    list_json_info

    data= {}
    for json_info in list_json_info:
        sleep(1)
        path = json_info['path']
        print(f'Retrieving data from {path}')
        f = open(path, "r")
        
        temp_data_dict = json.loads(f.read())
        
        for coin in temp_data_dict['data']:
            if coin in most_traded_coin_list:
                if coin not in data:
                    data[coin] = []
                for obs in temp_data_dict['data'][coin]:
                    if datetime.fromisoformat(obs['_id']) >= start_interval and datetime.fromisoformat(obs['_id']) <= end_interval:
                        data[coin].append(obs)
        del temp_data_dict

    n_coins = len(data)
    print(f'{n_coins} coins have been loaded')
    summary = {}
    for coin in data:
        summary[coin] = {'n_observations': len(data[coin])}
    df = pd.DataFrame(summary)
    df = df.transpose()

    return data, df