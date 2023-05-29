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


def analyze_events(data, buy_vol_field, vol_field, minutes_price_windows, event_buy_volume, event_volume, dynamic_benchmark_volume, multiprocessing=True):
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
    price_changes = {}
    events = {}
    #print(data)
    tot_n_coins = len(list(data.keys()))
    # analyze for each coin
    for coin in list(data.keys()):
        # initialize limit_window
        limit_window = datetime(2000,1,1)

        # get initial price of the coin
        initial_price = data[coin][0]['price']
        

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
                        # get dynamic volatility
                        volatility = str(get_volatility(dynamic_benchmark_volume[coin], obs['_id']))

                        # get key as coin + volatility
                        key = coin + '-' + volatility
                        # initialize if first time
                        if key not in price_changes:
                            price_changes[key] = []
        
                        #EVENT TRIGGERED
                        if volatility not in events:
                            events[volatility] = 0
                        events[volatility] += 1

                        limit_window  = datetime_obs + timedelta(minutes=minutes_price_windows)
                        # get all the price changes in the "minutes_price_windows"
                        for obs, obs_i in zip(data[coin][index:index+minutes_price_windows], range(minutes_price_windows)):
                            # if actual observation has occurred in the last minute and 10 seconds from last observation, let's add the price "change" in "price_changes":
                            actual_datetime = datetime.fromisoformat(data[coin][index+obs_i]['_id'])
                            if actual_datetime - datetime.fromisoformat(data[coin][index+obs_i-1]['_id']) <= timedelta(minutes=1,seconds=10):
                                change = (obs['price'] - initial_price)/initial_price
                                if not np.isnan(change):
                                    price_changes[key].append(change)


    total_changes = {}
    coins = {}
    # iterate through each coin
    for key in price_changes:
        coin = key.split('-')[0]
        volatility = key.split('-')[1]

        if volatility not in total_changes:
            total_changes[volatility] = []

        if len(price_changes[key]) > 0:
            # save coin name if not exists based on volatility

            if volatility not in coins:
                coins[volatility] = []

            coins[volatility].append(coin)
            # Let's keep all the changes. This will  be conserverd through all the iterations in wrap_analize_events_multiprocessing.
            # the np.mean will be executed only during the function "show_output"
            total_changes[volatility] += price_changes[key]

    # for volatility in list(total_changes.keys()):
    #     total_changes[volatility] = np.mean(total_changes[volatility])

    if multiprocessing:
        return total_changes, events, coins

    else:
        # mean_total_changes = np.mean(total_changes)*100
        # std_total_changes = np.std(total_changes)*100
        return total_changes, events, coins
    
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
    output = {}
    for key in list(shared_data.keys()):
        if key is not 'coins' or key is not 'events':
            if shared_data[key]['events'] > 0:
                shared_data[key]['price_changes'] = np.array(shared_data[key]['price_changes'])

                isfinite = np.isfinite(shared_data[key]['price_changes'])
                shared_data[key]['price_changes'] = shared_data[key]['price_changes'][isfinite]

                mean_weighted = np.mean(shared_data[key]['price_changes'])*100
                std_weighted = np.std(shared_data[key]['price_changes'])*100

                output[key] = {'mean': mean_weighted, 'std': std_weighted, 'lower_bound': mean_weighted - std_weighted, 'n_coins': len(shared_data[key]['coins']), 'n_events': shared_data[key]['events']}
            else:
                output[key] = {'mean': None, 'std': None, 'lower_bound': None, 'n_coins': 0, 'n_events': 0}

    return output


def wrap_analyze_events_multiprocessing(data, data_i, list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, dynamic_benchmark_volume, lock, shared_data):
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


    temp = {}
    for buy_vol_field in list_buy_vol:
        for vol_field in list_vol:
            for minutes_price_windows in list_minutes:
                for event_buy_volume in  list_event_buy_volume:
                    for event_volume in list_event_volume:
                        
                        price_changes, events, coins = analyze_events(data, buy_vol_field, vol_field, minutes_price_windows, event_buy_volume, event_volume, dynamic_benchmark_volume)
                        
                        # price changes is a dict with keys regarding the volatility: {'1': [...], '2': [...], ..., '5': [...], ...}
                        # the value of the dict[volatility] is a list of all price_changes of a particular event
                        for volatility in price_changes:
                            # let's define the key: all event info + volatility coin
                            key = str(buy_vol_field) + ':' + str(event_buy_volume) + '/' + str(vol_field) + ':' + str(event_volume) + '/' + 'timeframe:' + str(minutes_price_windows) + '/' + 'vlty:' + volatility
                            temp[key] = {}

                            temp[key]['price_changes'] = price_changes[volatility]
                            temp[key]['coins'] = coins[volatility]
                            temp[key]['events'] = events[volatility]


    del price_changes, events, coins

    # Lock Shared Variable
    # this lock is essential in multiprocessing which permits to work on shared resources
    with lock:
        resp = json.loads(shared_data.value)

        for key in list(temp.keys()):
            
            if key not in resp:
                resp[key] = {}

            if 'price_changes' in resp[key]:
                resp[key]['price_changes'] += temp[key]['price_changes'] # concatenate lists
                resp[key]['events'] += temp[key]['events'] # sum numbers
                resp[key]['coins'] = list(set(resp[key]['coins']) | set(temp[key]['coins'])) # union of lists

            else:
                # initialize the keys of resp[key]
                resp[key]['price_changes'] = temp[key]['price_changes']
                resp[key]['coins'] = temp[key]['coins']
                resp[key]['events'] = temp[key]['events']

        shared_data.value = json.dumps(resp)

def start_wrap_analyze_events_multiprocessing(data, list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, start_interval, end_interval, n_processes):
    '''
    this function starts "wrap_analyze_events_multiprocessing" function
    '''
    # get dynamic benchmark volume. This will be used for each observation for each coin, to see what was the current volatility (std_dev / mean).
    benchmark_info = get_benchmark_info()
    dynamic_benchmark_volume = get_dynamic_volume_avg(benchmark_info)


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
    # TODO: load files form json_analysis
    
    shared_data = manager.Value(str, json.dumps({}))
    lock = Manager().Lock()


    # Create a multiprocessing Pool
    pool = Pool()

    # Execute the function in parallel
    pool.starmap(wrap_analyze_events_multiprocessing, [(arg, arg_i, list_buy_vol, list_vol, list_minutes,
                                        list_event_buy_volume, list_event_volume, dynamic_benchmark_volume, lock, shared_data) for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])

    # Close the pool
    pool.close()
    pool.join()
    t2 = time()
    print(t2-t1, ' seconds')

    # TODO: save file to json_analysis

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

def wrap_analyze_events(data, list_buy_vol, list_vol, list_minutes, list_event_buy_volume, list_event_volume, start_interval, end_interval, multiprocessing=False):
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
                        total_changes, events, coins = analyze_events(data, buy_vol_field, vol_field, minutes_price_windows, event_buy_volume, event_volume, dynamic_benchmark_volume, multiprocessing)

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
    '''
    This functions loads all the data from "start_interval" until "end_interval".
    The data should be already download through "getData.ipynb" and stored in "analysis/json" path.
    '''
    
    # get the most traded coins from function "get_benchmark_info"
    start_coin = filter_position[0]
    end_coin = filter_position[1]
    benchmark_info, df_benchmark = get_benchmark_info()
    volume_info = []
    for coin in benchmark_info:
        volume_info.append({'coin': coin, 'volume_30': benchmark_info[coin]['volume_30_avg']})
    
    volume_info.sort(key=lambda x: x['volume_30'], reverse=True)
    most_traded_coin_list = [info['coin'] for info in volume_info[start_coin:end_coin]]

    
    path_dir = "/home/alberto/Docker/Trading/analysis/json"
    list_json = os.listdir(path_dir)
    full_paths = [path_dir + "/{0}".format(x) for x in list_json]
    print(full_paths)

    list_json_info = []
    for full_path in full_paths:
        json_ = {'path': full_path, 'time': os.path.getmtime(full_path)}
        list_json_info.append(json_)

    list_json_info.sort(key=lambda x: x['time'], reverse=False)

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
    keys_data = list(data.keys())
    for coin in keys_data:
        if len(data[coin]) > 0:
            # get first obs from data[coin]
            start_coin = data[coin][0]['_id']
            # get standard dev on mean (std.dev / mean)
            std_on_mean = benchmark_info[coin]['volume_30_std']  / benchmark_info[coin]['volume_30_avg'] 
            # update summary
            summary[coin] = {'n_observations': len(data[coin]), 'position': most_traded_coin_list.index(coin), 'vol_30_avg': benchmark_info[coin]['volume_30_avg'], 'std_on_mean': std_on_mean, 'first_obs': start_coin}
        else:
            del data[coin]

    df = pd.DataFrame(summary)
    df = df.transpose()
    df = df.sort_values(by=['vol_30_avg'], ascending=False)
    return data, df, summary

def get_volume_info():
    ENDPOINT = 'https://algocrypto.eu'
    #ENDPOINT = 'http://localhost'

    method_most_traded_coins = '/analysis/get-volumeinfo'

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
    full_path = '/home/alberto/Docker/Trading/analysis/benchmark_json/' + file

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

    #benchmark_info = json.loads(benchmark_info)
    df = pd.DataFrame(benchmark_info).transpose()
    df.drop('volume_series', inplace=True, axis=1)

    # Modify DF
    st_dev_ON_mean_30 = df['volume_30_std'] / df['volume_30_avg']
    df.insert (2, "st_dev_ON_mean_30", st_dev_ON_mean_30)
    df = df.sort_values(by=['volume_30_avg'], ascending=False)
    return benchmark_info, df

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
            dynamic_benchmark_volume[coin][date] = std_one_date / mean_one_date

    return dynamic_benchmark_volume
        


            



            
            