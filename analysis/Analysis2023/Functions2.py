import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime, timedelta
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
from Helpers import round_, load_data, data_preparation, load_analysis_json_info, get_volatility, get_benchmark_info, get_dynamic_volume_avg, updateAnalysisJson, get_substring_between, pooled_standard_deviation, getsubstring_fromkey


'''
Functions2.py define a set of functions that are used to analyze the strategy instant volumes triggering
The idea is that if coin has had a slow activity and slow movement price in the last day and all of sudden vol_1m and buy_vol_1_m go high, then some uptrend is starting.
The selling is not based on timeframe as Functions.py but on the percentage of BUYERS/SELLERS. 
'''

ROOT_PATH = os.getcwd()


def main_instant_trigger(buy_vol_1m_Buy_List, vol_1m_Buy_List, sell_vol_Sell_List, sell_vol_Sell_field_List, price_changes_List, previous_volume_activity_List, analysis_timeframe=7, INTEGRATION=False):

    # get dynamic benchmark volume. This will be used for each observation for each coin, to see what was the current volatility (std_dev / mean) of the last 30 days.
    benchmark_info = get_benchmark_info()
    dynamic_benchmark_volume = get_dynamic_volume_avg(benchmark_info)
    del benchmark_info


    # LOAD ANALYSIS JSON v2
    analysis_json_path = ROOT_PATH + '/analysis_json_v2/analysis.json'
    start_interval, end_interval = load_analysis_json_info(analysis_json_path, analysis_timeframe=analysis_timeframe)

    # LOAD DATA FROM JSON
    data = load_data(start_interval=datetime.fromisoformat(start_interval), end_interval=end_interval)

    # DATA PREPARATATION
    data_arguments = data_preparation(data)

    # define "end_interval_analysis". this is variable says "do not analyze events that are older than this date".
    end_interval_analysis = datetime.fromisoformat(data['BTCUSDT'][-1]['_id']) - timedelta(days=1) #last datetime from btcusdt - 3 days
    del data
    # This is going to be also the starting time for next analysis
    start_next_analysis = end_interval_analysis.isoformat()
    print(f'Events from {start_interval} to {start_next_analysis} will be analyzed')

    # START MULTIPROCESSING
    # initialize Manager for "shared_data". Used for multiprocessing
    manager = Manager()
    
    # initialize lock for processing shared variable between multiple processes
    lock = Manager().Lock()

    # Create a multiprocessing Pool
    pool = Pool()

    #initialize shared_data
    shared_data = manager.Value(str, json.dumps({}))

    # Execute the function "" in parallel
    pool.starmap(wrapper_instant_trigger, [(arg, arg_i,
                                             buy_vol_1m_Buy_List, vol_1m_Buy_List, sell_vol_Sell_List, sell_vol_Sell_field_List, previous_volume_activity_List, price_changes_List,
                                                dynamic_benchmark_volume, end_interval_analysis, lock, shared_data)
                                                    for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])


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



def wrapper_instant_trigger(data, data_i, 
                            buy_vol_1m_Buy_List, vol_1m_Buy_List, sell_vol_Sell_List, sell_vol_field_Sell_List, previous_volume_activity_List, price_changes_List,
                            dynamic_benchmark_volume, end_interval_analysis, lock, shared_data):


    if data_i == 1:
            total_combinations = len(buy_vol_1m_Buy_List) * len(vol_1m_Buy_List) * len(sell_vol_Sell_List) * len(sell_vol_field_Sell_List)
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
    for buy_vol_1m_Buy in buy_vol_1m_Buy_List:
        for vol_1m_Buy in vol_1m_Buy_List:
            for sell_vol_Sell in sell_vol_Sell_List:
                for sell_vol_field_Sell in  sell_vol_field_Sell_List:
                    for previous_volume_activity in previous_volume_activity_List:
                        for price_change in price_changes_List:
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

                            results = analyze_instant_trigger(data, buy_vol_1m_Buy, vol_1m_Buy, sell_vol_Sell, sell_vol_field_Sell, previous_volume_activity, price_change,
                                                    dynamic_benchmark_volume, end_interval_analysis)
                            
                            # price changes is a dict with keys regarding the volatility: {'1': [...], '2': [...], ..., '5': [...], ...}
                            # the value of the dict[volatility] is a list of all price_changes of a particular event
                            for volatility in results:
                                # let's define the key: all event info + volatility coin
                                key = 'vol1m:' + str(vol_1m_Buy) + '/' + 'buyvol1m:' + str(buy_vol_1m_Buy) + '/' + str(sell_vol_field_Sell) + ':' + str(sell_vol_Sell) + '/' + 'prevActivity:' + str(previous_volume_activity) + '/' + 'priceChange:' + str(price_change) + '/' + 'vlty:' + volatility
                                temp[key] = {}
                                temp[key]['info'] = results[volatility]


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
    
                    
def analyze_instant_trigger(data, buy_vol_1m_Buy, vol_1m_Buy, sell_vol_Sell, sell_vol_field_Sell, previous_volume_activity, price_change,
                                            dynamic_benchmark_volume, end_interval_analysis):
    
    results = {}
    #print(data)
    # analyze for each coin
    for coin in list(data.keys()):
        
        # check through each observation of the coin
        for obs, index in zip(data[coin], range(len(data[coin]))):
            SKIP=False
            if index == 0:
                continue

            prev_obs = data[coin][index-1]
            datetime_start_obs = datetime.fromisoformat(obs['_id'])

            # the observation must not be younger than "end_interval_analysis".
            if datetime_start_obs < end_interval_analysis:
                
                if ((obs['price'] - prev_obs['price']) / prev_obs['price']) < price_change:
                    continue
                
                for field in list(prev_obs.keys()):
                    if field in ['vol_15m', 'vol_30m', 'vol_60m', 'vol_3h', 'vol_6h', 'vol_24h']:
                        if prev_obs[field] and prev_obs[field] > previous_volume_activity:
                            SKIP = True
                
                if SKIP:
                    continue
                    
                if obs['vol_1m'] > vol_1m_Buy and obs['buy_vol_1m'] > buy_vol_1m_Buy:

                    # EVENT_TRIGGERED
                    initial_price = obs['price']
                    #price_changes = []
                    volatility = str(get_volatility(dynamic_benchmark_volume[coin], obs['_id']))

                    # initialize results variable
                    if volatility not in results:
                        results[volatility] = {}

                    if coin not in results[volatility]:
                        results[volatility][coin] = []
                    
                    max_return = 0
                    max_loss = 0
                    
                    # let's analyze what happens from trigger until end of available data
                    for obs_event in data[coin][index+1:]:
                        
                        # current profit/loss
                        price_change = (obs_event['price'] - initial_price) / initial_price
                        #price_changes.append(price_change)
                        max_return = max(price_change, max_return)
                        max_loss = min(price_change, max_loss)

                        # Sell order ACTIVATED
                        if obs_event[sell_vol_field_Sell] and obs_event[sell_vol_field_Sell] < sell_vol_Sell:
                            doc = {'event': datetime_start_obs.isoformat(), 'return': round_(price_change,5),
                                    #'mean_return': round_(np.mean(price_changes)), 
                                    'max_return': round_(max_return,5), 'max_loss': round_(max_loss,5),
                                    'exit_timestamp': obs_event['_id'],
                                    'start': obs,
                                    'prev': prev_obs}
                            
                            results[volatility][coin].append(doc)
                            break
    
    return results


def download_show_output(minimum_event_number, minimum_coin_number, mean_threshold, frequency_threshold, group_coins=False, best_coins_volatility=None, shared_data=None, early_validation=False):
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
    - early_validation: [datetime] this analyses the keys until a certain point in time. Used to check how analysis behaves in the future
    '''

    # download data if shared_data is None
    if shared_data == None:
        path = ROOT_PATH + "/analysis_json_v2/"
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

    if not group_coins:
        for key in list(shared_data.keys()):
            # compute number of events
            n_coins = len(shared_data[key]['info'])
            n_events = 0
            mean_list = []
            std_list = []
            max_return_list = []
            max_loss_list = []

            # I want to get earliest and latest event
            # intitialize first_event and last_event
            random_coin = list(shared_data[key]['info'].keys())[0]
            first_event = datetime(2023,6,11)
            last_event =  datetime(1970,1,1)
            
            for coin in shared_data[key]['info']:
                for event in shared_data[key]['info'][coin]:
                    if early_validation and datetime.fromisoformat(event['event']) > early_validation:
                        continue
                    else:
                        n_events += 1
                        mean_list.append(event['return'])
                        max_return_list.append(event['max_return'])
                        max_loss_list.append(event['max_loss'])
                        last_event = max(datetime.fromisoformat(event['event']), last_event)
            
            #print(n_coins, n_events)
            # get frequency event per month
            time_interval_analysis = (last_event - first_event).days
            if time_interval_analysis != 0:
                event_frequency_month = round_((n_events / time_interval_analysis) * 30,2)

            
            if n_events >= minimum_event_number and n_coins >= minimum_coin_number: #and event_frequency_month >= frequency_threshold:

                volatility = key.split('vlty:')[1]
                
                mean = round_(np.mean(np.array(mean_list))*100,2)
                max_return = round_(np.mean(np.array(max_return_list))*100,2)
                max_loss = round_(np.mean(np.array(max_loss_list))*100,2)
                std = round_(np.std(np.array(mean_list))*100,2)
                upper_bound = mean + std
                lower_bound = mean - std

                if mean > mean_threshold:

                    if best_coins_volatility:
                        if volatility not in volatility_list:
                            volatility_list[volatility] = [{'key': key, 'upper_bound': upper_bound}]
                        else:
                            volatility_list[volatility].append({'key': key, 'upper_bound': upper_bound})
                
                    complete_info[key] = {'info': shared_data[key]['info'], 'coins': n_coins, 'events': n_events, 'frequency/month': event_frequency_month}
                    output[key] = {'mean': mean, 'max_return': max_return, 'max_loss': max_loss, 'std': std, 'upper_bound': upper_bound, 'lower_bound': lower_bound,
                                    'n_coins': n_coins, 'n_events': n_events, 'frequency/month': event_frequency_month}
                    
                    #print(output)
        
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

            key_without_volatility = key.split('vlty:')[0]
            n_events = 0
            mean_list = []
            max_return_list = []
            max_loss_list = []
            n_coins = len(shared_data[key]['info'])

            for coin in shared_data[key]['info']:
                for event in shared_data[key]['info'][coin]:
                    if early_validation and datetime.fromisoformat(event['event']) > early_validation:
                        continue
                    else:
                        n_events += 1
                        mean_list.append(event['return'])
                        max_return_list.append(event['max_return'])
                        max_loss_list.append(event['max_loss'])

            # mean = round_(np.mean(np.array(mean_list))*100,2)
            # std = round_(np.mean(np.array(std_list))*100,2)
            # max_return= round_(np.mean(np.array(max_return_list))*100,2)
            # max_loss = round_(np.mean(np.array(max_loss_list))*100,2)

            if key_without_volatility not in output:
                output[key_without_volatility] = {'mean': mean_list, 'max_return': max_return_list, 'max_loss': max_loss_list,
                                    'n_coins': n_coins, 'n_events': n_events}
            else:
                output[key_without_volatility]['mean'] + mean_list
                output[key_without_volatility]['max_return'] + max_return_list
                output[key_without_volatility]['max_loss'] + max_loss_list
                output[key_without_volatility]['n_coins'] += n_coins
                output[key_without_volatility]['n_events'] += n_events
        
        delete_keys = []
        for key in output:
            if output[key]['n_events'] >= minimum_event_number and round_(np.mean(output[key]['mean']),2) > mean_threshold:
                output[key]['mean'] = round_(np.mean(output[key]['mean']),2)
                output[key]['std'] = round_(np.std(output[key]['mean']),2)
                output[key]['max_return'] = round_(np.mean(output[key]['max_return']),2)
                output[key]['max_loss'] = round_(np.mean(output[key]['max_loss']),2)
            
                output[key]['upper_bound'] = output[key]['mean'] + output[key]['std']
                output[key]['lower_bound'] = output[key]['mean'] - output[key]['std']
            else:
                delete_keys.append(key)

        for key in delete_keys:
            output.pop(key)
        complete_info = None

    
    
    t3 = time()
    #delta_t_2 = round_(t3 - t2,2)
    #print(f'Data Preparation completed in {delta_t_2} seconds')


    return output, complete_info