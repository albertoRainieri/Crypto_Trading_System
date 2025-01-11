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
from Helpers import round_, load_data, data_preparation, load_analysis_json_info, updateAnalysisJson, pooled_standard_deviation
from Helpers import getsubstring_fromkey, load_timeseries, getTimeseries, plotTimeseries, get_substring_between, riskmanagement_data_preparation
import seaborn as sns
import shutil
import requests

'''
Functions2.py define a set of functions that are used to analyze the strategy instant volumes triggering
The idea is that if coin has had a slow activity and slow movement price in the last day and all of sudden vol_1m and buy_vol_1_m go high, then some uptrend is starting.
The selling is not based on timeframe as Functions.py but on the percentage of BUYERS/SELLERS. 
'''

ROOT_PATH = os.getcwd()


def main_instant_trigger(buy_vol_5m_Buy_List, vol_5m_Buy_List, timeframe, analysis_timeframe=7, INTEGRATION=False):

    # LOAD ANALYSIS JSON v2
    analysis_json_path = ROOT_PATH + '/analysis_json_v3/analysis.json'
    start_interval, end_interval = load_analysis_json_info(analysis_json_path, analysis_timeframe=analysis_timeframe, INTEGRATION=INTEGRATION)

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
                                             buy_vol_5m_Buy_List, vol_5m_Buy_List,
                                                timeframe, end_interval_analysis, lock, shared_data)
                                                    for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])


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



def wrapper_instant_trigger(data, data_i, buy_vol_5m_Buy_List, vol_5m_Buy_List, timeframe, end_interval_analysis, lock, shared_data):


    if data_i == 1:
            total_combinations = len(buy_vol_5m_Buy_List) * len(vol_5m_Buy_List) * len(timeframe)
            print(f'Total Number of combinations: {total_combinations}')
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
    for buy_vol_5m_Buy in buy_vol_5m_Buy_List:
        for vol_5m_Buy in vol_5m_Buy_List:
            for minutes_price_windows in timeframe:
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

                results = analyze_instant_trigger(data, buy_vol_5m_Buy, vol_5m_Buy, minutes_price_windows, end_interval_analysis)
                

                # let's define the key: all event info + volatility coin
                key = 'buy_vol_5m:' + str(buy_vol_5m_Buy) + '/' + 'vol_5m:' + str(vol_5m_Buy) + '/' + 'timeframe' + ':' + str(minutes_price_windows)
                temp[key] = {}
                temp[key]['info'] = results


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

def filter_out_events(vol_5m_Buy, buy_vol_5m_Buy):
    SKIP = False

    if vol_5m_Buy <= 50 and buy_vol_5m_Buy < 0.95:
        SKIP = True


    return SKIP
                    
def analyze_instant_trigger(data, buy_vol_5m_Buy, vol_5m_Buy, minutes_price_windows, end_interval_analysis):
        
    results = {}
    nan = {}
    #print(data)
    # analyze for each coin
    for coin in list(data.keys()):

        # initialize limit_window
        limit_window = datetime(2000,1,1)
        
        # check through each observation of the coin
        for obs, index in zip(data[coin], range(len(data[coin]))):
            SKIP=False
            if index == 0:
                continue

            prev_obs = data[coin][index-1]
            datetime_start_obs = datetime.fromisoformat(obs['_id'])

            # the observation must not be younger than "end_interval_analysis".
            if datetime_start_obs < end_interval_analysis:
                
                if obs['vol_5m'] and obs['buy_vol_5m'] and obs['vol_5m'] >= vol_5m_Buy and obs['buy_vol_5m'] >= buy_vol_5m_Buy and datetime_start_obs > limit_window:
                    
                    if filter_out_events(vol_5m_Buy, buy_vol_5m_Buy):
                            continue
                    
                    # EVENT_TRIGGERED
                    initial_price = obs['price']
                    if coin not in results:
                        results[coin] = []
                    
                    max_price_change = 0
                    min_price_change = 0

                    timestamp_event_triggered = obs['_id']
                    price_changes = []
                    limit_window  = datetime_start_obs + timedelta(minutes=minutes_price_windows)
                    for obs_event, obs_i in zip(data[coin][index:index+minutes_price_windows], range(minutes_price_windows)):
                        # if actual observation has occurred in the last minute and 10 seconds from last observation, let's add the price "change" in "price_changes":
                        actual_datetime = datetime.fromisoformat(data[coin][index+obs_i]['_id'])
                        if actual_datetime - datetime.fromisoformat(data[coin][index+obs_i-1]['_id']) <= timedelta(minutes=10,seconds=10):
                            
                            change = (obs_event['price'] - initial_price)/initial_price
                            if not np.isnan(change):
                                max_price_change = max(max_price_change, change)
                                min_price_change = min(min_price_change, change)

                                price_changes.append(change)
                            else:
                                if coin not in nan:
                                    nan[coin] = []
                                nan[coin].append(timestamp_event_triggered)

                    results[coin].append({'event': timestamp_event_triggered, 'mean': round_(np.mean(price_changes),4), 'std': round(np.std(price_changes, ddof=1),4),
                                                            'max': max_price_change, 'min': min_price_change})
    
    return results

def load_analysis_json():
    path = ROOT_PATH + "/analysis_json_v3/"
    file_path = path + 'analysis' + '.json'
    with open(file_path, 'r') as file:
        t1 = time()
        print(f'Downloading {file_path}')
        # Retrieve shared memory for JSON data and "start_interval"
        analysis_json = json.load(file)
        t2 = time()
        delta_t_1 = round_(t2 - t1,2)
        print(f'Download completed in {delta_t_1} seconds')
    
    return analysis_json['data']


def download_show_output(minimum_event_number, minimum_coin_number, mean_threshold, frequency_threshold, max_threshold, lb_threshold, std_multiplier,
                          best_coins_timeframe, early_validation=False, filter_key=None):
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
    - represent only the best keys grouped by volatility. This is activated only if "group_coins=False". "best_coins_timeframe" must be an integer in this case.
    - early_validation: [datetime] this analyses the keys until a certain point in time. Used to check how analysis behaves in the future
    '''

    # download data if shared_data is None
    analysis_json = load_analysis_json()

    #return shared_data
    output = {}
    complete_info = {}

    # group best keys by volatility == False
    
    for key in list(analysis_json.keys()):
        if key != 'coins' or key != 'events':

            # compute number of events
            n_coins = len(analysis_json[key]['info'])
            n_events = 0
            mean_list = []
            max_list = []
            min_list = []
            std_list = []

            # I want to get earliest and latest event
            # intitialize first_event and last_event
            if len(list(analysis_json[key]['info'].keys())) > 0:
                random_coin = list(analysis_json[key]['info'].keys())[0]
                first_event = datetime.fromisoformat(analysis_json[key]['info'][random_coin][0]['event'])
                last_event =  datetime(1970,1,1)
            
            for coin in analysis_json[key]['info']:
                for event in analysis_json[key]['info'][coin]:
                    if early_validation and datetime.fromisoformat(event['event']) > early_validation:
                        continue
                    else:
                        n_events += 1
                        
                        mean_list.append(event['mean'])
                        max_list.append(event['max'])
                        min_list.append(event['min'])
                        #print(max_list)
                        std_list.append(event['std'])
                        first_event = min(datetime.fromisoformat(event['event']), first_event)
                        last_event = max(datetime.fromisoformat(event['event']), last_event)
            
                        # get frequency event per month
                        time_interval_analysis = (last_event - first_event).days
                        if time_interval_analysis != 0:
                            event_frequency_month = round_((n_events / time_interval_analysis) * 30,2)
                        else:
                            event_frequency_month = 1

                        
            if n_events >= minimum_event_number and n_coins >= minimum_coin_number and event_frequency_month >= frequency_threshold:

                vol, vol_value, buy_vol, buy_vol_value, timeframe, lvl = getsubstring_fromkey(key)
                
                mean = round_(np.mean(np.array(mean_list))*100,2)
                max_return = round_(np.mean(np.array(max_list))*100,2)
                min_return = round_(np.mean(np.array(min_list))*100,2)
                std = round_(pooled_standard_deviation(np.array(std_list), sample_size=int(timeframe))*100,2)
                std_max = round_(np.std(np.array(max_list))*100,2)
                best_max = round_(max(np.array(max_list))*100,2)

                perc90_max = round_(np.percentile(np.array(max_list),90)*100,2)
                perc75_max = round_(np.percentile(np.array(max_list),75)*100,2)
                perc50_max = round_(np.percentile(np.array(max_list),50)*100,2)
                perc25_max = round_(np.percentile(np.array(max_list),25)*100,2)
                perc10_max = round_(np.percentile(np.array(max_list),10)*100,2)

                upper_bound = mean + std
                lower_bound = mean - std

                # if key == 'buy_vol_5m:0.6/vol_5m:100/timeframe:1440':
                #     print(n_events)

                if mean > mean_threshold and np.abs(mean) * std_multiplier > std and max_return > max_threshold:
                    complete_info[key] = {'info': analysis_json[key]['info'], 'coins': n_coins, 'events': n_events, 'frequency/month': event_frequency_month}
                    output[key] = {'mean_max': max_return, 'perc90_max': perc90_max, 'perc75_max': perc75_max,
                                    'perc50_max': perc50_max, 'perc25_max': perc25_max, 'perc10_max': perc10_max,
                                   'std_max': std_max, 'best_max': best_max, 'n_coins': n_coins, 'n_events': n_events, 'frequency/month': event_frequency_month,
                                   'mean': mean, 'std': std, 'mean_min': min_return}
                # else:
                #     print(mean * std_multiplier > std)
    if best_coins_timeframe:
        # ORDER BY PERFORMANCE
        keys_filtered_by_timeframe = {}
        for key in output:
            key_without_timeframe = key.split('/timeframe')[0]
            if key_without_timeframe not in keys_filtered_by_timeframe:
                keys_filtered_by_timeframe[key_without_timeframe] = []
            keys_filtered_by_timeframe[key_without_timeframe].append({'key': key, 'mean': output[key]['mean_max']})
        
        # GET THE BEST KEYS
        keys_to_keep = {}
        for key_without_timeframe in keys_filtered_by_timeframe:
            keys_filtered_by_timeframe[key_without_timeframe].sort(key=lambda x: x['mean'], reverse=True)
            keys_filtered_by_timeframe[key_without_timeframe] = keys_filtered_by_timeframe[key_without_timeframe][:best_coins_timeframe]
            keys_to_keep[key_without_timeframe] = [x['key'] for x in keys_filtered_by_timeframe[key_without_timeframe]]

        # UPDATE OUTPUT AND COMPLLETE INFO
        new_output = {}
        new_info = {}
        for key_without_volatility in output:
            key_without_timeframe = key_without_volatility.split('/timeframe')[0]
            if key_without_volatility in keys_to_keep[key_without_timeframe]:
                if filter_key == None:
                    new_output[key_without_volatility] = output[key_without_volatility]
                    new_info[key_without_volatility] = complete_info[key_without_volatility]
                elif key_without_volatility == filter_key:
                    new_output[key_without_volatility] = output[key_without_volatility]
                    new_info[key_without_volatility] = complete_info[key_without_volatility]

    del output, complete_info
    return new_output, new_info

def inspect_key(event_key, percentile_list, showPercentile, DISCOVER=False):
    pd.set_option('display.max_columns', None)

    # Load Analysis Json
    analysis_json = load_analysis_json()
    vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = getsubstring_fromkey(event_key)

    # Initialize variables
    mean_list = [] # list of mean value for each event
    max_list = [] # list of max value for each event
    min_list = [] # list of minimum value for each event
    full_summary_max = {}
    
    for coin in analysis_json[event_key]['info']:
        for event in analysis_json[event_key]['info'][coin]:
            mean_list.append(event['mean'])
            max_list.append(event['max'])
            min_list.append(event['min'])

    # lets get the percentiles for max list and initialize full_summary_max
    # full_summary_max will collect all events (from analysis.json) divided per percentile
    percX_max_list = np.percentile(max_list, percentile_list)
    percentiles = []
    for percentile in percentile_list:
        perc_str = f'{str(percentile)}perc'
        percentiles.append(perc_str)
        full_summary_max[perc_str] = []

    # assign events to their percentiles
    for coin in analysis_json[event_key]['info']:
        for event in analysis_json[event_key]['info'][coin]:
            event['coin'] = coin
            for percX_max, perc_str in zip(percX_max_list, percentiles):
                if event['max'] >= percX_max:
                    # if perc_str == '99perc':
                    #     print(event)
                    full_summary_max[perc_str].append(event)
                    break

    # print([len(full_summary_max[i]) for i in full_summary_max])
    # print(percX_max_list)

    # Load or Download Timeseries
    key_json = event_key.replace(':', '_')
    key_json = key_json.replace('/', '_')
    timeseries = load_timeseries(key_json)
    if timeseries == None or DISCOVER == True:
        retry = True
        while retry:
            retry = getTimeseries(analysis_json, event_key, check_past=int(timeframe)/4, look_for_newdata=True, plot=False)
        
        timeseries = load_timeseries(key_json)

    # lets get some info (minimum_to_max, biggest_drop_to_max, minutes_to_max_percX) for all events per each percentile
    minimum_to_max_percX = {}
    biggest_drop_percX = {}
    minutes_to_max_percX = {}
    minimum_to_max_list = {}
    minutes_to_max_list = {}
    biggest_drop_list = {}
    n_events = []

    for percentile in percentiles:
        minimum_to_max_percX[percentile] = []
        biggest_drop_percX[percentile] = []
        minutes_to_max_percX[percentile] = []
        minimum_to_max_list[percentile] = []
        minutes_to_max_list[percentile] = []
        biggest_drop_list[percentile] = []

    new_timeseries = {} # this is created for plotting purposes
    max50 = []
    drop50 = []
    mix50 = []
    minimum50 = []

    # fill variables iterating through each event of each percentile
    for percentile_outer in percentiles:
        new_timeseries[percentile_outer] = {}
        n_events.append(len(full_summary_max[percentile_outer]))

        for event in full_summary_max[percentile_outer]:
            event_id = event['event']
            coin = event['coin']
            #print(coin)
            timeseries_x = timeseries[coin][event_id]

            # if percentile_outer == '90perc':
            #     print(timeseries[coin][event_id]['statistics'])


            if coin not in new_timeseries[percentile_outer]:
                new_timeseries[percentile_outer][coin] = {}
            new_timeseries[percentile_outer][coin][event_id] = timeseries[coin][event_id]
            

            #return timeseries_x.keys()
            SKIP = True
            max_return = 0
            minimum_return = 0
            minimum_to_max = 0
            biggest_drop_current = 0
            
            for obs in timeseries_x['data']:
                
                if SKIP and obs['_id'] == event_id:
                    SKIP = False
                    initial_price = obs['price']
                if not SKIP:
                    if datetime.fromisoformat(obs['_id']) < datetime.fromisoformat(event_id) + timedelta(minutes=int(timeframe)):
                        current_change = (obs['price'] - initial_price) / initial_price
                        
                        minimum_return = min(minimum_return, current_change)
                        biggest_drop_current = max(max_return - current_change, biggest_drop_current)

                        if current_change > max_return:
                            biggest_drop = biggest_drop_current
                            max_return = current_change
                            minimum_to_max = minimum_return
                            minutes_to_max = int((datetime.fromisoformat(obs['_id']) - datetime.fromisoformat(event_id)).total_seconds() / 60)
                    else:
                        #if percentile_outer == '50perc':
                                # test_id = event_id
                                # test_coin = coin
                                # if coin == 'CELRUSDT' and event_id == '2023-11-02T03:28:02.310968':
                                #     max50.append(max_return)
                                #     drop50.append(biggest_drop)
                                #     mix50.append((max_return,biggest_drop, minimum_to_max, minutes_to_max))
                        biggest_drop_list[percentile_outer].append(biggest_drop)
                        minutes_to_max_list[percentile_outer].append(minutes_to_max)
                        minimum_to_max_list[percentile_outer].append(minimum_to_max)
                        break
        
        for percentile_inner, percentile_value in zip(percentiles, percentile_list):

            minimum_to_max_percX[percentile_inner].append(round_(np.percentile(np.array(minimum_to_max_list[percentile_outer]),percentile_value),3))
            biggest_drop_percX[percentile_inner].append(round_(np.percentile(np.array(biggest_drop_list[percentile_outer]),percentile_value),2))
            minutes_to_max_percX[percentile_inner].append(int(np.percentile(np.array(minutes_to_max_list[percentile_outer]),percentile_value)))

    del timeseries
 
    # let's group all these info in one variable for plotting purposes
    plot_info = {'percentiles': {
        'minimum_to_max': minimum_to_max_percX,
        'biggest_drop_to_max': biggest_drop_percX,
        'minutes_to_max': minutes_to_max_percX},
                 'valuesList': {
        'minimum_to_max': minimum_to_max_list,
        'biggest_drop_to_max': biggest_drop_list,
        'minutes_to_max': minutes_to_max_list}
                 }

    # Let's define pandas dataframe
    doc_pandas = {'index': percentiles, 'max_bottom': percX_max_list, 'n_events': n_events}
    
    for percentile_value, percentile in zip(percentile_list, percentiles):
        minimum_to_max_key = 'minimum_to_max_' + str(percentile_value)
        doc_pandas[minimum_to_max_key] = minimum_to_max_percX[percentile]

    for percentile_value, percentile in zip(percentile_list, percentiles):
        biggest_drop_key = 'biggest_drop_' + str(percentile_value)
        doc_pandas[biggest_drop_key] = biggest_drop_percX[percentile]

    for percentile_value, percentile in zip(percentile_list, percentiles):
        minutes_to_max_key = 'minutes_to_max_' + str(percentile_value)
        doc_pandas[minutes_to_max_key] = minutes_to_max_percX[percentile]

    df = pd.DataFrame(doc_pandas)

    plotDensityFunction(max_list, percX_max_list, plot_info, event_key, percentiles, percentile_list)

    vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = getsubstring_fromkey(event_key)
    fields = [vol_field, buy_vol_field, timeframe, buy_vol_value, vol_value]
    if showPercentile != None:
        plotTimeseries(new_timeseries[showPercentile], fields, check_past=120, plot=True, filter_start=False, filter_best=False)
    return df

def plotDensityFunction(max_list, percX_max_list, plot_info, event_key, percentiles, percentile_list):
    
    vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = getsubstring_fromkey(event_key)
    # lets plot a density function of the max and draw some information for each percentile
    sns.set(style="whitegrid")
    plt.figure(figsize=(15, 6))
    #sns.kdeplot(max_list, fill=True, common_norm=False)
    plt.hist(max_list, bins=100, density=False, alpha=0.6, color='g')
    plt.xticks(np.arange(min(max_list), max(max_list),0.05))

    # PLOT MAX DENSITY FUNCTION
    # Add percentiles as vertical lines
    colors = ['blue', 'green', 'red', 'cyan', 'magenta', 'yellow', 'black', 'white', 'orange', 'purple']
    for percX_max, color, percentile_range in zip(percX_max_list, colors[:len(percentiles)], percentile_list):
        plt.axvline(x=percX_max, linestyle='--', color=color,  label=f'percentile {percentile_range}: {round_(percX_max,2)}')
    # Add labels and title
    plt.xlabel("Maximum Values")
    plt.ylabel("Density")
    plt.title(f"Density Plot for Maximum values for {event_key}")
    # Show the legend
    plt.legend()
    # Show the plot
    plt.show()

    #PLOT ALL OTHER GRAPHS, SPECIFYING THE PERCENTILE
    # Create subplots
    for type_graph in plot_info['percentiles']:
        fig, axes = plt.subplots(nrows=len(percentile_list), ncols=1, figsize=(15,12), sharex=False,
                                 gridspec_kw={'height_ratios': [2]*len(percentile_list)})

        for ax, percentile_focus in zip(axes, percentile_list):
            percentile_focus = str(percentile_focus) + 'perc'
            sns.set(style="whitegrid")
            
            # Create a density plot for each type of graph
            ax.hist(plot_info['valuesList'][type_graph][percentile_focus], bins=30, density=False, alpha=0.6, color='g')
            #sns.kdeplot(plot_info['valuesList'][type_graph][percentile_focus], fill=True, ax=ax, common_norm=True)

            PERC_X_LIST = np.percentile(np.array(plot_info['valuesList'][type_graph][percentile_focus]), percentile_list)
            #print(np.array(plot_info['valuesList'][type_graph][percentile_focus]))
            # Add percentiles as vertical lines with different colors
            for percX_Y, color, percentile_range in zip(PERC_X_LIST, colors[:len(percentiles)], percentile_list):
                ax.axvline(x=percX_Y, linestyle='--', color=color,  label=f'percentile {percentile_range}: {round_(percX_Y,3)}')

            # Add labels and title
            #ax.set_xlabel(f"{type_graph} - {percentile_focus}")
            ax.set_ylabel(f"{percentile_focus}")
            #ax.set_title(f"{percentile_focus} Density Plot for {type_graph}")
        
            # Show the legend
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')

        # Adjust layout
        #plt.tight_layout(rect=[0, 0, 1, 0.97])
        plt.suptitle(f"Density Plot for {type_graph}")
        #plt.subplots_adjust(top=0.9, hspace=5)
        plt.show()
        plt.close()

def areSameTimestamps(timestamp_a, timestamp_b):
    '''
    check if timestamps coincide to the minute, not second
    '''

    datetime_a = datetime.fromisoformat(timestamp_a)
    datetime_b = datetime.fromisoformat(timestamp_b)


    if datetime_a.year == datetime_b.year and datetime_a.month == datetime_b.month and datetime_a.day == datetime_b.day and datetime_a.hour == datetime_b.hour and datetime_a.minute == datetime_b.minute:
        return True
    else:
        return False

def delete_99_perc(info):
    pass

    

def RiskConfiguration(info, riskmanagement_conf, optimized_gain_threshold, mean_gain_threshold, early_validation=False, std_multiplier=3, delete_X_perc=False, DISCOVER=False):
    '''
    This functions has the objective to define the best risk configuration for a selected number of keys.
    "download_show_output" function will provide the input
    the riskmanagement_configuration will be used during the live trading for optimization /risk management

    this function is also called by "earlyValidation"
    '''
    t1 = time()
    keys_list = list(info.keys())
    n_keys = len(keys_list)
    print(f'{n_keys} keys will be analyzed in terms of risk configuration')
    risk_configuration = {}
    total_optimized_riskconfiguration_results = {}

    for key, key_i in zip(keys_list, range(1,len(keys_list)+1)):
        if DISCOVER:
            vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = getsubstring_fromkey(key)
            print(f'Downloading Timeseries {key_i}/{n_keys}: {key}')
            print(f'Timeframe detected: {timeframe}')
            retry = True        
            while retry:
                retry = getTimeseries(info, key, check_past=int(timeframe)/4, look_for_newdata=True, plot=False)

    for key, key_i in zip(keys_list, range(1,len(keys_list)+1)):
        print(key)
        print(f'ITERATION {key_i}/{n_keys} has started')
        frequency = info[key]["frequency/month"]
        n_events =  info[key]["events"]

        df1, df2, risk, optimized_riskconfiguration_results = RiskManagement(info, key, early_validation, n_events, delete_X_perc=delete_X_perc)
        best_risk_key = risk['best_risk_key']
        best_mean_print = risk['best_mean_print']
        best_std_print = risk['best_std_print']
        statistica_all_configs = risk['statistics']
        mean = statistica_all_configs['mean']
        std = statistica_all_configs['std']
        median = statistica_all_configs['median']

        risk_retracement_zone_str = get_substring_between(best_risk_key, "risk_retracement_zone:", "_retracement:")
        retracement_str = get_substring_between(best_risk_key, "_retracement:", "_retracement_step:")
        retracement_step_str = get_substring_between(best_risk_key, "_retracement_step:", "_loss_zone:")
        loss_zone_str = get_substring_between(best_risk_key, "_loss_zone:", "_step_loss:")
        step_loss = best_risk_key.split('_step_loss:')[1]

        total_optimized_riskconfiguration_results[key] = optimized_riskconfiguration_results

        # FILTER ONLY THE BEST KEY EVENTS
        obj = {
                'riskmanagement_conf': {
                    'risk_retracement_zone': risk_retracement_zone_str,
                    'retracement': retracement_str,
                    'retracement_step': retracement_step_str,
                    'loss_zone': loss_zone_str,
                    'step_loss': step_loss,
                    'optimized_gain': best_mean_print,
                    'optimized_std': best_std_print,
                    'frequency': frequency,
                    'n_events' : n_events,
                    'mean_gain_all_configs': mean,
                    'median_gain_all_configs': median,
                    'std_gain_all_configs': std,
                }}

        if best_mean_print >= optimized_gain_threshold and mean >= mean_gain_threshold:
            risk_configuration[key] = obj
        

    key_list = []
    risk_retracement_zone_list = []
    retracement_list = []
    retracement_step_list = []
    loss_zone_list = []
    step_loss_list = []
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
        "best_coins_volatility": riskmanagement_conf[6],
        "std_multiplier": riskmanagement_conf[7],
        "early_validation": riskmanagement_conf[8]
    }

    if riskmanagement_conf[8] != False:
        risk_configuration_dict['early_validation'] = risk_configuration_dict['early_validation'].isoformat()
    else:

        file_path = ROOT_PATH + '/analysis_json/analysis.json'
        with open(file_path, 'r') as file:
            analysis_json = json.load(file)
        start_next_analysis = analysis_json['start_next_analysis']
        del analysis_json

        risk_configuration_dict['early_validation'] = datetime.fromisoformat(start_next_analysis).isoformat()

    
     # PREPARE FILES FOR PANDAS AND FOR SAVING

    for key in risk_configuration:
        key_list.append(key)
        risk_retracement_zone_list.append(risk_configuration[key]['riskmanagement_conf']['risk_retracement_zone'])
        retracement_list.append(risk_configuration[key]['riskmanagement_conf']['retracement'])
        retracement_step_list.append(risk_configuration[key]['riskmanagement_conf']['retracement_step'])
        loss_zone_list.append(risk_configuration[key]['riskmanagement_conf']['loss_zone'])
        step_loss_list.append(risk_configuration[key]['riskmanagement_conf']['step_loss'])
        optimized_gain_list.append(risk_configuration[key]['riskmanagement_conf']['optimized_gain'])
        optimized_std_list.append(risk_configuration[key]['riskmanagement_conf']['optimized_std'])
        mean_gain_list.append(risk_configuration[key]['riskmanagement_conf']['mean_gain_all_configs'])
        median_gain_list.append(risk_configuration[key]['riskmanagement_conf']['median_gain_all_configs'])
        std_gain_list.append(risk_configuration[key]['riskmanagement_conf']['std_gain_all_configs'])
        frequency_list.append(risk_configuration[key]['riskmanagement_conf']['frequency'])
        n_events_list.append(risk_configuration[key]['riskmanagement_conf']['n_events'])
            

    average_gain_percentage = sum(np.array(optimized_gain_list) * (np.array(frequency_list)/sum(frequency_list))) / 100
    df_dict = {"keys": key_list, "risk_retracement_zone": risk_retracement_zone_list, 'retracement_%': retracement_list, 'retracement_gain': retracement_step_list,
                'loss_zone': loss_zone_list, 'step_loss': step_loss_list, 'optimized_gain': optimized_gain_list, 'optimized_std': optimized_std_list,
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
        dst_backup = f"{ROOT_PATH}/riskmanagement_backup/riskmanagement-{year}-{month}-{day}-{minimum_event_number}-{mean_threshold}-MeanThrsl{mean_gain_threshold}-{random_id}.json"
        dst_optimized = f"{ROOT_PATH}/optimized_results_backup/optimized_results-{year}-{month}-{day}-{minimum_event_number}-{mean_threshold}-MeanThrsl{mean_gain_threshold}-{random_id}.json"
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

                
def RiskManagement(info, key, early_validation, n_events, investment_per_event=100, delete_X_perc=False):

    '''
    This function is called by RiskConfiguration.
    This function focuses on one key event and prepares for multiprocessing
    '''
    LOSS_ZONES = [-0.015, -0.025, -0.5, -0.075]
    STEPS_LOSS = [0.01, 0.025, 0.05, 0.1, 0.2, 0.3]
    RETRACEMENT = [0.1, 0.15, 0.2, 0.25, 0.3] # relative percentage to max_profit
    RETRACEMENT_GAIN = [0.025, 0.05, 0.1, 0.15] # absolute percentage
    RETRACEMENT_ZONE = [0.15, 0.2, 0.25, 0.3] # absolute percentage
    
    n_combinations = len(LOSS_ZONES) * len(STEPS_LOSS) * len(RETRACEMENT) * len(RETRACEMENT_GAIN) * len(RETRACEMENT_ZONE)
    print(f'Number of combinations {n_combinations}')

    # get timeseries
    event_key_path = key.replace(':', '_')
    event_key_path = event_key_path.replace('/', '_')
    
    timeseries_json = load_timeseries(event_key_path)

    if not early_validation:
        n_events_timeseries_json = sum([len(timeseries_json[coin]) for coin in timeseries_json])
    else:
        n_events_timeseries_json = sum([1 for coin in timeseries_json for timestamp_start in timeseries_json[coin] if datetime.fromisoformat(timestamp_start) < early_validation ])

    # in case there is a mismatch between the events in analysis.json and timeseries, let's delete and download again the timeseries
    if n_events != n_events_timeseries_json:
        raise ValueError(f"Events Timeseries Json: {n_events_timeseries_json} -- Event Analysis Json: {n_events} -- {event_key_path}")

    # get timeframe
    vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = getsubstring_fromkey(key)
    timeframe = int(timeframe)

    # MULTIPROCESSING
    print('Dividing the dataset for multiprocessing')
    data_arguments = riskmanagement_data_preparation(timeseries_json, n_processes=8, delete_X_perc=delete_X_perc, key=key)
    manager = Manager()
    results = manager.Value(str, json.dumps({}))
    lock = Manager().Lock()
    pool = Pool()
    print('Starting the multiprocessing Task for Risk Configuration')
    pool.starmap(RiskManagement_multiprocessing, [(arg, arg_i, LOSS_ZONES, STEPS_LOSS, RETRACEMENT_GAIN,
                                        RETRACEMENT, RETRACEMENT_ZONE, timeframe, lock, early_validation, results) for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])
    print('Multiprocessing Task is Completed')
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
    

def RiskManagement_multiprocessing(timeseries_json, arg_i, LOSS_ZONES, STEPS_LOSS, RETRACEMENT_GAIN,
                                        RETRACEMENT, RETRACEMENT_ZONE, timeframe, lock, early_validation, results):
    '''
    This function is used by def "RiskManagement". The purpose is to analyze the best combinations of variables for handling the risk management
    Similar to "total_function_multiprocessing", a multiprocessing task gets carried out.
    '''

    if arg_i == 1:
        total_combinations = len(LOSS_ZONES) * len(STEPS_LOSS) * len(RETRACEMENT_GAIN) * len(RETRACEMENT) * len(RETRACEMENT_ZONE)
        PERC_10 = True
        THRESHOLD_10 = total_combinations * 0.1
        PERC_25 = True
        THRESHOLD_25 = total_combinations * 0.25
        PERC_50 = True
        THRESHOLD_50 = total_combinations * 0.5
        PERC_75 = True
        THRESHOLD_75 = total_combinations * 0.75
        iteration = 0
    
    tmp = {}
    for LOSS_ZONE_original in LOSS_ZONES:
        for STEP_LOSS_original in STEPS_LOSS:
            for RETRACEMENT_GAIN_original in RETRACEMENT_GAIN:
                for RETRACEMENT_original in RETRACEMENT:
                    for RETRACEMENT_ZONE_original in RETRACEMENT_ZONE:
                        if arg_i == 1:
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


                        #risk_key = 'risk_golden_zone:' + str(GOLDEN_ZONE_original) + '_step:' + str(STEP_original) + '_step_no_golden:' + str(STEP_NOGOLDEN) + '_extratime:' + str(round_(extratimeframe,2))
                        risk_key = 'risk_retracement_zone:' + str(RETRACEMENT_ZONE_original) + '_retracement:' + str(RETRACEMENT_original) + '_retracement_step:' + str(RETRACEMENT_GAIN_original) + '_loss_zone:' + str(LOSS_ZONE_original) + '_step_loss:' + str(STEP_LOSS_original)
                        tmp[risk_key] = {}

                        # iterate through each coin
                        n_events_risk_key = sum([len(timeseries_json[coin]) for coin in timeseries_json])

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
                                    RETRACEMENT_GAIN_original, RETRACEMENT_original, RETRACEMENT_ZONE_original, STEP_LOSS_original, timeframe, LOSS_ZONE_original)
                        
    with lock:
        resp = json.loads(results.value)
        for key in list(tmp.keys()):
            
            if key not in resp:
                resp[key] = {}

            # update complete "info"
            for start_timestamp in tmp[key]:
                resp[key][start_timestamp] = tmp[key][start_timestamp]

        results.value = json.dumps(resp)

def RiskManagement_lowest_level(tmp, timeseries_json, coin, start_timestamp, risk_key,
                                 RETRACEMENT_GAIN_original, RETRACEMENT_original, RETRACEMENT_ZONE_original, STEP_LOSS_original, timeframe, LOSS_ZONE_original):
    '''
    this is the lowest level in the Riskmanagemnt process
    It is called by "RiskManagement_multiprocessing"
    '''
    
    RETRACEMENT_GAIN = copy(RETRACEMENT_GAIN_original)
    RETRACEMENT = copy(RETRACEMENT_original)
    RETRACEMENT_ZONE_1 = copy(RETRACEMENT_ZONE_original)
    LOSS_STEP = copy(STEP_LOSS_original)
    LOSS_ZONE = copy(LOSS_ZONE_original)
    
    #these are outdated parameters
    # STEP_NOGOLDEN = 0.01
    # extratimeframe = 0

    #STEP = 0.05
    RETRACEMENT_ZONE_BOOL_1 = False
    RETRACEMENT_ZONE_BOOL_2 = False
    LOSS_ZONE_BOOL = False

    # INITIALIZE THESE PARAMETERS. these will be better defined in "manageLossZoneChanges"
    STOP_LOSS = LOSS_ZONE + LOSS_STEP
    MINIMUM = LOSS_ZONE

    iterator = timeseries_json[coin][start_timestamp]['data']
    #initial_price = iterator[0]['price']
    SKIP = True
    max_price=0

    # iterate through each obs
    for obs, obs_i in zip(iterator, range(1, len(iterator) + 1)):
        #print(RETRACEMENT_ZONE_BOOL, GOLDEN_ZONE_LB, GOLDEN_ZONE_UB, STEP)
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

            if RETRACEMENT_ZONE_BOOL_2 and current_change >= STOP_GAIN:
                status = 'PROFIT_EXIT'
                tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, True,
                                                    max_price, min_price, status)                        
                break

            elif RETRACEMENT_ZONE_BOOL_1:
                if current_change > max_profit:
                    max_profit = current_change
                    RETRACEMENT_ZONE_2 = max_profit - (RETRACEMENT * max_profit)   
                elif current_change <= RETRACEMENT_ZONE_2:
                    RETRACEMENT_ZONE_BOOL_2 = True
                    STOP_GAIN = current_change + RETRACEMENT_GAIN
                
            elif current_change >= RETRACEMENT_ZONE_1:
                RETRACEMENT_ZONE_BOOL_1 = True
                max_profit = current_change
                RETRACEMENT_ZONE_2 = max_profit - (RETRACEMENT * max_profit)

            elif LOSS_ZONE_BOOL:
                SELL, LOSS_ZONE_BOOL, STOP_LOSS, MINIMUM = manageLossZoneChanges(current_change, LOSS_ZONE_BOOL, LOSS_ZONE, LOSS_STEP, MINIMUM, STOP_LOSS)
                if SELL:
                    status = 'LOSS_EXIT'
                    tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, True,
                                                        max_price, min_price, status)                        
                    break

            # check if price went below loss zone
            elif current_change <= LOSS_ZONE:
                status = 'LOSS_EXIT'
                SELL, LOSS_ZONE_BOOL, STOP_LOSS, MINIMUM = manageLossZoneChanges(current_change, LOSS_ZONE_BOOL, LOSS_ZONE, LOSS_STEP, MINIMUM, STOP_LOSS)
                tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, True,
                                                        max_price, min_price, status)  


            # check if minimum time window has passed
            if datetime.fromisoformat(obs['_id']) > datetime.fromisoformat(start_timestamp) + timedelta(minutes=timeframe):
                status = 'EXIT'
                tmp[risk_key][start_timestamp] = (current_change, obs['_id'], initial_price, current_price, coin, False,
                                                         max_price, min_price, status)
                break
                
    return tmp




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
    status_list = {}

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
        status_list[risk_key] = []

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
            status_list[risk_key].append(results[risk_key][start_timestamp][8])
            
            start_timestamp_list[risk_key].append(start_timestamp)

    # choose the best risk key
    best_risk_key = ''
    best_profit = - 10

    risk_key_df = []
    profit_mean_list_df = []
    profit_std_list_df = []

    risk_configuration_list = []
    for risk_key in profit_list:
        profit = np.mean(profit_list[risk_key]) # meean of all the events with a the strategy defined by risk_key
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
    response = requests.post(url='http://localhost/analysis/get-pricechanges', json=request)
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
                           'coin': coin_list[best_risk_key], 'early_sell': early_sell[best_risk_key],
                             'max_price': max_price_list[best_risk_key], 'min_price': min_price_list[best_risk_key], 'status': status_list[best_risk_key]}
    
    # update optimized_riskconfiguration_results with the lists of "info_obj"
    for info_key in info_obj:
        optimized_riskconfiguration_results[info_key] = info_obj[info_key]
    
    return optimized_riskconfiguration_results, (n_events, best_profit, best_risk_key, best_profit_print, risk_key_df, profit_mean_list_df, profit_std_list_df, best_std_print)
    
def manageRetracementZone1Changes(current_change, RETRACEMENT_GAIN, RETRACEMENT_ZONE_BOOL):
    pass

def manageLossZoneChanges(current_change, LOSS_ZONE_BOOL, LOSS_ZONE, LOSS_STEP, MINIMUM, STOP_LOSS):
    SELL = False
    

    # if I'm in loss Zone
    if LOSS_ZONE_BOOL:
        # check If I hit the STOP_LOSS, in this scenario current_change is usually lower than STOP_LOSS.
        if current_change > STOP_LOSS:
            SELL = True
        # update MINIMUM and STOP LOSS if we have hit the deepest fall so far
        elif current_change < MINIMUM:
            MINIMUM = current_change
            STOP_LOSS = current_change + LOSS_STEP

    else:
        # CHECK IF i'm entering the LOSS scenario
        if current_change < LOSS_ZONE:
            SELL = False
            LOSS_ZONE_BOOL = True
            STOP_LOSS = current_change + LOSS_STEP
            MINIMUM = current_change

    return SELL, LOSS_ZONE_BOOL, STOP_LOSS, MINIMUM
