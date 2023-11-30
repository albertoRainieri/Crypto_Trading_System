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
from Helpers import round_, load_data, data_preparation, load_analysis_json_info, updateAnalysisJson, pooled_standard_deviation, getsubstring_fromkey, load_timeseries, getTimeseries
import seaborn as sns


'''
Functions2.py define a set of functions that are used to analyze the strategy instant volumes triggering
The idea is that if coin has had a slow activity and slow movement price in the last day and all of sudden vol_1m and buy_vol_1_m go high, then some uptrend is starting.
The selling is not based on timeframe as Functions.py but on the percentage of BUYERS/SELLERS. 
'''

ROOT_PATH = os.getcwd()


def main_instant_trigger(buy_vol_5m_Buy_List, vol_5m_Buy_List, timeframe, analysis_timeframe=7, INTEGRATION=False):

    # LOAD ANALYSIS JSON v2
    analysis_json_path = ROOT_PATH + '/analysis_json_v3/analysis.json'
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
                          best_coins_timeframe, early_validation=False):
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

                vol, vol_value, buy_vol, buy_vol_value, timeframe = getsubstring_fromkey(key)
                
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
                new_output[key_without_volatility] = output[key_without_volatility]
                new_info[key_without_volatility] = complete_info[key_without_volatility]

    del output, complete_info
    return new_output, new_info

def inspect_key(event_key):
    analysis_json = load_analysis_json()
    vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe = getsubstring_fromkey(event_key)


    percentile_list = [90,75,50,25,10]
    mean_list = []
    max_list = []
    min_list = []
    
    for coin in analysis_json[event_key]['info']:
        for event in analysis_json[event_key]['info'][coin]:
            mean_list.append(event['mean'])
            max_list.append(event['max'])
            min_list.append(event['min'])

    perc90_max = np.percentile(np.array(max_list),90)
    perc75_max = np.percentile(np.array(max_list),75)
    perc50_max = np.percentile(np.array(max_list),50)
    perc25_max = np.percentile(np.array(max_list),25)
    perc10_max = np.percentile(np.array(max_list),10)
    percX_max_list = [perc90_max, perc75_max, perc50_max, perc25_max, perc10_max]

    full_summary_max = {'90perc': [], '75perc': [], '50perc': [], '25perc': [], '10perc': []}

    # assign events to their percentiles
    for coin in analysis_json[event_key]['info']:
        for event in analysis_json[event_key]['info'][coin]:
            event['coin'] = coin
            if event['max'] >= perc90_max:
                full_summary_max['90perc'].append(event)
            elif event['max'] >= perc75_max:
                full_summary_max['75perc'].append(event)
            elif event['max'] >= perc50_max:
                full_summary_max['50perc'].append(event)
            elif event['max'] >= perc25_max:
                full_summary_max['25perc'].append(event)
            elif event['max'] >= perc10_max:
                full_summary_max['10perc'].append(event)

    key_json = event_key.replace(':', '_')
    key_json = key_json.replace('/', '_')
    timeseries = load_timeseries(key_json)

    if timeseries == None:
        retry = True
        while retry:
            retry = getTimeseries(analysis_json, event_key, check_past=1440, look_for_newdata=True, plot=False)
        
        timeseries = load_timeseries(event_key)

    response = {}
    percentiles = ['90perc', '75perc', '50perc', '25perc', '10perc']
    minimum_to_max_perc90_all_percentiles = []
    minimum_to_max_perc75_all_percentiles = []
    minimum_to_max_perc50_all_percentiles = []
    minimum_to_max_perc25_all_percentiles = []
    minimum_to_max_perc10_all_percentiles = []

    biggest_drop_perc90_all_percentiles = []
    biggest_drop_perc75_all_percentiles = []
    biggest_drop_perc50_all_percentiles = []
    biggest_drop_perc25_all_percentiles = []
    biggest_drop_perc10_all_percentiles = []

    minutes_to_max_perc90_all_percentiles = []
    minutes_to_max_perc75_all_percentiles = []
    minutes_to_max_perc50_all_percentiles = []
    minutes_to_max_perc25_all_percentiles = []
    minutes_to_max_perc10_all_percentiles = []



    n_events = []

    for percentile in percentiles:

        minimum_to_max_list = []
        minutes_to_max_list = []
        biggest_drop_list = []
        n_events.append(len(full_summary_max[percentile]))

        for event in full_summary_max[percentile]:
            event_id = event['event']
            coin = event['coin']
            #print(coin)
            timeseries_x = timeseries[coin][event_id]

            #return timeseries_x.keys()
            SKIP = True
            max_return = 0
            minimum_return = 0
            minimum_to_max = 0
            biggest_drop = 0
            
            
            
            for obs in timeseries_x['data']:
                
                if SKIP and obs['_id'] == event_id:
                    SKIP = False
                    initial_price = obs['price']
                    

            
                if not SKIP:
                    if datetime.fromisoformat(obs['_id']) < datetime.fromisoformat(event_id) + timedelta(minutes=int(timeframe)):
                        current_change = (obs['price'] - initial_price) / initial_price
                        
                        minimum_return = min(minimum_return, current_change)
                        biggest_drop = max(max_return - current_change, biggest_drop)

                        if current_change > max_return:
                            max_return = current_change
                            minimum_to_max = minimum_return
                            minutes_to_max = int((datetime.fromisoformat(obs['_id']) - datetime.fromisoformat(event_id)).total_seconds() / 60)
                    else:
                        biggest_drop_list.append(biggest_drop)
                        minutes_to_max_list.append(minutes_to_max)
                        minimum_to_max_list.append(minimum_to_max)
                        break

        minimum_to_max_perc90_all_percentiles.append(round_(np.percentile(np.array(minimum_to_max_list),90),2))
        minimum_to_max_perc75_all_percentiles.append(round_(np.percentile(np.array(minimum_to_max_list),75),2))
        minimum_to_max_perc50_all_percentiles.append(round_(np.percentile(np.array(minimum_to_max_list),50),2))
        minimum_to_max_perc25_all_percentiles.append(round_(np.percentile(np.array(minimum_to_max_list),25),2))
        minimum_to_max_perc10_all_percentiles.append(round_(np.percentile(np.array(minimum_to_max_list),10),2))

        biggest_drop_perc90_all_percentiles.append(round_(np.percentile(np.array(biggest_drop_list),90),2))
        biggest_drop_perc75_all_percentiles.append(round_(np.percentile(np.array(biggest_drop_list),75),2))
        biggest_drop_perc50_all_percentiles.append(round_(np.percentile(np.array(biggest_drop_list),50),2))
        biggest_drop_perc25_all_percentiles.append(round_(np.percentile(np.array(biggest_drop_list),25),2))
        biggest_drop_perc10_all_percentiles.append(round_(np.percentile(np.array(biggest_drop_list),10),2))

        minutes_to_max_perc90_all_percentiles.append(np.percentile(np.array(minutes_to_max_list),90))
        minutes_to_max_perc75_all_percentiles.append(np.percentile(np.array(minutes_to_max_list),75))
        minutes_to_max_perc50_all_percentiles.append(np.percentile(np.array(minutes_to_max_list),50))
        minutes_to_max_perc25_all_percentiles.append(np.percentile(np.array(minutes_to_max_list),25))
        minutes_to_max_perc10_all_percentiles.append(np.percentile(np.array(minutes_to_max_list),10))

    df = pd.DataFrame({'index': percentiles, 'max_bottom': percX_max_list, 'n_events': n_events,
                                         'min_to_max_90': minimum_to_max_perc90_all_percentiles,
                                         'min_to_max_75': minimum_to_max_perc75_all_percentiles,
                                         'min_to_max_50': minimum_to_max_perc50_all_percentiles,
                                         'min_to_max_25': minimum_to_max_perc25_all_percentiles,
                                         'min_to_max_10': minimum_to_max_perc10_all_percentiles,
                                         'biggest_drop_90': biggest_drop_perc90_all_percentiles,
                                         'biggest_drop_75': biggest_drop_perc75_all_percentiles,
                                         'biggest_drop_50': biggest_drop_perc50_all_percentiles,
                                         'biggest_drop_25': biggest_drop_perc25_all_percentiles,
                                         'biggest_drop_10': biggest_drop_perc10_all_percentiles,
                                         'minutes_to_max_90': minutes_to_max_perc90_all_percentiles,
                                         'minutes_to_max_75': minutes_to_max_perc75_all_percentiles,
                                         'minutes_to_max_50': minutes_to_max_perc50_all_percentiles,
                                         'minutes_to_max_25': minutes_to_max_perc25_all_percentiles,
                                         'minutes_to_max_10': minutes_to_max_perc10_all_percentiles,
                                         })
    

    # Calculate percentiles
    percentile_list = [10, 25, 50, 75, 90]

    summary = {'max_list': max_list, 'biggest_drop': }
    percentiles = np.percentile(max_list, percentile_list)

    # Create a density plot using seaborn
    sns.set(style="whitegrid")
    sns.kdeplot(max_list, fill=True)

    # Add percentiles as vertical lines
    colors = ['r', 'g', 'b', 'c', 'm']
    for percentile, color, percentile_range in zip(percentiles, colors, percentile_list):
        plt.axvline(x=percentile, linestyle='--', color=color,  label=f'percentile {percentile_range}: {round_(percentile,2)}')
        
        # Annotate the line with the percentile value
        #plt.text(percentile, 0.005, f'{percentile:.2f}', color='b', rotation=0, va='bottom', ha='right')

    # Add labels and title
    plt.xlabel("Values")
    plt.ylabel("Density")
    plt.title(f"Density Plot for List of Max for {event_key}")

    # Show the legend
    plt.legend()

    # Show the plot
    plt.show()   
    return df

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


                
            

    