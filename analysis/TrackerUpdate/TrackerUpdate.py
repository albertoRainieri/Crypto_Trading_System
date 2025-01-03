import json
import os,sys
sys.path.insert(0,'..')
sys.path.insert(0,'../..')
from datetime import datetime, timedelta
from analysis.Analysis2023.Helpers import round_, data_preparation
from multiprocessing import Pool, Manager
import calendar
import numpy as np
from operator import itemgetter
ROOT_DIRECTORY = "/Users/albertorainieri/Personal"
USB_ROOT_DIRECTORY = "/Volumes/PortableSSD/Alberto/Trading"


def update_tracker_with_n_trades():

    time_interval_legend = {'trades_5m': {'timedelta': timedelta(minutes=5), 't': 5}, 'trades_15m': {'timedelta': timedelta(minutes=15), 't': 15},
                            'trades_30m': {'timedelta': timedelta(minutes=30), 't': 30}, 'trades_60m': {'timedelta': timedelta(minutes=60), 't': 60}, 
                      'trades_3h': {'timedelta': timedelta(minutes=180), 't': 180}, 'trades_6h': {'timedelta': timedelta(minutes=360), 't': 360}}
    

    folder_market = f"{USB_ROOT_DIRECTORY}/Json/json_market/"
    folder_tracker = f"{USB_ROOT_DIRECTORY}/Json/json_tracker/"

    files_market = sort_files_by_date(folder_market)
    files_tracker = sort_files_by_date(folder_tracker)

    path_tracker_path_analyzed = f'{ROOT_DIRECTORY}/analysis/TrackerUpdate/json/tracker_path_analyzed_trackerupdate.json'
    path_market_path_analyzed = f'{ROOT_DIRECTORY}/analysis/TrackerUpdate/json/market_path_analyzed_trackerupdate.json'
    path_last_market_data_analyzed = f'{ROOT_DIRECTORY}/analysis/TrackerUpdate/json/last_market_data_analyzed.json'

    last_market_data_analyzed = {}

    path_benchmark_new = f'{ROOT_DIRECTORY}/analysis/benchmark_json/benchmark-NEW.json'

    if os.path.exists(path_last_market_data_analyzed):
        with open(path_last_market_data_analyzed, 'r') as f:
            last_market_data_analyzed = json.load(f)
    else:
        last_market_data_analyzed = {}

    if os.path.exists(path_tracker_path_analyzed):
        with open(path_tracker_path_analyzed, 'r') as f:
            tracker_path_analyzed = json.load(f)
            tracker_path_analyzed = tracker_path_analyzed['list_tracker_paths_analyzed']
    else:
        tracker_path_analyzed = []

    if os.path.exists(path_market_path_analyzed):
        with open(path_market_path_analyzed, 'r') as f:
            market_path_analyzed = json.load(f)
            market_path_analyzed = market_path_analyzed['list_market_paths_analyzed']
    else:
        market_path_analyzed = []

    if os.path.exists(path_benchmark_new):
        with open(path_benchmark_new, 'r') as file:
            # Retrieve shared memory for JSON data and "start_interval"
            benchmark_json = json.load(file)
    else:
        raise IOError("Benchmark path does not exist.")

    for market_path in files_market:
        
        if market_path in market_path_analyzed:
           continue

        data_market = load_market(market_path)

        
        #######################################################################################################################
        ################################################ NEW METHOD - MULTIPROCESSING #########################################
        #######################################################################################################################
        # get data slices for multiprocessing. .e.g divide the data in batches for allowing multiprocessing
        data_arguments = data_preparation(data_market, n_processes = 6)
        del data_market
        for data_arg in data_arguments:
            n_coins = len(data_arg)
            print(f'{n_coins} coins for slice')


        # initialize Manager for "shared_data". Used for multiprocessing
        manager = Manager()
        
        # initialize lock for processing shared variable between multiple processes
        lock = Manager().Lock()

        # Create a multiprocessing Pool
        pool = Pool()

        # initialize shared_data
        shared_data = manager.Value(str, json.dumps({'n_trades': {}, 'last_market_data_analyzed': last_market_data_analyzed}))

        pool.starmap(update_tracker, [(arg, arg_i, last_market_data_analyzed, benchmark_json, time_interval_legend, lock,
                                        shared_data) for arg, arg_i in zip(data_arguments, range(1,len(data_arguments)+1))])
        
        # Close the pool
        pool.close()
        print('pool closed')
        pool.join()
        print('pool joined')
        del data_arguments

        shared_data = json.loads(shared_data.value)
        last_market_data_analyzed = shared_data["last_market_data_analyzed"]
        n_trades = shared_data['n_trades']
        n_trades_btc = shared_data['n_trades']['BTCUSDT']
        sorted_ts_n_trades = sorted([datetime.fromisoformat(date) for date in list(n_trades_btc.keys())])
        start_ts_n_trades = sorted_ts_n_trades[0]
        end_ts_n_trades = sorted_ts_n_trades[-1]

        

        legend_key_n_trades = ['trades_1m', 'trades_5m', 'trades_15m', 'trades_30m',
                            'trades_60m','trades_3h', 'trades_6h']
        
        # SALVA I DATI DA N_TRADES A TRACKER_DATA. QUANDO OBS_ID SFORA TRACKER_DATA, CARICA IL JSON SUCCESSIVO
        print("Starting Update Process for Tracker")
        for file_tracker in files_tracker:
            if file_tracker not in tracker_path_analyzed:
                
                # retrieve tracker data
                print(f'Loading for Tracker: {file_tracker}')
                with open(file_tracker, 'r') as file:
                    # Retrieve shared memory for JSON data and "start_interval"
                    tracker_data_complete = json.load(file)
                    tracker_data = tracker_data_complete['data']
                    end_tracker_data = datetime.fromisoformat(tracker_data['BTCUSDT'][-1]['_id'])
                
                # retrieve new_tracker_data if exists already otherwise initialize
                new_file_tracker = file_tracker.split('.json')[0] + "-test-update.json"
                new_file_tracker = new_file_tracker.replace("json_tracker", "json_tracker_update")
                if os.path.exists(new_file_tracker):
                    with open(new_file_tracker, 'r') as file:
                        new_tracker_data = json.load(file)
                else:
                    new_tracker_data = {'datetime_creation': tracker_data_complete['datetime_creation'], 'data': {}}
                    
                
                for coin in tracker_data:
                    if coin not in new_tracker_data['data']:
                        new_tracker_data['data'][coin] = []
                    for obs in tracker_data[coin]:
                        obs_id_datetime = datetime.fromisoformat(obs['_id']).replace(second=0, microsecond=0)
                        obs_id = obs_id_datetime.isoformat()
                        if obs_id in n_trades[coin]:
                            # this is the case when the end of tracker_window is not beyond the end of n_trades window

                            for key in legend_key_n_trades:
                                obs[key] = n_trades[coin][obs_id][key]
                            #if obs not in new_tracker_data['data'][coin]:
                            new_tracker_data['data'][coin].append(obs)
                        elif obs_id_datetime > end_ts_n_trades:
                            break

                with open(new_file_tracker, 'w') as f:
                    json.dump(new_tracker_data, f)

                if end_tracker_data < end_ts_n_trades:
                    tracker_path_analyzed.append(file_tracker)
                else:
                    last_tracker_iteration_updated = new_tracker_data['data']['BTCUSDT'][-1]['_id']
                    print(f'next tracker window is not within n_trades window. Stopping Update. This Iteration is terminated at {last_tracker_iteration_updated}')
                    break
                    
                del tracker_data, new_tracker_data
            else:
                print(f'{file_tracker} is already analyzed')


        with open(path_last_market_data_analyzed, 'w') as f:
            json.dump(last_market_data_analyzed, f, separators=(",", ":"))

        market_path_analyzed.append(market_path)
        with open(path_market_path_analyzed, 'w') as f:
            market_path_analyzed_dict = {"list_market_paths_analyzed": market_path_analyzed}
            json.dump(market_path_analyzed_dict, f, indent=4)

        with open(path_tracker_path_analyzed, 'w') as f:
            tracker_path_analyzed_dict = {"list_tracker_paths_analyzed": tracker_path_analyzed}
            json.dump(tracker_path_analyzed_dict, f, indent=4)
                        
        #######################################################################################################################
        #######################################################################################################################
        #######################################################################################################################
              


def update_tracker(data_market, arg_i, last_market_data_analyzed, benchmark_json, time_interval_legend, lock, shared_data):

    # with lock:
    #     resp = json.loads(shared_data.value)
    #     last_market_data_analyzed = resp['last_market_data_analyzed']

  
    c = 0
    n_trades_arg_i = {}
    for coin in data_market:
        n_trades_arg_i[coin] = {}

        c += 1
        #print(f'arg_{arg_i}: {c} coin - {coin}')

        # if coin != 'BTCUSDT':
        #   continue
        if len(data_market[coin]) == 0:
            continue
    
        if coin not in last_market_data_analyzed:
            # initialize this variable with the oldest possible date for each coin
            last_market_data_analyzed[coin] = data_market[coin][0]['_id']

        benchmark_dates_sorted = sorted([datetime.strptime(date, '%Y-%m-%d') for date in list(benchmark_json[coin]['n_orders_series'].keys())])
        first_date_benchmark = benchmark_dates_sorted[0]

        last_day = datetime.fromisoformat(data_market[coin][0]['_id']).day
        benchmark = get_dynamic_ntrades_benchmark(benchmark_json, coin, data_market[coin][0]['_id'])
        #print(f'first_date_benchmark: {first_date_benchmark}')
        i=0
        for obs in data_market[coin]:
            i+=1
            if i % 5000 == 0:
                now = datetime.now()
                obs_id = obs['_id']
                print(f'arg_{arg_i} - coin {c} - {now} - {obs_id}')
            
            
        
            datetime_obs = datetime.fromisoformat(obs['_id'])
            if datetime_obs < datetime.fromisoformat(last_market_data_analyzed[coin]):
                # in case the obs has been already analyzed
                continue

            day = datetime.fromisoformat(obs['_id']).day
            n_trades_obs = {'trades_5m': None, 'trades_15m': None, 'trades_30m': None,
                            'trades_60m': None,'trades_3h': None, 'trades_6h': None}

            if day != last_day:
                
                benchmark = get_dynamic_ntrades_benchmark(benchmark_json, coin, obs['_id'])
                last_day = day

        
            #if datetime_obs > first_date_benchmark + timedelta(days=31):
            if benchmark != 0:
                obs_window = get_observation_window(data_market[coin], datetime_obs, obs)
                
                if obs_window != None:
                    
                    for time_interval in n_trades_obs:
                        #if time_interval != "trades_5m" or len(obs_window) > 0.985 * 360:
                        obs_window_x = obs_window[-time_interval_legend[time_interval]['t']:]
                        n_trades_obs[time_interval] = round_(np.mean(obs_window_x) / benchmark,2)

                    n_trades_obs['trades_1m'] = round_(obs['n_trades'] / benchmark,2)

            
            if 'trades_1m' not in n_trades_obs:
                n_trades_obs['trades_1m'] = None

            obs_id = datetime.fromisoformat(obs['_id']).replace(second=0, microsecond=0).isoformat()
        
            n_trades_arg_i[coin][obs_id] = n_trades_obs
        
        # SAVE n_trades on tracker_data
                
        if is_last_element(obs, data_market[coin]):
            last_timestamp_saved = obs['_id']
            last_market_data_analyzed[coin] = last_timestamp_saved
            #print(f'Last timestamp saved: {last_timestamp_saved}')

    
    with lock:
        resp = json.loads(shared_data.value)
        for coin in n_trades_arg_i:
            resp['n_trades'][coin] = n_trades_arg_i[coin]
            if coin in last_market_data_analyzed:
                resp['last_market_data_analyzed'][coin] = last_market_data_analyzed[coin]

        shared_data.value = json.dumps(resp)



def load_market(market_path):
    print(f'loading {market_path}')
    with open(market_path, 'r') as file:
        # Retrieve shared memory for JSON data and "start_interval"
        data_market = json.load(file)
        data_market = data_market['data']

    
    # market_path_2 = files_market[market_path_i]
    # print(f'loading {market_path_2}')
    # with open(market_path_2, 'r') as file:
    #     # Retrieve shared memory for JSON data and "start_interval"
    #     data_market2 = json.load(file)
    #     data_market2 = data_market2['data']

    
    # print(f'merging {market_path_2} into {market_path}')
    # for coin in data_market2:
    #     if coin not in data_market:
    #         data_market[coin] = []
    #     for obs in data_market2[coin]:
    #         data_market[coin].append(obs)

    return data_market


def get_dynamic_ntrades_benchmark(benchmark_json, coin, isotimestamp):

    current_datetime = datetime.fromisoformat(isotimestamp)
    first_ntrades_occurrence_datetime = min([datetime.strptime(date, '%Y-%m-%d') for date in list(benchmark_json[coin]['n_orders_series'].keys())])
    #first_ntrades_occurrence = first_ntrades_occurrence_datetime.strftime('%Y-%m-%d')

    if current_datetime > first_ntrades_occurrence_datetime + timedelta(days=31):
        n_trades = 0
        obs = 0 
        for day in range(1,31):
            date_benchmark = (current_datetime - timedelta(days=day)).strftime('%Y-%m-%d')
            if date_benchmark in benchmark_json[coin]['n_orders_series']:
                obs += 1
                n_trades += benchmark_json[coin]['n_orders_series'][date_benchmark][0]
                print(benchmark_json[coin]['n_orders_series'][date_benchmark][0])
        
        if obs > 25:
           avg_n_trades_30 = n_trades / obs
           return round_(avg_n_trades_30,1)
        else:
           print(f'########## WARNING: get_dynamic_ntrades_benchmark - {coin} - {isotimestamp}')
           return 0
    else:
       return 0
            



def get_observation_window(data_market_coin, start_datetime_obs, obs):

    t = 360
    position_index_obs = data_market_coin.index(obs)
    obs_window = data_market_coin[position_index_obs-t:position_index_obs+1]

    d=0
    for obs_w in obs_window:
        if datetime.fromisoformat(obs_w['_id']) < start_datetime_obs - timedelta(minutes=t):
            d += 1
        else:
            break
    
    del obs_window[:d]
    if len(obs_window) > 0.98 * 360:
        return [obs['n_trades'] for obs in obs_window]
        
    else:
        obs_len = len(obs_window)
        #print(f'Not enough obs: {obs_len}')
        return None
    
def create_volume_standings(benchmark_json=None):
    '''
    This function delivers the standings of volumes for each coin for each day
    { BTCUSDT --> { "2024-09-26" : 1 } ... } , { "2024-09-27" : 1 } }
    '''
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    path_volume_standings = ROOT_DIRECTORY + f'/analysis/Analysis2024/benchmark_json/volume_standings_{year}-{month}-{day}.json'
    if os.path.exists(path_volume_standings):
        print(f'svolume standings is up to date')
    else:
        print(f'{path_volume_standings} does not exist')
        if benchmark_json == None:
            path_benchmark_new = f'/Users/albertorainieri/Personal/analysis/benchmark_json/benchmark-2024-12-30.json'
            if os.path.exists(path_benchmark_new):
                with open(path_benchmark_new, 'r') as file:
                    # Retrieve shared memory for JSON data and "start_interval"
                    benchmark_json = json.load(file)

        list_dates = list(benchmark_json["BTCUSDT"]["volume_series"].keys())

        #orig_list.sort(key=lambda x: x.count, reverse=True)
        summary = {}
        for coin in benchmark_json:
            for date in list_dates:
                if date not in summary:
                    summary[date] = []
                if date not in benchmark_json[coin]["volume_series"]:
                    continue
                total_volume_30_days = [benchmark_json[coin]["volume_series"][date][0]]
                current_datetime = datetime.strptime(date, "%Y-%m-%d")
                for i in range(1,31):
                    datetime_past = current_datetime - timedelta(days=i)
                    previous_date = datetime_past.strftime("%Y-%m-%d")
                    if previous_date in benchmark_json[coin]["volume_series"]:
                        total_volume_30_days.append(benchmark_json[coin]["volume_series"][previous_date][0])

                summary[date].append({"coin":coin,"volume_date": round_(np.mean(total_volume_30_days),2)})
        
        standings = {}
        #print(summary)
        for date in summary:
            if date not in standings:
                standings[date] = []
            
            list_volumes = summary[date]
            standings[date] = sorted(list_volumes, key=itemgetter('volume_date'), reverse=True)
            new_list = {}
            for coin_position, i in zip(standings[date], range(1,len(standings[date])+1)):
                coin = coin_position["coin"]
                if i <= 10:
                    new_list[coin] = 1
                elif i <= 50:
                    new_list[coin] = 2
                elif i <= 100:
                    new_list[coin] = 3
                elif i <= 200:
                    new_list[coin] = 4
                else:
                    new_list[coin] = 5
            standings[date] = new_list
        
        with open(path_volume_standings, 'w') as f:
            json.dump(standings, f, indent=4)


def update_benchmark():
  '''
  This function is used to for updating the db_benchmark. We want to get the average of orders number per day for each coin.

  benchmark_json --> {COIN} --> 
  '''

  
  path_benchmark = f'{ROOT_DIRECTORY}/analysis/benchmark_json/benchmark-2024-8-22.json'
  path_benchmark_new = f'{ROOT_DIRECTORY}/analysis/benchmark_json/benchmark-NEW.json'
  path_market_path_analyzed = f'{ROOT_DIRECTORY}/analysis/TrackerUpdate/market_path_analyzed_benchmarkupdate.json'
  market_dir = f"{USB_ROOT_DIRECTORY}/Json/json_market/"
  order_series_key = 'n_orders_series'

  market_paths = sort_files_by_date(market_dir)


  if os.path.exists(path_benchmark_new):
    with open(path_benchmark_new, 'r') as file:
        # Retrieve shared memory for JSON data and "start_interval"
        benchmark_json = json.load(file)
        
  else:
    with open(path_benchmark, 'r') as file:
        # Retrieve shared memory for JSON data and "start_interval"
        benchmark_json = json.load(file)
  
  
  if os.path.exists(path_market_path_analyzed):
    with open(path_market_path_analyzed, 'r') as f:
        market_path_analyzed = json.load(f)
        market_path_analyzed = market_path_analyzed['list_market_paths_analyzed']
  else:
    market_path_analyzed = []

  len_market_paths = len(market_paths)

  j = 0
  for market_path in market_paths:
    j += 1
    
    if market_path in market_path_analyzed:
       continue
    
    with open(market_path, 'r') as file:
      data = json.load(file)
      data = data['data']
    
    tot_n_coins = len(data)
    print(f'{j}/{len_market_paths} : {tot_n_coins} coins')
    #i = 0
    for coin in data:
      #i+=1
      #print(f'{i}/{tot_n_coins} : {coin}')

      if len(data[coin]) == 0:
         continue
      last_day = get_yyyy_mm_dd(data[coin][0]['_id'])

      if order_series_key not in benchmark_json[coin]:
        benchmark_json[coin][order_series_key] = {}
        tot_trades_day = 0
        n_obs = 0
      elif last_day not in benchmark_json[coin][order_series_key]:
        tot_trades_day = 0
        n_obs = 0
      else:
        tot_trades_day = benchmark_json[coin][order_series_key][last_day][0]
        n_obs = benchmark_json[coin][order_series_key][last_day][1]
        

      for obs in data[coin]:
        n_trades = obs['n_trades']

        if last_day == get_yyyy_mm_dd(obs['_id']):
           tot_trades_day += n_trades
           n_obs += 1
           benchmark_json[coin][order_series_key][last_day] = [round_(tot_trades_day / n_obs,1), n_obs]
        else:
           #print(f'{coin} - {last_day} - {tot_trades_day}')
           benchmark_json[coin][order_series_key][last_day] = [round_(tot_trades_day / n_obs,1), n_obs]
           last_day =  get_yyyy_mm_dd(obs['_id'])
           tot_trades_day = n_trades
           n_obs = 1

    market_path_analyzed.append(market_path)
    market_path_analyzed_dict = {'list_market_paths_analyzed': market_path_analyzed}

    with open(path_market_path_analyzed, 'w') as f:
      json.dump(market_path_analyzed_dict, f, indent=4)

    with open(path_benchmark_new, 'w') as f:
      json.dump(benchmark_json, f, indent=4)



def is_last_element(element, list_):
  """Checks if an element is the last one in a list.

  Args:
    element: The element to check.
    list_: The list to search.

  Returns:
    True if the element is the last one in the list, False otherwise.
  """

  return list_.index(element) == len(list_) - 1


def get_yyyy_mm_dd(iso_timestamp):
   # Parse the ISO timestamp into a datetime object
  datetime_obj = datetime.fromisoformat(iso_timestamp)

  # Extract the date in "yyyy-mm-dd" format
  date_str = datetime_obj.strftime("%Y-%m-%d")
  return date_str


def sort_files_by_date(directory):
  """Sorts files in a directory by date based on their names.

  Args:
    directory: The path to the directory.

  Returns:
    list: A list of sorted file paths.
  """

  files = os.listdir(directory)
  list_dir = []
  for file in files:
      if file[:4] == 'data':
          list_dir.append(file)

  def get_file_datetime(file_name):
    # Extract the date and time from the file name
    date_time_str = file_name.split('-')[1:4]
    date_time_str = '-'.join(date_time_str).replace(".json", "")
    return datetime.strptime(date_time_str, '%Y-%m-%d')

  sorted_files = sorted(list_dir, key=get_file_datetime)
  full_sorted_files = [directory + file for file in sorted_files]
  return full_sorted_files