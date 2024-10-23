import json
import os,sys
sys.path.insert(0,'..')
sys.path.insert(0,'../..')
from datetime import datetime, timedelta
from analysis.Analysis2023.Helpers import round_
import calendar
import numpy as np

ROOT_DIRECTORY = "/Users/albertorainieri/Personal"
USB_ROOT_DIRECTORY = "/Volumes/PortableSSD/Alberto/Trading"

def visualization():

    folder_usb = f"{USB_ROOT_DIRECTORY}/Json/json_tracker/"
    summary_visualization_path = f"{ROOT_DIRECTORY}/analysis/Visualization/summary_visualization.json"
    files_market = sort_files_by_date(folder_usb)

    if os.path.exists(summary_visualization_path):
        with open(summary_visualization_path, 'r') as f:
            summary = json.load(f)
    else:
        summary = {'path_analyzed': [], 'data': {}}

    for market_path in files_market:
        if market_path in summary['path_analyzed']:
            print(f'{market_path} already analyzed')
            continue

        with open(market_path, 'r') as file:
            # Retrieve shared memory for JSON data and "start_interval"
            data = json.load(file)
            data = data['data']
        
        n_coins = len(list(data.keys()))
        print(f'{market_path} - {n_coins} coins')
        i=0

        

        for coin in data:
            i+=1
            #print(f'{i}/{n_coins} coins - {coin}')
            if len(data[coin]) == 0:
                continue
            first_ts_detected = data[coin][0]['_id']

            if coin not in summary['data']:
                print(f'Initialization. {coin} - first ts: {first_ts_detected}')
                summary['data'][coin] = {}
          
            
            first_dt_detected = datetime.fromisoformat(first_ts_detected).replace(second=0)
            last_minute_of_month = first_dt_detected.replace(day=get_last_day_of_month_from_iso(first_ts_detected), hour=23, minute=59, second=0)


            if not summary['data'][coin]:
                last_month_detected = first_dt_detected.strftime("%m-%Y")
                minutes_remaining = int((last_minute_of_month - first_dt_detected).total_seconds() / 60)
                n_obs = 0
            else:
                last_month_detected = get_latest_month(summary['data'][coin])[0]
                minutes_remaining = int((last_minute_of_month - first_dt_detected).total_seconds() / 60) + minutes_past_month(first_ts_detected)
                if first_dt_detected.strftime("%m-%Y") == last_month_detected:
                    n_obs = summary['data'][coin][last_month_detected]
                else:
                    n_obs = 0
            
            last_month = last_month_detected

            # we dont iterate each obs (too much time), instead we pick the 60th everytime
            tot_n_obs = len(data[coin])
            multiple = 60 #minutes. Jump between obs picks
            picks = tot_n_obs // multiple
            previous_ts = datetime.fromisoformat(data[coin][0]['_id'])
            if tot_n_obs < multiple:
               summary['data'][coin][last_month] = len(data[coin])
               continue

            for pick in range(1,picks+1):
                i = pick*multiple

                obs = data[coin][i-1]
                current_ts = datetime.fromisoformat(obs['_id'])

                data_acquired = int(((current_ts - previous_ts).total_seconds() + 3) // 60)
                lost_data = data_acquired - multiple
                # if lost_data != 0 and pick != 1:
                #    print(f'{coin} Lost {lost_data} obs')
                n_obs += multiple - lost_data
                previous_ts = current_ts

                month = datetime.fromisoformat(obs['_id']).strftime("%m-%Y")

                position_obs = data[coin].index(obs)
                #print(f'{coin} - Position {position_obs} for total of {tot_n_obs} obs')
                if position_obs > tot_n_obs - multiple:
                    #print(f'Last Iteration for {coin}')
                    minutes_remaining_data = tot_n_obs % multiple
                    if datetime.fromisoformat(obs['_id']).replace(second=0) + timedelta(minutes=minutes_remaining_data) > last_minute_of_month:
                        n_obs += tot_n_obs - (multiple*pick)
                        #print_saving_process(n_obs, minutes_remaining, coin, obs)
                        summary['data'][coin][last_month] = round_(n_obs/minutes_remaining,3)
                    else:
                        
                        #print_saving_process(n_obs, minutes_remaining, coin, obs)
                        summary['data'][coin][last_month] = n_obs + tot_n_obs - (multiple*pick)

                elif month == last_month:
                    continue
                else:
                    print_saving_process(n_obs, minutes_remaining, coin, obs)
                    summary['data'][coin][last_month]= round_(n_obs/minutes_remaining,3)
                    n_obs = 0
                    last_month = month
                    first_ts_detected = obs['_id']
                    first_dt_detected = datetime.fromisoformat(first_ts_detected).replace(second=0)
                    last_minute_of_month = first_dt_detected.replace(day=get_last_day_of_month_from_iso(first_ts_detected), hour=23, minute=59, second=0)
                    minutes_remaining = get_last_day_of_month_from_iso(first_ts_detected)*24*60
        
            #print(summary)
        summary['path_analyzed'].append(market_path)
        with open(summary_visualization_path, 'w') as f:
            json.dump(summary, f, indent=4)

    return summary

def is_day_changed(iso1, iso2):
    """
    Checks if two ISO 8601 timestamps are in different months.

    Args:
        timestamp1: The first ISO 8601 timestamp.
        timestamp2: The second ISO 8601 timestamp.

    Returns:
        True if the timestamps are in different months, False otherwise.
    """

    # Parse the timestamps into datetime objects
    datetime1 = datetime.datetime.fromisoformat(iso1)
    datetime2 = datetime.datetime.fromisoformat(iso2)

    # Compare the months
    return datetime1.day != datetime2.day

def minutes_until_end_of_month(iso_timestamp):
  """Calculates the minutes remaining until the end of the month.

  Args:
    iso_timestamp: The ISO 8601 timestamp.

  Returns:
    The number of minutes remaining until the end of the month.
  """

  # Convert ISO 8601 timestamp to datetime
  dt = datetime.datetime.fromisoformat(iso_timestamp)

  # Calculate total minutes in the month
  days_in_month = calendar.monthrange(dt.year, dt.month)[1]
  total_minutes = days_in_month * 24 * 60

  # Calculate current minutes past the beginning of the month
  current_minutes = dt.hour * 60 + dt.minute + (dt.day - 1) * 24 * 60

  # Calculate remaining minutes
  remaining_minutes = total_minutes - current_minutes
  return remaining_minutes

def minutes_past_month(iso_timestamp):
  """Calculates minutes past the beginning of the month from an ISO 8601 timestamp.

  Args:
    iso_timestamp: The ISO 8601 timestamp.

  Returns:
    The number of minutes past the beginning of the month.
  """

  dt = datetime.fromisoformat(iso_timestamp)

  return dt.hour * 60 + dt.minute + (dt.day - 1) * 24 * 60

def print_saving_process(n_obs, minutes_remaining, coin, obs):
  ts = obs['_id']
  print(f'{coin}: ts {ts} saving {n_obs} out of {minutes_remaining}')


def get_latest_month(d):
  """Gets the key with the latest mm-yyyy format in a dictionary.

  Args:
    d: The dictionary.

  Returns:
    str: The key with the latest mm-yyyy format.
  """

  latest_key = None
  latest_month_year = None

  for key, value in d.items():
    month_year = key.split('-')
    month = int(month_year[0])
    year = int(month_year[1])

    if latest_key is None or month > latest_month_year[0] or (month == latest_month_year[0] and year > latest_month_year[1]):
      latest_key = key
      latest_month_year = (month, year)

  return latest_key, month, year


def get_last_day_of_month_from_iso(iso_timestamp):
  """Gets the last day of the month from an ISO 8601 timestamp.

  Args:
    iso_timestamp: The ISO 8601 timestamp.

  Returns:
    datetime.date: The last day of the month.
  """

  # Convert ISO 8601 timestamp to datetime object
  dt = datetime.fromisoformat(iso_timestamp)
  last_day_of_month = calendar.monthrange(dt.year, dt.month)[1]

  return last_day_of_month


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