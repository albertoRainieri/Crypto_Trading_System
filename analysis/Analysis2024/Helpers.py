import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from matplotlib import collections as matcoll
from datetime import datetime, timedelta
import requests
import os, sys
from time import sleep
import pytz

# from scipy.stats import pearsonr
import pandas as pd
from time import time
from multiprocessing import Pool, Manager
from copy import copy
from random import randint
import shutil
import re
from operator import itemgetter
from typing import Literal
from functools import reduce


ROOT_PATH = os.getcwd()
TargetVariable1 = Literal["mean", "max", "min"]
KEYS_TIMESERIES = ["vol_1m", "buy_vol_1m", "vol_5m", "buy_vol_5m", "vol_15m", "buy_vol_15m", "vol_30m", "buy_vol_30m", "vol_60m", "buy_vol_60m"]
N_KEYS_TIMESERIES = len(KEYS_TIMESERIES)
#ALGOCRYPTO_SERVER = 'https://algocrypto.eu'
ALGOCRYPTO_SERVER = 'http://149.62.189.91'

def round_(number, decimal):
    return float(format(number, f".{decimal}f"))


def create_event_keys(event_keys_path, list_minutes, analysis_name):

    with open(event_keys_path, "r") as file:
        # Retrieve shared memory for JSON data and "start_interval"
        X = json.load(file)
    event_keys = []

    for xi in X:
        lvl = X[xi]["lvl"]
        for time_interval in X[xi]["info"]:
            for buy_vol in X[xi]["info"][time_interval]["buy_vol"]:
                for vol in X[xi]["info"][time_interval]["vol"]:
                    if lvl == None:
                        event_key = f"buy_vol_{time_interval}:{buy_vol}/vol_{time_interval}:{vol}/timeframe:{list_minutes}"
                    else:
                        event_key = f"buy_vol_{time_interval}:{buy_vol}/vol_{time_interval}:{vol}/timeframe:{list_minutes}/lvl:{lvl}"
                    event_keys.append(event_key)

    riskmanagement_json = {}
    for event_key in event_keys:
        riskmanagement_json[event_key] = []

    with open(f"{ROOT_PATH}/riskmanagement_json/{analysis_name}.json", "w") as outfile:
        json.dump(riskmanagement_json, outfile)

    return event_keys


def has_structure(data):
    """
    Checks if the given data has the structure:
    {key1: {key2: [elem1, elem2, ...]}}

    Args:
      data: The data to be checked.

    Returns:
      True if the data has the specified structure, False otherwise.
    """
    try:
        # Check if data is a dictionary
        if not isinstance(data, dict):
            return False

        # Check if all values in the top-level dictionary are also dictionaries
        for key1, value1 in data.items():
            if not isinstance(value1, dict):
                return False

        # Check if all values in the nested dictionaries are lists
        for key1, value1 in data.items():
            for key2, value2 in value1.items():
                if not isinstance(value2, list):
                    return False

        return True

    except (IndexError, KeyError):
        return False

def get_orderbook_file_path(event_key_path):
    base_path = f"{ROOT_PATH}/order_book/{event_key_path}"
    
    # Find all existing part files
    part_files = []
    for filename in os.listdir(f"{ROOT_PATH}/order_book"):
        if filename.startswith(f"{event_key_path}_part") and filename.endswith(".json"):
            part_num = int(filename.replace(f"{event_key_path}_part_", "").replace(".json", ""))
            part_files.append((part_num, f"{ROOT_PATH}/order_book/{filename}"))
    
    # Sort by part number
    part_files.sort(key=lambda x: x[0])
    
    # If no part files exist, create part_0
    if not part_files:
        new_path = f"{base_path}_part_0.json"
        return new_path, {}, "part_0"
        
    # Use the latest part file if it exists
    latest_part_num, latest_part_path = part_files[-1]
    file_size_mb = os.path.getsize(latest_part_path) / (1024 * 1024)
    
    if file_size_mb < 100:
        with open(latest_part_path, "r") as f:
            return latest_part_path, json.loads(f.read()), f"part_{latest_part_num}"
    else:
        # Latest part file exceeds 100MB, create new part
        new_part_num = latest_part_num + 1
        new_path = f"{base_path}_part_{new_part_num}.json"
        return new_path, {}, f"part_{new_part_num}"

def get_order_book(initial_request):

    # check initial_request data structure
    is_initial_request_ok = has_structure(initial_request)
    # GET METADATA
    if is_initial_request_ok:
        path_order_book_metadata = f"{ROOT_PATH}/order_book/metadata.json"
        if os.path.exists(path_order_book_metadata):
            f = open(path_order_book_metadata, "r")
            metadata_order_book = json.loads(f.read())
            metadata_order_book_backup = metadata_order_book.copy()
        else:
            metadata_order_book = {}

        # Check if the order_book_event has been already downloaded and create request
        orderbook_events = 0
        request = {}
        to_update = False
        for event_key in initial_request:
            if event_key not in metadata_order_book:
                metadata_order_book[event_key] = {}
            for coin in initial_request[event_key]:
                if coin not in metadata_order_book[event_key]:
                    metadata_order_book[event_key][coin] = {}
                for _id in initial_request[event_key][coin]:
                    # in case the order_book event has never been downloaded, insert in request
                    if (
                        _id not in metadata_order_book[event_key][coin]
                        and datetime.fromisoformat(_id) > datetime(2025, 1, 10)
                        and datetime.fromisoformat(_id)
                        < datetime.now() - timedelta(days=1)
                    ):
                        print
                        orderbook_events += 1
                        to_update = True
                        if event_key not in request:
                            request[event_key] = {}
                        if coin not in request[event_key]:
                            request[event_key][coin] = [_id]
                        else:
                            request[event_key][coin].append(_id)

        # make the request to the server
        requests_server = 0
        done_request = {}
        LIMIT_EVENTS_PER_REQUEST = 10
        if to_update:
            while requests_server < orderbook_events:
                i = 0
                tmp_request = {}
                for event_key in request:
                    for coin in request[event_key]:
                        for id_ in request[event_key][coin]:
                            if i > LIMIT_EVENTS_PER_REQUEST:
                                break
                            elif (
                                event_key in done_request
                                and coin in done_request[event_key]
                                and id_ in done_request[event_key][coin]
                            ):
                                continue
                            else:
                                if event_key not in tmp_request:
                                    tmp_request[event_key] = {}
                                if event_key not in done_request:
                                    done_request[event_key] = {}

                                if coin not in tmp_request[event_key]:
                                    tmp_request[event_key][coin] = []
                                if coin not in done_request[event_key]:
                                    done_request[event_key][coin] = []

                                tmp_request[event_key][coin].append(id_)
                                done_request[event_key][coin].append(id_)

                                i += 1
                                requests_server += 1

                # print(tmp_request)
                # print(done_request)
                print(
                    f"{requests_server}/{orderbook_events} events for orderbook are going to be extracted in {ALGOCRYPTO_SERVER}"
                )


                # url = "https://algocrypto.eu/analysis/get-order-book"
                url = f"{ALGOCRYPTO_SERVER}/analysis/get-order-book"
                # url = "http://localhost/analysis/get-order-book"

                response = requests.post(url=url, json=tmp_request)
                status_code = response.status_code

                # get the response, and update data_order_book_event_key and metadata
                if status_code == 200:
                    response = json.loads(response.text)
                    for event_key in response:
                        event_key_path = event_key.replace(":", "_").replace("/", "_")
                        
                        # Find the latest part file or create a new one
                        path_order_book_event_key, data_order_book_event_key, part_id = get_orderbook_file_path(event_key_path)
                        
                        print(f"Using order book file: {path_order_book_event_key}" + (f" (Part: {part_id})" if part_id else ""))
                        
                        for coin in response[event_key]:
                            if coin not in data_order_book_event_key:
                                data_order_book_event_key[coin] = {}
                            for _id in response[event_key][coin]:
                                if len(response[event_key][coin][_id]) > 0:
                                    print(f'saving orderbook: {_id} - coin: {coin} - n_obs: {len(response[event_key][coin][_id][0]["data"].keys())}')
                                    data_order_book_event_key[coin][_id] = response[event_key][coin][_id]
                                else:
                                    print(f'ORDERBOOK EMPTY: {_id} - coin: {coin} - n_obs: 0')
                                    data_order_book_event_key[coin][_id] = response[event_key][coin][_id]
                                    #sys.exit(f"Order book for {coin} at {_id} is empty. Please check the server response.")
                                #print(response[event_key][coin][_id])
                                # Store part information in metadata if it exists
                                if part_id:
                                    metadata_order_book[event_key][coin][_id] = part_id
                                else:
                                    metadata_order_book[event_key][coin][_id] = "part_0"
                        # Save the data_order_book_event_key
                        with open(path_order_book_event_key, "w") as outfile_data:
                            json.dump(data_order_book_event_key, outfile_data)

                        
                        today_str = datetime.now().strftime("%Y-%m-%d")
                        file_name_metadata_backup = f"{ROOT_PATH}/order_book/metadata-{today_str}.json"
                        if not os.path.exists(file_name_metadata_backup):
                            with open(file_name_metadata_backup, "w") as outfile_metadata:
                                json.dump(metadata_order_book_backup, outfile_metadata, indent=4)
                        # Save the metadata
                        with open(path_order_book_metadata, "w") as outfile_metadata:
                            json.dump(metadata_order_book, outfile_metadata, indent=4)
                else:
                    sys.exit(f"Status Code: get-order-book {status_code}, error received from server")
        else:
            print("Order Book Metadata is up to date")

        return metadata_order_book
    else:
        print(" data structure of the initial request is not correct ")


def get_volatility(dynamic_benchmark_info_coin, full_timestamp):
    """
    This function outputs the volatility of the coin (timeframe: last 30 days) in a specific point in time
    """
    # get timestamp of the previous day, since benchmark are updated each midnight
    correct_full_datetime = datetime.fromisoformat(full_timestamp) - timedelta(days=1)
    correct_full_timestamp = correct_full_datetime.isoformat()
    short_timestamp = correct_full_timestamp.split("T")[0]
    while short_timestamp not in dynamic_benchmark_info_coin:
        short_timestamp = (
            (correct_full_datetime - timedelta(days=1)).isoformat()
        ).split("T")[0]
        correct_full_datetime = correct_full_datetime - timedelta(days=1)
        # print(short_timestamp)

    volatility = int(dynamic_benchmark_info_coin[short_timestamp])
    return volatility


def get_volume_standings(volume_standings_full, full_timestamp, coin):
    """
    This function outputs the volume standings of the coin (timeframe: last 30 days) in a specific point in time
    """
    # get timestamp of the previous day, since benchmark are updated each midnight
    date = datetime.fromisoformat(full_timestamp).strftime("%Y-%m-%d")
    if coin not in volume_standings_full[date]:
        return 1000
    return volume_standings_full[date][coin]


def data_preparation(data, n_processes=8):
    """
    This function prepares the input for the function "wrap_analyze_events_multiprocessing", input "data".
    In particular, this function outputs a list of sliced_data that will be fed for multiprocessing analysis
    """
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
        # print(f'Length of data argument {index}: {len_data_argument}')

    total_coins = 0
    if "XRPUSDT" in data:
        start_interval = data["XRPUSDT"][0]["_id"]
        end_interval = data["XRPUSDT"][-1]["_id"]
        print("XRPUSDT")
        print(start_interval)
        print(end_interval)
    else:
        random_coin = list(data.keys())[0]
        start_interval = data[random_coin][0]["_id"]
        end_interval = data[random_coin][-1]["_id"]

    for slice_coins in data_arguments:
        total_coins += len(slice_coins)

    print(
        f"Data for {total_coins} coins are loaded from {start_interval} to {end_interval}"
    )

    for var in list(locals()):
        if var.startswith("__") and var.endswith("__"):
            continue  # Skip built-in variables
        if var == "data_arguments":
            continue
        del locals()[var]
    return data_arguments


def retrieve_datetime_for_load_data(path, list_paths, index):
    """
    this function is used from "load_data" to retrieve the datetimes between two close json in analysis/json
    these two datetimes represent the first timestamps in the list of observations
    """
    # retrieve first datetime of list_json
    path_split = path.split("-")
    day = int(path_split[3])
    month = int(path_split[2])
    year = int(path_split[1])
    hour = int(path_split[4])
    minute = int(path_split[5].split(".")[0])
    if minute == 60:
        minute = 59
    datetime1 = datetime(year=year, month=month, day=day, hour=hour, minute=minute)

    # retrieve first datetime of the next list_json
    next_path = list_paths[index + 1]
    next_path_split = next_path.split("-")
    day = int(next_path_split[3])
    month = int(next_path_split[2])
    year = int(next_path_split[1])
    hour = int(next_path_split[4])
    minute = int(next_path_split[5].split(".")[0])
    if minute == 60:
        minute = 59
    datetime2 = datetime(year=year, month=month, day=day, hour=hour, minute=minute)

    return datetime1, datetime2


def updateData_for_load_data(
    data, path, most_traded_coin_list, start_interval, end_interval
):
    print(f"Retrieving data from {path}")
    f = open(path, "r")

    temp_data_dict = json.loads(f.read())

    for coin in temp_data_dict["data"]:
        if coin in most_traded_coin_list:
            if coin not in data:
                data[coin] = []
            for obs in temp_data_dict["data"][coin]:
                if (
                    datetime.fromisoformat(obs["_id"]) >= start_interval
                    and datetime.fromisoformat(obs["_id"]) <= end_interval
                ):
                    new_obs = {}
                    for field in list(obs.keys()):
                        if (
                            "std" not in field
                            and "trd" not in field
                            and "%" not in field
                        ):
                            new_obs[field] = obs[field]
                    data[coin].append(new_obs)

    for var in list(locals()):
        if var.startswith("__") and var.endswith("__"):
            continue  # Skip built-in variables
        if var == "data":
            continue
        del locals()[var]

    return data


def get_date_key(path):
    # Define a regular expression pattern to match the date part of the filename
    date_pattern = r"data-(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{1,2})"

    match = re.search(date_pattern, path)
    if match:
        year, month, day, hour, minute = map(int, match.groups())
        # Create a tuple to use for sorting (year, month, day)
        return (year, month, day, hour, minute)
    else:
        # Handle cases where the filename format doesn't match
        return ()


def load_data(
    start_interval=datetime(2023, 5, 7, tzinfo=pytz.UTC),
    end_interval=datetime.now(tz=pytz.UTC),
    filter_position=(0, 500),
    coin=None,
):
    """
    This functions loads all the data from "start_interval" until "end_interval".
    The data should be already download through "getData.ipynb" and stored in "analysis/json" path.
    """

    # get the most traded coins from function "get_benchmark_info"
    start_coin = filter_position[0]
    end_coin = filter_position[1]
    benchmark_info, df_benchmark, volatility = get_benchmark_info()
    volume_info = []
    for coin in benchmark_info:
        volume_info.append(
            {"coin": coin, "volume_30": benchmark_info[coin]["volume_30_avg"]}
        )
    # sort the list by the volume_average of the last 30 days
    volume_info.sort(key=lambda x: x["volume_30"], reverse=True)
    most_traded_coin_list = [info["coin"] for info in volume_info[start_coin:end_coin]]

    del volume_info, df_benchmark

    # get all the json paths in "analysis/json"
    # path_dir = "/Volumes/PortableSSD/Alberto/Trading/Json/json_tracker_update/" #IN TRACKER_UPDATE YOU WILL FIND ORDER CONCENTRATION
    path_dir = "/Volumes/PortableSSD/Alberto/Trading/Json/json_tracker/"
    list_json = os.listdir(path_dir)
    # print(list_json)
    list_files = sorted(list_json, key=get_date_key)
    # print(list_files)
    list_paths_full = [file for file in list_files if file.startswith("data")]
    list_paths = [path_dir + file for file in list_paths_full]
    # for path_print in list_paths:
    #     print(path_print)

    STOP_LOADING = False

    data = {}
    for path, index in zip(list_paths, range(len(list_paths))):
        if STOP_LOADING:
            break
        # check if this is not the last json saved
        if list_paths.index(path) + 1 != len(list_paths):
            # retrieve first datetime of path and first datetime of the next path
            datetime1, datetime2 = retrieve_datetime_for_load_data(
                path, list_paths, index
            )
            # if start_interval is between these 2 datetimes, retrieve json
            if start_interval > datetime1 and start_interval < datetime2:

                print(path)
                data = updateData_for_load_data(
                    data, path, most_traded_coin_list, start_interval, end_interval
                )
                # if end_interval terminates before datetime2, then loading is completed. break the loop
                if end_interval < datetime2:
                    break
                else:
                    # let's determine the new list_paths
                    list_paths = list_paths[index + 1 :]
                    # iterate through the new "list_paths" for loading the other json
                    for path, index2 in zip(list_paths, range(len(list_paths))):

                        data = updateData_for_load_data(
                            data,
                            path,
                            most_traded_coin_list,
                            start_interval,
                            end_interval,
                        )

                        # if this is the last json than break the loop, the loading is completed
                        if list_paths.index(path) + 1 == len(list_paths):
                            STOP_LOADING = True
                            break
                        else:

                            datetime1, datetime2 = retrieve_datetime_for_load_data(
                                path, list_paths, index2
                            )
                            if end_interval > datetime1 and end_interval < datetime2:
                                STOP_LOADING = True
                                break

            # go to the next path
            else:
                # print(f'Nothing to retrieve from {path}')
                continue

        else:
            data = updateData_for_load_data(
                data, path, most_traded_coin_list, start_interval, end_interval
            )

    for var in list(locals()):
        if var.startswith("__") and var.endswith("__"):
            continue  # Skip built-in variables
        if var == "data":
            continue
        del locals()[var]

    return data


def get_benchmark_info():
    """
    this function queries the benchmark info from all the coins from the db on server
    from Analysis2023
    """
    now = datetime.now(tz=pytz.UTC) - timedelta(days=1)

    year = now.year
    month = now.month
    day = now.day
    file = "benchmark-" + str(day) + "-" + str(month) + "-" + str(year)
    full_path = ROOT_PATH + "/benchmark_json/" + file

    if os.path.exists(full_path):
        print(f"{full_path} exists. Loading the file...")
        f = open(full_path)
        benchmark_info = json.load(f)
    else:
        print(f"{full_path} does not exist. Making the request to the server..")
        METHOD = "/analysis/get-benchmarkinfo"

        url_mosttradedcoins = ALGOCRYPTO_SERVER + METHOD
        print(f"Making request to {url_mosttradedcoins}")
        response = requests.get(url_mosttradedcoins)
        print(f"StatusCode for getting get-benchmarkinfo: {response.status_code}")
        benchmark_info = response.json()
        with open(full_path, "w") as outfile:
            json.dump(benchmark_info, outfile)

    # check if there is any 0 in "volume_30_avg"
    # Also let's count how volatility is distributed across all the coins
    volatility = {}
    for coin in benchmark_info:
        if benchmark_info[coin]["volume_30_avg"] == 0:
            benchmark_info[coin]["volume_30_avg"] = 1
            benchmark_info[coin]["volume_30_std"] = 1
        else:
            coin_volatility = str(
                int(
                    benchmark_info[coin]["volume_30_std"]
                    / benchmark_info[coin]["volume_30_avg"]
                )
            )
            if coin_volatility not in volatility:
                volatility[coin_volatility] = 1
            else:
                volatility[coin_volatility] += 1

    # benchmark_info = json.loads(benchmark_info)
    df = pd.DataFrame(benchmark_info).transpose()
    df.drop("volume_series", inplace=True, axis=1)

    # Modify DF
    st_dev_ON_mean_30 = df["volume_30_std"] / df["volume_30_avg"]
    df.insert(2, "st_dev_ON_mean_30", st_dev_ON_mean_30)
    df = df.sort_values(by=["volume_30_avg"], ascending=False)

    for var in list(locals()):
        if var.startswith("__") and var.endswith("__"):
            continue  # Skip built-in variables
        if var == "benchmark_info" and var == "df":
            continue
        del locals()[var]

    return benchmark_info, df, volatility


def get_benchmark_info_deprecated():
    """
    this function queries the benchmark info from all the coins from the db on server
    """

    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    full_path = f"{ROOT_PATH}/benchmark_json/benchmark-30-12-2024"

    f = open(full_path)
    benchmark_info = json.load(f)

    # check if there is any 0 in "volume_30_avg"
    # Also let's count how volatility is distributed across all the coins
    volatility = {}
    for coin in benchmark_info:
        if benchmark_info[coin]["volume_30_avg"] == 0:
            benchmark_info[coin]["volume_30_avg"] = 1
            benchmark_info[coin]["volume_30_std"] = 1
        else:
            coin_volatility = str(
                int(
                    benchmark_info[coin]["volume_30_std"]
                    / benchmark_info[coin]["volume_30_avg"]
                )
            )
            if coin_volatility not in volatility:
                volatility[coin_volatility] = 1
            else:
                volatility[coin_volatility] += 1

    # benchmark_info = json.loads(benchmark_info)
    df = pd.DataFrame(benchmark_info).transpose()
    df.drop("volume_series", inplace=True, axis=1)

    # Modify DF
    st_dev_ON_mean_30 = df["volume_30_std"] / df["volume_30_avg"]
    df.insert(2, "st_dev_ON_mean_30", st_dev_ON_mean_30)
    df = df.sort_values(by=["volume_30_avg"], ascending=False)

    for var in list(locals()):
        if var.startswith("__") and var.endswith("__"):
            continue  # Skip built-in variables
        if var == "benchmark_info" and var == "df":
            continue
        del locals()[var]

    return benchmark_info, df, volatility


def get_dynamic_volatility(benchmark_info):
    """
    This functions processes from the output of def "get_benchmark_info" and delivers a timeseries (daily) for each coin of this equation: (volume_30_std / volume_30_avg)
    In other terms, this function computes dynamically the average of the last 30 days for each date available in "volume_series" from db_benchmark
    """
    dynamic_volatility = {}
    days = 30

    for coin in benchmark_info:
        # get "volume series" for a coin, as per db_benchmark

        volume_series = benchmark_info[coin]["volume_series"]

        # turn volume_series from an object of objects to a list of tuples. See the example below
        list_info = list(volume_series.items())

        # iterate through each tuple. 1st element is date, 2nd element is a list: [volume, std]
        for info_1day in list_info:
            volume_avg_list = []
            volume_std_list = []
            date = info_1day[0]
            position = list_info.index(info_1day)

            if position < days:
                # if there are "lte" (lower or equal than) "days" days available, let's get the last "position"s.
                # if position == 15 --> (0,15) observations
                range_ = range(position + 1)
            else:
                # otherwise let's get the last "days" observations from position
                # if position == 45 --> (15,45) observations
                range_ = range(position + 1 - days, position + 1)

            for i in range_:
                """
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
                """

                # print(position)
                volume_avg_i = list_info[i][1][0]
                volume_std_i = list_info[i][1][1]
                volume_avg_list.append(volume_avg_i)
                volume_std_list.append(volume_std_i)

            # get the mean average for a specific date (mean of all the observations until "date")
            mean_one_date = round_(np.mean(volume_avg_list), 2)
            std_one_date = round_(np.mean(volume_std_list), 2)

            if coin not in dynamic_volatility:
                dynamic_volatility[coin] = {}

            # dynamic_volatility[coin][date] = {'vol_avg': mean_one_date, 'vol_std': std_one_date}

            # get percentage std over mean
            if mean_one_date != 0:
                dynamic_volatility[coin][date] = std_one_date / mean_one_date
            else:
                dynamic_volatility[coin][date] = 1

    for var in list(locals()):
        if var.startswith("__") and var.endswith("__"):
            continue  # Skip built-in variables
        if var == "dynamic_volatility":
            continue
        del locals()[var]

    return dynamic_volatility


def get_substring_between(original_string, start_substring, end_substring):
    start_index = original_string.find(start_substring)
    end_index = original_string.find(end_substring, start_index + len(start_substring))

    if start_index == -1 or end_index == -1:
        return None

    return original_string[start_index + len(start_substring) : end_index]


def pooled_standard_deviation(stds, sample_size):

    pooled_std = np.sqrt(
        sum((sample_size - 1) * std**2 for std in stds)
        / (len(stds) * (sample_size - 1))
    )
    return pooled_std


def load_analysis_json_info(
    analysis_json_path,
    analysis_timeframe=7,
    INTEGRATION=False,
    start_interval=datetime(2023, 5, 11).isoformat(),
):

    if os.path.exists(analysis_json_path):
        with open(analysis_json_path, "r") as file:
            print(f"Loading analysis_json in {analysis_json_path}")
            # Retrieve shared memory for JSON data and "start_interval"
            analysis_json = json.load(file)
            start_interval = analysis_json["start_next_analysis"]
            del analysis_json

    end_interval = min(
        datetime.now(),
        datetime.fromisoformat(start_interval) + timedelta(days=analysis_timeframe),
    )

    print(f"start_interval at {start_interval}")
    print(f"end_interval at {end_interval}")
    return start_interval, end_interval


def updateAnalysisJson(
    shared_data_value,
    file_path,
    start_next_analysis,
    slice_i=None,
    start_next_analysis_str=None,
    INTEGRATION=False,
):

    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            # Retrieve shared memory for JSON data and "start_interval"
            analysis_json = json.load(file)
            print(f"analysis_json is loaded with file_path: {file_path}")
    else:
        analysis_json = {"data": {}}

    new_data = json.loads(shared_data_value)

    print("new data was jsonized")
    del shared_data_value

    for key in list(new_data.keys()):

        if key not in analysis_json["data"]:
            analysis_json["data"][key] = {}

        if "info" in analysis_json["data"][key]:

            # update complete "info"
            for coin in new_data[key]["info"]:
                if coin not in analysis_json["data"][key]["info"]:
                    analysis_json["data"][key]["info"][coin] = []
                for event in new_data[key]["info"][coin]:
                    if event not in analysis_json["data"][key]["info"][coin]:
                        analysis_json["data"][key]["info"][coin].append(event)
                        # print(event)
                        # if coin == "TROYUSDT":
                        #     print(f"{coin}: {event}")

            del new_data[key]

        else:
            analysis_json["data"][key]["info"] = new_data[key]["info"]

    del new_data

    if slice_i != None:
        if slice_i == 1:
            start_next_analysis_str_2 = "start_next_analysis_2"
            if start_next_analysis_str_2 in analysis_json:
                json_to_save = {
                    start_next_analysis_str_2: analysis_json[start_next_analysis_str_2],
                    start_next_analysis_str: start_next_analysis,
                    "data": analysis_json["data"],
                }
            else:
                json_to_save = {
                    start_next_analysis_str_2: datetime(2023, 6, 7).isoformat(),
                    start_next_analysis_str: start_next_analysis,
                    "data": analysis_json["data"],
                }

        else:
            start_next_analysis_str_1 = "start_next_analysis_1"
            json_to_save = {
                start_next_analysis_str_1: analysis_json[start_next_analysis_str_1],
                start_next_analysis_str: start_next_analysis,
                "data": analysis_json["data"],
            }
    else:
        if INTEGRATION:
            json_to_save = {
                "start_next_analysis": analysis_json["start_next_analysis"],
                "data": analysis_json["data"],
                "start_next_analysis_integration": start_next_analysis,
            }
        else:
            if "start_next_analysis_integration" in analysis_json:
                json_to_save = {
                    "start_next_analysis": start_next_analysis,
                    "data": analysis_json["data"],
                    "start_next_analysis_integration": analysis_json[
                        "start_next_analysis_integration"
                    ],
                }
            else:
                json_to_save = {
                    "start_next_analysis": start_next_analysis,
                    "data": analysis_json["data"],
                }

    with open(file_path, "w") as file:
        json.dump(json_to_save, file)


def getsubstring_fromkey(text):
    """
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
    lvl --> if exists
    """
    match = re.search(
        r"vol_(\d+m):(\d+(?:\.\d+)?)/vol_(\d+m):(\d+(?:\.\d+)?)/timeframe:(\d+)", text
    )
    if match:
        if "lvl" in text:
            lvl = int(text.split("lvl:")[-1])
        else:
            lvl = None

        buy_vol = "buy_vol_" + match.group(1)
        buy_vol_value = float(match.group(2))
        vol = "vol_" + match.group(3)
        vol_value = int(match.group(4))
        timeframe = int(match.group(5))

    return vol, vol_value, buy_vol, buy_vol_value, timeframe, lvl


def sort_files_in_json_dir():
    # Directory containing the files
    directory_path = (
        "/Users/albertorainieri/Projects/Personal/analysis/json_tracker_link/"
    )

    # Define a regex pattern to extract the date
    date_pattern = r"data-(\d{2})-(\d{2})-(\d{4})-(\d{2})-(\d{2}).json"

    # List all files in the specified directory
    file_list = os.listdir(directory_path)

    # Create a list of tuples containing the file name and the extracted date
    file_date_tuples = []
    for file in file_list:
        match = re.search(date_pattern, file)
        if match:
            day, month, year, hour, minute = match.groups()
            file_date = (
                file,
                datetime(
                    year=int(year),
                    month=int(month),
                    day=int(day),
                    hour=int(hour),
                    minute=int(minute),
                ),
            )
            file_date_tuples.append(file_date)

    # Sort the list of tuples based on the date
    sorted_file_date_tuples = sorted(file_date_tuples, key=lambda x: x[1], reverse=True)

    # Extract the sorted file names
    sorted_file_name_tuple = [file_date[0] for file_date in sorted_file_date_tuples]

    return sorted_file_name_tuple


def update_optimized_results(optimized_results_path):

    f = open(optimized_results_path, "r")
    optimized_results_obj = json.loads(f.read())

    request = {}

    for event_key in optimized_results_obj:
        # initialize risk
        if event_key not in request:
            request[event_key] = {}
        # iterate trough list of key "events" and take position "i" for getting "coin"
        for event, i in zip(
            optimized_results_obj[event_key]["events"],
            range(len(optimized_results_obj[event_key]["events"])),
        ):
            coin = optimized_results_obj[event_key]["coin"][i]
            # initialize coin
            if coin not in request[event_key]:
                request[event_key][coin] = []
            # append start_timestamp
            request[event_key][coin].append(event)

    # url = "https://algocrypto.eu/analysis/get-pricechanges"
    url = f"{ALGOCRYPTO_SERVER}/analysis/get-pricechanges"
    response = requests.post(url=url, json=request)
    response = json.loads(response.text)
    pricechanges = response["data"]
    msg = response["msg"]
    print(pricechanges)

    # msg shows all nan values replaced with different timeframes
    msg = response["msg"]
    if len(msg) > 0:
        text = "\n".join(msg)
        # Specify the file path where you want to save the text
        now_isoformat = datetime.now().isoformat().split(".")[0]
        file_path = ROOT_PATH + "/logs/" + now_isoformat + "-nanreplaced.txt"
        print("NaN values detected, check this path ", file_path)

        # Open the file for writing
        with open(file_path, "w") as file:
            # Write the text to the file
            file.write(text)

    # get info keys defined in "list_timeframe" in the route get-pricechanges
    random_event_key = list(pricechanges.keys())[0]
    random_coin = list(pricechanges[random_event_key].keys())[0]
    random_start_timestamp = list(pricechanges[random_event_key][random_coin].keys())[0]
    info_keys = list(
        pricechanges[random_event_key][random_coin][random_start_timestamp].keys()
    )

    for event_key in optimized_results_obj:
        # initialize all info keys (price, vol and buy)

        for info_key in info_keys:
            timeframe = info_key.split("_")[-1]  # example: "1h" or "3h" or ... "7d"
            price_key = "price_%_" + timeframe
            vol_key = "vol_" + timeframe
            buy_vol_key = "buy_" + timeframe

            optimized_results_obj[event_key][price_key] = [None] * len(
                optimized_results_obj[event_key]["events"]
            )
            optimized_results_obj[event_key][vol_key] = [None] * len(
                optimized_results_obj[event_key]["events"]
            )
            optimized_results_obj[event_key][buy_vol_key] = [None] * len(
                optimized_results_obj[event_key]["events"]
            )

        # iterate through each event, take position and coin
        for event in optimized_results_obj[event_key]["events"]:
            position = optimized_results_obj[event_key]["events"].index(event)
            coin = optimized_results_obj[event_key]["coin"][position]

            # finally fill the value for each info_key of each event
            for info_key in info_keys:
                timeframe = info_key.split("_")[-1]  # example: "1h" or "3h" or ... "7d"
                price_key = "price_%_" + timeframe
                vol_key = "vol_" + timeframe
                buy_vol_key = "buy_" + timeframe

                optimized_results_obj[event_key][price_key][position] = pricechanges[
                    event_key
                ][coin][event][info_key][0]
                optimized_results_obj[event_key][vol_key][position] = pricechanges[
                    event_key
                ][coin][event][info_key][1]
                optimized_results_obj[event_key][buy_vol_key][position] = pricechanges[
                    event_key
                ][coin][event][info_key][2]

    with open(optimized_results_path, "w") as outfile:
        json.dump(optimized_results_obj, outfile)

    return optimized_results_obj


def getTimeseriesPaths(event_key_path):
    path = ROOT_PATH + "/timeseries_json/"
    timeseries_list = os.listdir(path)
    timeseries_paths = []

    if "vlty" not in event_key_path:
        VOLATILITY_GROUP = True
    else:
        VOLATILITY_GROUP = False

    for timeseries_path in timeseries_list:
        if VOLATILITY_GROUP and "vlty" in timeseries_path:
            continue

        if event_key_path in timeseries_path:
            timeseries_paths.append(path + timeseries_path)

    return timeseries_paths


def getnNewInfoForVolatilityGrouped(event_key, info):
    new_info = {}
    for key in info:
        if event_key in key:
            if event_key not in new_info:
                new_info[event_key] = {"info": info[key]["info"]}

            else:

                for coin in info[key]["info"]:
                    if coin not in new_info[event_key]["info"]:
                        new_info[event_key]["info"][coin] = []
                    for event in info[key]["info"][coin]:
                        new_info[event_key]["info"][coin].append(event)

    return new_info


def load_timeseries(event_key_path):
    # FIND THE THE TIMESERIES json. some Timeseries json might be divided in PART<n> if the file is too big
    timeseries_paths = getTimeseriesPaths(event_key_path)
    timeseries_json = {}

    if len(timeseries_paths) == 1:
        # print(f'There is only one JSON associated with {event_key_path}')
        timeseries_json_path = timeseries_paths[0]
        with open(timeseries_json_path, "r") as file:
            # print(f'Loading {timeseries_json_path}')
            timeseries_json = json.load(file)
    elif len(timeseries_paths) > 1:
        len_timeseries_json = len(timeseries_paths)
        # print(f'There are {len_timeseries_json} JSON associated with {event_key_path}')
        # Order the list based on PART numbers in ascending order
        ordered_files = sorted(
            timeseries_paths,
            key=lambda x: (
                int(re.search(r"PART(\d+)", x).group(1))
                if re.search(r"PART(\d+)", x)
                else float("inf")
            ),
        )
        i = 0
        for timeseries_path_PART in ordered_files:
            with open(timeseries_path_PART, "r") as file:
                tmp_timeseries = json.load(file)

            for coin in tmp_timeseries:
                if coin not in timeseries_json:
                    timeseries_json[coin] = {}
                for start_timestamp in tmp_timeseries[coin]:
                    i += 1
                    timeseries_json[coin][start_timestamp] = tmp_timeseries[coin][
                        start_timestamp
                    ]

            del tmp_timeseries
        # print(f'{i} events have been loaded')
    else:
        # print('Timeseries Json does not exist. Add code in this section for downloading the timeseries from local server or Set DISCOVER to True')
        return None

    # print('Timeseries has been downloaded')
    return timeseries_json


def extract_part_number(filename):
    match = re.search(r"PART(\d+)", filename)
    if match:
        return int(match.group(1))
    return -1


def sort_filenames(filenames):
    return sorted(filenames, key=extract_part_number)


def extract_date_time(iso_string, precision="seconds"):
    """
    Extracts 'yyyy-MM-ddTHH:MM' from an ISO 8601 formatted string.

    Args:
      iso_string: The ISO 8601 formatted string (e.g., "2025-01-14T09:36:08").

    Returns:
      A string in the format 'yyyy-MM-ddTHH:MM'.
    """
    try:
        dt = datetime.fromisoformat(iso_string)
        ts_second = dt.strftime("%Y-%m-%dT%H:%M:%S")
        ts_minute = dt.strftime("%Y-%m-%dT%H:%M")
        return ts_second, ts_minute

    except ValueError:
        print(f"Invalid ISO 8601 format: {iso_string}")
        return None


def filter_request_with_orderbook_available(
    complete_info, metadata_order_book, event_key
):
    request = {"info": {}}
    n_events = 0
    for coin in complete_info["info"]:
        if coin not in metadata_order_book[event_key]:
            continue
        else:
            request["info"][coin] = []
        for event in complete_info["info"][coin]:
            timestamp_start = event["event"]
            if timestamp_start in metadata_order_book[event_key][coin]:
                n_events += 1
                request["info"][coin].append(event)

    # print(f'{n_events} timeseries will be downloaded')
    if n_events != 0:
        return request
    else:
        return None


def get_timeseries_from_server(initial_request, check_past, check_future, min_datetime_available_timeseries):
    # check initial_request data structure
    is_initial_request_ok = has_structure(initial_request)
    # GET METADATA
    if is_initial_request_ok:
        path_timeseries_metadata = f"{ROOT_PATH}/timeseries_json/metadata.json"
        if os.path.exists(path_timeseries_metadata):
            f = open(path_timeseries_metadata, "r")
            metadata_timeseries = json.loads(f.read())
        else:
            metadata_timeseries = {}

        # Check if the order_book_event has been already downloaded and create request
        orderbook_events = 0
        request_event_keys = {}
        to_update = False
        for event_key in initial_request:
            if event_key not in metadata_timeseries:
                metadata_timeseries[event_key] = {}
            for coin in initial_request[event_key]:
                if coin not in metadata_timeseries[event_key]:
                    metadata_timeseries[event_key][coin] = []
                for _id in initial_request[event_key][coin]:

                    # in case the timeseries event has never been downloaded, insert in request
                    if ( _id not in metadata_timeseries[event_key][coin] and datetime.fromisoformat(_id) > datetime(2025, 1, 10)
                        and 
                        datetime.fromisoformat(_id)  < datetime.now() - timedelta(days=1) ):

                        orderbook_events += 1
                        to_update = True
                        if event_key not in request_event_keys:
                            request_event_keys[event_key] = {}
                        if coin not in request_event_keys[event_key]:
                            request_event_keys[event_key][coin] = [_id]
                        else:
                            request_event_keys[event_key][coin].append(_id)

        # make the request to the server
        if to_update:
            for event_key in request_event_keys:
                print(event_key)
                request = {"info": {event_key: request_event_keys[event_key]}}
                request["check_past"] = check_past
                request["check_future"] = check_future
                vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = (
                    getsubstring_fromkey(event_key)
                )
                request["timeframe"] = timeframe
                print(
                    f"{orderbook_events} events for timeseries are going to be extracted in localhost"
                )
                url = "http://localhost/analysis/get-timeseries"
                response = requests.post(url=url, json=request)
                status_code = response.status_code

                # get the response, and update data_timeseries_event_key and metadata
                if status_code == 200:
                    response = json.loads(response.text)
                    event_key_path = event_key.replace(":", "_").replace("/", "_")
                    path_timeseries_event_key = (
                        f"{ROOT_PATH}/timeseries_json/{event_key_path}.json"
                    )
                    if os.path.exists(path_timeseries_event_key):
                        f = open(path_timeseries_event_key, "r")
                        data_timeseries_event_key = json.loads(f.read())
                    else:
                        data_timeseries_event_key = {}
                    #print(response)
                    for coin in response["data"]:
                        if coin not in data_timeseries_event_key:
                            data_timeseries_event_key[coin] = {}
                        for _id in response["data"][coin]:
                            metadata_timeseries[event_key][coin].append(_id)
                            data_timeseries_event_key[coin][_id] = response["data"][coin][_id]

                    # Save the data_timeseries_event_key
                    with open(path_timeseries_event_key, "w") as outfile_data:
                        json.dump(data_timeseries_event_key, outfile_data)

                    # Save the metadata
                    with open(path_timeseries_metadata, "w") as outfile_metadata:
                        json.dump(metadata_timeseries, outfile_metadata, indent=4)
                else:
                    sys.exit(f"Status Code: {status_code}, error received from server")
        else:
            print("Timeseries is up to date")

def update_full_timeseries(start_timestamp, coin, order_book_json, timeseries_json, full_timeseries):
    if ( coin in order_book_json and len(order_book_json[coin][start_timestamp]) != 0 ):
        if coin in timeseries_json:
            if start_timestamp not in timeseries_json[coin]:
                
                print(f"WARNING!!!!! Start Timestamp {start_timestamp} not in timeseries_json for coin {coin}")
                #print('Download Data through backend --> analysis_mode.py')
                return False, coin, start_timestamp
                sys.exit()
                #continue

            timeseries = timeseries_json[coin][start_timestamp]["data"]
            order_book = order_book_json[coin][start_timestamp][0]["data"]
            ranking = order_book_json[coin][start_timestamp][0]["ranking"]
            full_timeseries[coin][start_timestamp] = {
                "data": {},
                "ranking": ranking,
            }
            ts_legend = {}
            for obs_order_book in order_book:

                ts, ts_minute = extract_date_time(obs_order_book, precision="seconds")
                if ts_minute not in ts_legend:
                    ts_legend[ts_minute] = [ts]
                else:
                    ts_legend[ts_minute].append(ts)

                price = order_book[obs_order_book][0]
                bid_volume = order_book[obs_order_book][1]
                ask_volume = order_book[obs_order_book][2]
                bid_orders = order_book[obs_order_book][3]
                ask_orders = order_book[obs_order_book][4]

                n_elements_data = 5 + N_KEYS_TIMESERIES # price, bid_volume, ask_volume, bid_orders, ask_orders, vol_1m, buy_vol_1m, vol_5m, buy_vol_5m, vol_15m, buy_vol_15m, vol_30m, buy_vol_30m, vol_60m, buy_vol_60m

                data = [None] * n_elements_data
                full_timeseries[coin][start_timestamp]["data"][ts] = data

                full_timeseries[coin][start_timestamp]["data"][ts][ 0 ] = price
                full_timeseries[coin][start_timestamp]["data"][ts][ N_KEYS_TIMESERIES + 1 ] = bid_volume
                full_timeseries[coin][start_timestamp]["data"][ts][ N_KEYS_TIMESERIES + 2 ] = ask_volume
                full_timeseries[coin][start_timestamp]["data"][ts][ N_KEYS_TIMESERIES + 3 ] = bid_orders
                full_timeseries[coin][start_timestamp]["data"][ts][ N_KEYS_TIMESERIES + 4 ] = ask_orders

            ts_list = list(full_timeseries[coin][start_timestamp]["data"].keys())
            for obs_timeseries in timeseries:
                ts, ts_minute = extract_date_time(obs_timeseries["_id"], precision="minutes")
                if ts_minute in ts_legend:
                    for ts_second in ts_legend[ts_minute]:
                        for i in range(1,N_KEYS_TIMESERIES+1):
                            full_timeseries[coin][start_timestamp]["data"][ts_second][i] = obs_timeseries[KEYS_TIMESERIES[i-1]]
            
            # Sort the data chronologically
            sorted_timestamps = sorted(full_timeseries[coin][start_timestamp]["data"].keys())   
            sorted_data = {
                ts: full_timeseries[coin][start_timestamp]["data"][ts] 
                for ts in sorted_timestamps
            }
            full_timeseries[coin][start_timestamp]["data"] = sorted_data
            
            if len(ts_list) > 0:
                ts_list = sorted_timestamps  # Now we can use the already sorted list
                ts_start = ts_list[0]
                ts_end = ts_list[-1]
                print(f'COIN: {coin}, Timedelta: {datetime.fromisoformat(ts_end) - datetime.fromisoformat(ts_start)}, n_obs: {len(ts_list)}')
    
    return True, None, None


def get_full_timeseries_file_path(event_key_path):
    """
    Similar to get_orderbook_file_path, this function finds or creates the appropriate
    part file for full_timeseries data.
    """
    base_path = f"{ROOT_PATH}/full_timeseries/{event_key_path}"
    
    # Find all existing part files
    part_files = []
    for filename in os.listdir(f"{ROOT_PATH}/full_timeseries"):
        if filename.startswith(f"{event_key_path}_part") and filename.endswith(".json"):
            part_num = int(filename.replace(f"{event_key_path}_part_", "").replace(".json", ""))
            part_files.append((part_num, f"{ROOT_PATH}/full_timeseries/{filename}"))
    
    # Sort by part number
    part_files.sort(key=lambda x: x[0])
    
    # If no part files exist, create part_0
    if not part_files:
        new_path = f"{base_path}_part_0.json"
        return new_path, {}, "part_0"
        
    # Use the latest part file if it exists
    latest_part_num, latest_part_path = part_files[-1]
    file_size_mb = os.path.getsize(latest_part_path) / (1024 * 1024)
    
    if file_size_mb < 100:
        with open(latest_part_path, "r") as f:
            return latest_part_path, json.loads(f.read()), f"part_{latest_part_num}"
    else:
        # Latest part file exceeds 100MB, create new part
        new_part_num = latest_part_num + 1
        new_path = f"{base_path}_part_{new_part_num}.json"
        return new_path, {}, f"part_{new_part_num}"

def get_full_timeseries(event_key, metadata_order_book):
    """
    This function mixes orderbook and timeseries
    """
    vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = (
        getsubstring_fromkey(event_key)
    )
    #1m, 5m, 15m, 30m, 60m
    KEYS_TIMESERIES = ["vol_1m", "buy_vol_1m", "vol_5m", "buy_vol_5m", "vol_15m", "buy_vol_15m", "vol_30m", "buy_vol_30m", "vol_60m", "buy_vol_60m"]
    N_KEYS_TIMESERIES = len(KEYS_TIMESERIES)

    file_name = event_key.replace(":", "_").replace("/", "_")
    path_timeseries = ROOT_PATH + "/timeseries_json/" + file_name + ".json"
    path_full_timeseries_metadata = ROOT_PATH + "/full_timeseries/metadata.json"

    # Check if order_book and full_timeseries metadata coincide
    if os.path.exists(path_full_timeseries_metadata):
        with open(path_full_timeseries_metadata, "r") as f:
            metadata_full_timeseries = json.loads(f.read())
            metadata_full_timeseries_backup = metadata_full_timeseries.copy()
    else:
        metadata_full_timeseries = {}

    # Define which events are missing in full_timeseries
    create_full_timeseries_summary = {event_key: {}}
    if event_key not in metadata_full_timeseries:
        metadata_full_timeseries[event_key] = {}
    for coin in metadata_order_book[event_key]:
        create_full_timeseries_summary[event_key][coin] = []
        if coin not in metadata_full_timeseries[event_key]:
            metadata_full_timeseries[event_key][coin] = []
        for timestamp_start in metadata_order_book[event_key][coin]:
            # Check if timestamp exists in any part file
            timestamp_found = False
            for ts_entry in metadata_full_timeseries[event_key][coin]:
                if isinstance(ts_entry, dict) and timestamp_start in ts_entry:
                    timestamp_found = True
                    break
                
            if not timestamp_found:
                create_full_timeseries_summary[event_key][coin].append(timestamp_start)

    # Check if there's any work to do
    has_timestamps_to_process = any(len(create_full_timeseries_summary[event_key][coin]) > 0 
                                  for coin in create_full_timeseries_summary[event_key])
    
    if not has_timestamps_to_process:
        print(f"Full Timeseries is up to date")
        return

    # Load the timeseries data once
    print(f"Starts updating Full Timeseries")
    if os.path.exists(path_timeseries):
        with open(path_timeseries, "r") as f:
            timeseries_json = json.loads(f.read())
    else:
        print(f"Timeseries file {path_timeseries} not found")
        return

    # Get the path for the full_timeseries file
    path_full_timeseries, full_timeseries_data, full_timeseries_part = get_full_timeseries_file_path(file_name)
    full_timeseries = full_timeseries_data

    # Load metadata for part files
    order_book_metadata_path = ROOT_PATH + "/order_book/metadata.json"
    with open(order_book_metadata_path, "r") as f:
        order_book_metadata = json.loads(f.read())

    # Determine which part files are needed for orderbook
    needed_parts = []
    # print(create_full_timeseries_summary[event_key])
    # print('--------------------------------')
    # print(order_book_metadata[event_key]['ZECUSDT'])
    for coin in create_full_timeseries_summary[event_key]:
        for timestamp_start in create_full_timeseries_summary[event_key][coin]:
            # Find which part file contains this timestamp
            for ts_entry in order_book_metadata[event_key][coin]:
                for ts in ts_entry:

                    if ts == timestamp_start:
                        part = ts_entry[ts]
                        part_num = int(part.split('_')[1])
                        if part_num not in needed_parts:
                            needed_parts.append(part_num)
                        break
    
    # Load all needed part files at once
    #TODO UNCOMMENT FOLLOWING LINE
    # Discover available part files
    needed_parts = []
    part_num = 0
    while True:
        part_path = f"{ROOT_PATH}/order_book/{file_name}_part_{part_num}.json"
        if not os.path.exists(part_path):
            break
        needed_parts.append(str(part_num))
        part_num += 1
    #needed_parts = ["0","1", "2"]
    order_book_json = {}
    print(f'needed_parts: {needed_parts} for {file_name}')
    for part_num in needed_parts:
        part_path = f"{ROOT_PATH}/order_book/{file_name}_part_{part_num}.json"
        if not os.path.exists(part_path):
            print(f"Order book part file {part_path} not found")
            #sys.exit()
            continue
            
        with open(part_path, "r") as f:
            part_data = json.loads(f.read())
            
        # Merge part data into consolidated order_book_json
        for coin in part_data:
            if coin not in order_book_json:
                order_book_json[coin] = {}
            for timestamp in part_data[coin]:
                order_book_json[coin][timestamp] = part_data[coin][timestamp]
    
    # Process each coin and timestamp
    for coin in create_full_timeseries_summary[event_key]:
        if len(create_full_timeseries_summary[event_key][coin]) == 0:
            continue
            
        if coin not in full_timeseries:
            full_timeseries[coin] = {}
        
        for start_timestamp in create_full_timeseries_summary[event_key][coin]:
            # Skip if data is not found in loaded part files
            if coin not in order_book_json or start_timestamp not in order_book_json[coin]:
                print(f"Data for {coin}, {start_timestamp} not found in loaded part files")
                timestamp_entry = {start_timestamp: full_timeseries_part}
                metadata_full_timeseries[event_key][coin].append(timestamp_entry)
                sys.exit()
                
            # Update full timeseries with this timestamp
            status, missing_coin, missing_ts = update_full_timeseries(
                start_timestamp, coin, order_book_json, timeseries_json, full_timeseries
            )
            
            if status == False:
                # Handle failed update
                metadata_order_book[event_key][coin].remove(start_timestamp)
                print(f'Deleted {start_timestamp} from metadata_order_book: event_key: {event_key}, coin: {coin}')
                # Update metadata
                with open(order_book_metadata_path, "w") as file:
                    json.dump(metadata_order_book, file)
            else:
                # Add to metadata with part info
                timestamp_entry = {start_timestamp: full_timeseries_part}
                metadata_full_timeseries[event_key][coin].append(timestamp_entry)

    # Save results
    if len(full_timeseries) != 0:
        with open(path_full_timeseries, "w") as file:
            json.dump(full_timeseries, file)
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        filename_backup_path = f"{ROOT_PATH}/full_timeseries/metadata_{today_str}.json"
        if not os.path.exists(filename_backup_path):
            with open(filename_backup_path, "w") as file:
                json.dump(metadata_full_timeseries_backup, file, indent=4)

        with open(path_full_timeseries_metadata, "w") as file:
            json.dump(metadata_full_timeseries, file, indent=4)


def hit_jump_price_levels_range(
    current_price,
    dt,
    bid_price_levels,
    neighborhood_of_price_jump=0.005,
    distance_jump_to_current_price=0.01,
    min_n_obs_jump_level=5,
):
    """
    This function defines all the historical level whereas a jump price change was existent
    Since it can happen that price_jump_level are not always in the same point (price) I check if the jump price is in the neighboorhood of the historical jump price (average with np.mean)

    # THIS IS THE INPUT OF BID_PRICE_LEVELS
    Structure of bid price_levels: IT IS A LIST OF LISTS
    - bid_price_levels: e.g. [[13.978], [13.958], [13.958], [13.978], [13.949, 12.942], [13.97], [13.939, 12.933], [14.053]]
      EACH SUBLIST containes the jump prices at dt

    # THIS THE STRUCTURE OF SUMMARY_JUMP_PRICE
    [LIST [TUPLES]]
        [ (average price jump level, list_of_jump_price_levels )]
    """
    if distance_jump_to_current_price is None:
        return True

    summary_jump_price_level = {}
    #print(f'bid_price_levels: {bid_price_levels}')
    # iterate throgh
    for bid_price_levels_dt_info in bid_price_levels:
        dt = bid_price_levels_dt_info[1]
        bid_price_levels_dt = bid_price_levels_dt_info[0]

        for abs_price in bid_price_levels_dt:
            if abs_price == None:
                continue
            if len(summary_jump_price_level) != 0:
                IS_NEW_X = True
                for x in summary_jump_price_level:
                    historical_price_level = summary_jump_price_level[x][0]
                    historical_price_level_list = summary_jump_price_level[x][1]
                    # print(historical_price_level, abs_price)
                    if abs_price <= historical_price_level * (
                        1 + neighborhood_of_price_jump
                    ) and abs_price >= historical_price_level * (
                        1 - neighborhood_of_price_jump
                    ):
                        historical_price_level_list.append(abs_price)
                        historical_price_level = np.mean(historical_price_level_list)
                        summary_jump_price_level[x] = (
                            historical_price_level,
                            historical_price_level_list,
                        )
                        IS_NEW_X = False
                        break
                if IS_NEW_X:
                    list_x = [int(i) for i in list(summary_jump_price_level.keys())]
                    new_x = str(max(list_x) + 1)
                    summary_jump_price_level[new_x] = (abs_price, [abs_price])
            else:
                # print(abs_price)
                summary_jump_price_level["1"] = (abs_price, [abs_price])

    # print(summary_jump_price_level)
    for x in summary_jump_price_level:
        jump_price_level = summary_jump_price_level[x][0]
        jump_price_level_distance = abs((current_price - jump_price_level) / current_price)
        n_obs_jump_level = len(summary_jump_price_level[x][1])
        if (
            jump_price_level_distance
            <= distance_jump_to_current_price
            and n_obs_jump_level >= min_n_obs_jump_level
        ):
            print(f'Jump Price Level: {jump_price_level} vs Current Price: {current_price}. N_obs: {n_obs_jump_level}')
            return True
    return False


def simulate_entry_position(
    price_list,
    list_datetime,
    start_datetime,
    bid_price_levels,
    ask_order_distribution_list,
    price_change_jump,
    coin,
    start_timestamp,
    ask_orders,
    max_limit=0.1,
    price_drop_limit=0.05,
    distance_jump_to_current_price=0.03,
    max_ask_order_distribution_level=0.2,
    last_i_ask_order_distribution=1,
    min_n_obs_jump_level=5,
    n_orderbook_levels=1,
):
    """
    INPUT DESCRIPTION
    - bid_price_levels: e.g. [[13.978], [13.958], [13.958], [13.978], [13.949, 12.942], [13.97], [13.939, 12.933], [14.053]]
    - price_drop_limit: (PHASE 1) THRESHOLD OF price drop. if current price drop is higher than go to PHASE 2
    - distance_from_jump_levels: (PHASE 2) It is the DISTANCE between current price and historical jump levels
    - max_ask_order_distribution_level: (PHASE 3) LIMIT of the cumulative volume. if the nearest price ranges (0-2.5% - 2.5-5%) have a lower cumulative volume than here is the opportunity
    """

    # get price_list, datetime_list from start_datetime
    # return

    ask_order_distribution_list_copy = ask_order_distribution_list.copy()
    dt = list_datetime[-1]
    #dt_ask = ( dt - timedelta(minutes=last_i_ask_order_distribution) + timedelta(seconds=10) )
    price = price_list[-1]
    start_datetime = list_datetime[0]

    position_start_datetime = list_datetime.index(start_datetime)
    price_list = price_list[position_start_datetime:]
    max_price = max(price_list)
    list_datetime = list_datetime[position_start_datetime:]



    # PHASE 1
    # DEFINE PRICE DROP (price drop from initial price or from max price)
    max_change = (max_price - price_list[0]) / price_list[0]
    max_datetime = list_datetime[price_list.index(max_price)]
    current_timedelta_from_max = dt - max_datetime

    current_price_drop = abs((price - max_price) / max_price)

    # if coin == "HMSTRUSDT":
    #     obj = ask_order_distribution_list[-1]["ask"]
    #     print(f'dt: {dt} - {price} current_price_drop: {current_price_drop} - price_drop_limit: {price_drop_limit} - max_change: {max_change} - max_limit: {max_limit}')
    #     print(obj)

    if max_limit == None:
        max_limit = 100

    #print(f'Current Price Drop at {dt} of {round_(current_price_drop,4)*100}%')
    if current_price_drop >= price_drop_limit and max_change <= max_limit:
        #print(f'Price Drop at {dt} of {round_(current_price_drop,4)*100}%')
        # print(f'Price Drop at {ts} of {round_(current_price_drop,4)*100}%')

        #print(f'Price Drop at {dt} of {round_(current_price_drop,4)*100}%')
        # get keys of ask_order_distribution [0.025, 0.05, 0.075, ...]
        if len(ask_order_distribution_list) == 1:
            keys_ask_order_distribution = list( ask_order_distribution_list[0]["ask"].keys() )
            keys_ask_order_distribution.sort()


            # initialize new var for average ask order distribution
            # selected_ask_order_distribution_list = []
            avg_ask_order_distribution = ask_order_distribution_list[0]["ask"]
            # if coin == "HMSTRUSDT":
            #     print(f'dt: {dt} - avg_ask_order_distribution: {avg_ask_order_distribution}')
                #print(f'keys_ask_order_distribution: {keys_ask_order_distribution}')

            # for lvl in keys_ask_order_distribution:
            #     avg_ask_order_distribution[lvl] = []

            # # determine how many of the last x elements of ask_order_distribution_list to select.
            # # based on the "dt_ask" variable
            # for ask_order_distribution in ask_order_distribution_list:
            #     selected_ask_order_distribution_list.append( ask_order_distribution )
            #     for lvl in ask_order_distribution["ask"]:
            #         avg_ask_order_distribution[lvl].append( ask_order_distribution["ask"][lvl] )

            # compute the mean
            # for lvl in avg_ask_order_distribution:
            #     avg_ask_order_distribution[lvl] = sum(avg_ask_order_distribution[lvl] ) / len(avg_ask_order_distribution[lvl])

            # Check all order book levels up to n_orderbook_levels
            all_levels_valid = True
            for level in range(n_orderbook_levels):
                if avg_ask_order_distribution[keys_ask_order_distribution[level]] >= max_ask_order_distribution_level:
                    all_levels_valid = False
                    break

            
            # if coin == "HMSTRUSDT" and "2025-06-09T05:13" in dt.isoformat():
            #     obj = ask_order_distribution_list[-1]["ask"]
            #     print(f'dt: {dt} - {price} current_price_drop: {current_price_drop} - price_drop_limit: {price_drop_limit} - max_change: {max_change} - max_limit: {max_limit}')
            #     avg_value = avg_ask_order_distribution[keys_ask_order_distribution[0]]
            #     print(f'ask_order_distribution_list: {ask_order_distribution_list}')
            #     print(f'avg_value: {avg_value}')
            #     #print(obj)
            
            if all_levels_valid:

                is_jump_price_level = hit_jump_price_levels_range(
                    current_price=price,
                    dt=dt,
                    bid_price_levels=bid_price_levels,
                    distance_jump_to_current_price=distance_jump_to_current_price,
                    min_n_obs_jump_level=min_n_obs_jump_level,
                )

                if is_jump_price_level:
                    info_buy = (price, dt, ask_order_distribution_list)
                    print(f"BUY-EVENT: coin: {coin}, start_timestamp: {start_timestamp}")
                    print(f'avg_ask_order_distribution: {avg_ask_order_distribution}')
                    print(f'ask_orders: {ask_orders}')
                    return info_buy
    return None


def simulate_exit_position(
    price_list,
    bid_order_distribution_list,
    list_datetime,
    price_change_jump,
    last_i_bid_order_distribution,
    max_bid_order_distribution_level,
):

    bid_price = price_list[-1]
    dt = list_datetime[-1]
    dt_bid = (
        dt - timedelta(minutes=last_i_bid_order_distribution) + timedelta(seconds=10)
    )
    # get keys of bid_order_distribution [0.025, 0.05, 0.075, ...]
    if len(bid_order_distribution_list) >= last_i_bid_order_distribution:
        keys_bid_order_distribution = list(
            bid_order_distribution_list[-1]["bid"].keys()
        )

        # initialize new var for average bid order distribution
        selected_bid_order_distribution_list = []
        avg_bid_order_distribution = {}

        for lvl in keys_bid_order_distribution:
            avg_bid_order_distribution[lvl] = []

        # determine how many of the last x elements of bid_order_distribution_list to select.
        # based on the "dt_bid" variable
        for bid_order_distribution in bid_order_distribution_list:
            if datetime.fromisoformat(bid_order_distribution["dt"]) > dt_bid:
                selected_bid_order_distribution_list.append(bid_order_distribution)
                for lvl in bid_order_distribution["bid"]:
                    avg_bid_order_distribution[lvl].append(
                        bid_order_distribution["bid"][lvl]
                    )

        # compute the mean
        for lvl in avg_bid_order_distribution:
            avg_bid_order_distribution[lvl] = np.mean(avg_bid_order_distribution[lvl])

        # TODO: check only first level, or next ones too ?
        if (
            avg_bid_order_distribution[str(price_change_jump)]
            < max_bid_order_distribution_level
        ):
            info_sell = (bid_price, dt, selected_bid_order_distribution_list)
            print(f"n_bid_order: {len(selected_bid_order_distribution_list)}")
            # print(price, dt, bid_order_distribution[str(price_change_jump)], bid_order_distribution[str(price_change_jump+price_change_jump)])
            return info_sell
    return None


def analyze_timeseries(
    event_key,
    check_past,
    check_future,
    jump,
    limit,
    price_change_jump,
    max_limit,
    price_drop_limit,
    distance_jump_to_current_price,
    max_ask_order_distribution_level,
    last_i_ask_order_distribution,
    min_n_obs_jump_level,
    n_orderbook_levels,
    buy_duration,
    stop_loss_limit,
    strategy_result,
    save_plot,
    analysis_name=None,
    retry_analysis=False,
    start_analysis=datetime(2025,1,1)
):
    """
    INPUT DESCRITION
    "jump" is used to see there are jumps in the order book. E.g. from -5% and -9% (price change) there are not orders (THIS IS A JUMP)
    "limit" is the price window range, where order books are checked. if limit = 0.4, then only the order books within the 40% price range are analyzed
    "price_change_jump" is used for the orde
    """
    # get result strategy is exists

    strategy_result = json.loads(strategy_result.value)
    event_key_path = event_key.replace(":", "_").replace("/", "_")
    path_plot_root = f"{ROOT_PATH}/plot_timeseries/{event_key_path}"
    if not os.path.exists(path_plot_root):
        os.makedirs(path_plot_root)

    tmp_strategy_result = {}

    vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = (
        getsubstring_fromkey(event_key)
    )
    if analysis_name is not None:
        KEYS_TIMESERIES = [vol_field, buy_vol_field]
        N_KEYS_TIMESERIES = len(KEYS_TIMESERIES)
    else:
        KEYS_TIMESERIES = ["vol_1m", "buy_vol_1m", "vol_5m", "buy_vol_5m", "vol_15m", "buy_vol_15m", "vol_30m", "buy_vol_30m", "vol_60m", "buy_vol_60m"]
        N_KEYS_TIMESERIES = len(KEYS_TIMESERIES)

    # List files in full_timeseries directory that contain event_key_path
    full_timeseries_dir = ROOT_PATH + "/full_timeseries/"
    matching_files = []
    for filename in os.listdir(full_timeseries_dir):
        if event_key_path in filename:
            matching_files.append(filename)

    for filename in matching_files:
        path_full_timeseries = ROOT_PATH + "/full_timeseries/" + filename
        f = open(path_full_timeseries, "r")
        full_timeseries = json.loads(f.read())

        dark_violet = "#9400D3"
        violet = "#EE82EE"
        red = "#FF0000"
        light_red = "#FFB6C1"
        light_yellow = "#FFFFE0"
        white = "#FFFFFF"
        dark_orange_hex = "#E67A00"

        order_colors = [
            white,
            light_yellow,
            light_red,
            violet,
            dark_orange_hex,
            red,
            dark_violet,
        ]
        price_range_colors = [
            (0, 0.025),
            (0.025, 0.05),
            (0.05, 0.1),
            (0.1, 0.15),
            (0.15, 0.2),
            (0.2, 0.3),
            (0.3, 1),
        ]
        assert len(order_colors) == len(price_range_colors)
        order_distribution_color_legend = {}

        for color, price_range in zip(order_colors, price_range_colors):
            order_distribution_color_legend[price_range] = color

        for coin in full_timeseries:
            #print(f"Analyzing {coin} - file: {filename}")
            path_plot_coin = f"{path_plot_root}/{coin}"
            if not os.path.exists(path_plot_coin):
                os.makedirs(path_plot_coin)
            for start_timestamp in full_timeseries[coin]:
                if datetime.fromisoformat(start_timestamp) < start_analysis:
                    continue
                start_timestamp_path = start_timestamp.replace(":", "-").split(".")[0]
                path_plot = f"{path_plot_coin}/{start_timestamp_path}.png"
                plot_exists = os.path.exists(path_plot)

                if retry_analysis:
                    pass
                elif (
                    event_key in strategy_result
                    and coin in strategy_result[event_key]
                    and start_timestamp in strategy_result[event_key][coin]
                    and save_plot == False
                ):
                    continue
                elif (
                    event_key in strategy_result
                    and coin in strategy_result[event_key]
                    and start_timestamp in strategy_result[event_key][coin]
                    and save_plot == True
                    and plot_exists
                ):
                    continue

                if coin not in tmp_strategy_result:
                    tmp_strategy_result[coin] = {}
                tmp_strategy_result[coin][start_timestamp] = []

                start_datetime = datetime.fromisoformat(start_timestamp)
                start_datetime = start_datetime.replace(second=0).replace(microsecond=0)
                prestart_datetime = start_datetime - timedelta(minutes=check_past)
                postend_datetime = start_datetime + timedelta(minutes=int(timeframe)) + timedelta(minutes=check_future)

                data = full_timeseries[coin][start_timestamp]["data"]
                list_ts = sorted(list(data.keys()))
                

                if len(list_ts) == 0:
                    continue

                end_datetime = datetime.fromisoformat(list_ts[-1])


                price_list = []
                max_min_price_list = []
                vol_list = []
                buy_vol_list = []
                bid_volume_list = []
                ask_volume_list = []
                bid_price_levels = []
                ask_price_levels = []
                bid_ask_volume_list = []
                list_datetime = []
                list_datetime_order = []
                bid_order_distribution_colour_list = []
                ask_order_distribution_colour_list = []
                bid_order_distribution_list = []
                ask_order_distribution_list = []
                bid_color_list = []
                ask_color_list = []
                bid_volume_wrt_total = []
                ask_volume_wrt_total = []
                previous_dt = datetime.fromisoformat(list_ts[0])
                initial_price = data[list_ts[0]][0]

                if save_plot:
                    fig, ax = plt.subplots(5, 1, sharex=True, figsize=(20, 10))
                info_buy = None
                info_sell = {}
                info_buy_list = {}
                dt_buy_summary = {}
                buy_price_summary = {}
                ask_order_distribution_summary = {}
                dt_buy_isoformat_summary = {}
                max_change_buy_summary = {}
                min_change_buy_summary = {}
                dt_min_change_buy_summary = {}
                dt_max_change_buy_summary = {}
                max_buy_ask_distribution_summary = {}
                max_buy_bid_distribution_summary = {}
                min_buy_ask_distribution_summary = {}
                min_buy_bid_distribution_summary = {}
                max_change_until_buy_summary = {}
                min_change_until_buy_summary = {}
                SELL = {}
                sell_price_summary = {}
                datetime_sell_summary = {}
                gain_summary = {}
                timedelta_max_buy = {}
                timdelta_min_buy = {}   

                LAST_TS = list_ts[-1]

                for ts in list_ts:
                    dt = datetime.fromisoformat(ts)

                    if dt < previous_dt:
                        continue
                    else:
                        previous_dt = dt

                    if dt < prestart_datetime or dt > postend_datetime:
                        continue

                    list_datetime.append(dt)
                    price = data[ts][0]
                    price_list.append(price)
                    vol_list.append(data[ts][1])
                    buy_vol_list.append(data[ts][2])

                    if dt <= end_datetime and dt >= start_datetime:
                        max_min_price_list.append(price)
                        max_price = max(max_min_price_list)
                        min_price = min(max_min_price_list)
                        final_price = price

                    bid_orders = data[ts][N_KEYS_TIMESERIES + 3]
                    ask_orders = data[ts][N_KEYS_TIMESERIES + 4]

                    # print(f'bid_orders: {bid_orders}')
                    # print(f'ask_orders: {ask_orders}')

                    if ask_orders != None:
                        total_bid_volume = data[ts][N_KEYS_TIMESERIES + 1]
                        total_ask_volume = data[ts][N_KEYS_TIMESERIES + 2]
                        summary_bid_orders = []
                        summary_ask_orders = []
                        for price_i, qty_i in bid_orders:
                            price_change = float(price_i)
                            quantity_order = float(qty_i)
                            if abs(price_change) > limit:
                                break
                            summary_bid_orders.append((
                                price_change,
                                quantity_order
                            ))
                        for price_i, qty_i in ask_orders:
                            price_change = float(price_i)
                            quantity_order = float(qty_i)
                            if abs(price_change) > limit:
                                break
                            summary_ask_orders.append(( price_change, quantity_order ))
                        # print(f'summary_ask_orders: {summary_ask_orders}')
                        
                        bid_price_level, bid_order_distribution, bid_cumulative_level = get_price_levels( price, summary_bid_orders, jump, limit, price_change_jump, delta=0.005 )
                        # if summary_bid_orders != []:
                        #     #print(f'summary_bid_orders: {summary_bid_orders}')
                        #     #print(f'bid_price_level: {bid_price_level}')
                        #     #print(f'bid_order_distribution: {bid_order_distribution}')
                        #     if bid_price_level[0][0] != None:
                        #         print(f'bid_price_level: {bid_price_level}')
                        #         print(f'summary_bid_orders: {summary_bid_orders}')
                                #print(f'bid_cumulative_level: {bid_cumulative_level}')

                        ask_price_level, ask_order_distribution, ask_cumulative_level = get_price_levels( price, summary_ask_orders, jump, limit, price_change_jump, delta=0.005 ) #0.01 provokes error
                        
                        # if coin == 'NEIROUSDT' and start_timestamp == '2025-05-18T13:39:03.393562':
                        #     print(f'price: {price}')
                        #     print(f'summary_ask_orders: {summary_ask_orders}')
                        #     print(f'ask_orders: {ask_orders}')
                        #     print(f'ask_order_distribution: {ask_order_distribution}')
                        #     print('--------------------------------')

                        ask_order_distribution_list = [
                            {
                                "ask": ask_order_distribution,
                                "bid_level": bid_cumulative_level,
                                "ask_level": ask_cumulative_level,
                                "total_bid_volume": total_bid_volume,
                                "total_ask_volume": total_ask_volume,
                                "dt": ts,
                            }
                        ]
                        bid_order_distribution_list.append( {"bid": bid_order_distribution, "dt": ts} )
                        # print(bid_order_distribution)

                        # bid/ask_actual_jump can be a number or False, it checks if there a was a jump, otherwise just the get the cumulative volume at the limit price change
                        bid_actual_jump = bid_price_level[0][3]
                        ask_actual_jump = ask_price_level[0][3]

                        # for each jump level, just retrieve the absolute price
                        bid_price_levels_dt = []  # all bid price levels at dt
                        ask_price_levels_dt = []  # all ask price levels at dt
                        for lvl in bid_price_level:
                            bid_price_levels_dt.append(lvl[0])
                        for lvl in ask_price_level:
                            ask_price_levels_dt.append(lvl[0])
                        bid_price_levels.append( (bid_price_levels_dt, dt) )  # this is a list of lists. each sublist contains all prices where a jump has occurred
                        ask_price_levels.append((ask_price_levels_dt, dt))

                        # plot the jump prices
                        if bid_actual_jump and save_plot:
                            for lvl_bid in bid_price_levels_dt:
                                ax[0].plot(dt, lvl_bid, "go", markersize=1)
                                ax[1].plot(dt, lvl_bid, "go", markersize=1)
                        if ask_actual_jump and save_plot:
                            for lvl_ask in ask_price_levels_dt:
                                ax[0].plot(dt, lvl_ask, "ro", markersize=1)
                                ax[1].plot(dt, lvl_ask, "go", markersize=1)

                        # plot the order book distribution
                        if len(bid_order_distribution) != 0 and save_plot:
                            for lvl_bid in bid_order_distribution:
                                for lvl_col in order_distribution_color_legend:
                                    if (bid_order_distribution[lvl_bid] >= lvl_col[0] and bid_order_distribution[lvl_bid] < lvl_col[1]):
                                        order_distribution_dt = [(mdates.date2num(dt),price* (1 - float(lvl_bid) + price_change_jump)), (mdates.date2num(dt),price * (1 - float(lvl_bid)) ) ]
                                        bid_order_distribution_colour_list.append(order_distribution_dt )
                                        bid_color_list.append(order_distribution_color_legend[lvl_col])
                                        break
                            for lvl_ask in ask_order_distribution:
                                for lvl_col in order_distribution_color_legend:
                                    if (ask_order_distribution[lvl_ask] >= lvl_col[0]and ask_order_distribution[lvl_ask] < lvl_col[1]):
                                        order_distribution_dt = [(mdates.date2num(dt),price* (1 + float(lvl_ask) - price_change_jump)),(mdates.date2num(dt),price * (1 + float(lvl_ask)),)]
                                        ask_order_distribution_colour_list.append(order_distribution_dt)
                                        ask_color_list.append(order_distribution_color_legend[lvl_col])
                                        break

                        # this is the cumulative level (from 0 to 100) based on $limit
                        # Basically I want to get the total bid/volume with respect to the limit

                        bid = total_bid_volume * bid_cumulative_level
                        ask = total_ask_volume * ask_cumulative_level
                        bid_volume_list.append(bid)
                        ask_volume_list.append(ask)
                        bid_volume_wrt_total.append(bid_cumulative_level)
                        ask_volume_wrt_total.append(ask_cumulative_level)
                        bid_ask_volume_list.append(bid + ask)
                    else:
                        bid_volume_list.append(0)
                        ask_volume_list.append(0)
                        bid_ask_volume_list.append(0)
                        bid_volume_wrt_total.append(0)
                        ask_volume_wrt_total.append(0)

                    # print(f'dt: {dt} - start_datetime: {start_datetime} - end_datetime: {end_datetime} - info_buy: {info_buy}')

                    if ( bid_orders != None and dt > start_datetime and dt < end_datetime):# and info_buy == None ):
                        #print(f'dt: {dt} - start_datetime: {start_datetime} - end_datetime: {end_datetime} - info_buy: {info_buy}')
                        info_buy = simulate_entry_position(
                            price_list,
                            list_datetime,
                            start_datetime,
                            bid_price_levels,
                            ask_order_distribution_list,
                            price_change_jump,
                            coin,
                            start_timestamp,
                            ask_orders,
                            max_limit,
                            price_drop_limit,
                            distance_jump_to_current_price,
                            max_ask_order_distribution_level,
                            last_i_ask_order_distribution,
                            min_n_obs_jump_level,
                            n_orderbook_levels
                        )
                        if info_buy != None:
                            #print('infobuy is no longer none')
                            dt_buy = info_buy[1]
                            ts_buy = dt_buy.isoformat()
                            ts_buy = dt_buy.strftime("%Y-%m-%dT%H-%M")
                            if ts_buy in info_buy_list:
                                continue
                            info_buy_list[ts_buy] = info_buy
                            SELL[ts_buy] = False
                            info_sell[ts_buy] = None

                            dt_buy_summary[ts_buy] = dt_buy
                            buy_price = info_buy[0]
                            buy_price_summary[ts_buy] = buy_price
                            ask_order_distribution_buy = info_buy[2]
                            ask_order_distribution_summary[ts_buy] = ask_order_distribution_buy
                            dt_buy_isoformat = dt_buy.isoformat()
                            dt_buy_isoformat_summary[ts_buy] = dt_buy_isoformat
                            max_change_buy = 0
                            max_change_buy_summary[ts_buy] = max_change_buy
                            min_change_buy = 0
                            min_change_buy_summary[ts_buy] = min_change_buy
                            dt_min_change_buy = dt
                            dt_min_change_buy_summary[ts_buy] = dt_min_change_buy
                            dt_max_change_buy = dt
                            dt_max_change_buy_summary[ts_buy] = dt_max_change_buy
                            max_change_until_buy_summary[ts_buy] = max_change
                            min_change_until_buy_summary[ts_buy] = min_change
                            max_buy_ask_distribution = None
                            max_buy_ask_distribution_summary[ts_buy] = max_buy_ask_distribution
                            max_buy_bid_distribution = None
                            max_buy_bid_distribution_summary[ts_buy] = max_buy_bid_distribution
                            min_buy_ask_distribution = None
                            min_buy_ask_distribution_summary[ts_buy] = min_buy_ask_distribution
                            min_buy_bid_distribution = None
                            min_buy_bid_distribution_summary[ts_buy] = min_buy_bid_distribution
                    # TODO: SIMULATE EXIT POSITION:
                    # if info_buy != None and not SELL and info_sell == None:
                    #     info_sell = simulate_exit_position(price_list, bid_order_distribution_list, list_datetime, price_change_jump, last_i_ask_order_distribution, max_ask_order_distribution_level)

                    ###################################
                    # check stop loss
                    for ts_buy in info_buy_list:
                        if not SELL[ts_buy]:
                            if stop_loss_limit != 'None':
                                if price < buy_price_summary[ts_buy] * (1 - stop_loss_limit):
                                    SELL[ts_buy] = True
                                    sell_price = price
                                    sell_price_summary[ts_buy] = sell_price
                                    datetime_sell = dt
                                    datetime_sell_summary[ts_buy] = datetime_sell
                                    gain = round_(((sell_price - buy_price_summary[ts_buy]) / buy_price_summary[ts_buy]) * 100, 2)
                                    gain_summary[ts_buy] = gain
                                    buy_price_coin = buy_price_summary[ts_buy]
                                    sell_price_coin = sell_price_summary[ts_buy]
                                    dt_buy_coin = dt_buy_isoformat_summary[ts_buy]
                                    print( f"Stoploss-Gain: {gain} - buy-price = {buy_price_coin} - sell-price = {sell_price_coin} -dt_buy: {dt_buy_coin}" )

                            change = round_(((price - buy_price_summary[ts_buy]) / buy_price_summary[ts_buy]) * 100, 2)
                            if change > max_change_buy_summary[ts_buy]:
                                dt_max_change_buy = dt
                                max_change_buy = change
                                max_change_buy_summary[ts_buy] = max_change_buy
                                if dt <= end_datetime:
                                    max_buy_ask_distribution = ask_order_distribution
                                    max_buy_bid_distribution = bid_order_distribution
                                    max_buy_ask_distribution_summary[ts_buy] = max_buy_ask_distribution
                                    max_buy_bid_distribution_summary[ts_buy] = max_buy_bid_distribution
                                else:
                                    max_buy_ask_distribution = None
                                    max_buy_bid_distribution = None
                                    max_buy_ask_distribution_summary[ts_buy] = max_buy_ask_distribution
                                    max_buy_bid_distribution_summary[ts_buy] = max_buy_bid_distribution
                            if change < min_change_buy_summary[ts_buy]:
                                dt_min_change_buy = dt
                                min_change_buy = change
                                min_change_buy_summary[ts_buy] = min_change_buy
                                if dt <= end_datetime:
                                    min_buy_ask_distribution = ask_order_distribution
                                    min_buy_bid_distribution = bid_order_distribution
                                    min_buy_ask_distribution_summary[ts_buy] = min_buy_ask_distribution
                                    min_buy_bid_distribution_summary[ts_buy] = min_buy_bid_distribution
                                else:
                                    min_buy_ask_distribution = None
                                    min_buy_bid_distribution = None
                                    min_buy_ask_distribution_summary[ts_buy] = min_buy_ask_distribution
                                    min_buy_bid_distribution_summary[ts_buy] = min_buy_bid_distribution
                        # check if end of buy-timeframe, only if already bought
                        if dt >= dt_buy_summary[ts_buy] + timedelta(minutes=buy_duration) and not SELL[ts_buy]:
                            SELL[ts_buy] = True
                            if info_sell[ts_buy] == None:
                                sell_price = price
                                sell_price_summary[ts_buy] = sell_price
                                datetime_sell = dt
                                datetime_sell_summary[ts_buy] = datetime_sell
                            else:
                                sell_price = info_sell[ts_buy][0]
                                datetime_sell = info_sell[ts_buy][1]
                                sell_price_summary[ts_buy] = sell_price
                                datetime_sell_summary[ts_buy] = datetime_sell

                            
                            gain = round_(((sell_price - buy_price_summary[ts_buy]) / buy_price_summary[ts_buy]) * 100, 2)
                            gain_summary[ts_buy] = gain
                            timedelta_max_buy = dt_max_change_buy - dt_buy_summary[ts_buy]
                            timdelta_min_buy = dt_min_change_buy - dt_buy_summary[ts_buy]
                            dt_buy_isoformat_coin = dt_buy_isoformat_summary[ts_buy]
                            max_change_buy_coin = max_change_buy_summary[ts_buy]
                            min_change_buy_coin = min_change_buy_summary[ts_buy]
                            print( f"Gain: {gain} -buy-price: {buy_price_summary[ts_buy]} -sell-price: {sell_price} -dt_buy: {dt_buy_isoformat_coin} - max_change_buy: {max_change_buy_coin} - min_change_buy: {min_change_buy_coin} - dt_max_change_buy: {timedelta_max_buy} - dt_min_change_buy: {timdelta_min_buy}" )
                            print('--------------------------------')

                    max_change = round_((max_price - initial_price) * 100 / initial_price, 2)
                    min_change = round_((min_price - initial_price) * 100 / initial_price, 2)
                    dt_max_price = list_datetime[price_list.index(max_price)]
                    dt_min_price = list_datetime[price_list.index(min_price)]

                for ts_buy in info_buy_list:
                    if SELL[ts_buy]:
                        performance_doc = {
                            "initial_price": initial_price,
                            "final_price": final_price,
                            "buy_price": buy_price_summary[ts_buy],
                            "sell_price": sell_price_summary[ts_buy],
                            "max_change": max_change,
                            "min_change": min_change,
                            "max_change_until_buy": max_change_until_buy_summary[ts_buy],
                            "min_change_until_buy": min_change_until_buy_summary[ts_buy],
                            "gain": gain_summary[ts_buy],
                            "datetime_buy": dt_buy_isoformat_summary[ts_buy],
                            "datetime_sell": datetime_sell_summary[ts_buy].isoformat(),
                            "datetime_end": end_datetime.isoformat(),
                            "ask_order_distribution": ask_order_distribution_summary[ts_buy],
                            "max_change_buy": max_change_buy_summary[ts_buy],
                            "dt_max_change_buy": dt_max_change_buy_summary[ts_buy].isoformat(),
                            "min_change_buy": min_change_buy_summary[ts_buy],
                            "dt_min_change_buy": dt_min_change_buy_summary[ts_buy].isoformat(),
                            # "max_buy_ask_distribution": max_buy_ask_distribution_summary[ts_buy],
                            # "max_buy_bid_distribution": max_buy_bid_distribution_summary[ts_buy],
                            # "min_buy_ask_distribution": min_buy_ask_distribution_summary[ts_buy],
                            # "min_buy_bid_distribution": min_buy_bid_distribution_summary[ts_buy],
                            "path_png": path_plot,
                        }

                    else:
                        performance_doc = {
                            "initial_price": initial_price,
                            "final_price": final_price,
                            "buy_price": None,
                            "sell_price": None,
                            "max_change": max_change_buy_summary[ts_buy],
                            "min_change": min_change_buy_summary[ts_buy],
                            "gain": None,
                            "datetime_buy": None,
                            "datetime_sell": None,
                            "datetime_end": end_datetime.isoformat(),
                            "ask_order_distribution": None,
                            "max_change_buy": None,
                            "dt_max_change_buy": None, 
                            "min_change_buy": None,
                            "dt_min_change_buy": None,
                            "max_buy_ask_distribution": None,
                            "max_buy_bid_distribution": None,
                            "min_buy_ask_distribution": None,
                            "min_buy_bid_distribution": None,
                            "path_png": path_plot,
                        }

                    #print(f'coin: {coin} - start_timestamp: {start_timestamp} - filename: {filename}')
                    tmp_strategy_result[coin][start_timestamp].append(performance_doc)

                if save_plot:

                    ax[0].plot(list_datetime, price_list, linewidth=0.5, color="black")
                    ax[0].axvline(x=start_datetime, color="blue", linestyle="--")
                    ax[0].axvline(x=end_datetime, color="blue", linestyle="--")
                    ax[0].set_ylabel("Price")
                    ax[0].grid(True)
                    ax[0].annotate( f"+{max_change}%", xy=(dt_max_price, max_price), xytext=(dt_max_price, max_price * (1 - ((max_change / 100)))), textcoords="data", ha="center", va="top", arrowprops=dict(arrowstyle="->"),)
                    ax[0].annotate( f"-{min_change}%", xy=(dt_min_price, min_price), xytext=(dt_min_price, min_price * (1 - ((max_change / 100)))), textcoords="data", ha="center", va="top", arrowprops=dict(arrowstyle="->"),)

                    if SELL:
                        ax[0].annotate(
                            f"buy: {buy_price}",
                            xy=(dt_buy, buy_price),
                            xytext=(dt_buy, buy_price * (1.05)),
                            textcoords="data",
                            ha="center",
                            va="top",
                            arrowprops=dict(arrowstyle="->"),
                        )
                        ax[0].annotate(
                            f"sell: {sell_price}",
                            xy=(end_datetime, sell_price),
                            xytext=(end_datetime, sell_price * (0.95)),
                            textcoords="data",
                            ha="center",
                            va="top",
                            arrowprops=dict(arrowstyle="->"),
                        )

                    title = f"{coin} -- {start_timestamp} -- Event_key: {event_key} -- Initial Price: {initial_price} - Max: {max_change}% - Min: {min_change}% - Last_ts: {LAST_TS}"
                    print(title)
                    ax[0].set_title(title)
                    y0_min, y0_max = ax[0].get_ylim()

                    if len(bid_order_distribution_colour_list) > 0:
                        # create legend
                        for color, price_range_color in zip(
                            order_colors, price_range_colors
                        ):
                            label = "> " + str(price_range_color[0] * 100) + "%"
                            ax[1].plot([], [], color=color, label=label)

                        ax[1].legend(loc="upper right")

                        for lvl in np.arange(
                            price_change_jump,
                            limit + price_change_jump,
                            price_change_jump * 2,
                        ):
                            ax[1].annotate(
                                f"-{round_(lvl*100,1)}%",
                                xy=(start_datetime, initial_price * (1 - lvl)),
                                xytext=(
                                    start_datetime - timedelta(hours=1),
                                    initial_price * (1 - lvl),
                                ),
                                textcoords="data",
                                ha="center",
                                va="top",
                                arrowprops=dict(arrowstyle="->"),
                            )
                            ax[1].annotate(
                                f"+{round_(lvl*100,1)}%",
                                xy=(start_datetime, initial_price * (1 + lvl)),
                                xytext=(
                                    start_datetime - timedelta(hours=1),
                                    initial_price * (1 + lvl),
                                ),
                                textcoords="data",
                                ha="center",
                                va="top",
                                arrowprops=dict(arrowstyle="->"),
                            )
                        bid_linecoll = matcoll.LineCollection(
                            bid_order_distribution_colour_list,
                            colors=bid_color_list,
                            zorder=0,
                        )
                        ask_linecoll = matcoll.LineCollection(
                            ask_order_distribution_colour_list,
                            colors=ask_color_list,
                            zorder=0,
                        )
                        ax[1].add_collection(bid_linecoll)
                        ax[1].add_collection(ask_linecoll)
                        ax[1].plot(list_datetime, price_list, linewidth=1.5, color="grey")
                        ax[1].axvline(x=start_datetime, color="blue", linestyle="--")
                        ax[1].axvline(x=end_datetime, color="blue", linestyle="--")
                        ax[1].set_ylabel("Price")
                        ax[1].grid(True, zorder=2)
                        ax[1].set_ylim(y0_min, y0_max)

                    ax[2].plot(
                        list_datetime,
                        bid_ask_volume_list,
                        color="red",
                        linewidth=0.5,
                        alpha=0.8,
                    )
                    ax[2].fill_between(
                        list_datetime,
                        bid_ask_volume_list,
                        bid_volume_list,
                        alpha=0.3,
                        color="red",
                        label="Area Between",
                    )
                    ax[2].plot(
                        list_datetime,
                        bid_volume_list,
                        color="green",
                        linewidth=0.5,
                        alpha=0.8,
                    )
                    ax[2].fill_between(
                        list_datetime,
                        bid_volume_list,
                        0,
                        alpha=0.3,
                        color="green",
                        label="Area Under 2",
                    )  # Fill to zero
                    ax[2].axvline(x=start_datetime, color="blue", linestyle="--")
                    ax[2].axvline(x=end_datetime, color="blue", linestyle="--")
                    ax[2].set_ylabel(f"Bid-Ask Abs Vol ({limit*100}%")
                    ax[2].grid(True)

                    # consider the cumulative price different from zero
                    list_mean_bid_volume_wrt_total = []
                    list_mean_ask_volume_wrt_total = []
                    for dt, bid, ask in zip(
                        list_datetime, bid_volume_wrt_total, ask_volume_wrt_total
                    ):
                        if bid != 0:
                            ax[3].plot(dt, bid, "go", markersize=1)
                            list_mean_bid_volume_wrt_total.append(bid)
                        if ask != 0:
                            ax[3].plot(dt, ask, "ro", markersize=1)
                            list_mean_ask_volume_wrt_total.append(ask)

                    mean_bid_volume_wrt_total = round_(
                        np.mean(list_mean_bid_volume_wrt_total) * 100, 2
                    )
                    mean_ask_volume_wrt_total = round_(
                        np.mean(list_mean_ask_volume_wrt_total) * 100, 2
                    )

                    ax[3].axvline(x=start_datetime, color="blue", linestyle="--")
                    ax[3].axvline(x=end_datetime, color="blue", linestyle="--")
                    ax[3].set_ylabel(f"Bid-Ask Rel {limit*100}%")
                    ax[3].grid(True)
                    ax[3].annotate(
                        f"avg {mean_bid_volume_wrt_total}%",
                        xy=(end_datetime, mean_bid_volume_wrt_total / 100),
                        xytext=(
                            end_datetime + timedelta(hours=2),
                            mean_bid_volume_wrt_total / 100,
                        ),
                        textcoords="data",
                        ha="center",
                        va="top",
                        arrowprops=dict(arrowstyle="->"),
                    )
                    ax[3].annotate(
                        f"avg {mean_ask_volume_wrt_total}%",
                        xy=(end_datetime, mean_ask_volume_wrt_total / 100),
                        xytext=(
                            end_datetime + timedelta(hours=2),
                            mean_ask_volume_wrt_total / 100,
                        ),
                        textcoords="data",
                        ha="center",
                        va="top",
                        arrowprops=dict(arrowstyle="->"),
                    )

                    ax[4].plot(list_datetime, vol_list, linewidth=0.5)
                    ax[4].axvline(
                        x=datetime.fromisoformat(start_timestamp),
                        color="blue",
                        linestyle="--",
                    )
                    ax[4].axvline(x=end_datetime, color="blue", linestyle="--")
                    ax[4].set_ylabel("Volume")
                    ax[4].grid(True)

                    if not plot_exists:
                        plt.savefig(path_plot)
                    # plt.show()
                    plt.close()
    #print(f"tmp_strategy_result: {tmp_strategy_result}")
    if tmp_strategy_result == {}:
        print(f'Event Key: {event_key} - No NEW Data Available')
    return tmp_strategy_result


def print_event_key(event_key_i, n_event_keys, event_key):
    print("")
    print("")
    print("")
    print("#####################################################################")
    print(f"{event_key_i}/{n_event_keys} Event Key: {event_key}")
    print("#####################################################################")

def get_metadata_order_book():
    
    # Make request to algocrypto server to get orderbook metadata

    url = f"{ALGOCRYPTO_SERVER}/analysis/get-order-book-metadata"
    #url = f"http://localhost/analysis/get-order-book-metadata"
    max_start_timestamp = datetime(2025,5,1)
    # with open(f"{ROOT_PATH}/order_book/metadata.json", "r") as file:
    #     metadata_order_book = json.load(file)
    #     for event_key in metadata_order_book:
    #         for coin in metadata_order_book[event_key]:
    #             for timestamp in metadata_order_book[event_key][coin]:
    #                 if max_start_timestamp < datetime.fromisoformat(timestamp):
    #                     max_start_timestamp = datetime.fromisoformat(timestamp)

    request = {}
    request["start_timestamp"] = max_start_timestamp.isoformat()

    try:
        response = requests.post(url, json=request)
        metadata_order_book = response.json()

        
        if response.status_code == 200:
            #print(metadata_order_book)
            max_start_timestamp = max([datetime.fromisoformat(ts['_id']) for ts in metadata_order_book['data']])
            print(f"Orderbook with the latest timestamp: {max_start_timestamp}")
            min_datetime_available_timeseries = max_start_timestamp + timedelta(days=1)
            print(f"Minimum datetime available timeseries: {min_datetime_available_timeseries}")
            return metadata_order_book, min_datetime_available_timeseries
        else:
            print(f"Error getting orderbook metadata: {response.status_code}")
            print(response.json())
            sys.exit()

    except Exception as e:
        print(f"Exception getting orderbook metadata: {e}")
        print(f'Request: {request}')
        print(f"Response from {url}: {response.text}")
        sys.exit()

def update_request_order_book(metadata_order_book, request_order_book, event_key):
    for obj in metadata_order_book['data']:
        if obj['event_key'] != event_key:
            continue
        if obj['event_key'] not in request_order_book:
            request_order_book[obj['event_key']] = {}
        if obj['coin'] not in request_order_book[obj['event_key']]:
            request_order_book[obj['event_key']][obj['coin']] = []
        if obj['_id'] not in request_order_book[obj['event_key']][obj['coin']]:
            request_order_book[obj['event_key']][obj['coin']].append(obj['_id'])
    return request_order_book

def check_tracker_data_available(min_datetime_available_timeseries):
    url = f"http://localhost/analysis/get-last-timestamp-tracker"
    #url = f"http://localhost/analysis/get-last-timestamp-tracker"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            response = json.loads(response.text)
            last_timestamp = datetime.fromisoformat(response['data'])
            if last_timestamp < min_datetime_available_timeseries:
                print(f"Tracker data is not available")
                sys.exit()
            else:
                print(f"Tracker data is available)")
        else:
            print(f"Error getting tracker data: {response.status_code}")
            print(response.json())
            sys.exit()
    except Exception as e:
        print(f"backend container is not running: {e}")
        sys.exit()

def get_timeseries(
    info,
    check_past=1440,
    check_future=1440,
    info_strategy={},
    save_plot=False,
    analyze=True,
    event_keys_filter=[],
    n_processes=8,
    skip_if_strategy_exists=False,
    analysis_name=None,
    load_from_orderbook_metadata=True,
    sleep_after_download=True,
    retry_analysis=False,
    start_analysis=datetime(2025,1,1)
):
    """
    check_past: minutes before event trigger
    check_future: minutes after the end of event (usually after 1 days from event trigger)
    jump: jump from price levels in terms of cumulative volume order (from 0 to 1)
    limit: get the window of price change (from 0 to 1) (e.g. 0.15 check only the orders whose level is within 15% price change from current price)
    """
    jump = info_strategy["strategy_jump"]
    limit = info_strategy["limit"]
    price_change_jump = info_strategy["price_change_jump"]
    max_limit = info_strategy["max_limit"]
    price_drop_limit = info_strategy["price_drop_limit"]
    buy_duration = info_strategy["buy_duration"]
    stop_loss_limit = info_strategy["stop_loss_limit"]
    distance_jump_to_current_price = info_strategy["distance_jump_to_current_price"]
    max_ask_order_distribution_level = info_strategy["max_ask_od_level"]
    last_i_ask_order_distribution = info_strategy["last_i_ask_od"]
    min_n_obs_jump_level = info_strategy["min_n_obs_jump_level"]
    n_orderbook_levels = info_strategy["lvl"]

    name_strategy = f"strategy_jump={jump}_limit={limit}_price_change_jump={price_change_jump}_max_limit={max_limit}_price_drop_limit={price_drop_limit}_buy_duration={buy_duration}_stop_loss_limit={stop_loss_limit}"
    name_strategy += f"_distance_jump_to_current_price={distance_jump_to_current_price}_max_ask_od_level={max_ask_order_distribution_level}_last_i_ask_od={last_i_ask_order_distribution}_min_n_obs_jump_level={min_n_obs_jump_level}_lvl={n_orderbook_levels}"

    n_event_keys = len(info)

    remote_metadata_order_book, min_datetime_available_timeseries  = get_metadata_order_book()
    if load_from_orderbook_metadata:
        check_tracker_data_available(min_datetime_available_timeseries)

    for event_key, event_key_i in zip(info, range(1, n_event_keys + 1)):
        if event_keys_filter != [] and event_key not in event_keys_filter:
            continue
        print_event_key(event_key_i, n_event_keys, event_key)

        request_order_book = {event_key: {}}
        for coin in info[event_key]["info"]:
            request_order_book[event_key][coin] = []
            for event in info[event_key]["info"][coin]:
                request_order_book[event_key][coin].append(event["event"])

        # get full_timeseries
        if load_from_orderbook_metadata:
            # metadata_full_timeseries, full_timeseries = get_full_timeseries(request_order_book, plot)
            request_order_book = update_request_order_book(remote_metadata_order_book, request_order_book, event_key)


        # Download all data (order_book and timeseries)
        #print(request_order_book[event_key]['ONTUSDT'])
        metadata_order_book = get_order_book(request_order_book)
        # get all timeseries (even when orderbook no available)
        get_timeseries_from_server(request_order_book, check_past, check_future, min_datetime_available_timeseries)
        # get fulltimeseries (mix of orderbook and timeseries)
        get_full_timeseries(event_key, metadata_order_book)

        if sleep_after_download:
            print("Sleeping for 5 seconds...")
            sleep(5)
        
    if analyze:
        event_keys = list(info.keys())
        # get data slices for multiprocessing. .e.g divide the data in batches for allowing multiprocessing
        event_keys_lists = event_keys_preparation(event_keys, n_processes=n_processes)

        # initialize Manager for "shared_data". Used for multiprocessing
        manager = Manager()

        # initialize lock for processing shared variable between multiple processes
        lock = Manager().Lock()

        # Create a multiprocessing Pool
        pool = Pool()

        # initialize shared_data
        if analysis_name is None:
            path_strategy = ROOT_PATH + f"/strategy/{name_strategy}"
        else:
            path_strategy = ROOT_PATH + f"/strategy_old/{name_strategy}"

        if os.path.exists(path_strategy) and not skip_if_strategy_exists:
            path_strategy = path_strategy + "/result.json"
            with open(path_strategy, "r") as file:
                result = json.load(file)
        elif os.path.exists(path_strategy) and skip_if_strategy_exists:
            print(f"Strategy {name_strategy} already exists. Skipping...")
            return
        else:
            os.mkdir(path_strategy)
            path_strategy = path_strategy + "/result.json"
            result = {}
        result_json = manager.Value(str, json.dumps(result))

        print("")
        print("START ANALYSIS")
        print("")

        # Execute the function "wrap_analyze_events_multiprocessing" in parallel
        pool.starmap(
            wrap_analyze_timeseries,
            [
                (
                    event_keys,
                    check_past,
                    check_future,
                    jump,
                    limit,
                    price_change_jump,
                    max_limit,
                    price_drop_limit,
                    distance_jump_to_current_price,
                    max_ask_order_distribution_level,
                    last_i_ask_order_distribution,
                    min_n_obs_jump_level,
                    n_orderbook_levels,
                    buy_duration,
                    stop_loss_limit,
                    save_plot,
                    lock,
                    result_json,
                    analysis_name,
                    retry_analysis,
                    start_analysis
                )
                for event_keys in event_keys_lists
            ],
        )

        # Close the pool
        pool.close()
        print("pool closed")
        pool.join()
        print("pool joined")

        result_json = json.loads(result_json.value)
        with open(path_strategy, "w") as f:
            json.dump(result_json, f, indent=4)

    print(f'Strategy {name_strategy} completed')


def wrap_analyze_timeseries(
    event_keys,
    check_past,
    check_future,
    jump,
    limit,
    price_change_jump,
    max_limit,
    price_drop_limit,
    distance_jump_to_current_price,
    max_ask_order_distribution_level,
    last_i_ask_order_distribution,
    min_n_obs_jump_level,
    n_orderbook_levels,
    buy_duration,
    stop_loss_limit,
    save_plot,
    lock,
    result_json,
    analysis_name=None,
    retry_analysis=False,
    start_analysis=datetime(2025,1,1)
):

    tmp = {}
    n_event_keys = len(event_keys)
    for event_key, event_key_i in zip(event_keys, range(1, n_event_keys + 1)):
        print(f"{event_key_i}/{n_event_keys} Event Key: {event_key}")
        result_tmp = analyze_timeseries(
            event_key,
            check_past,
            check_future,
            jump,
            limit,
            price_change_jump,
            max_limit,
            price_drop_limit,
            distance_jump_to_current_price,
            max_ask_order_distribution_level,
            last_i_ask_order_distribution,
            min_n_obs_jump_level,
            n_orderbook_levels,
            buy_duration,
            stop_loss_limit,
            result_json,
            save_plot,
            analysis_name,
            retry_analysis,
            start_analysis
        )
        tmp[event_key] = result_tmp

    with lock:
        result = json.loads(result_json.value)

        for event_key in tmp:
            if event_key not in result:
                result[event_key] = {}

            for coin in tmp[event_key]:
                for start_timestamp in tmp[event_key][coin]:
                    if coin not in result[event_key]:
                        result[event_key][coin] = {}
                    result[event_key][coin][start_timestamp] = tmp[event_key][coin][
                        start_timestamp
                    ]

        result_json.value = json.dumps(result)


def event_keys_preparation(event_keys, n_processes):
    n_event_keys = len(event_keys)
    event_keys_per_iteration = n_event_keys // n_processes
    remainder = n_event_keys % n_processes
    slice_start = 0
    slice_end = slice_start + event_keys_per_iteration
    remainder_event_keys = event_keys[-remainder:]

    event_key_lists = []
    for _ in range(n_processes):
        event_key_list = []
        for event_key in event_keys[slice_start:slice_end]:
            event_key_list.append(event_key)
        event_key_lists.append(event_key_list)
        slice_start = slice_end
        slice_end += event_keys_per_iteration

    if remainder != 0:
        for event_key, index in zip(
            remainder_event_keys, range(len(remainder_event_keys))
        ):
            event_key_lists[index].append(event_key)

    print(event_key_lists)
    return event_key_lists


def get_event_info(observations, timestamp_start, vol_field, buy_vol_field):
    """
    this returns the price, vol_value and buy_value registered at the event triggering
    """
    position = 0
    for obj in observations:
        position += 1
        if obj.get("_id") == timestamp_start:
            return obj["price"], obj[vol_field], obj[buy_vol_field]

    print('something went wrong "get_position_from_list_of_objects"')
    return None


def riskmanagement_data_preparation(
    data,
    n_processes,
    delete_X_perc=False,
    key=None,
    analysis_json_path="/analysis_json_v3/",
):
    # CONSTANTS
    data_arguments = []
    coins_list = list(data.keys())
    n_events = sum([1 for coin in data for _ in data[coin]])
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

    events_discarded = 0
    if delete_X_perc:
        new_data_arguments = []
        file_path = ROOT_PATH + analysis_json_path + "analysis.json"
        with open(file_path, "r") as file:
            print(f"Downloading {file_path}")
            # Retrieve shared memory for JSON data and "start_interval"
            analysis_json = json.load(file)
            analysis_json = analysis_json["data"]

        max_list = []  # list of max value for each event

        for coin in analysis_json[key]["info"]:
            for event in analysis_json[key]["info"][coin]:
                max_list.append(event["max"])

        perc99_max = np.percentile(max_list, delete_X_perc)
        events_99 = []

        # get events in 99 perc
        for coin in analysis_json[key]["info"]:
            for event in analysis_json[key]["info"][coin]:
                event["coin"] = coin
                if event["max"] >= perc99_max:
                    events_99.append((coin, event["event"]))

        for index in range(len(data_arguments)):
            data_i = {}
            for coin in data_arguments[index]:
                for start_timestamp in data_arguments[index][coin]:
                    if (coin, start_timestamp) not in events_99:
                        if coin not in data_i:
                            data_i[coin] = {}
                        data_i[coin][start_timestamp] = data[coin][start_timestamp]
                    else:
                        print(f"{coin} - {start_timestamp} outlier discarded")
                        events_discarded += 1
            new_data_arguments.append(data_i)

        data_arguments = new_data_arguments

    print(
        f"{events_discarded} events have been discarded for {delete_X_perc} percentile"
    )
    n_events = n_events - events_discarded
    n_events_divided = sum(
        [
            1
            for data_i in range(len(data_arguments))
            for coin in data_arguments[data_i]
            for _ in data_arguments[data_i][coin]
        ]
    )

    if n_events != n_events_divided:
        print(
            f"WARNING: Mismatch in data_preparation: expected: {n_events}, current: {n_events_divided}"
        )

    return data_arguments


def get_volume_standings_file(path_volume_standings, benchmark_json=None):
    """
    This function delivers the standings of volumes for each coin for each day
    { XRPUSDT --> { "2024-09-26" : 1 } ... } , { "2024-09-27" : 1 } }
    """
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    if os.path.exists(path_volume_standings):
        print(f"volume standings is up to date")
    else:
        print(f"{path_volume_standings} does not exist")
        if benchmark_json == None:
            path_benchmark_new = f"/Users/albertorainieri/Personal/analysis/benchmark_json/benchmark-2024-12-30.json"
            if os.path.exists(path_benchmark_new):
                with open(path_benchmark_new, "r") as file:
                    # Retrieve shared memory for JSON data and "start_interval"
                    benchmark_json = json.load(file)

        list_dates = list(benchmark_json["XRPUSDT"]["volume_series"].keys())

        # orig_list.sort(key=lambda x: x.count, reverse=True)
        summary = {}
        for coin in benchmark_json:
            for date in list_dates:
                if date not in summary:
                    summary[date] = []
                if date not in benchmark_json[coin]["volume_series"]:
                    continue
                total_volume_30_days = [benchmark_json[coin]["volume_series"][date][0]]
                current_datetime = datetime.strptime(date, "%Y-%m-%d")
                for i in range(1, 31):
                    datetime_past = current_datetime - timedelta(days=i)
                    previous_date = datetime_past.strftime("%Y-%m-%d")
                    if previous_date in benchmark_json[coin]["volume_series"]:
                        total_volume_30_days.append(
                            benchmark_json[coin]["volume_series"][previous_date][0]
                        )

                summary[date].append(
                    {
                        "coin": coin,
                        "volume_avg": round_(np.mean(total_volume_30_days), 2),
                    }
                )

        standings = {}

        # workoaround 2025-05-15


        # print(summary)
        for date in summary:
            if date not in standings:
                standings[date] = []

            list_volumes = summary[date]
            standings[date] = sorted(
                list_volumes, key=itemgetter("volume_avg"), reverse=True
            )

        final_standings = {}
        for date in standings:
            final_standings[date] = {}
            position = 1
            for obj in standings[date]:
                coin = obj["coin"]
                final_standings[date][coin] = position
                position += 1

        # workoaround 2025-05-15
        final_standings["2025-05-15"] = final_standings["2025-05-14"]


        with open(path_volume_standings, "w") as f:
            json.dump(final_standings, f, indent=4)

    return final_standings


def load_volume_standings():
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    path_volume_standings = (
        ROOT_PATH + f"/benchmark_json/volume_standings_{year}-{month}-{day}.json"
    )
    if os.path.exists(path_volume_standings):
        print(f"svolume standings is up to date, loading then..")
        with open(path_volume_standings, "r") as file:
            volume_standings = json.load(file)
    else:
        benchmark_json, df_benchmark, volatility = get_benchmark_info()
        volume_standings = get_volume_standings_file(
            path_volume_standings, benchmark_json
        )

    return volume_standings


def get_volatility_binance(start_dt=datetime(2025, 1, 15)):
    benchmark_json, df_benchmark, volatility = get_benchmark_info()
    volume_standings = load_volume_standings()
    summary = {}
    for coin in benchmark_json:
        if coin in ["BTCUSDT", "ETHUSDT"]:
            continue
        for date in benchmark_json[coin]["volume_series"]:
            dt = datetime.strptime(date, "%Y-%m-%d")
            if dt > start_dt:
                volume_coin = benchmark_json[coin]["volume_series"][date][0]
                # workaround bug benchmark_json
                if date == '2025-05-21':
                    date = '2025-05-20'
                position = volume_standings[date][coin]
                if volume_coin == 0 and position > 300:
                    continue
                if date not in summary:
                    summary[date] = 0
                summary[date] += volume_coin

    list_dts = list(summary.keys())
    list_volumes = [round_(summary[i], 3) for i in summary]
    return list_dts, list_volumes


def get_currency_coin():
    return ["GBUSDT", "FDUSDUSDT", "EURUSDT"]


def frequency_events_analysis(complete_info):

    summary = {}
    for event_key in complete_info:
        for coin in complete_info[event_key]["info"]:
            for event in complete_info[event_key]["info"][coin]:
                year_month_datetime = datetime.fromisoformat(event["event"])
                year_month = (
                    f"{year_month_datetime.year}-{year_month_datetime.month:02d}"
                )
                year_month = year_month[2:]
                if year_month not in summary:
                    summary[year_month] = 1
                else:
                    summary[year_month] += 1

    cols = 2
    rows = int(len(summary) / cols + 1)
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(12, 6))
    # y = i % cols
    # x = i // cols
    sorted_data = dict(sorted(summary.items()))
    months = list(sorted_data.keys())
    values = list(sorted_data.values())
    # plt.figure(figsize=(10, 6))
    axes.bar(months, values, color="skyblue")
    axes.set_xticks(months[::2])
    axes.set_title(event_key)
    # Adjust spacing between subplots
    plt.tight_layout()
    # Show the plot
    plt.show()


def filter_xth_percentile(obj, filter_field, xth_percentile):
    """
    Filters keys in an object based on the 99th percentile of their 'val1' values.

    Args:
        obj: An object (e.g., dictionary) where values are lists of
            tuples (or similar) with at least one element named 'val1'.

    Returns:
        A new object containing only the keys whose 'val1' values
        are below the 99th percentile of all 'val1' values across all keys.
    """

    filter_mapping = {"mean": 0, "std": 1, "max": 2, "min": 3}
    position_to_filter = filter_mapping[filter_field]

    # Extract 'val1' values from all keys

    all_valx = [values[position_to_filter] for key, values in obj.items()]
    events_discarded = []

    # Calculate the 99th percentile of 'val1' values
    if len(all_valx) > 0:
        val1_threshold = np.percentile(all_valx, xth_percentile)

        # Filter keys based on the threshold
        filtered_obj = {}
        for key, values in obj.items():
            if values[0] <= val1_threshold:
                filtered_obj[key] = values
            else:
                events_discarded.append(key)

        return filtered_obj, events_discarded
    else:
        return {}, None


def create_strategy_configuration(strategy_configuration_parameters):
    output, complete_info = get_analysis()
    event_keys = list(complete_info.keys())
    strategy_configuration = {
        "event_keys": event_keys,
        "parameters": strategy_configuration_parameters,
    }

    strategy_path = (
        "/Users/albertorainieri/Personal/backend/riskmanagement/riskmanagement.json"
    )
    with open(strategy_path, "w") as file:
        json.dump(strategy_configuration, file, indent=4)


def filter_complete_info_by_current_eventkeys(output, complete_info):
    """
    This function filters complete_info with the event_keys that are used in production
    """
    path_riskmanagement = "/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json_production/riskmanagement.json"
    new_complete_info = {}
    new_output = {}
    with open(path_riskmanagement, "r") as f:
        riskmanagement = json.load(f)
        riskmanagement_event_keys = list(riskmanagement.keys())
    filtered_event_keys = 0
    for event_key in complete_info:
        if event_key in riskmanagement_event_keys:
            new_complete_info[event_key] = complete_info[event_key]
            new_output[event_key] = output[event_key]
        else:
            filtered_event_keys += 1
    print(f"Filtered {filtered_event_keys} event_keys")

    return new_output, new_complete_info


def get_price_levels(
    price,
    orders,
    cumulative_volume_jump=0.04,
    price_change_limit=0.25,
    price_change_jump=0.025,
    delta=0.005,
    LOG=False
):
    """
    this function outputs the pricelevels (tuple: 4 elements), order_distribution, cumulative_level
    price_levels (TUPLE):
        [1] : absolute price_level
        [2] : relative price_level (percentage from current price)
        [3] : cumulative_level (from 0 to 100, which percentage of bid/ask total volume this level corresponds to) (NOT USED)
        [4] : this is the JUMP. distance from previous level (from 0 to 100, gt > "$jump"). Only if greater than "cumulative_volume_jump"
    order_distribution (OBJ): (KEYS: price_change; VALUES: density of this price level
      (i.e. 5% of the bid/ask volume is concentrated in the first 2.5% price range, considering ONLY the "price_change_limit" (e.g. 40% price range from current price) ))
        {0.025: 0.05,
         0.05:  0.02,
         ...
         }
    previous_cumulative_level: it is the percentage of the bid/ask volume considering only the "price_change_limit" (e.g. 40% price range from current price)
                                with respect to the total bid/ask volume
    """
    #print(f'jump: {cumulative_volume_jump}')
    previous_level = 0
    price_levels = []
    n_decimals_price = count_decimals(price)
    n_decimals_orderbook = count_decimals(delta) - 1
    cumulative_level_without_jump = 0
    price_change_level = price_change_jump
    order_distribution = {}
    for i in np.arange(
        price_change_jump, price_change_limit + price_change_jump, price_change_jump
    ):
        order_distribution[str(round_(i, 3))] = 0
    previous_cumulative_level = 0
    # print(f'orders: {orders}')

    for level in orders:
        cumulative_level = level[1]
        price_change = level[0]
        if LOG:
            print(f'cumulative_level: {cumulative_level} - price_change: {price_change}')
        
        # print(f'cumulative_level: {cumulative_level}')
        # print(f'price_change: {price_change} vs {price_change_level}')

        # in this "if condition", I see how orders are distributed along the (0,price_change_limit) range,
        #  the chunks of orders are divided relative to "price_change_jump" (i.e. if var=0.025, I check in the [0,2.5%] price change window how deep is the order book and so on)
        # if price_change is below next threshold, keep updating the cumulative volume level for that price level

        if abs(price_change) <= price_change_level:
            order_distribution[str(price_change_level)] = cumulative_level
            # print(f'order_distribution if: {order_distribution}')

        # in case is above next threshold, update price_change_level and initialize next price_change_level
        else:
            # before moving to the next price level, update the relative level
            order_distribution[str(price_change_level)] = round_( order_distribution[str(price_change_level)] - previous_cumulative_level, n_decimals_orderbook )
            if LOG:
                print(f'order_distribution else: {order_distribution}')

            # now update the next price change level
            previous_cumulative_level += order_distribution[str(price_change_level)]
            price_change_level = round_(price_change_level + price_change_jump, 3)

            # in case some price level is empty, skip to the next level
            while abs(price_change) > price_change_level:
                price_change_level = round_(price_change_level + price_change_jump, 3)

            # next chunk is below next thrshold
            if ( abs(price_change) <= price_change_level and abs(price_change) <= price_change_limit):
                order_distribution[str(price_change_level)] = cumulative_level
            if LOG:
                print(f'order_distribution next chunk: {order_distribution}')

        if level == orders[-1]:
            if price_change_level in [float(i) for i in list(order_distribution.keys())]:
                order_distribution[str(price_change_level)] = round_( order_distribution[str(price_change_level)] - previous_cumulative_level, n_decimals_orderbook )
                previous_cumulative_level = cumulative_level
                if LOG:
                    print(f'order_distribution last chunk: {order_distribution}')
            elif LOG:
                print(f'price_change_level: {price_change_level} not in order_distribution: {list(order_distribution.keys())}')   
        
        # here, I discover the jumps, the info is stored in "price_levels"
        if ( cumulative_level - previous_level >= cumulative_volume_jump
            and abs(price_change) <= price_change_limit and abs(price_change) >= delta ):

            actual_jump = round_(cumulative_level - previous_level, 3)
            price_level = price * (1 + price_change)
            info = (
                round_(price_level, n_decimals_price),
                price_change,
                cumulative_level,
                actual_jump,
            )
            # print(info, cumulative_level, price_change)
            price_levels.append(info)
        elif abs(price_change) <= price_change_limit:
            cumulative_level_without_jump = cumulative_level

        if abs(price_change) > price_change_limit:
            break
        previous_level = cumulative_level

    # scale order_distribution to [0,100] range
    # print(f'order_distribution: {order_distribution}')
    # print(f'previous_cumulative_level: {previous_cumulative_level}')
    for lvl in order_distribution:
        if previous_cumulative_level != 0:
            order_distribution[lvl] = round_(
                order_distribution[lvl] / previous_cumulative_level, 3
            )
        else:
            order_distribution[lvl] = 0
    # print(f'order_distribution: {order_distribution}')
    sum_cumulative_level = sum(order_distribution.values())
    if orders != [] and (sum_cumulative_level < 0.99 or sum_cumulative_level > 1.01):
        print(f'computation order_distribution error')
        print(f'orders: {orders}')
        print(f'order_distribution: {order_distribution}')
        print(f'sum(order_distribution.values()): {sum(order_distribution.values())}')
        sys.exit()
    # else:
    #     print(f'sum(order_distribution.values()): {sum(order_distribution.values())}')
    # if there are not jumps, at least I want to get the cumulative volume at the limit price level
    if len(price_levels) == 0:
        info = (None, None, cumulative_level_without_jump, False)
        price_levels.append(info)

    if cumulative_level_without_jump == 0 and len(orders) != 0:
        print('analyze this line: cumulative_level_without_jump == 0')
        #print(orders)

    return price_levels, order_distribution, round_(previous_cumulative_level, 3)


def count_decimals(num):
    """
    Determines the number of decimal places in a given number.

    Args:
      num: The number to check.

    Returns:
      The number of decimal places, or 0 if the number is an integer.
    """
    try:
        str_num = str(num)
        decimal_index = str_num.index(".")
        return len(str_num) - decimal_index
    except ValueError:
        # If no decimal point is found, it's an integer
        return 1


def get_analysis(analysis_name=None):
    import sys

    sys.path.insert(0, "..")
    from Functions import download_show_output
    from Helpers import filter_complete_info_by_current_eventkeys
    import pandas as pd
    from datetime import datetime

    pd.set_option("display.max_rows", None)

    minimum_event_number = 1
    minimum_event_number_list = [minimum_event_number]
    mean_threshold = -10
    frequency_threshold = 0
    std_multiplier = 10
    early_validation = False
    # file_paths = ["/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json/analysis-buy-50-150-450.json",
    #              "/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json/analysis-sell-50-150-450.json",
    #              "/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json/analysis-buy-sell-10-250-highfrequency.json"]

    if analysis_name is None:
        file_paths = [
            "/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json_production/analysis.json"
        ]
        start_analysis = datetime(2025, 4, 28, 9)
    else:
        file_paths = [
            f"/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json/{analysis_name}.json"
        ]
        start_analysis = datetime(2025, 1, 12, 9)


    early_validation = datetime(2028, 1, 1)
    xth_percentile = 100
    filter_field = "mean"  # mean, std, max, min
    output, complete_info, daily_events = download_show_output(
        minimum_event_number=minimum_event_number,
        mean_threshold=mean_threshold,
        frequency_threshold=frequency_threshold,
        early_validation=early_validation,
        std_multiplier=std_multiplier,
        file_paths=file_paths,
        start_analysis=start_analysis,
        DELETE_99_PERCENTILE=True,
        filter_field=filter_field,
        xth_percentile=xth_percentile,
    )

    if analysis_name is None:
        output, complete_info = filter_complete_info_by_current_eventkeys(
            output, complete_info
        )

    df = pd.DataFrame(output).transpose()
    n_event_keys = len(df["mean"])
    print(f"Number of event_keys: {n_event_keys}")
    daily_frequency_all_events = int(sum(df["frequency/month"]) / 30)
    print(f"Daily frequency of events: {daily_frequency_all_events}")

    return output, complete_info


def get_plots(info_strategy, min_gain, max_gain, buy_events=True):
    from IPython.display import Image, display

    strategy = ""
    print("")
    print("##################### STRATEGY INFO #####################")
    for parameter_key, value in info_strategy.items():
        print(f"{parameter_key}: {value} ")
        strategy += f"{parameter_key}={value}_"
    strategy = strategy[:-1]
    print("##################### STRATEGY INFO #####################")
    print("")

    path = f"/Users/albertorainieri/Personal/analysis/Analysis2024/strategy/{strategy}/result.json"
    n = 0
    avg_gain_list = []
    paths_png = []
    gain_png_paths = []

    with open(path, "r") as file:
        result = json.load(file)
    for event_key in result:
        for coin in result[event_key]:
            for start_timestamp in result[event_key][coin]:
                gain = result[event_key][coin][start_timestamp]["gain"]
                if (
                    buy_events
                    and gain != None
                    and gain / 100 <= max_gain
                    and gain / 100 > min_gain
                ):
                    buy_price = result[event_key][coin][start_timestamp]["buy_price"]
                    sell_price = result[event_key][coin][start_timestamp]["sell_price"]
                    datetime_sell = result[event_key][coin][start_timestamp][
                        "datetime_sell"
                    ]
                    datetime_buy = result[event_key][coin][start_timestamp][
                        "datetime_buy"
                    ]
                    ask_order_distribution = result[event_key][coin][start_timestamp][
                        "ask_order_distribution"
                    ]
                    max_change = (
                        result[event_key][coin][start_timestamp]["max_change"] / 100
                    )
                    initial_price = result[event_key][coin][start_timestamp][
                        "initial_price"
                    ]
                    max_price = initial_price * (1 + max_change)
                    drop_from_max = round_(
                        ((buy_price - max_price) / max_price) * 100, 2
                    )

                    print(coin, start_timestamp, gain, event_key)
                    print(
                        f"Initial Price: {initial_price} - drop_from_max: {drop_from_max}%"
                    )
                    print(
                        f"buy: {datetime_buy} - {buy_price}. sell: {datetime_sell} - {sell_price}"
                    )
                    print(ask_order_distribution)
                    path_png = result[event_key][coin][start_timestamp]["path_png"]
                    display(Image(filename=path_png))
                    n += 1
                elif not buy_events and gain == None:
                    n += 1
                    max_change = (
                        result[event_key][coin][start_timestamp]["max_change"] / 100
                    )
                    min_change = (
                        result[event_key][coin][start_timestamp]["min_change"] / 100
                    )
                    initial_price = result[event_key][coin][start_timestamp][
                        "initial_price"
                    ]
                    final_price = result[event_key][coin][start_timestamp][
                        "final_price"
                    ]
                    max_price = initial_price * (1 + max_change)
                    min_price = initial_price * (1 - min_change)
                    avg_price = np.mean([max_price, min_price, final_price])
                    avg_gain = (avg_price - initial_price) / initial_price
                    avg_gain_list.append(avg_gain)
                    print(
                        f"Initial Price: {initial_price} - Final Price: {final_price}"
                    )
                    print(f"Max Price: {max_price} - Min Price: {min_price}")
                    path_png = result[event_key][coin][start_timestamp]["path_png"]
                    paths_png.append(path_png)
                    gain_png_paths.append((avg_gain, path_png))
                    # display(Image(filename=path_png))
    if buy_events:
        print(f"{n} events whose gain is between {min_gain*100}% and {max_gain*100}%")
    else:
        gain_png_paths = sorted(gain_png_paths, key=lambda item: item[0], reverse=True)
        print(gain_png_paths)
        avg_gain = round_(np.mean(avg_gain_list), 3) * 100
        print(f"{n} events not traded. Average Gain: {avg_gain}%")
        for gain_png in gain_png_paths[-10:]:
            display(Image(filename=gain_png[1]))


def get_top_crypto(initial_investment=1000):
    crypto_timeseries = get_crypto_performance()
    top_crypto = {"top_10": {}, "top_50": {}, "top_150": {}, "top_250": {}}
    top_crypto_keys = list(top_crypto.keys())
    threshold_list = [int(i.split("_")[1]) for i in list(top_crypto.keys())]

    # topx timeseries
    for coin in crypto_timeseries:
        dates = list(crypto_timeseries[coin].keys())
        if len(dates) > 0:
            first_date = dates[0]
        for date in dates:
            for top_crypto_key in top_crypto_keys:
                if date not in top_crypto[top_crypto_key] and date != first_date:
                    top_crypto[top_crypto_key][date] = []
            previous_date = (
                datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)
            ).strftime("%Y-%m-%d")
            if previous_date in crypto_timeseries[coin]:

                price_previous_day = crypto_timeseries[coin][previous_date][0]
                price_current_day = crypto_timeseries[coin][date][0]
                gain = (price_current_day - price_previous_day) / price_previous_day
                position = crypto_timeseries[coin][date][2]

                for threshold in threshold_list:
                    if position <= threshold:
                        top_x = "top_" + str(threshold)
                        top_crypto[top_x][date].append(gain)
                        break

    # btc timeseries
    top_crypto["btc"] = {}
    dates = list(crypto_timeseries["XRPUSDT"].keys())
    for date in dates:
        previous_date = ( datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1) ).strftime("%Y-%m-%d")
        if previous_date in crypto_timeseries["XRPUSDT"]:
            price_previous_day = crypto_timeseries["XRPUSDT"][previous_date][0]
            price_current_day = crypto_timeseries["XRPUSDT"][date][0]
            gain = (price_current_day - price_previous_day) / price_previous_day
            top_crypto["btc"][date] = gain

    len_dates_btc = len(top_crypto["btc"])
    
    for top_x in top_crypto_keys:
        print(f"The number of dates for {top_x} is {len(top_crypto[top_x])}")
        len_dates_top_x = len(top_crypto[top_x])
        #assert len_dates_top_x == len_dates_btc, f"The number of dates for {top_x} is not equal to the number of dates for btc: {len_dates_top_x} != {len_dates_btc}"

    for top_x in top_crypto_keys:
        # list_dt = top_crypto[]
        for date in dates:
            if date not in top_crypto[top_x]:
                continue
            if len(top_crypto[top_x][date]) > 0:
                top_crypto[top_x][date] = float(np.mean(top_crypto[top_x][date]))
            else:
                top_crypto[top_x][date] = 0 # workaround bug benchmark_json

    # return top_crypto
    crypto_performance = {}
    for top_x in top_crypto:
        list_dates = list(top_crypto['btc'].keys())
        first_date = list_dates[0]
        if top_x not in crypto_performance:
            crypto_performance[top_x] = [
                initial_investment * (1 + top_crypto[top_x][first_date])
            ]
        for date in list_dates[1:]:
            previous_balance = crypto_performance[top_x][-1]
            crypto_performance[top_x].append(
                previous_balance * (1 + top_crypto[top_x][date])
            )

    list_datetime = [datetime.strptime(date, "%Y-%m-%d") for date in list_dates]
    return crypto_performance, list_datetime


def get_crypto_performance():
    volume_standings_full = load_volume_standings()
    # return volume_standings_full
    url = "http://localhost//analysis/get-crypto-timeseries"
    path_crypto_timeseries = ROOT_PATH + "/crypto_timeseries/crypto.json"
    crypto_timeseries = {}
    now = datetime.now()
    now_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_date = now.strftime("%Y-%m-%d")

    if os.path.exists(path_crypto_timeseries):
        with open(path_crypto_timeseries, "r") as file:
            crypto_timeseries = json.load(file)
            last_date_btc = list(crypto_timeseries["XRPUSDT"].keys())[-1]
            start_datetime_split = last_date_btc.split("-")
            start_datetime = datetime(
                year=int(start_datetime_split[0]),
                month=int(start_datetime_split[1]),
                day=int(start_datetime_split[2]),
            )
            timedelta_ = now - start_datetime
            print(f"Last date saved: {last_date_btc}")
            print(timedelta_)
            if now - start_datetime < timedelta(days=2):
                print("Data is up to date")
                return crypto_timeseries
    else:
        start_datetime = datetime(2025, 1, 12)

    while True:
        print(f"Data Download from {start_datetime.isoformat()} + 7days")
        request = {}
        for date in volume_standings_full:
            datetime_i = datetime.strptime(date, "%Y-%m-%d")
            if datetime_i <= start_datetime:
                continue
            for coin in volume_standings_full[date]:
                if coin in volume_standings_full[date]:
                    position = volume_standings_full[date][coin]
                    if position > 250:
                        continue
                    if coin not in request:
                        request[coin] = [datetime_i.isoformat()]
                    else:
                        request[coin].append(datetime_i.isoformat())

        # print(request)
        for coin in request:
            # print(coin)
            start_ts = request[coin][0]
            if len(request[coin]) == 1:
                end_ts = (start_datetime + timedelta(days=2)).isoformat()
            # elif request[coin][0] == request[coin][1] == now_midnight:
            #     continue
            # elif request[coin][0] == request[coin][1]:
            #     end_ts = (start_datetime + timedelta(days=2)).isoformat()
            else:
                # print(request[coin])
                end_ts = min(
                    datetime.fromisoformat(request[coin][0]) + timedelta(days=7),
                    datetime.fromisoformat(request[coin][-1]) + timedelta(days=2),
                ).isoformat()

            request[coin] = [start_ts, end_ts]

        #print(request)
        response = requests.post(url=url, json=request)
        #print(response.text)
        response = json.loads(response.text)
        all_data_downloaded = response["all_data_downloaded"]
        response = response["data"]
        today_date = datetime.now().strftime("%Y-%m-%d")

        if not os.path.exists(path_crypto_timeseries):
            for coin in response:
                for date in response[coin]:
                    position = volume_standings_full[date][coin]
                    response[coin][date].append(position)
            with open(path_crypto_timeseries, "w") as file:
                json.dump(response, file)
        else:
            for coin in response:
                if coin not in crypto_timeseries:
                    crypto_timeseries[coin] = {}
                for date in response[coin]:
                    if date != today_date:
                        if date == '2025-05-21':
                            date = '2025-05-20'
                        if coin in volume_standings_full[date]:
                            position = volume_standings_full[date][coin]
                            # print(response[coin][date])
                            response[coin][date].append(position)
                            crypto_timeseries[coin][date] = response[coin][date]
            with open(path_crypto_timeseries, "w") as file:
                json.dump(crypto_timeseries, file)

        print(crypto_timeseries)
        start_datetime_split = list(crypto_timeseries["XRPUSDT"].keys())[-1].split("-")
        start_datetime = datetime(
            year=int(start_datetime_split[0]),
            month=int(start_datetime_split[1]),
            day=int(start_datetime_split[2]),
        )

        if all_data_downloaded:
            break

    return crypto_timeseries


def clean_obs(dict_, delete_obs):
    new_dict = []
    for dt in dict_:
        event_key = dt[-2]
        st = dt[-1]
        id_ = event_key + "--" + st
        if id_ not in delete_obs:
            new_dict.append(dt)
    return new_dict


def plot_strategy_result(info_strategy=None, limit_n_transactions=None, minimal=False, strategy=None, analysis_name=None, MINUTES_NO_DUPLICATE=5):
    """
    Analyzes and visualizes the results of a trading strategy.
    
    Parameters:
    - info_strategy: Dictionary containing strategy parameters
    - limit_n_transactions: Optional limit on number of transactions to analyze
    - minimal: If True, returns only basic metrics without plotting
    - strategy: Optional strategy name instead of constructing from info_strategy
    - analysis_name: Optional name for analysis folder ('strategy' or 'strategy_old')
    
    Returns:
    - When minimal=False: Tuple of two DataFrames (events_overview, event_keys_overview)
    - When minimal=True: Dictionary with basic metrics (n_orders, profit, average_profit_per_event, commission)
    """

    # Validate that either info_strategy or strategy name is provided
    assert info_strategy is not None or strategy is not None, "Either info_strategy or path must be provided"
    
    # Determine which folder to use based on analysis_name
    if analysis_name is None:
        folder_strategy = "strategy"
    else:
        folder_strategy = "strategy_old"
        print(f'##########################')
        print(f'ATTENTION: analysis {analysis_name} will be used')
        print(f'##########################')

    # Construct strategy name from parameters or use provided strategy name
    if strategy is None:
        strategy = ""
        # Build strategy string from parameters
        for parameter_key, value in info_strategy.items():
            if parameter_key == "n_orderbook_levels" and value == 1:
                continue
            strategy += f"{parameter_key}={value}_"
        strategy = strategy[:-1]  # Remove the trailing underscore

        path = f"/Users/albertorainieri/Personal/analysis/Analysis2024/{folder_strategy}/{strategy}/result.json"
    else:
        path = ROOT_PATH + f"/{folder_strategy}/{strategy}/result.json"

    # Load strategy results from JSON file
    if os.path.exists(path):
        with open(path, "r") as file:
            result = json.load(file)
    else:
        return None

    # Initialize tracking variables for performance analysis
    events = 0
    initial_investment = 10000  # Starting investment amount
    current_investment = initial_investment  # Current balance with strategy
    current_investment_2 = initial_investment  # Alternative strategy comparison
    total_current_investment = initial_investment  # For all events analysis
    
    # Lists to track various aspects of trading events
    buy_events_list = []  # Events where a buy was triggered by strategy
    total_events_list = []  # All events analyzed
    events_list_wt_orderbook_strategy = []
    dt_buy_sell_list = []  # Timestamps of buys and sells
    dt_total_list = []  # Timestamps of all events
    dt_list = []
    coin_list = []
    gain_list = []  # Gains with strategy
    gain_wt_list = []  # Gains without strategy
    
    # Define gain distribution ranges for histogram analysis
    # Keys are (min_gain, max_gain) tuples, values count events in each range
    gain_distribution = {
        (-1, -0.4): 0,
        (-0.4, -0.3): 0,
        (-0.3, -0.2): 0,
        (-0.2, -0.1): 0,
        (-0.1, -0.05): 0,
        (-0.05, 0): 0,
        (0, 0.05): 0,
        (0.05, 0.1): 0,
        (0.1, 0.2): 0,
        (0.2, 0.3): 0,
        (0.3, 0.4): 0,
        (0.4, 10): 0,
    }
    # Same structure for events not triggered by strategy
    gain_distribution_no_strategy = {
        (-1, -0.4): 0,
        (-0.4, -0.3): 0,
        (-0.3, -0.2): 0,
        (-0.2, -0.1): 0,
        (-0.1, -0.05): 0,
        (-0.05, 0): 0,
        (0, 0.05): 0,
        (0.05, 0.1): 0,
        (0.1, 0.2): 0,
        (0.2, 0.3): 0,
        (0.3, 0.4): 0,
        (0.4, 10): 0,
    }
    
    # Setup for tracking performance by event keys
    event_keys_overview = {}
    n_events_per_event_keys = {}
    df_event_keys_overview = {
        "event_keys": [],
        "n_events": [],
        "gain": [],
        "max": [],
        "min": [],
    }
    
    # Generate monthly columns for tracking performance over time
    start_dt = datetime(2025, 4, 28)
    dt = start_dt
    months_year_list = []

    while dt < datetime.now():
        # Create columns for each month in the analysis period
        month_year = dt.strftime("%Y-%m")
        df_event_keys_overview[month_year] = []
        months_year_list.append(month_year)
        
        # Move to next month
        month = dt.month
        if month != 12:
            dt = dt.replace(month=month + 1)
        else:
            year = dt.year + 1
            dt = dt.replace(month=1).replace(year=year + 1)

    # Process each event from the results
    for event_key in result:
        # Track performance metrics for each event key by month
        gains_per_event_key = {}
        max_per_event_key = {}
        min_per_event_key = {}
        gain_no_strategy_overview = {}

        # Lists to collect overall stats for this event key
        total_gain = []
        total_max = []
        total_min = []
        
        # Process each coin and timestamp for this event key
        for coin in result[event_key]:
            if result[event_key][coin] == {}:
                continue
            for start_timestamp in result[event_key][coin]:
                for single_event in result[event_key][coin][start_timestamp]:
                    if single_event == {}:
                        continue
                    else:
                        events += 1

                        # Determine if this is a buy event (strategy triggered)
                        if single_event["gain"] != None:
                            is_buy_event = True
                        else:
                            is_buy_event = False

                        # Extract price data and calculate performance
                        initial_price = single_event["initial_price"]
                        final_price = single_event["final_price"]
                        max_change = single_event["max_change"]
                        min_change = single_event["min_change"]
                        max_price = initial_price * (1 + (max_change / 100))
                        min_price = initial_price * (1 + (min_change / 100))
                        
                        # Calculate gain without strategy (average of min, max, final prices)
                        gain_no_strategy = (
                            ((min_price + max_price + final_price) / 3) - initial_price
                        ) / initial_price
                        
                        # Track event timestamps
                        datetime_start = datetime.fromisoformat(start_timestamp)
                        datetime_end = datetime.fromisoformat(start_timestamp) + timedelta(days=1)
                        dt_total_list.append((datetime_start, "buy", event_key, start_timestamp))
                        dt_total_list.append((datetime_end, "sell", event_key, start_timestamp))
                        
                        # Add to list of all events
                        total_events_list.append(
                            (
                                datetime_start,
                                max_change,
                                min_change,
                                gain_no_strategy,
                                coin,
                                initial_price,
                                final_price,
                                event_key,
                                start_timestamp,
                            )
                        )

                        # Get month-year for grouping performance by time period
                        month_year = datetime.fromisoformat(start_timestamp).strftime("%Y-%m")

                        # Initialize monthly trackers if this is the first event for this month
                        if month_year not in gains_per_event_key:
                            gains_per_event_key[month_year] = []
                            max_per_event_key[month_year] = []
                            min_per_event_key[month_year] = []
                            gain_no_strategy_overview[month_year] = []
                            
                        # Process buy events with detailed data (strategy triggered)
                        if is_buy_event:
                            gain = single_event["gain"]

                            # Store metrics for this event by month
                            gains_per_event_key[month_year].append(gain)
                            max_per_event_key[month_year].append(max_change)
                            min_per_event_key[month_year].append(min_change)
                            gain_no_strategy_overview[month_year].append(gain_no_strategy * 100)
                            
                            # Add to overall stats for this event key
                            total_gain.append(gain)
                            total_max.append(max_change)
                            total_min.append(min_change)

                            # Extract buy/sell details
                            buy_price = single_event["buy_price"]
                            sell_price = single_event["sell_price"]
                            datetime_buy = datetime.fromisoformat(single_event["datetime_buy"])
                            #print(f'coin: {coin} - datetime_buy: {datetime_buy.isoformat()}')
                            datetime_sell = datetime.fromisoformat(single_event["datetime_sell"])
                            ask_order_distribution = single_event["ask_order_distribution"]
                            
                            # Track buy/sell timestamps
                            dt_buy_sell_list.append((datetime_buy, "buy", event_key, datetime_buy.isoformat()))
                            dt_buy_sell_list.append((datetime_sell, "sell", event_key, datetime_buy.isoformat()))
                            
                            # Store complete event details
                            buy_events_list.append(
                                (
                                    datetime_buy,
                                    datetime_sell,
                                    datetime_start,
                                    max_change,
                                    min_change,
                                    gain,
                                    gain_no_strategy,
                                    coin,
                                    initial_price,
                                    final_price,
                                    buy_price,
                                    sell_price,
                                    event_key,
                                    datetime_buy.isoformat()
                                )
                            )
                            
                            # Categorize gains for histogram
                            for gain_range in gain_distribution:
                                if gain / 100 >= gain_range[0] and gain / 100 < gain_range[1]:
                                    gain_distribution[gain_range] += 1
                                    break
                        else:
                            # Categorize non-strategy gains for histogram (events not triggered)
                            for gain_range in gain_distribution_no_strategy:
                                if gain_no_strategy >= gain_range[0] and gain_no_strategy < gain_range[1]:
                                    gain_distribution_no_strategy[gain_range] += 1
                                    break

        # Record event key statistics for the overview dataframe
        df_event_keys_overview["event_keys"].append(event_key)
        df_event_keys_overview["gain"].append(round_(np.mean(total_gain), 2))
        df_event_keys_overview["max"].append(round_(np.mean(total_max), 2))
        df_event_keys_overview["min"].append(round_(np.mean(total_min), 2))
        df_event_keys_overview["n_events"].append(len(total_gain))

        # Record monthly performance for each event key
        for month_year in months_year_list:
            if month_year not in gains_per_event_key:
                df_event_keys_overview[month_year].append(None)
                continue

            # Format monthly performance metrics
            if len(gains_per_event_key[month_year]) > 0:
                gain_strategy = round_(np.mean(gains_per_event_key[month_year]), 2)
                gain_no_strategy = round_(np.mean(gain_no_strategy_overview[month_year]), 2)
                max_ = round_(np.mean(max_per_event_key[month_year]), 2)
                min_ = round_(np.mean(min_per_event_key[month_year]), 2)
                n_events = len(gains_per_event_key[month_year])
                performance = f"{str(gain_strategy)} vs {str(gain_no_strategy)} / max:{max_} / min:{min_} / n:{n_events}"
            else:
                performance = None

            df_event_keys_overview[month_year].append(performance)

    # Prepare data for gain distribution histogram
    bin_edges = sorted(
        list(
            set(
                [k[0] for k in gain_distribution.keys()]
                + [k[1] for k in gain_distribution.keys()]
            )
        )
    )  # Extract all unique values and sort them
    frequencies = [
        gain_distribution[k] for k in sorted(gain_distribution.keys())
    ]  # Extract frequencies in the same order as bin edges
    bin_edges[-1] = 0.5  # Cap the max bin edge
    bin_edges[0] = -0.5  # Cap the min bin edge
    
    # Same process for non-strategy gain distribution
    bin_edges_no_strategy = sorted(
        list(
            set(
                [k[0] for k in gain_distribution_no_strategy.keys()]
                + [k[1] for k in gain_distribution_no_strategy.keys()]
            )
        )
    )
    frequencies_no_strategy = [
        gain_distribution_no_strategy[k]
        for k in sorted(gain_distribution_no_strategy.keys())
    ]
    bin_edges_no_strategy[-1] = 0.5
    bin_edges_no_strategy[0] = -0.5

    # Remove duplicate events (same coin executed multiple times within 24 hours)
    # Sort events by timestamp for sequential processing
    buy_events_list_ascending_order = sorted(buy_events_list, key=lambda item: item[-1])
    total_events_list_ascending_order = sorted(total_events_list, key=lambda item: item[0])

    # Identify duplicate events (same coin within 24 hours)
    delete_obs = []
    for obs in buy_events_list_ascending_order:
        #print(obs)
        coin = obs[7]
        st = obs[-1]
        dt = datetime.fromisoformat(st)
        event_key = obs[-2]
        id_ = event_key + "--" + st
        if id_ not in delete_obs:
            index = buy_events_list_ascending_order.index(obs)
            # Check subsequent events for duplicates
            for obs_s in buy_events_list_ascending_order[index + 1:]:
                event_key_s = obs_s[-2]
                st_s = obs_s[-1] # buy_timestamp
                id_s = event_key_s + "--" + st_s
                dt_s = datetime.fromisoformat(st_s)
                # If same coin within 24 hours and not already marked for deletion
                MINUTES = MINUTES_NO_DUPLICATE
                if dt_s < dt + timedelta(minutes=MINUTES) and obs_s[7] == coin:
                    if id_s not in delete_obs:
                        print(f'SAME COIN TRADED MORE THAN ONCE WITHIN {MINUTES} MINUTES. {coin} FIRST: {st} SECOND: {st_s}')
                        delete_obs.append(id_s)

    # Report on duplicate removal
    n_delete_obs = len(delete_obs)
    tot_obs = len(buy_events_list_ascending_order)
    if not minimal:
        print(f"Deleting {n_delete_obs} duplicates from initial {tot_obs} events")

    # Record counts before duplicate removal
    n_total_events_list_ascending_order_pre = len(total_events_list_ascending_order)
    n_buy_events_list_ascending_order_pre = len(buy_events_list_ascending_order)
    
    # Remove duplicates from all event lists
    buy_events_list_ascending_order = clean_obs(buy_events_list_ascending_order, delete_obs)
    total_events_list_ascending_order = clean_obs(total_events_list_ascending_order, delete_obs)

    # Sort events by buy datetime
    dt_total_list_ascending_order = sorted(dt_total_list, key=lambda item: item[0])
    dt_buy_sell_list_ascending_order = sorted(dt_buy_sell_list, key=lambda item: item[0])
    buy_events_list_ascending_order = sorted(buy_events_list_ascending_order, key=lambda item: item[0])

    # Record counts of timestamp lists before duplicate removal
    n_dt_buy_sell_list_ascending_order_pre = len(dt_buy_sell_list_ascending_order)
    n_dt_total_list_ascending_order_pre = len(dt_total_list_ascending_order)
    # print(f'dt_total_list_ascending_order_pre:')
    # for obj in dt_total_list_ascending_order:
    #     event_key = obj[-2]
    #     st = obj[-1]
    #     id_ = event_key + "--" + st
    #     print(f'id_: {id_}')

    print(f'delete_obs: {delete_obs}')

    # Remove duplicates from timestamp lists
    dt_buy_sell_list_ascending_order = clean_obs(dt_buy_sell_list_ascending_order, delete_obs)
    #dt_total_list_ascending_order = clean_obs(dt_total_list_ascending_order, delete_obs)
    # print(f'dt_total_list_ascending_order:')
    # for obj in dt_total_list_ascending_order:
    #     event_key = obj[-2]
    #     st = obj[-1]
    #     id_ = event_key + "--" + st
    #     print(f'id_: {id_}')

    # Get counts after duplicate removal
    n_dt_buy_sell_list_ascending_order = len(dt_buy_sell_list_ascending_order)
    n_dt_total_list_ascending_order = len(dt_total_list_ascending_order)
    n_total_events_list_ascending_order = len(total_events_list_ascending_order)
    n_buy_events_list_ascending_order = len(buy_events_list_ascending_order)

    # Verify the correct number of observations were removed
    #print(f'n_dt_total_list_ascending_order_pre: {n_dt_total_list_ascending_order_pre} - n_dt_total_list_ascending_order: {n_dt_total_list_ascending_order} - n_delete_obs: {n_delete_obs}')
    assert ( n_dt_buy_sell_list_ascending_order_pre - n_dt_buy_sell_list_ascending_order == n_delete_obs * 2 ), print(n_dt_buy_sell_list_ascending_order_pre, n_dt_buy_sell_list_ascending_order)
    #assert (n_dt_total_list_ascending_order_pre - n_dt_total_list_ascending_order == n_delete_obs * 2), print(n_dt_total_list_ascending_order_pre, n_dt_total_list_ascending_order)
    #assert (n_total_events_list_ascending_order_pre - n_total_events_list_ascending_order == n_delete_obs), print(n_total_events_list_ascending_order_pre, n_total_events_list_ascending_order)
    assert (n_buy_events_list_ascending_order_pre - n_buy_events_list_ascending_order == n_delete_obs), print(n_buy_events_list_ascending_order_pre, n_buy_events_list_ascending_order)
    
    # Print event counts before and after duplicate removal
    if not minimal:
        print( f"Number Buy Events: pre deletion: {n_buy_events_list_ascending_order_pre}, post deletion: {n_buy_events_list_ascending_order}.")
        print( f"Number Total Events: pre deletion: {n_total_events_list_ascending_order_pre}, post deletion: {n_total_events_list_ascending_order}.")

    # Track concurrent orders and apply transaction limit if specified
    dt_buy_list = []
    dt_total_list = []
    delete_obs = []
    buy_current_number_orders = []  # Tracks how many orders are active at each point in time
    total_current_number_orders = []
    n = 0  # Counter for active orders
    
    # Process buy/sell events chronologically and track concurrent orders
    for dt in dt_buy_sell_list_ascending_order:
        print(f'dt: {dt}')
        event_key = dt[2]
        st = dt[-1]
        id_ = event_key + "--" + st
        # Skip if beyond transaction limit or already marked for deletion
        if limit_n_transactions != None and n > limit_n_transactions and dt[1] == "buy":
            delete_obs.append(id_)
            continue
        elif id_ in delete_obs:
            continue
            
        dt_buy_list.append(dt[0])
        # Increment counter for buys, decrement for sells
        if dt[1] == "buy":
            n += 1
        else:
            n -= 1
        buy_current_number_orders.append(n)

    # Same process for all events
    for dt in dt_total_list_ascending_order:
        event_key = dt[2]
        st = dt[-1]
        id_ = event_key + "--" + st
        if id_ in delete_obs:
            continue
        dt_total_list.append(dt[0])
        if dt[1] == "buy":
            n += 1
        else:
            n -= 1
        total_current_number_orders.append(n)

    # Apply transaction limit if needed by removing excess events
    if len(delete_obs) != 0:
        print(f'delete_obs: {delete_obs}')
        dt_buy_sell_list_ascending_order = clean_obs(dt_buy_sell_list_ascending_order, delete_obs)
        dt_total_list_ascending_order = clean_obs(dt_total_list_ascending_order, delete_obs)
        buy_events_list_ascending_order = clean_obs(buy_events_list_ascending_order, delete_obs)

    # Calculate investment per order based on max concurrent orders
    max_cuncurrent_orders = max(buy_current_number_orders)
    investment_per_order = initial_investment / max_cuncurrent_orders
    
    # Plot concurrent orders chart if not in minimal mode
    if not minimal:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(dt_buy_list, buy_current_number_orders)
        ax.plot(dt_total_list, total_current_number_orders)
        plt.gcf().autofmt_xdate()  # Automatically rotate the date labels
        plt.tight_layout()
        plt.title("Number of Concurrent Orders")
        plt.show()

    # Plot Binance volatility if not in minimal mode
    if not minimal:
        list_dts, list_volumes = get_volatility_binance()
        fig, axes = plt.subplots(figsize=(12, 6))
        axes.bar(list_dts, list_volumes, color="skyblue")
        axes.set_xticks(list_dts[::4])
        plt.gcf().autofmt_xdate()
        plt.tight_layout()
        plt.title("Binance Volatility (Total USD Traded)")
        plt.show()

    # Initialize lists for tracking performance metrics
    dt_total_list = []
    dt_buy_list = []
    ts_buy_list = []
    ts_sell_list = []
    max_list = []
    min_list = []
    initial_price_list = []
    buy_price_list = []
    sell_price_list = []
    profit_event = []
    profit_wt_event = []
    total_gain_list = []
    total_profit_event = []
    event_key_list = []
    start_timestamp_list = []
    
    # Calculate performance for buy events including commissions
    total_commission = 0
    x_commission = 0
    
    # Process each buy event to calculate performance
    print(buy_events_list_ascending_order)
    for event in buy_events_list_ascending_order:
        # Extract event details
        datetime_buy = event[0]
        datetime_sell = event[1]
        datetime_start = event[2]
        max_change = event[3]
        min_change = event[4]
        gain = event[5]  # Percent gain from the strategy
        gain_no_strategy = event[6]  # Gain without strategy
        coin = event[7]
        initial_price = event[8]
        final_price = event[9]
        buy_price = event[10]
        sell_price = event[11]
        event_key = event[12]
        start_timestamp = event[13]
        
        # Calculate trading commission (0.07125% per transaction, buy and sell)
        commission = investment_per_order * 0.0007125 * 2
        total_commission += commission
        
        # Update investment values for different strategies
        current_investment_2 += gain_no_strategy * investment_per_order - commission
        dt_buy_list.append(datetime_buy)
        current_investment += (gain / 100) * investment_per_order - commission
        
        # Track performance metrics
        gain_list.append(current_investment)
        profit_event.append(gain)
        ts_buy_list.append(datetime_buy.isoformat())
        ts_sell_list.append(datetime_sell.isoformat())
        buy_price_list.append(buy_price)
        sell_price_list.append(sell_price)
        profit_wt_event.append(gain_no_strategy * 100)
        gain_wt_list.append(current_investment_2)
        coin_list.append(coin)
        max_list.append(max_change)
        min_list.append(min_change)
        initial_price_list.append(initial_price)
        event_key_list.append(event_key)
        start_timestamp_list.append(start_timestamp)
        
    # Calculate performance for all events (strategy and non-strategy)
    for event in total_events_list_ascending_order:
        datetime_start = event[0]
        max_change = event[1]
        min_change = event[2]
        gain = event[3]  # This is gain_no_strategy
        coin = event[4]
        initial_price = event[5]
        final_price = event[6]
        
        dt_total_list.append(datetime_start)
        commission = investment_per_order * 0.0007125 * 2
        x_commission += commission
        total_current_investment += (gain) * investment_per_order - commission
        total_gain_list.append(total_current_investment)
        total_profit_event.append(gain)

    # Plot performance comparison with top cryptocurrencies if not in minimal mode
    # Note: This section is commented out in the original code

    # Calculate final performance metrics
    profit = round_(current_investment - initial_investment, 2)
    n_orders = len(gain_list)
    average_profit_per_event = round_(np.mean(profit_event), 2)
    
    # Print summary statistics if not in minimal mode
    if not minimal:
        print(f"Initial Investment: {initial_investment}$")
        print(f"Investment per event: {round_(investment_per_order,2)}$")
        print(f"Commission: {round_(total_commission,2)}$")
        print(f"Total Investment {round_(n_orders*investment_per_order,2)}")
        print(f"Average Profit per event: {average_profit_per_event}%")
        print(f"Profit: {profit}$")
        print(f"Total Observations Under Analysis: {len(total_events_list_ascending_order)}")
        print(f"Total Events triggered by strategy: {len(buy_events_list_ascending_order)}")
        print(len(total_events_list_ascending_order))

        # Create detailed DataFrame of event results
        pd.set_option("display.max_rows", 10000)
        df_events_overview = pd.DataFrame(
            {
                "Timestamp Buy": ts_buy_list,
                "Timestamp Sell": ts_sell_list,
                "Balance": gain_list,
                "Coin": coin_list,
                "Profit": profit_event,
                "Investment": gain_list,
                "Profit_vol_strat": profit_wt_event,
                "max": max_list,
                "min": min_list,
                "Initial Price": initial_price_list,
                "Buy Price": buy_price_list,
                "Sell price": sell_price_list,
                "Event Key": event_key_list,
                "Start Timestamp": start_timestamp_list,
            }
        )

        # Create DataFrame with event key overview
        df_event_keys_overview = pd.DataFrame(df_event_keys_overview)

        # Return detailed DataFrames in full mode
        return (df_events_overview, df_event_keys_overview)
    else:
        # Return basic metrics in minimal mode
        return {'n_orders': n_orders, 'profit': profit, 'average_profit_per_event': average_profit_per_event, 'commission': total_commission}

def get_strategy_summary(limit_n_transactions=5):
    """
    Lists and reads all strategy JSON files in the strategy directory.
    Returns a dictionary containing the info_strategy object from each strategy directory.
    """
    strategy_files = {}
    strategy_dir = ROOT_PATH + "/strategy"

    # Check if strategy directory exists
    if not os.path.exists(strategy_dir):
        print("Strategy directory not found")
        return {}

    # List all directories in strategy folder
    strategy_dirs = [d for d in os.listdir(strategy_dir) 
                    if os.path.isdir(os.path.join(strategy_dir, d))]

    # Define the order of keys for info_strategy
    key_order = [
        'strategy_jump',
        'limit',
        'price_change_jump',
        'max_limit',
        'price_drop_limit',
        'buy_duration',
        'stop_loss_limit',
        'distance_jump_to_current_price',
        'max_ask_od_level',
        'last_i_ask_od',
        'min_n_obs_jump_level',
        'lvl'
    ]

    results = {'strategy': [], 'n_orders': [], 'profit': [], 'average_profit_per_event': [], 'commission': []}


    for strategy_name in strategy_dirs:
        
        #print(path)

        response = plot_strategy_result(limit_n_transactions=limit_n_transactions, minimal=True, strategy=strategy_name)

        if response is not None:
            results['strategy'].append(strategy_name)
            results['n_orders'].append(response['n_orders'])
            results['profit'].append(response['profit'])
            results['average_profit_per_event'].append(response['average_profit_per_event'])
            results['commission'].append(response['commission'])

    pd.set_option("display.max_rows", 10000)
    pd.set_option('display.max_colwidth', None)
    df_results = pd.DataFrame(results)
    return df_results


        # # Get the full path to the strategy directory
        # pd.set_option("display.max_rows", 10000)
        # df_events_overview = pd.DataFrame(
        #     {
        #         "Timestamp Buy": ts_buy_list,
        #         "Timestamp Sell": ts_sell_list,
        #         "Balance": gain_list,
        #         "Coin": coin_list,
        #         "Profit": profit_event,
        #         "Investment": gain_list,
        #         "Profit_vol_strat": profit_wt_event,
        #         "max": max_list,
        #         "min": min_list,
        #         "Initial Price": initial_price_list,
        #         "Buy Price": buy_price_list,
        #         "Sell price": sell_price_list,
        #     })

    #return strategy_files


def discover_orderbook(coin, start_timestamp, hours_range):

    metadata_orderbook_path = ROOT_PATH + "/order_book/metadata.json"
    with open(metadata_orderbook_path, 'r') as f:
        metadata_orderbook = json.load(f)

    if start_timestamp > datetime.now() - timedelta(hours=48):
        print('start_timestamp is too recent, data is not available')
        return None

    order_book_found = {}
    final_outcome = {}
    ORDERBOOK_FOUND = False
    for event_key in metadata_orderbook:
        for coin_metadata in metadata_orderbook[event_key]:
            if coin_metadata == coin:
                for timestamp in metadata_orderbook[event_key][coin_metadata]:
                    if event_key not in order_book_found:
                        order_book_found[event_key] = []
                    order_book_found[event_key].append(timestamp)
                    # Convert timestamp string to datetime object for comparison
                    timestamp_dt = datetime.fromisoformat(timestamp)
                    # Calculate hours difference between timestamps
                    time_diff_hours = abs((start_timestamp - timestamp_dt).total_seconds() / 3600)
                    if time_diff_hours <= 27 and time_diff_hours >= 0:
                        print(f'start_timestamp: {start_timestamp} - timestamp: {timestamp} -  time_diff: {time_diff_hours}')

                        #ORDERBOOK_FOUND = True
                        
                        file_name = event_key.replace(':', '_').replace('/', '_')
                        part_num = metadata_orderbook[event_key][coin_metadata][timestamp]
                        part_path = f"{ROOT_PATH}/order_book/{file_name}_{part_num}.json"
                        print(f'Found orderbook for {coin} - {timestamp} - {part_path}')
                        with open(part_path, 'r') as f:
                            order_book = json.load(f)
                        for coin_orderbook in order_book:
                            if coin_orderbook == coin:
                                for timestamp_orderbook in order_book[coin_orderbook]:
                                    if timestamp_orderbook == timestamp:
                                        print(f'FOund timestamp in part_path: {part_path} - {timestamp_orderbook}')
                                        ORDERBOOK_FOUND = True
                                        #print(order_book[coin_orderbook][timestamp_orderbook])
                                        data = order_book[coin_orderbook][timestamp_orderbook][0]['data']
                                        for ts in data:
                                            if datetime.fromisoformat(ts) > start_timestamp - timedelta(hours=hours_range) and datetime.fromisoformat(ts) < start_timestamp + timedelta(hours=hours_range):
                                                price = order_book[coin_orderbook][timestamp_orderbook][0]['data'][ts][0]
                                                price_orders = order_book[coin_orderbook][timestamp_orderbook][0]['data'][ts][4]
                                                price_levels, order_distribution, cumulative_level = get_price_levels(price, price_orders, delta=0.001)
                                                OBS = {'price': price, 'price_orders': price_orders, 'order_distribution': order_distribution}
                                                final_outcome[ts] = OBS
                                        return final_outcome
    
    if not ORDERBOOK_FOUND:                           
        print('No orderbook found')
        print(order_book_found)
        return None





