import os, sys

sys.path.insert(0, "../..")
import json
from database.DatabaseConnection import DatabaseConnection
from tracker.app.Helpers.Helpers import round_, timer_func
from tracker.app.Controller.LoggingController import LoggingController

from datetime import datetime, timedelta
import numpy as np
from tracker.constants.constants import *
from tracker.app.Helpers.Helpers import round_, timer_func


class Benchmark:

    def __init__(self) -> None:

        pass

    @staticmethod
    @timer_func
    def computeVolumeAverage(
        client=DatabaseConnection(), logger=LoggingController.start_logging()
    ):
        """
        This function is used to compute/update the average volumes of all the coins in db
        This function tries to be as much efficient as possible
        I don't compute everytime the average of a timeframe, but I compute and store the average volume of a day
        and I store it in a json
        This functions gets as input the data from market_db
        and then computes the average for each day (available in db_market) not only the last day
        """

        # compute reference time

        DAYS_1_MONTH = 28
        DAYS_2_MONTH = 28 * 2
        DAYS_3_MONTH = 28 * 3
        reference_90days_datetime = datetime.now() - timedelta(days=DAYS_3_MONTH)
        reference_60days_datetime = datetime.now() - timedelta(days=DAYS_2_MONTH)
        reference_30days_datetime = datetime.now() - timedelta(days=DAYS_1_MONTH)
        reference_90days = reference_90days_datetime.isoformat()

        volumes_30days_list = []
        volumes_60days_list = []
        volumes_90days_list = []

        # get db Market and Benchmark
        db_market = client.get_db(DATABASE_MARKET)
        db_benchmark = client.get_db(DATABASE_BENCHMARK)
        db_logger = client.get_db(DATABASE_LOGGING)
        db_volume_standings = client.get_db(DATABASE_VOLUME_STANDINGS)

        # Get the updated coin list
        coins_list = db_market.list_collection_names()
        f = open("/tracker/json/coins_list.json", "r")
        data = json.loads(f.read())
        # coin_list_subset = data["most_traded_coins"][:NUMBER_COINS_TO_TRADE_WSS]
        type_usdt = os.getenv("TYPE_USDT")
        coin_list_subset = data[type_usdt]
        len_coin_list_subset = len(coin_list_subset)
        logger.info(
            f"Number of coins FROM BINANCE EXCHANGE: {len_coin_list_subset}. type_usdt: {type_usdt}"
        )
        # coin_list_subset = ['BTC_USD']

        now_datetime = datetime.now() + timedelta(minutes=1)
        now = now_datetime.isoformat()
        today = now.split("T")[0]

        for coin in coins_list:

            volumes = {}
            # let us create the variable that will be saved in the db.
            # this will consider the average and standard deviation of the volume for each day
            volume_info = {}
            avg_volumes = []

            if coin not in coin_list_subset:
                msg = f"{coin} is no longer in the most_traded_coins. This happens if the coin is in db_market, but binance removes it from the listed coins"
                db_logger[DATABASE_BENCHMARK_INFO].insert_one(
                    {"_id": datetime.now().isoformat(), "msg": msg}
                )
                continue

            cursor_benchmark = list(db_benchmark[coin].find())

            list_dt = []

            # if benchmark was never created
            if len(cursor_benchmark) == 0:

                volumes_30days_list = []
                volumes_60days_list = []
                volumes_90days_list = []

                docs = db_market[coin].find(
                    {"_id": {"$gte": reference_90days}}, {"_id": 1, "volume": 1}
                )
                i = 0
                for doc in docs:
                    if i == 0:
                        # get first timestamp of the mongo cursor
                        first_timestamp = doc["_id"]
                        # get last timestamp in this format yyyy--MM--dd
                        last_timestamp = first_timestamp.split("T")[0]
                        volumes[last_timestamp] = []
                        i += 1

                    datetime_doc = datetime.fromisoformat(doc["_id"])

                    # AVOID DUPLICATES
                    datetime_minute = datetime_doc.replace(second=0).replace(
                        microsecond=0
                    )
                    if datetime_minute not in list_dt:
                        list_dt.append(datetime_minute)
                    else:
                        continue

                    # compute average for 30, 60, 90 days
                    # this will append each doc_volume (every minute of observation)
                    if datetime_doc > reference_30days_datetime:
                        volumes_30days_list.append(doc["volume"])
                        volumes_60days_list.append(doc["volume"])
                        volumes_90days_list.append(doc["volume"])

                    elif datetime_doc > reference_60days_datetime:
                        volumes_60days_list.append(doc["volume"])
                        volumes_90days_list.append(doc["volume"])

                    elif datetime_doc > reference_90days_datetime:
                        volumes_90days_list.append(doc["volume"])

                    # if day is still same append volume
                    if doc["_id"].split("T")[0] == last_timestamp:
                        volumes[last_timestamp].append(doc["volume"])
                    # if day is today, then skip
                    elif doc["_id"].split("T")[0] == today:
                        avg = round_(np.mean(volumes[last_timestamp]), 2)
                        std = round_(np.std(volumes[last_timestamp]), 2)
                        volume_info[last_timestamp] = (avg, std)
                        break
                    else:
                        # otherwise compute the average of the last day and initialize dict for new day
                        avg = round_(np.mean(volumes[last_timestamp]), 2)
                        std = round_(np.std(volumes[last_timestamp]), 2)
                        volume_info[last_timestamp] = (avg, std)

                        last_timestamp = doc["_id"].split("T")[0]
                        volumes[last_timestamp] = []
                        volumes[last_timestamp].append(doc["volume"])

                # this case hits in case there is only one "last timestamp"
                if not volume_info:
                    avg = round_(np.mean(volumes[last_timestamp]), 2)
                    std = round_(np.std(volumes[last_timestamp]), 2)
                    volume_info[last_timestamp] = (avg, std)

                # let us create the variable that will be saved in the db.
                # this will consider the average and standard deviation of the volume for each day

                avg_volume_30days = round_(np.mean(volumes_30days_list), 2)
                std_volume_30days = round_(np.std(volumes_30days_list), 2)

                avg_volume_60days = round_(np.mean(volumes_60days_list), 2)
                std_volume_60days = round_(np.std(volumes_60days_list), 2)

                avg_volume_90days = round_(np.mean(volumes_90days_list), 2)
                std_volume_90days = round_(np.std(volumes_90days_list), 2)

                doc = {
                    "volume_30_avg": avg_volume_30days,
                    "volume_30_std": std_volume_30days,
                    "volume_60_avg": avg_volume_60days,
                    "volume_60_std": std_volume_60days,
                    "volume_90_avg": avg_volume_90days,
                    "volume_90_std": std_volume_90days,
                    "volume_series": volume_info,
                }

                db_benchmark[coin].insert(doc)
            # benchmark needs to be updated
            else:
                # initialize volumes list
                volumes_30days_list_avg = []
                volumes_30days_list_std = []
                volumes_60days_list_avg = []
                volumes_60days_list_std = []
                volumes_90days_list_avg = []
                volumes_90days_list_std = []

                # get id of benchmark
                id_benchmark = cursor_benchmark[0]["_id"]
                filter = {"_id": id_benchmark}

                # get volume_series already saved in db and get last saved datetime
                volume_info = cursor_benchmark[0]["volume_series"]
                volumes = {}
                last_date = list(volume_info.keys())[-1]
                split_date = last_date.split("-")
                year = split_date[0]
                month = split_date[1]
                day = split_date[2]
                # this is the starting time from which looking for new observation.
                # minute=1 because I discard the midnight observation which still stands in case the coin has moved out of top300
                st_datetime = datetime(
                    year=int(year), month=int(month), day=int(day), minute=1
                ) + timedelta(days=1)
                et_datetime = (now_datetime - timedelta(days=1)).replace(
                    hour=23, minute=59, second=59
                )

                # this should happen during development, not in production
                if (
                    datetime.timestamp(et_datetime) - datetime.timestamp(st_datetime)
                    < 0
                ):
                    print(
                        "et - st < 0: this should happen during development, not in production"
                    )
                    et_datetime = now_datetime

                    # print(st_datetime)
                    # print(et_datetime)

                    # this case happens only in development, can not happen in production
                    if (
                        datetime.timestamp(et_datetime)
                        - datetime.timestamp(st_datetime)
                        < 0
                    ):
                        #     print('case 1')
                        continue
                    # else:
                    #     print(coin)
                    #     print('case 2')

                # retrieve obs from last saved datetime and past midnight
                docs = list(
                    db_market[coin].find(
                        {
                            "_id": {
                                "$gte": st_datetime.isoformat(),
                                "$lte": et_datetime.isoformat(),
                            }
                        },
                        {"_id": 1, "volume": 1},
                    )
                )

                # print(list(docs))
                if len(docs) != 0:
                    i = 0
                    for doc in docs:
                        if i == 0:
                            # get first timestamp of the mongo cursor
                            first_timestamp = doc["_id"]
                            # print(first_timestamp)
                            # get last timestamp in this format yyyy--MM--dd
                            last_timestamp = first_timestamp.split("T")[0]
                            volumes[last_timestamp] = []
                            i += 1

                        # if day is still same append volume
                        if doc["_id"].split("T")[0] == last_timestamp:
                            volumes[last_timestamp].append(doc["volume"])
                        # if day is today, then skip
                        elif doc["_id"].split("T")[0] == today:
                            avg = round_(np.mean(volumes[last_timestamp]), 2)
                            std = round_(np.std(volumes[last_timestamp]), 2)
                            volume_info[last_timestamp] = (avg, std)
                            break
                        else:
                            # otherwise compute the average of the last day and initialize dict for new day
                            avg = round_(np.mean(volumes[last_timestamp]), 2)
                            std = round_(np.std(volumes[last_timestamp]), 2)
                            volume_info[last_timestamp] = (avg, std)

                            last_timestamp = doc["_id"].split("T")[0]
                            volumes[last_timestamp] = []
                            volumes[last_timestamp].append(doc["volume"])

                    if isinstance(volumes[last_timestamp], list):
                        avg = round_(np.mean(volumes[last_timestamp]), 2)
                        std = round_(np.std(volumes[last_timestamp]), 2)
                        volume_info[last_timestamp] = (avg, std)
                        n_obs = len(volumes[last_timestamp])
                        if n_obs < 1430 and n_obs > 1440:
                            print(
                                f"Benchmark: {n_obs} obs in db_market for {coin}. Volume avg: {avg}"
                            )
                    else:
                        print(f"Benchmark: something is wrong for {coin}")

                    # adjust average of 30, 60, 90 days
                    for date in list(volume_info.keys()):
                        split_date = date.split("-")
                        year = split_date[0]
                        month = split_date[1]
                        day = split_date[2]
                        datetime_i = datetime(
                            year=int(year), month=int(month), day=int(day)
                        ) + timedelta(days=1)

                        if datetime_i > reference_30days_datetime:
                            volumes_30days_list_avg.append(volume_info[date][0])
                            volumes_60days_list_avg.append(volume_info[date][0])
                            volumes_90days_list_avg.append(volume_info[date][0])
                            volumes_30days_list_std.append(volume_info[date][1])
                            volumes_60days_list_std.append(volume_info[date][1])
                            volumes_90days_list_std.append(volume_info[date][1])

                        elif datetime_i > reference_60days_datetime:
                            volumes_60days_list_avg.append(volume_info[date][0])
                            volumes_90days_list_avg.append(volume_info[date][0])
                            volumes_60days_list_std.append(volume_info[date][1])
                            volumes_90days_list_std.append(volume_info[date][1])

                        elif datetime_i > reference_90days_datetime:
                            volumes_90days_list_avg.append(volume_info[date][0])
                            volumes_90days_list_std.append(volume_info[date][1])

                    avg_volume_30days = round_(np.mean(volumes_30days_list_avg), 2)
                    std_volume_30days = round_(np.mean(volumes_30days_list_std), 2)

                    avg_volume_60days = round_(np.mean(volumes_60days_list_avg), 2)
                    std_volume_60days = round_(np.mean(volumes_60days_list_std), 2)

                    avg_volume_90days = round_(np.mean(volumes_90days_list_avg), 2)
                    std_volume_90days = round_(np.mean(volumes_90days_list_std), 2)

                    new_volume_series = {
                        "$set": {
                            "volume_series": volume_info,
                            "volume_30_avg": avg_volume_30days,
                            "volume_30_std": std_volume_30days,
                            "volume_60_avg": avg_volume_60days,
                            "volume_60_std": std_volume_60days,
                            "volume_90_avg": avg_volume_90days,
                            "volume_90_std": std_volume_90days,
                        }
                    }

                    db_benchmark[coin].update_one(filter, new_volume_series)
                    # print(f'{coin} has been updated')

        Benchmark.computeVolumeStandings(db_benchmark, db_volume_standings, logger)

    def computeVolumeStandings(db_benchmark, db_volume_standings, logger):
        """
        This function gets executed everyday at midnight and creates a standings for all the coins based on the Benchmark Database
        """

        coins_list_benchmark = db_benchmark.list_collection_names()
        dt_7days_ago = datetime.now() - timedelta(days=7)
        dt_14days_ago = datetime.now() - timedelta(days=14)
        dt_28days_ago = datetime.now() - timedelta(days=28)

        dt_7 = []
        dt_14 = []
        dt_28 = []
        dt_all = []

        # this function is executed at 23:59, so it is okay to compare the today_date
        today_date = datetime.now().strftime("%Y-%m-%d")
        volume_list = (
            []
        )  # [{'coin': BTCUSDT, 'volume_30_avg': 10000000}, {'coin': ETHUSDT, 'volume_30_avg': 5000000}, ...]
        for coin in coins_list_benchmark:
            cursor_benchmark = list(db_benchmark[coin].find())
            if "volume_series" in cursor_benchmark[0]:
                volume_series = cursor_benchmark[0]["volume_series"]
                dates_volume_series = list(volume_series.keys())
                # Convert date strings to datetime objects
                date_objects = [
                    datetime.strptime(date_str, "%Y-%m-%d")
                    for date_str in dates_volume_series
                ]
                # Sort the datetime objects
                date_objects.sort()
                # Convert the sorted datetime objects back to strings
                sorted_date_strings = [
                    date_obj.strftime("%Y-%m-%d") for date_obj in date_objects
                ]
                last_date = sorted_date_strings[-1]
                if last_date == today_date:
                    volume_avg_30 = cursor_benchmark[0]["volume_30_avg"]
                    volume_list.append({"coin": coin, "volume_30_avg": volume_avg_30})
                elif datetime.strptime(last_date, "%Y-%m-%d") > dt_7days_ago:
                    dt_7.append(coin)
                elif datetime.strptime(last_date, "%Y-%m-%d") > dt_14days_ago:
                    dt_14.append(coin)
                elif datetime.strptime(last_date, "%Y-%m-%d") > dt_28days_ago:
                    dt_28.append(coin)
                else:
                    dt_all.append(coin)

        n_dt_7 = len(dt_7)
        n_dt_14 = len(dt_14)
        n_dt_28 = len(dt_28)
        n_dt_all = len(dt_all)

        logger.info(f"Volume Standings {n_dt_7} coins have benchmark are < 7 days")
        logger.info(
            f"Volume Standings {n_dt_14} coins have benchmark are 7-14 days old"
        )
        logger.info(
            f"Volume Standings {n_dt_28} coins have benchmark are 14-28 days old"
        )
        logger.info(
            f"Volume Standings {n_dt_all} coins have benchmark are > 28 days old"
        )

        if len(volume_list) != 0:
            volume_standings = Benchmark.sort_and_rank_by_volume(volume_list)

            # print(volume_standings)
            now = datetime.now() + timedelta(minutes=1)
            id_volume_standings_doc = now.strftime("%Y-%m-%d")

            db_volume_standings[COLLECTION_VOLUME_STANDINGS].insert_one(
                {"_id": id_volume_standings_doc, "standings": volume_standings}
            )
        else:
            print(
                "computeVolumeStandings: Standings could not be computed. Something went wrong"
            )

    def sort_and_rank_by_volume(data):
        """
        Sorts a list of objects with 'volume' and 'coin' keys by 'volume' in descending order
        and assigns a rank to each object.

        Args:
            data: A list of dictionaries, where each dictionary has 'volume' and 'coin' keys.

        Returns:
            A new list of dictionaries, where each dictionary contains
            'volume', 'coin', and 'rank' keys.
        """

        sorted_data = sorted(data, key=lambda x: x["volume_30_avg"], reverse=True)
        ranked_data = []

        for rank, item in enumerate(sorted_data, 1):  # Start ranking from 1
            item["rank"] = rank
            ranked_data.append(item)

        standings = {}
        for obj in ranked_data:
            coin = obj["coin"]
            standings[coin] = {"rank": obj["rank"], "volume": obj["volume_30_avg"]}

        return standings
