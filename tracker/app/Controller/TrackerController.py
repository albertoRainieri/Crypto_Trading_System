import os, sys

sys.path.insert(0, "../..")
from app.Controller.LoggingController import LoggingController
import json
from tracker.constants.constants import *
from datetime import datetime, timedelta
from tracker.app.Helpers.Helpers import round_, timer_func
import numpy as np
from tracker.database.DatabaseConnection import DatabaseConnection
from tracker.app.Controller.TradingController import TradingController
from tracker.constants.constants import *
from time import time


class TrackerController:
    def __init__(self) -> None:
        pass

    def isEqualToReferenceDatetime(
        actual_timestamp,
        reference_year_x_ago,
        reference_month_x_ago,
        reference_day_x_ago,
        reference_hour_x_ago,
        reference_minute_x_ago,
        logger,
    ):
        """
        This function check if actual timestamp is equal to reference datetime (1h, 3h, 6h ago)
        """
        actual_datetime = datetime.fromisoformat(actual_timestamp)
        # logger.info(f'Actual Datetime: {actual_datetime}')

        year_x_ago = actual_datetime.year
        month_x_ago = actual_datetime.month
        day_x_ago = actual_datetime.day
        hour_x_ago = actual_datetime.hour
        minute_x_ago = actual_datetime.minute

        if (
            year_x_ago == reference_year_x_ago
            and month_x_ago == reference_month_x_ago
            and day_x_ago == reference_day_x_ago
            and hour_x_ago == reference_hour_x_ago
            and minute_x_ago == reference_minute_x_ago
        ):
            return True
        else:
            return False

    # @timer_func
    # async def start_tracking(db_trades, db_tracker, db_benchmark, logger, db_logger):
    # @timer_func
    def start_tracking(
        best_x_coins,
        coins_list,
        db_trades,
        db_tracker,
        db_benchmark,
        logger,
        db_logger,
        volume_standings,
    ):
        now = datetime.now()

        reference_1h_datetime = now - timedelta(hours=1)
        reference_30m_datetime = now - timedelta(minutes=30)
        reference_15m_datetime = now - timedelta(minutes=15)
        reference_5m_datetime = now - timedelta(minutes=5)
        reference_10s_datetime = now - timedelta(seconds=10)

        reference_1hour = reference_1h_datetime.isoformat()
        coins_list_trades = db_trades.list_collection_names()

        f = open("/tracker/riskmanagement/riskmanagement.json", "r")
        risk_configuration = json.loads(f.read())

        coins_not_traded = []
        # logger.info(coin_list_subset)
        # logger.info(len(coin_list_subset))

        # iterate through each coin
        # The all cycle takes slightly less than 1 second (from localhost 15/08/2023)
        # logger.info(coins_list)
        # logger.info(best_x_coins)
        for coin in coins_list_trades:
            if coin not in coins_list["list_usdt_common_usdc"]:
                continue

            volume_coin = list(
                db_benchmark[coin].find(
                    {},
                    {
                        "_id": 1,
                        "volume_30_avg": 1,
                        "volume_30_std": 1,
                        "Last_30_Trades": 1,
                    },
                )
            )
            # print(volume_coin)

            # if benchmark exists, fetch it and use it to compute the relative volume wrt to average,
            #  otherwise I am going to skip the computation

            if len(volume_coin) != 0:

                avg_volume_1_month = volume_coin[0]["volume_30_avg"]

                # in case there is something wrong with "volume_30_avg" then skip
                if avg_volume_1_month == None or avg_volume_1_month == 0:
                    if now.hour == 0 and now.minute == 5:
                        msg = f"{coin} has a volume average == None or equal to zero. Computation in tracker will be skipped."
                        # logger.info(msg)
                        db_logger[DATABASE_TRACKER_INFO].insert_one(
                            {"_id": datetime.now().isoformat(), "msg": msg}
                        )
                    continue

                # here, the coin must pass some checks.
                # In particular, it is required that the coin has been in the "most_traded_coins" in the last consecutive "benchmark_days".
                # if this condition is not met, then the coin will not be analyzed and nothing will be saved to tracker.
                benchmark_days = 21  # how many days the coin must have been in the "most_traded_coins"

                try:
                    list_last_2w_trades = volume_coin[0]["Last_30_Trades"][
                        "list_last_30_trades"
                    ][-benchmark_days:]

                    if sum(list_last_2w_trades) < benchmark_days:
                        if now.hour == 0 and now.minute == 5:
                            msg = f"{coin} has not always been in most_traded_coins in the last {benchmark_days} days."
                            # logger.info(msg)
                            # db_logger[DATABASE_TRACKER_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
                        continue
                except:
                    if now.hour == 0 and now.minute == 5:
                        msg = f"{coin}: There are not enough observations in db_benchmark."
                        # logger.info(msg)
                        # db_logger[DATABASE_TRACKER_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
                    continue

            # skip the computation. volume_average does not exist
            else:
                if now.hour == 0 and now.minute == 5:
                    msg = f"{coin} does not have a volume average. Computation in tracker will be skipped."
                    # logger.info(msg)
                    # db_logger[DATABASE_TRACKER_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
                continue

            # logger.info(f'{coin}: {avg_volume_1_month}')
            # initialize these variables list for each coin

            volumes_60m_list = []
            volumes_30m_list = []
            volumes_15m_list = []
            volumes_5m_list = []

            buy_volume_60m_list = []
            buy_volume_30m_list = []
            buy_volume_15m_list = []
            buy_volume_5m_list = []

            docs = list(db_trades[coin].find({"_id": {"$gte": reference_1hour}}))

            # check if it is available an observation in "Market_Trades" in the last minute
            if len(docs) == 0:
                continue
            last_timestamp = docs[-1]["_id"]
            last_price = docs[-1]["price"]
            last_datetime = datetime.fromisoformat(last_timestamp)

            if last_price == None:
                continue

            if last_datetime > reference_10s_datetime:

                i = 1
                for doc in docs:
                    if i == 1:
                        i += 1

                    # logger.info(doc)
                    doc_vol = doc["volume"]
                    doc_buy_vol = doc["buy_volume"]
                    timestamp_trade = datetime.fromisoformat(doc["_id"])

                    # if timestamp trade is not older than 5 minutes
                    if timestamp_trade > reference_5m_datetime:
                        # logger.info(f'{timestamp_trade} --- {reference_5m_datetime}')
                        volumes_5m_list.append(doc_vol)
                        volumes_15m_list.append(doc_vol)
                        volumes_30m_list.append(doc_vol)
                        volumes_60m_list.append(doc_vol)
                        buy_volume_5m_list.append(doc_buy_vol)
                        buy_volume_15m_list.append(doc_buy_vol)
                        buy_volume_30m_list.append(doc_buy_vol)
                        buy_volume_60m_list.append(doc_buy_vol)
                        continue

                    # if timestamp trade is not older than 15 minutes
                    elif timestamp_trade > reference_15m_datetime:
                        volumes_15m_list.append(doc_vol)
                        volumes_30m_list.append(doc_vol)
                        volumes_60m_list.append(doc_vol)
                        buy_volume_15m_list.append(doc_buy_vol)
                        buy_volume_30m_list.append(doc_buy_vol)
                        buy_volume_60m_list.append(doc_buy_vol)
                        continue

                    # if timestamp trade is not older than 30 minutes
                    elif timestamp_trade > reference_30m_datetime:
                        volumes_30m_list.append(doc_vol)
                        volumes_60m_list.append(doc_vol)
                        buy_volume_30m_list.append(doc_buy_vol)
                        buy_volume_60m_list.append(doc_buy_vol)
                        continue

                    # if timestamp trade is not older than 60 minutes
                    elif timestamp_trade > reference_1h_datetime:
                        volumes_60m_list.append(doc_vol)
                        buy_volume_60m_list.append(doc_buy_vol)
                        continue
                # Compute the volume statistics of the last minute. Since we reach the end of the "docs" variable

                price_now = doc["price"]
                # logger.info(doc)
                volume_1m = round_(doc["volume"] / avg_volume_1_month, 2)
                if doc["volume"] != 0:
                    buy_volume_perc_1m = round_(doc["buy_volume"] / doc["volume"], 2)
                else:
                    buy_volume_perc_1m = 0.5

                if sum(volumes_60m_list) != 0 and len(volumes_60m_list) >= 50:
                    volumes_60m = round_(
                        np.mean(volumes_60m_list) / avg_volume_1_month, 2
                    )
                    buy_volume_perc_60m = round_(
                        sum(buy_volume_60m_list) / sum(volumes_60m_list), 2
                    )

                else:
                    volumes_60m = None
                    buy_volume_perc_60m = None

                if sum(volumes_30m_list) != 0 and len(volumes_30m_list) >= 24:
                    volumes_30m = round_(
                        np.mean(volumes_30m_list) / avg_volume_1_month, 2
                    )
                    buy_volume_perc_30m = round_(
                        sum(buy_volume_30m_list) / sum(volumes_30m_list), 2
                    )
                else:
                    volumes_30m = None
                    buy_volume_perc_30m = None

                if sum(volumes_15m_list) != 0 and len(volumes_15m_list) >= 12:
                    volumes_15m = round_(
                        np.mean(volumes_15m_list) / avg_volume_1_month, 2
                    )
                    buy_volume_perc_15m = round_(
                        sum(buy_volume_15m_list) / sum(volumes_15m_list), 2
                    )
                else:
                    volumes_15m = None
                    buy_volume_perc_15m = None

                if sum(volumes_5m_list) != 0 and len(volumes_5m_list) >= 4:
                    volumes_5m = round_(
                        np.mean(volumes_5m_list) / avg_volume_1_month, 2
                    )
                    buy_volume_perc_5m = round_(
                        sum(buy_volume_5m_list) / sum(volumes_5m_list), 2
                    )
                else:
                    volumes_5m = None
                    buy_volume_perc_5m = None

                doc_db = {
                    "_id": datetime.now().isoformat(),
                    "price": price_now,
                    "vol_1m": volume_1m,
                    "buy_vol_1m": buy_volume_perc_1m,
                    "vol_5m": volumes_5m,
                    "buy_vol_5m": buy_volume_perc_5m,
                    "vol_15m": volumes_15m,
                    "buy_vol_15m": buy_volume_perc_15m,
                    "vol_30m": volumes_30m,
                    "buy_vol_30m": buy_volume_perc_30m,
                    "vol_60m": volumes_60m,
                    "buy_vol_60m": buy_volume_perc_60m,
                }

                # CHECK IF THE EVENT CAN TRIGGER A BUY ORDER

                # ASYNC CALL ENABLED call. Enable only if cpu support is high. 4CPUs are probably minimum
                # asyncio.create_task(TradingController.check_event_triggering(coin, doc_db, volatility_coin, logger, db_logger, risk_configuration))

                # ASYNC DISABLED. THIS IS PREFERRED CHOICE even if CPU Support is high
                TradingController.buy_event_analysis(
                    coin, doc_db, risk_configuration, logger, volume_standings
                )
                # logger.info(doc_db)

                db_tracker[coin].insert(doc_db)

            elif coin in best_x_coins:
                coins_not_traded.append(coin)

        if len(coins_not_traded) != 0:
            n = len(coins_not_traded)
            tot = len(best_x_coins)
            msg = f"WARNING: {n}/{tot}:  {coins_not_traded} not traded"
            logger.info(msg)
            db_logger[DATABASE_API_ERROR].insert_one(
                {"_id": datetime.now().isoformat(), "msg": msg}
            )
