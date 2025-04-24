import websocket
import json
import requests
from typing import Dict, List, Optional
import threading
import time
import os
import sys
import random
import signal
sys.path.insert(1, os.path.join(sys.path[0], ".."))
from datetime import datetime, timedelta
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from signal import SIGKILL
from numpy import arange, linspace
from constants.constants import *
import re
from time import sleep


class PooledBinanceOrderBook:
    def __init__(self, coins: List[str]):
        
        self.coins = [coin.upper() for coin in coins]
        self.coins_lower = [coin.lower() for coin in coins]
        self.retry_connection = 0
        
        # Order book state - one for each coin
        self.order_books = {}
        self.buffered_events = {}
        self.running = False
        self.ws = None
        self.ws_thread = None
        self.should_exit = False  # Flag to indicate if script should exit

        # Trading state per coin
        self.under_observation = {}  # Tracks whether each coin is currently being monitored/observed
        self.BUY = {}
        self.summary_jump_price_level = {}
        self.ask_order_distribution_list = {}
        self.bid_order_distribution_list = {}
        self.max_price = {}
        self.initial_price = {}
        self.buy_price = {}
        self.last_db_update_time = {}
        self.last_ask_order_distribution_1level = {}
        self.last_bid_order_distribution_1level = {}
        self.ask_1firstlevel_orderlevel_detected = {}
        self.bid_1firstlevel_orderlevel_detected = {}
        self.current_price = {}
        self.bid_price_levels_dt = {}
        self.ask_price_levels_dt = {}
        self.coin_orderbook_initialized = {}
        
        # Parameters
        self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD = float(os.getenv('ORDER_DISTRIBUTION_1LEVEL_THRESHOLD')) 
        self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD = float(os.getenv('ORDER_DISTRIBUTION_2LEVEL_THRESHOLD'))
        self.DB_UPDATE_MIN_WAITING_TIME = int(os.getenv('DB_UPDATE_MIN_WAITING_TIME'))
        self.DB_UPDATE_MAX_WAITING_TIME = int(os.getenv('DB_UPDATE_MAX_WAITING_TIME'))
        self.MAX_WAITING_TIME_AFTER_BUY = int(os.getenv('MAX_WAITING_TIME_AFTER_BUY'))
        self.ping_interval = 30
        self.ping_timeout = 20  # Increased from 10 to 30 seconds
        self.connection_lock = threading.Lock()
        self.last_pong_time = None
        self.connection_restart_lock = threading.Lock()
        self.connection_restart_running = False
        last_snapshot_time = datetime.now() + timedelta(seconds=3)
        
        # Initialize per-coin data structures
        for coin in self.coins:
            last_snapshot_time = last_snapshot_time + timedelta(seconds=3)
            self.initialize_coin_status(coin=coin, last_snapshot_time=last_snapshot_time)
        
        # Database connection
        self.client = DatabaseConnection()
        self.db_orderbook = self.client.get_db(DATABASE_ORDER_BOOK)
        self.metadata_orderbook_collection = self.db_orderbook[COLLECTION_ORDERBOOK_METADATA]
        self.db_trading = self.client.get_db(DATABASE_TRADING)
        self.trading_collection = self.db_trading["albertorainieri"]
        
        # Logger
        self.logger = LoggingController.start_logging()
        
        # Combined stream URL
        self.parameters = [f"{coin.lower()}@depth" for coin in self.coins]
        self.ws_url = f"wss://stream.binance.com:9443/stream?streams="
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def initialize_coin_status(self, coin, last_snapshot_time=datetime.now(), start_script=True):

        if start_script:
            self.order_books[coin] = {
                'bids': {},
                'asks': {},
                'lastUpdateId': None
            }
        self.BUY[coin] = False
        self.under_observation[coin] = {'status': False}
        self.coin_orderbook_initialized[coin] = {'status': False, 'next_snapshot_time': last_snapshot_time}
        self.buffered_events[coin] = []
        self.BUY[coin] = {False}
        self.summary_jump_price_level[coin] = {}
        self.ask_order_distribution_list[coin] = []
        self.bid_order_distribution_list[coin] = []
        self.max_price[coin] = 0
        self.initial_price[coin] = 0
        self.buy_price[coin] = 0
        self.last_db_update_time[coin] = datetime.now()
        self.last_ask_order_distribution_1level[coin] = 0
        self.last_bid_order_distribution_1level[coin] = 0
        self.ask_1firstlevel_orderlevel_detected[coin] = False
        self.bid_1firstlevel_orderlevel_detected[coin] = False
        self.current_price[coin] = 0
        self.bid_price_levels_dt[coin] = []
        self.ask_price_levels_dt[coin] = []

    def get_riskmanagement_configuration(self):
        with open('/tracker/riskmanagement/riskmanagement.json', 'r') as f:
            riskmanagement_configuration = json.load(f)
        return riskmanagement_configuration


    def get_ongoing_analysis_coins(self):
        client = DatabaseConnection()
        db = client.get_db(DATABASE_ORDER_BOOK)
        yesterday = datetime.now() - timedelta(days=1)
        coins_ongoing_analysis = []
        coins_ongoing_buy = []
        for event_key in self.event_keys:
            db_collection = db[event_key]
            docs = list(
                db_collection.find(
                    {"_id": {"$gt": yesterday.isoformat()}},
                    {"_id": 1, "coin": 1, "buy": 1},
                )
            )
            for doc in docs:
                if doc["buy"] == True:
                    coins_ongoing_buy.append(doc["coin"])
                coins_ongoing_analysis.append(doc["coin"])
        client.close()
        return coins_ongoing_analysis, coins_ongoing_buy

    def initialize_order_book(self):
        """
        Initialize the order book for all coins.
        
        This function:
        1. Loads risk configuration and gets event keys
        2. Gets list of coins under ongoing analysis and buy events
        3. Reorders the coins list to prioritize:
           - First: coins under active buy events
           - Second: coins under ongoing analysis
           - Last: remaining coins
        4. Initializes order book status and snapshot timing for each coin
        """
        self.logger.info(f'initialize_order_book')
        f = open("/tracker/riskmanagement/riskmanagement.json", "r")
        self.risk_configuration = json.loads(f.read())
        self.event_keys = list(self.risk_configuration["event_keys"].keys())
        self.logger.info(f'event_keys: {self.event_keys}')
        coins_ongoing_analysis, coins_ongoing_buy = self.get_ongoing_analysis_coins()
        self.logger.info(f'coins_ongoing_analysis: {coins_ongoing_analysis}')
        self.logger.info(f'coins_ongoing_buy: {coins_ongoing_buy}')
        # Reorder coins list to prioritize coins under buy event and then coins under ongoing analysis
        self.coins = sorted(self.coins, key=lambda x: x not in coins_ongoing_buy)
        self.coins = sorted(self.coins, key=lambda x: x not in coins_ongoing_analysis)
        self.logger.info(f'Reordered coins list. First 10 coins: {self.coins[:10]}')

        last_snapshot_time = datetime.now()
        for coin in self.coins:
            last_snapshot_time = last_snapshot_time + timedelta(seconds=3)
            self.order_books[coin]['lastUpdateId'] = None
            self.coin_orderbook_initialized[coin] = {'status': False, 'next_snapshot_time': last_snapshot_time}

    def signal_handler(self, signum, frame):
        """Handle signals gracefully"""
        self.logger.info(f"Received signal {signum}, shutting down gracefully")
        self.should_exit = True
        self.stop()
        
    def on_message(self, ws, message):
        try:
            data = json.loads(message)

            # Combined stream messages have a different format
            # They include a 'stream' field that identifies the source
            stream = data.get('stream')
            stream_data = data.get('data')
            
            if not stream or not stream_data:
                self.logger.error(f"Received malformed message: {message[:100]}...")
                return
                
            # Extract coin from stream name (format: "btcusdt@depth")
            coin_with_depth = stream.split('@')[0]
            coin = coin_with_depth.upper()

            if self.under_observation[coin]['status'] and datetime.now() > self.under_observation[coin]['end_observation']:
                if self.BUY[coin]:
                    self.update_trading_event(coin)
                self.initialize_coin_status(coin=coin, start_script=False)

            if self.coin_orderbook_initialized[coin]['status'] == False and datetime.now() > self.coin_orderbook_initialized[coin]['next_snapshot_time']:
                self.get_snapshot(coin)
                self.coin_orderbook_initialized[coin]['status'] = True
                        
            if self.order_books[coin]['lastUpdateId'] is None:
                self.buffered_events[coin].append(stream_data)
                return

            self.process_update(stream_data, coin)
            
            #if datetime.now().second <= 10 and datetime.now().second >= 5:
            # self.logger.info(f'coin: {coin}; position: {self.coins.index(coin)}')
            
            # If the order distribution is greater than threshold and db update time not reached
            if (self.under_observation[coin]['status']) and (self.last_ask_order_distribution_1level.get(coin, 0) > self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD and
                self.last_bid_order_distribution_1level.get(coin, 0) > self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD and
                (datetime.now() - self.last_db_update_time[coin]).total_seconds() < self.DB_UPDATE_MAX_WAITING_TIME):
                pass
            elif (self.under_observation[coin]['status']) and (self.current_price[coin] != 0):
                self.analyze_order_book(coin)



        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            # Continue running despite errors

    def on_error(self, ws, error):
        self.logger.error(f"WebSocket Error: {error}")
        # Don't stop if there's an error, just log it

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        self.logger.info(f"WebSocket Connection Closed with code {close_status_code}: {close_msg}")
        if self.running and not self.should_exit:
            self.logger.info(f"Attempting to reconnect...")
            try:
                # No retry limit - continue trying forever
                self.retry_connection += 1
                
                # Reset connection-related state
                with self.connection_lock:
                    # Clear WebSocket references
                    self.ws = None
                    self.ws_thread = None
                    self.last_pong_time = None
                    
                    # Don't reset order book state for all coins
                    # Instead, we'll get new snapshots for each coin when we reconnect
                    self.running = False
                
                # Exponential backoff with jitter, but cap at 5 minutes
                backoff_time = min(300, 5 * (2 ** min(8, self.retry_connection - 1)))
                jitter = backoff_time * 0.2 * (2 * random.random() - 1)  # ±20% jitter
                sleep_time = max(1, backoff_time + jitter)
                
                self.logger.info(f"Waiting {sleep_time:.2f}s before reconnecting (attempt {self.retry_connection})")
                sleep(sleep_time)
                
                # Reset retry counter after long delay to avoid endless exponential growth
                if self.retry_connection > 10:
                    self.logger.info("Resetting retry counter after multiple attempts")
                    self.retry_connection = 0
                
                # Start new connection
                self.logger.info(f"Starting new WebSocket connection...")
                self.start()
            except Exception as e:
                self.logger.error(f"Error during reconnection: {e}")
                # If reconnection fails, try again after a delay
                self.logger.info(f"Retrying reconnection after error...")
                sleep(10)
                self.on_close(ws, close_status_code, close_msg)

    def on_open(self, ws):
        """Handle WebSocket connection open"""
        self.initialize_order_book()
        self.logger.info(f'parameters: {self.parameters}')
        subscribe_message = {"method": "SUBSCRIBE", "params": self.parameters, "id": 1}
        ws.send(json.dumps(subscribe_message))

    def on_ping(self, ws, message):
        """Handle ping from server by sending pong"""
        try:
            with self.connection_lock:
                if self.ws and self.ws.sock and self.ws.sock.connected:
                    ws.send(message, websocket.ABNF.OPCODE_PONG)
                    self.last_pong_time = datetime.now()
        except Exception as e:
            self.logger.error(f"Error sending pong: {e}")
            self.reconnect()

    def on_pong(self, ws, message):
        """Handle pong from server by updating last pong time"""
        try:
            with self.connection_lock:
                self.last_pong_time = datetime.now()
        except Exception as e:
            self.logger.error(f"Error handling pong: {e}")
            self.reconnect()

    def reconnect(self):
        """Handle reconnection logic"""
        if self.running and not self.should_exit:
            self.logger.info(f"Reconnecting...")
            self.stop()
            sleep(5)  # Wait before reconnecting
            self.start()

    def check_connection(self):
        """Periodically check connection health"""
        while self.running and not self.should_exit:
            try:
                if self.last_pong_time and (datetime.now() - self.last_pong_time).total_seconds() > 30:
                    self.logger.warning(f"No pong received for 30 seconds, reconnecting...")
                    self.reconnect()
                sleep(10)  # Check every 10 seconds
            except Exception as e:
                self.logger.error(f"Error in connection check: {e}")
                sleep(5)

    def get_snapshot(self, coin):
        """Get the initial order book snapshot"""
        # self.logger.info(f"Getting snapshot from {self.snapshot_url}")
        #self.logger.info(f'restart: {self.RESTART}')
        
        #self.wait_for_snapshot()
        try:
            if self.order_books[coin]['lastUpdateId'] is not None:
                self.logger.info(f'Getting snapshot for coin: {coin}')
            snapshot_url = f"https://api.binance.com/api/v3/depth?symbol={coin}&limit=5000"
            response = requests.get(snapshot_url)
        except Exception as e:
            self.logger.error(f"coin: {self.coin}; SNAPSHOT ERROR: {e}")
            return

        #self.logger.info(f"Snapshot response: {response.status_code}")
        if response.status_code == 200:
            snapshot = response.json()
            self.order_books[coin]['bids'] = {float(price): float(qty) for price, qty in snapshot['bids']}
            self.order_books[coin]['asks'] = {float(price): float(qty) for price, qty in snapshot['asks']}
            self.order_books[coin]['lastUpdateId'] = snapshot['lastUpdateId']
            
            # self.logger.info(f"Snapshot received with lastUpdateId: {self.order_books[coin]['lastUpdateId']}")
            
            # Initialize price if not set
            if self.initial_price is None:
                self.initial_price = float(snapshot['asks'][0][0])
                self.max_price = self.initial_price
                # self.logger.info(f"Initial price set to: {self.initial_price}")
            
            self.process_buffered_events(coin)
        else:
            self.logger.error(f"Error getting snapshot: {response.status_code}: {response.text}")

    def process_buffered_events(self, coin):
        """Process events that were received before the snapshot for a specific coin"""
        for event in self.buffered_events[coin]:
            if event['u'] <= self.order_books[coin]['lastUpdateId']:
                continue
            self.process_update(event, coin)
        self.buffered_events[coin] = []

    def process_update(self, event, coin):
        """Process a single order book update event for a specific coin"""
        if event['u'] <= self.order_books[coin]['lastUpdateId']:
            return

        if event['U'] > self.order_books[coin]['lastUpdateId'] + 1:
            self.order_books[coin]['lastUpdateId'] = None
            self.get_snapshot(coin)
            return

        for price, qty in event['b']:
            price = float(price)
            qty = float(qty)
            if qty == 0:
                self.order_books[coin]['bids'].pop(price, None)
            else:
                self.order_books[coin]['bids'][price] = qty

        for price, qty in event['a']:
            price = float(price)
            qty = float(qty)
            if qty == 0:
                self.order_books[coin]['asks'].pop(price, None)
            else:
                self.order_books[coin]['asks'][price] = qty

        if self.order_books[coin]['asks']:
            self.current_price[coin] = min(self.order_books[coin]['asks'].keys())
            if self.initial_price[coin] == 0:
                self.initial_price[coin] = self.current_price[coin]

        self.order_books[coin]['lastUpdateId'] = event['u']

    def update_ask_order_distribution_list(self, ask_order_distribution, coin):
        """Update the ask order distribution list for a specific coin"""
        self.ask_order_distribution_list[coin].append({
            'ask': ask_order_distribution,
            'dt': datetime.now()
        })
        # Keep only the last N objects based on datetime, where N is specified in strategy parameters
        if len(self.ask_order_distribution_list[coin]) > self.under_observation[coin]['riskmanagement_configuration']['last_i_ask_order_distribution']:
            self.ask_order_distribution_list[coin].sort(key=lambda x: x['dt'], reverse=True)
            self.ask_order_distribution_list[coin] = self.ask_order_distribution_list[coin][:self.under_observation[coin]['riskmanagement_configuration']['last_i_ask_order_distribution']]
        
        return self.ask_order_distribution_list[coin]

    def update_bid_order_distribution_list(self, bid_order_distribution, coin):
        """Update the bid order distribution list for a specific coin"""
        if not hasattr(self, 'bid_order_distribution_list'):
            self.bid_order_distribution_list = {}
        
        if coin not in self.bid_order_distribution_list:
            self.bid_order_distribution_list[coin] = []
            
        self.bid_order_distribution_list[coin].append({
            'bid': bid_order_distribution,
            'dt': datetime.now()
        })
        
        # Keep only the last N objects based on datetime
        if len(self.bid_order_distribution_list[coin]) > self.under_observation[coin]['riskmanagement_configuration']['last_i_bid_order_distribution']:
            self.bid_order_distribution_list[coin].sort(key=lambda x: x['dt'], reverse=True)
            self.bid_order_distribution_list[coin] = self.bid_order_distribution_list[coin][:self.under_observation[coin]['riskmanagement_configuration']['last_i_bid_order_distribution']]
        
        return self.bid_order_distribution_list[coin]

    def analyze_order_book(self, coin):
        """Analyze the order book and update trading state for a specific coin"""
        try:
            
            # Calculate price levels and distributions
            bid_orders = [(price, qty) for price, qty in self.order_books[coin]['bids'].items()]
            ask_orders = [(price, qty) for price, qty in self.order_books[coin]['asks'].items()]
            
            # Sort orders by price
            bid_orders.sort(key=lambda x: x[0], reverse=True)
            ask_orders.sort(key=lambda x: x[0])
            
            # Calculate cumulative volumes
            total_bid_volume = sum(price * qty for price, qty in bid_orders)
            total_ask_volume = sum(price * qty for price, qty in ask_orders)
            
            # Calculate summary orders with delta intervals
            delta = 0.01
            summary_bid_orders = []
            summary_ask_orders = []
            next_delta_threshold = 0 + delta
            
            # Calculate summary bid orders
            cumulative_bid_volume = 0
            for price, qty in bid_orders:
                price_order = float(price)
                quantity_order = float(qty)
                cumulative_bid_volume += price_order * quantity_order
                cumulative_bid_volume_ratio = self.round_((cumulative_bid_volume / total_bid_volume), 2)
                
                if cumulative_bid_volume_ratio >= next_delta_threshold:
                    summary_bid_orders.append((
                        self.round_((price_order - self.current_price[coin]) / self.current_price[coin], 3),
                        cumulative_bid_volume_ratio
                    ))
                    next_delta_threshold = cumulative_bid_volume_ratio + delta
            
            # Calculate summary ask orders
            next_delta_threshold = 0 + delta
            cumulative_ask_volume = 0
            for price, qty in ask_orders:
                price_order = float(price)
                quantity_order = float(qty)
                cumulative_ask_volume += price_order * quantity_order
                cumulative_ask_volume_ratio = self.round_((cumulative_ask_volume / total_ask_volume), 2)
                
                if cumulative_ask_volume_ratio >= next_delta_threshold:
                    summary_ask_orders.append((
                        self.round_((price_order - self.current_price[coin]) / self.current_price[coin], 3),
                        cumulative_ask_volume_ratio
                    ))
                    next_delta_threshold = cumulative_ask_volume_ratio + delta
            
            # Calculate price levels and distributions using summary orders
            if not hasattr(self, 'bid_price_levels_dt'):
                self.bid_price_levels_dt = {}
                self.ask_price_levels_dt = {}
                
            self.bid_price_levels_dt[coin], bid_order_distribution, bid_cumulative_level = self.get_price_levels(
                self.current_price[coin], summary_bid_orders, 
                self.under_observation[coin]['riskmanagement_configuration']['strategy_jump'],
                self.under_observation[coin]['riskmanagement_configuration']['limit'],
                self.under_observation[coin]['riskmanagement_configuration']['price_change_jump']
            )
            
            self.ask_price_levels_dt[coin], ask_order_distribution, ask_cumulative_level = self.get_price_levels(
                self.current_price[coin], summary_ask_orders,
                self.under_observation[coin]['riskmanagement_configuration']['strategy_jump'],
                self.under_observation[coin]['riskmanagement_configuration']['limit'],
                self.under_observation[coin]['riskmanagement_configuration']['price_change_jump']
            )

            self.logger.info(f'coin: {coin}; ask_order_distribution: {ask_order_distribution}; bid_order_distribution: {bid_order_distribution}')
            
            # Update order distribution tracking
            self.last_ask_order_distribution_1level[coin] = ask_order_distribution[str(self.under_observation[coin]['riskmanagement_configuration']['price_change_jump'])]
            self.last_bid_order_distribution_1level[coin] = bid_order_distribution[str(self.under_observation[coin]['riskmanagement_configuration']['price_change_jump'])]
            
            # Check BUY Trading Conditions or Update Order Book Record based on thresholds
            current_time = datetime.now()
            if self.last_ask_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD:
                # Update if enough time passed or if we detected low order distribution first time
                if ((current_time - self.last_db_update_time[coin]).total_seconds() >= self.DB_UPDATE_MIN_WAITING_TIME or 
                    (self.last_ask_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD and 
                     not self.ask_1firstlevel_orderlevel_detected[coin])):
                    
                    self.ask_order_distribution_list[coin] = self.update_ask_order_distribution_list(ask_order_distribution, coin)
                    if not self.BUY[coin]:
                        self.check_buy_trading_conditions(coin)
                    self.update_order_book_record(
                        total_bid_volume,
                        total_ask_volume,
                        summary_bid_orders,
                        summary_ask_orders,
                        coin
                    )
                    if self.last_ask_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
                        self.ask_1firstlevel_orderlevel_detected[coin] = True
                    self.last_db_update_time[coin] = current_time
            
            elif self.last_bid_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD:
                if ((current_time - self.last_db_update_time[coin]).total_seconds() >= self.DB_UPDATE_MIN_WAITING_TIME or
                    (self.last_bid_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD and 
                     not self.bid_1firstlevel_orderlevel_detected[coin])):
                    
                    self.update_order_book_record(
                        total_bid_volume,
                        total_ask_volume,
                        summary_bid_orders,
                        summary_ask_orders,
                        coin
                    )
                    if self.last_bid_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
                        self.bid_1firstlevel_orderlevel_detected[coin] = True
                    self.last_db_update_time[coin] = current_time
            else:
                self.last_db_update_time[coin] = current_time
            
            # Reset the low order level detected flag if the order distribution is greater than threshold
            if self.ask_1firstlevel_orderlevel_detected[coin] and self.last_ask_order_distribution_1level[coin] > self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
                self.ask_1firstlevel_orderlevel_detected[coin] = False
            if self.bid_1firstlevel_orderlevel_detected[coin] and self.last_bid_order_distribution_1level[coin] > self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
                self.bid_1firstlevel_orderlevel_detected[coin] = False
                
        except Exception as e:
            self.logger.error(f"Error analyzing order book for {coin}: {e}")

    @staticmethod
    def round_(number, decimal):
        """Round a number to a specified number of decimal places"""
        return float(format(number, f".{decimal}f"))

    @staticmethod
    def count_decimals(num):
        """Determines the number of decimal places in a given number"""
        try:
            str_num = str(num)
            decimal_index = str_num.index('.')
            return len(str_num) - decimal_index
        except ValueError:
            # If no decimal point is found, it's an integer
            return 1

    def update_order_book_record(self, total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, coin):
        """Update the order book record in the database for a specific coin"""
        try:
            now = datetime.now().replace(microsecond=0).isoformat()
            new_data = [
                self.current_price[coin],  # ask_price
                total_bid_volume,
                total_ask_volume,
                summary_bid_orders,
                summary_ask_orders
            ]

            filter_query = {"_id": self.under_observation[coin]['start_observation']}
            update_doc = {
                "$set": {
                    f"data.{now}": new_data,
                    "weight": 250,
                    "max_price": self.max_price[coin],
                    "initial_price": self.initial_price[coin],
                    "summary_jump_price_level": self.summary_jump_price_level.get(coin, {}),
                    "ask_order_distribution_list": self.ask_order_distribution_list.get(coin, []),
                    "buy": self.BUY[coin],
                    "buy_price": self.buy_price[coin]
                }
            }
            
            result = self.db_collection.update_one(filter_query, update_doc)
            if result.modified_count != 1:
                self.logger.error(f"Order Book update failed for {self.event_key} with id {self.id} for coin {coin}")
        except Exception as e:
            self.logger.error(f"Error updating order book record for {coin}: {e}")

    def get_price_levels(self, price, orders, cumulative_volume_jump=0.03, price_change_limit=0.4, price_change_jump=0.025):
        '''
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
        '''
        previous_level = 0
        price_levels = []
        n_decimals = self.count_decimals(price)
        cumulative_level_without_jump = 0
        price_change_level = price_change_jump
        order_distribution = {}
        for i in arange(price_change_jump, price_change_limit+price_change_jump, price_change_jump):
            order_distribution[str(self.round_(i,3))] = 0
        previous_cumulative_level = 0

        for level in orders:
            cumulative_level = level[1]
            price_change = level[0]

            # in this "if condition", I see how orders are distributed along the (0,price_change_limit) range,
            #  the chunks of orders are divided relative to "price_change_jump" (i.e. if var=0.025, I check in the [0,2.5%] price change window how deep is the order book and so on)
            # if price_change is below next threshold, keep updating the cumulative volume level for that price level
            if abs(price_change) <= price_change_level:
                order_distribution[str(price_change_level)] = cumulative_level
            
            # in case is above next threshold, update price_change_level and initialize next price_change_level
            else:
                # before moving to the next price level, update the relative level
                order_distribution[str(price_change_level)] = self.round_(order_distribution[str(price_change_level)] - previous_cumulative_level,2)

                # now update the next price change level
                previous_cumulative_level += order_distribution[str(price_change_level)]
                price_change_level = self.round_(price_change_level + price_change_jump,3)

                # in case some price level is empty, skip to the next level
                while abs(price_change) > price_change_level:
                    price_change_level = self.round_(price_change_level + price_change_jump,3)

                # next chunk is below next thrshold
                if abs(price_change) <= price_change_level and abs(price_change) <= price_change_limit:
                    order_distribution[str(price_change_level)] = cumulative_level

            # here, I discover the jumps, the info is stored in "price_levels"
            if cumulative_level - previous_level >= cumulative_volume_jump and abs(price_change) <= price_change_limit and abs(price_change) >= 0.01:
                actual_jump = self.round_(cumulative_level - previous_level,3)
                price_level = price * (1+price_change)
                #info = (self.round_(price_level,n_decimals), price_change, cumulative_level, actual_jump)
                #price_levels.append(info)
                price_levels.append(self.round_(price_level,n_decimals))
            elif abs(price_change) <= price_change_limit:
                cumulative_level_without_jump = cumulative_level

            if abs(price_change) > price_change_limit:
                break
            previous_level = cumulative_level
        
        # scale order_distribution to [0,100] range
        for lvl in order_distribution:
            if previous_cumulative_level != 0:
                order_distribution[lvl] = self.round_(order_distribution[lvl] / previous_cumulative_level,3)
            else:
                order_distribution[lvl] = 0
        
        # if there are not jumps, at least I want to get the cumulative volume at the limit price level
        if len(price_levels) == 0:
            info = (None, None, cumulative_level_without_jump, False)
            price_levels.append(None)
        
        return price_levels, order_distribution, self.round_(previous_cumulative_level,3)

    def wait_for_snapshot(self):
        """Wait for the snapshot to be ready, in order to avoid rate limit ban"""
        try:
            _, live_order_book_scripts_number, _, last_snapshots = PooledBinanceOrderBook.get_current_number_of_orderbook_scripts(self.db, self.event_keys, self.RESTART)
            
            if live_order_book_scripts_number >= 20:
                not_ready_to_go = True
                while not_ready_to_go and not self.should_exit:
                    if not self.RESTART and len(last_snapshots) > 20:
                        remaining_wait = 60 - (datetime.now() - last_snapshots[-1]['dt']).total_seconds()
                        if remaining_wait > 0:
                            sleep(remaining_wait)
                        _, _, _, last_snapshots = PooledBinanceOrderBook.get_current_number_of_orderbook_scripts(self.db, self.event_keys, self.RESTART)
                        if len(last_snapshots) < 20:
                            not_ready_to_go = False
                    elif self.RESTART:
                        for obj in last_snapshots:
                            if obj['id'] == self.id:
                                position = last_snapshots.index(obj)
                                sleep(3*position)
                                not_ready_to_go = False
                    else:
                        sleep_seconds = (self.number_script % 20) * 3
                        current_second = datetime.now().second
                        if current_second > sleep_seconds:
                            sleep(60 - (current_second - sleep_seconds))
                        else:
                            sleep(sleep_seconds - current_second)
                        not_ready_to_go = False

            # Update last restart time even if there's an error
            self.db_collection.update_one(
                {"_id": self.id},
                {"$set": {"last_restart": datetime.now().isoformat()}}
            )
        except Exception as e:
            self.logger.error(f"Error in wait_for_snapshot: {e}")
            # Continue despite errors, with a small delay
            sleep(5)

    def start(self):
        """Start the WebSocket connection for all coins"""
        with self.connection_lock:
            if self.running:
                return
            self.running = True
        
        try:
            # Wait for snapshot to avoid rate limit
            #self.wait_for_snapshot()
            self.initialize_order_book()
            websocket.enableTrace(False)
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open,
                on_ping=self.on_ping,
                on_pong=self.on_pong
            )

            self.ws.run_forever()
            
            # self.ws_thread = threading.Thread(target=self.ws.run_forever, kwargs={
            #     'ping_interval': self.ping_interval,
            #     'ping_timeout': self.ping_timeout
            # })
            # self.ws_thread.daemon = True
            # self.ws_thread.start()
            
        except Exception as e:
            self.logger.error(f"Error in start method: {e}")
            # Release the lock if start fails
            with self.connection_lock:
                self.running = False
            # Try to restart after a delay
            if not self.should_exit:
                self.logger.info("Attempting to restart after error...")
                sleep(10)
                self.start()

    def stop(self):
        """Stop the WebSocket connection"""
        with self.connection_lock:
            if not self.running:
                return
            self.running = False
            if self.ws:
                try:
                    self.ws.close()
                except Exception as e:
                    self.logger.error(f"Error closing WebSocket: {e}")

    def update_trading_event(self, coin):
        """Update trading event with sell information for a specific coin"""
        try:

            sell_price = self.current_price[coin]
            sell_ts = datetime.now().isoformat()
            gain = (sell_price - self.buy_price[coin]) / self.buy_price[coin]
            self.logger.info(f"SELL EVENT: {coin} at {sell_price}. GAIN: {gain}")
            # Update metadata collection with sell price
            
            metadata_filter = {
                "_id": self.under_observation[coin]['start_observation'],
            }
            update_doc = {
                "$set": {
                    "sell_price": sell_price,
                    "status": "completed"
                }
            }
            
            result = self.metadata_orderbook_collection.update_one(metadata_filter, update_doc)
            if result.modified_count != 1:
                self.logger.error(f"Metadata Collection update failed for {coin}")
            
            filter_query = {"_id": self.under_observation[coin]['start_observation']}
            update_doc = {
                "$set": {
                    "gain": gain,
                    "sell_ts": sell_ts,
                    "sell_price": sell_price
                }
            }
            
            result = self.trading_collection.update_one(filter_query, update_doc)
            if result.modified_count != 1:
                self.logger.error(f"Trading Collection update failed for {coin} with id {self.id}_{coin}")
            
        except Exception as e:
            self.logger.error(f"Error updating trading event for {coin}: {e}")

    def check_buy_trading_conditions(self, coin):
        """Check if trading conditions are met for a specific coin"""
        try:
            if self.current_price[coin] > self.max_price[coin]:  
                self.max_price[coin] = self.current_price[coin]
                
            max_change = (self.max_price[coin] - self.initial_price[coin]) / self.initial_price[coin]
            current_price_drop = abs((self.current_price[coin] - self.max_price[coin]) / self.max_price[coin])
            is_jump_price_level = self.hit_jump_price_levels_range(coin)
            
            if (is_jump_price_level and 
                current_price_drop >= self.strategy_parameters['price_drop_limit'] and 
                max_change <= self.strategy_parameters['max_limit']):
                
                # Check ask order distribution
                all_levels_valid = True
                avg_distribution = self.calculate_average_distribution(self.ask_order_distribution_list[coin])
                keys_ask_order_distribution = list(avg_distribution.keys())[:self.strategy_parameters['lvl_ask_order_distribution_list']]
                
                for lvl in keys_ask_order_distribution:
                    if avg_distribution[lvl] >= self.strategy_parameters['max_ask_order_distribution_level']:
                        all_levels_valid = False
                        break

                if all_levels_valid:
                    self.BUY[coin] = True
                    self.buy_price[coin] = self.current_price[coin]
                    self.logger.info(f"BUY EVENT: {coin} at {self.current_price[coin]}")
                    self.save_trading_event(coin)
        except Exception as e:
            self.logger.error(f"Error checking buy trading conditions for {coin}: {e}")

    def calculate_average_distribution(self, distributions):
        """Calculate average order distribution"""
        avg_distribution = {}
        try:
            for dist in distributions:
                for level, value in dist['ask'].items():
                    if level not in avg_distribution:
                        avg_distribution[level] = []
                    avg_distribution[level].append(value)
            
            return {level: sum(values) / len(values) for level, values in avg_distribution.items()}
        except Exception as e:
            self.logger.error(f"Error calculating average distribution: {e}")
            return avg_distribution

    def hit_jump_price_levels_range(self, coin, neighborhood_of_price_jump = 0.005):
        '''
        This function defines all the historical level whereas a jump price change was existent
        Since it can happen that price_jump_level are not always in the same point (price) I check if the jump price is in the neighboorhood of the historical jump price (average with np_mean)

        # THIS IS THE INPUT OF bid_price_levels
        Structure of ask price_levels: IT IS A LIST OF LISTS
        - bid_price_levels: e.g. [[13.978], [13.958], [13.958], [13.978], [13.949, 12.942], [13.97], [13.939, 12.933], [14.053]]
        EACH SUBLIST containes the jump prices at dt

        # THIS THE STRUCTURE OF SUMMARY_JUMP_PRICE
        [LIST [TUPLES]]
            [ (average price jump level, list_of_jump_price_levels )]
        '''
        try:
            if not hasattr(self, 'bid_price_levels_dt') or coin not in self.bid_price_levels_dt:
                return False
                
            # Make sure the summary_jump_price_level for this coin is initialized
            if coin not in self.summary_jump_price_level:
                self.summary_jump_price_level[coin] = {}
                
            # iterate through price levels
            for abs_price in self.bid_price_levels_dt[coin]:
                if abs_price is None:
                    continue
                    
                if len(self.summary_jump_price_level[coin]) != 0:
                    IS_NEW_X = True
                    for x in self.summary_jump_price_level[coin]:
                        historical_price_level = self.summary_jump_price_level[coin][x][0]
                        historical_price_level_n_obs = self.summary_jump_price_level[coin][x][1]
                        if abs_price <= historical_price_level * (1 + neighborhood_of_price_jump) and abs_price >= historical_price_level * (1 - neighborhood_of_price_jump):
                            historical_price_level = (historical_price_level*historical_price_level_n_obs + abs_price) / (historical_price_level_n_obs + 1)
                            self.summary_jump_price_level[coin][x] = (historical_price_level, historical_price_level_n_obs + 1)
                            IS_NEW_X = False
                            break
                    if IS_NEW_X:
                        list_x = [int(i) for i in list(self.summary_jump_price_level[coin].keys())]
                        new_x = str(max(list_x) + 1) if list_x else '1'
                        self.summary_jump_price_level[coin][new_x] = (abs_price, 1)
                else:
                    self.summary_jump_price_level[coin]['1'] = (abs_price, 1)
                    
            for x in self.summary_jump_price_level[coin]:
                jump = abs((self.current_price[coin] - self.summary_jump_price_level[coin][x][0]) / self.current_price[coin])
                historical_price_level_n_obs = self.summary_jump_price_level[coin][x][1]
                
                if jump <= self.strategy_parameters['distance_jump_to_current_price'] and historical_price_level_n_obs >= self.strategy_parameters['min_n_obs_jump_level']:
                    return True
            
            return False
        except Exception as e:
            self.logger.error(f"Error in hit_jump_price_levels_range for {coin}: {e}")
            return False
    
    def save_trading_event(self, coin):
        """Save trading event to database"""
        try:
            buy_ts = datetime.now().isoformat()
            self.trading_collection.insert_one({
                "_id": self.under_observation[coin]['start_observation'],
                "coin": coin,
                "gain": '',
                "buy_price": self.buy_price[coin],
                "sell_price": '',
                "buy_ts": buy_ts,
                "sell_ts": '',
                "ranking": self.under_observation[coin]['ranking'],
                "initial_price": self.initial_price[coin],
                "max_price": self.max_price[coin],
                "strategy": self.under_observation[coin]['riskmanagement_configuration']
            })
            end_observation = max(self.under_observation[coin]['end_observation'], self.datetime_now + timedelta(minutes=self.MAX_WAITING_TIME_AFTER_BUY))
            self.metadata_orderbook_collection.update_one({"_id": self.under_observation[coin]['start_observation']}, {"$set": {"buy_price": self.buy_price[coin], "end_observation": end_observation}})
        except Exception as e:
            self.logger.error(f"Error saving trading event for {coin}: {e}")

    # def monitor_and_restart(self):
    #     """Monitor the main connection and restart if needed"""
    #     while not self.should_exit:
    #         sleep(60)  # Check every minute
            
    #         # Only restart if not running and we're not trying to exit
    #         if not self.running and not self.should_exit:
    #             self.logger.info("Connection monitor detected stopped connection, restarting...")
    #             try:
    #                 # Make sure client is closed before reconnecting
    #                 if self.client:
    #                     try:
    #                         self.client.close()
    #                     except:
    #                         pass
    #                 # Reestablish database connection
    #                 self.client = DatabaseConnection()
    #                 self.db = self.client.get_db(DATABASE_ORDER_BOOK)
    #                 self.db_collection = self.db[event_key]
                    
    #                 # Start the connection
    #                 self.start()
    #             except Exception as e:
    #                 self.logger.error(f"Error in monitor_and_restart: {e}")
        
    #     # Final cleanup when exiting
    #     if self.client:
    #         try:
    #             self.client.close()
    #         except:
    #             pass

    @staticmethod
    def get_current_number_of_orderbook_scripts(db, event_keys, restart):
        """Get the current number of order book scripts running"""
        live_order_book_scripts_number = 0
        numbers_filled = []
        current_weight = 0
        last_minute_snapshots = []
        for collection in event_keys['event_keys']:
            if collection != COLLECTION_ORDERBOOK_METADATA:
                minutes_timeframe = int(PooledBinanceOrderBook.extract_timeframe(collection))
                yesterday = datetime.now() - timedelta(minutes=minutes_timeframe)
                query = {"_id": {"$gt": yesterday.isoformat()}}
                docs = list(db[collection].find(query, {"_id": 1, "number": 1, "weight":1, "last_restart": 1}))

                for doc in docs:
                    numbers_filled.append(doc["number"])
                    if "weight" in doc:
                        current_weight += doc["weight"]
                    else:
                        current_weight += 250
                    if "last_restart" in doc:
                        last_minute_snapshots.append({'id': doc['_id'], 'dt': datetime.fromisoformat(doc["last_restart"])})
                    else:
                        last_minute_snapshots.append({'id': doc['_id'], 'dt': datetime.fromisoformat(doc["_id"])})
                
                live_order_book_scripts_number += len(docs)

        if not restart:
            last_minute_snapshots = [dt for dt in last_minute_snapshots if (datetime.now() - dt['dt']).total_seconds() <= 60]
            last_minute_snapshots.sort(key=lambda x: x['dt'], reverse=True)

        return numbers_filled, live_order_book_scripts_number, current_weight, last_minute_snapshots

    @staticmethod
    def extract_timeframe(input_string):
        """Extract the timeframe value from the input string"""
        match = re.search(r"timeframe:(\d+)", input_string)
        if match:
            return match.group(1)
        else:
            return None

    @staticmethod
    def get_number_script(numbers_filled, live_order_book_scripts_number):
        """Get the next available script number"""
        for i in range(live_order_book_scripts_number + 1):
            if i not in numbers_filled:
                number_script = i
                break
        return number_script

    @staticmethod
    def get_coins():
        type_USDT = os.getenv("TYPE_USDT")
        dir_info = "/tracker/json" 
        coins_list_path = f"{dir_info}/coins_list.json"
        f = open(coins_list_path, "r")
        coins_list_info = json.loads(f.read())
        coins = coins_list_info[type_USDT][:200]
        #coins = coins[:20]
        return coins

    def search_volatility_event_trigger(self, start_script=False):
        """Search for volatility event trigger"""
        if start_script:
            status = "running"
            timeframe_36hours = (datetime.now() - timedelta(days=1) - timedelta(hours=self.MAX_WAITING_TIME_AFTER_BUY)).isoformat()
            timeframe_24hours = (datetime.now() - timedelta(days=1)).isoformat()
            
            # Query to find:
            # - Documents created in last 36 hours (_id > timeframe_36hours)
            # - With status "running" 
            # - That have a buy value (buy not equal to 0)
            # - And either:
            #   - Have no sell value (sell = 0) OR
            # Returns only coin and event_key fields

            metadata_docs = list(self.metadata_orderbook_collection.find({ "_id": {"$gt": timeframe_36hours}, "status": status,"buy_price": {"$ne": 0},"sell_price": 0},
                                                                {"coin": 1, "event_key": 1, "end_observation": 1, "riskmanagement_configuration": 1, "buy_price": 1, "ranking": 1} ))
            if len(metadata_docs) != 0:
                for doc in metadata_docs:
                    self.under_observation[doc["coin"]] = {'status': True, 'start_observation': doc["_id"], 'end_observation': doc["end_observation"], 'riskmanagement_configuration': doc["riskmanagement_configuration"], "ranking": doc["ranking"]}
                    self.buy_price[doc["coin"]] = doc["buy_price"]
                    self.BUY[doc["coin"]] = True
                    self.logger.info(f"Coin {doc['coin']} is under observation. Buy Status: True")

            metadata_docs = list(self.metadata_orderbook_collection.find({ "_id": {"$gt": timeframe_24hours}, "status": status}, {"coin": 1, "event_key": 1, "end_observation": 1, "riskmanagement_configuration": 1, "ranking": 1} ))
            if len(metadata_docs) != 0:
                for doc in metadata_docs:
                    if not self.BUY[doc["coin"]]:
                        self.under_observation[doc["coin"]] = {'status': True, 'start_observation': doc["_id"], 'end_observation': doc["end_observation"], 'riskmanagement_configuration': doc["riskmanagement_configuration"], "ranking": doc["ranking"]}
                        self.logger.info(f"Coin {doc['coin']} is under observation. Buy Status: False")
            return
        else:
            while True:
                status = "pending"
                timeframe_1day = (datetime.now() - timedelta(days=1)).isoformat()
                metadata_docs = list(self.metadata_orderbook_collection.find({"_id": {"$gt": timeframe_1day}, "status": status}, {"coin": 1, "event_key": 1, "end_observation": 1,"ranking": 1}))
                self.logger.info(f"metadata_docs: {metadata_docs}")
                if len(metadata_docs) != 0:
                    riskmanagement_configuration = self.get_riskmanagement_configuration()
                    for doc in metadata_docs:
                        self.logger.info(f"Coin {doc['coin']} is under observation. Buy Status: False")
                        self.under_observation[doc["coin"]] = {'status': True, 'start_observation': doc["_id"], 'end_observation': doc["end_observation"], 'riskmanagement_configuration': riskmanagement_configuration, "ranking": doc["ranking"]}
                        update_doc = { "$set": { "status": "running", "riskmanagement_configuration": riskmanagement_configuration } }
                        self.metadata_orderbook_collection.update_one({"_id": doc["_id"]}, update_doc)

                next_run = 60 - datetime.now().second - datetime.now().microsecond / 1000000 + 5
                self.logger.info(f"next_run: {next_run}")
                sleep(next_run)
                        

    def restart_connection(self):
        with self.connection_restart_lock:
            if self.connection_restart_running:
                return
            self.connection_restart_running = True

        try:
            now = datetime.now()
            # compute how many seconds until next restart
            remaining_seconds = 60 - now.second + 5
            minutes_remaining = 59 - now.minute
            hours_remaining = 24 - now.hour - 1

            # total seconds until next wss restart
            total_remaining_seconds = remaining_seconds + minutes_remaining * 60 + hours_remaining * 60 * 60

            # ONLY FOR TESTING
            #total_remaining_seconds = 1 + remaining_seconds

            # define timestamp for next wss restart
            wss_restart_timestamp = (now + timedelta(seconds=total_remaining_seconds)).isoformat()
            self.logger.info(f'on_restart_connection: Next wss restart {wss_restart_timestamp}')

            sleep(total_remaining_seconds)
            self.logger.info(f"Restart Connection")
        finally:
            with self.connection_restart_lock:
                self.connection_restart_running = False
                self.ws.close()

if __name__ == "__main__":
    # Get command line arguments
    coins = PooledBinanceOrderBook.get_coins()
    print(coins)
    pooled_order_book = PooledBinanceOrderBook(coins=coins)
    # in case of restart, check if there are already coins under observation
    pooled_order_book.search_volatility_event_trigger(start_script=True)
    # set a restart connection thread to be executed every at midnight
    threading.Thread(target=pooled_order_book.restart_connection).start()
    # set a thread to be executed every 60 seconds to check if there are coins under observation
    threading.Thread(target=pooled_order_book.search_volatility_event_trigger).start()
    # start the pooled order book script
    pooled_order_book.start()