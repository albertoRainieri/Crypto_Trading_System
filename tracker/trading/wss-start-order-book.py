import websocket
import json
import requests
from typing import Dict, List, Optional
import threading
import time
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))
from datetime import datetime, timedelta
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from signal import SIGKILL
from numpy import arange, linspace
from constants.constants import *
import re
from time import sleep


class BinanceOrderBook:
    def __init__(self, coin: str, event_key: str, id: str, ranking: str, strategy_parameters: dict, event_keys: list, stop_script_datetime: datetime, restart: bool = False, number_script: int = 0,
                 ask_order_distribution_list: list = [], summary_jump_price_level: dict = {}, initial_price: float = 0, max_price: float = 0, buy_price: float = 0, BUY: bool = False, minutes_timeframe: int = 0):
        
        self.coin = coin.upper()
        self.coin_lower = coin.lower()
        self.event_key = event_key
        self.id = id
        self.ranking = ranking
        self.strategy_parameters = strategy_parameters
        self.retry_connection = 0
        # Order book state
        self.order_book = {
            'bids': {},  # price -> quantity
            'asks': {},  # price -> quantity
            'lastUpdateId': None
        }
        
        # Trading state
        self.buffered_events = []
        self.ws = None
        self.running = False

        # Order book state
        self.BUY = BUY
        self.RESTART = restart
        self.event_keys = event_keys
        self.stop_script_datetime = stop_script_datetime
        self.summary_jump_price_level = summary_jump_price_level
        self.ask_order_distribution_list = ask_order_distribution_list
        self.max_price = max_price
        self.initial_price = initial_price
        self.buy_price = buy_price
        self.last_db_update_time = datetime.now()
        self.last_ask_order_distribution_1level = 0
        self.last_bid_order_distribution_1level = 0
        self.ask_1firstlevel_orderlevel_detected = False
        self.bid_1firstlevel_orderlevel_detected = False
        self.current_price = 0
        self.last_pong_time = None
        self.connection_check_thread = None
        self.number_script = number_script
        # Parameters
        self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD = float(os.getenv('ORDER_DISTRIBUTION_1LEVEL_THRESHOLD')) #0.05
        self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD = float(os.getenv('ORDER_DISTRIBUTION_2LEVEL_THRESHOLD')) #0.10
        self.DB_UPDATE_MIN_WAITING_TIME = int(os.getenv('DB_UPDATE_MIN_WAITING_TIME'))  #SECONDS
        self.DB_UPDATE_MAX_WAITING_TIME = int(os.getenv('DB_UPDATE_MAX_WAITING_TIME')) #SECONDS
        self.MAX_WAITING_TIME_AFTER_BUY = int(os.getenv('MAX_WAITING_TIME_AFTER_BUY')) #HOURS
        self.ping_interval = 20  # Send ping every 20 seconds
        self.ping_timeout = 10  # Timeout after 10 seconds
        self.ws_thread = None
        self.connection_lock = threading.Lock()
        self.minutes_timeframe = minutes_timeframe
        # API endpoints
        self.snapshot_url = f"https://api.binance.com/api/v3/depth?symbol={self.coin}&limit=5000"
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.coin_lower}@depth"
        
        # Database connection
        self.client = DatabaseConnection()
        self.db = self.client.get_db(DATABASE_ORDER_BOOK)
        self.db_collection = self.db[event_key]
        
        # Logger
        self.logger = LoggingController.start_logging()

    def monitor_resources(self):
        """Monitor system resource consumption"""
        import psutil
        
        # Get process info
        process = psutil.Process()
        
        # Get system info
        system_memory = psutil.virtual_memory()
        total_memory_mb = system_memory.total / (1024 * 1024)
        available_memory_mb = system_memory.available / (1024 * 1024)
        
        # CPU usage (absolute number of cores used)
        cpu_percent = process.cpu_percent(interval=1.0)
        cpu_cores = psutil.cpu_count()  # Total number of CPU cores
        cpu_cores_used = (cpu_percent / 100) * cpu_cores  # Convert percentage to number of cores
        
        # Memory usage
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
        
        # Thread count
        thread_count = process.num_threads()
        
        # Network I/O
        net_io = process.io_counters()
        bytes_sent = net_io.read_bytes / (1024 * 1024)  # Convert to MB
        bytes_recv = net_io.write_bytes / (1024 * 1024)  # Convert to MB
        
        # Log resource usage
        self.logger.info(f"Resource Usage - "
                        f"CPU: {cpu_cores_used:.2f} cores used out of {cpu_cores} total cores, "
                        f"Memory: {memory_mb:.2f}MB used (System: {total_memory_mb:.2f}MB total, {available_memory_mb:.2f}MB available), "
                        f"Threads: {thread_count}, "
                        f"Network I/O: {bytes_sent:.2f}MB sent, {bytes_recv:.2f}MB received")
        
        return {
            'cpu_cores_used': cpu_cores_used,
            'cpu_cores_total': cpu_cores,
            'memory_mb': memory_mb,
            'total_memory_mb': total_memory_mb,
            'available_memory_mb': available_memory_mb,
            'thread_count': thread_count,
            'bytes_sent': bytes_sent,
            'bytes_recv': bytes_recv
        }

    def on_message(self, ws, message):
        data = json.loads(message)

        if datetime.now() > self.stop_script_datetime:
            self.logger.info(f"Stopping script for {self.coin}")
            self.stop()
            sys.exit(0)
                    
        if self.order_book['lastUpdateId'] is None:
            #self.logger.info("Buffering message as snapshot not received yet")
            self.buffered_events.append(data)
            return

        self.process_update(data)
        
        # If the order distribution is greater than 10% and 60 seconds have not passed from the last db update, pass
        if (self.last_ask_order_distribution_1level > self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD) and \
             (self.last_bid_order_distribution_1level > self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD and \
                  (datetime.now() - self.last_db_update_time).total_seconds() < self.DB_UPDATE_MAX_WAITING_TIME):
            pass
        elif self.current_price != 0:
            self.analyze_order_book()

    def on_error(self, ws, error):
        self.logger.error(f"WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        self.logger.info(f"{self.coin} - WebSocket Connection Closed with code {close_status_code}: {close_msg}")
        if self.running:
            self.logger.info(f"{self.coin} - Attempting to reconnect...")
            try:
                if self.retry_connection >= 10:
                    self.logger.info(f"{self.coin} - Reached maximum number of retries, stopping script")
                    self.stop()
                    sys.exit(0)
                self.retry_connection += 1
                # Reset all connection-related state
                with self.connection_lock:
                    # Clear WebSocket references
                    self.ws = None
                    self.ws_thread = None
                    self.last_pong_time = None
                    
                    # Clear order book state that might be stale
                    self.order_book = {
                        'bids': {},  # price -> quantity
                        'asks': {},  # price -> quantity
                        'lastUpdateId': None
                    }
                    self.buffered_events = []
                    self.running = False
                    
                    # Reset price tracking
                    self.current_price = 0
                    if self.initial_price == 0:
                        self.initial_price = None
                
                # Wait before reconnecting
                sleep(5)
                
                # Start new connection
                self.logger.info(f"{self.coin} - Starting new WebSocket connection...")
                self.start()
            except Exception as e:
                self.logger.error(f"{self.coin} - Error during reconnection: {e}")
                # If reconnection fails, try again after a delay
                self.logger.info(f"{self.coin} - Retrying reconnection after error...")
                sleep(5)
                self.on_close(ws, close_status_code, close_msg)

    def on_open(self, ws):
        """Handle WebSocket connection open"""
        with self.connection_lock:
            self.last_pong_time = datetime.now()
            self.ws = ws
            self.get_snapshot()

    def wait_for_snapshot(self):
        """Wait for the snapshot to be ready, in order to avoid rate limit ban"""
        _, live_order_book_scripts_number, _, last_snapshots = BinanceOrderBook.get_current_number_of_orderbook_scripts(self.db, self.event_keys, self.RESTART)
        #self.logger.info(f"live_order_book_scripts_number: {live_order_book_scripts_number}")
        # self.logger.info(f"Last minute snapshots: {last_snapshots}")
        if live_order_book_scripts_number >= 20:
            not_ready_to_go = True
            while not_ready_to_go:
                # this is the rare case when 20 scripts are activated within 1 minute
                if not self.RESTART and len(last_snapshots) > 20:
                    # self.logger.info(f"self.coin: {self.coin}; Waiting for snapshot to be ready")
                    sleep(60 - (datetime.now() - last_snapshots[-1]['dt']).total_seconds())
                    _, _, _, last_snapshots = BinanceOrderBook.get_current_number_of_orderbook_scripts(self.db, self.event_keys, self.RESTART)
                    if len(last_snapshots) < 20:
                        # self.logger.info(f"SNAPSHOT: {self.coin}. Number: {self.number_script}/{live_order_book_scripts_number}")
                        not_ready_to_go = False
                # this is the case when the script is restarted, apply 3 seconds delay for each script so that maximum 20 scripts are activated within 1 minute
                elif self.RESTART:
                    for obj in last_snapshots:
                        if obj['id'] == self.id:
                            position = last_snapshots.index(obj)
                            sleep(3*position)
                            # self.logger.info(f"SNAPSHOT: {self.coin}. Number: {self.number_script}/{live_order_book_scripts_number}")
                            not_ready_to_go = False
                else:
                    
                    sleep_seconds = (self.number_script % 20) * 3
                    current_second = datetime.now().second
                    if current_second > sleep_seconds:
                        sleep(60 - (current_second - sleep_seconds))
                    else:
                        sleep(sleep_seconds - current_second)
                    
                    # self.logger.info(f"SNAPSHOT: {self.coin}. Number: {self.number_script}/{live_order_book_scripts_number}")
                    not_ready_to_go = False


        self.db_collection.update_one(
            {"_id": self.id},
            {"$set": {"last_restart": datetime.now().isoformat()}}
        )
    def on_ping(self, ws, message):
        """Handle ping from server by sending pong"""
        try:
            with self.connection_lock:
                if self.ws and self.ws.sock and self.ws.sock.connected:
                    ws.send(message, websocket.ABNF.OPCODE_PONG)
                    self.last_pong_time = datetime.now()
        except Exception as e:
            self.logger.error(f"{self.coin} - Error sending pong: {e}")
            self.reconnect()

    def on_pong(self, ws, message):
        """Handle pong from server by updating last pong time"""
        try:
            with self.connection_lock:
                self.last_pong_time = datetime.now()
        except Exception as e:
            self.logger.error(f"{self.coin} - Error handling pong: {e}")
            self.reconnect()

    def reconnect(self):
        """Handle reconnection logic"""
        if self.running:
            self.logger.info(f"{self.coin} - Reconnecting...")
            self.stop()
            sleep(5)  # Wait before reconnecting
            self.start()

    def check_connection(self):
        """Periodically check connection health"""
        while self.running:
            try:
                if self.last_pong_time and (datetime.now() - self.last_pong_time).total_seconds() > 30:
                    self.logger.warning(f"{self.coin} - No pong received for 30 seconds, reconnecting...")
                    self.reconnect()
                sleep(10)  # Check every 10 seconds
            except Exception as e:
                self.logger.error(f"Error in connection check: {e}")
                sleep(5)

    def get_snapshot(self):
        """Get the initial order book snapshot"""
        # self.logger.info(f"Getting snapshot from {self.snapshot_url}")
        #self.logger.info(f'restart: {self.RESTART}')
        self.wait_for_snapshot()
        
        try:
            response = requests.get(self.snapshot_url)
        except Exception as e:
            self.logger.error(f"coin: {self.coin}; SNAPSHOT ERROR: {e}")
            return

        #self.logger.info(f"Snapshot response: {response.status_code}")
        if response.status_code == 200:
            snapshot = response.json()
            self.order_book['bids'] = {float(price): float(qty) for price, qty in snapshot['bids']}
            self.order_book['asks'] = {float(price): float(qty) for price, qty in snapshot['asks']}
            self.order_book['lastUpdateId'] = snapshot['lastUpdateId']
            
            # self.logger.info(f"Snapshot received with lastUpdateId: {self.order_book['lastUpdateId']}")
            
            # Initialize price if not set
            if self.initial_price is None:
                self.initial_price = float(snapshot['asks'][0][0])
                self.max_price = self.initial_price
                # self.logger.info(f"Initial price set to: {self.initial_price}")
            
            self.process_buffered_events()
        else:
            self.logger.error(f"Error getting snapshot: {response.status_code}: {response.text}")

    def process_buffered_events(self):
        """Process events that were received before the snapshot"""
        for event in self.buffered_events:
            if event['u'] <= self.order_book['lastUpdateId']:
                continue
            self.process_update(event)
        self.buffered_events = []

    def process_update(self, event):
        """
        Process a single order book update event from the WebSocket stream.
        
        This function handles the core logic of maintaining the local order book state.
        It follows Binance's recommended approach for processing depth updates.
        
        Parameters:
        -----------
        event : dict
            The WebSocket event containing order book updates. Expected format:
            {
                'e': 'depthUpdate',  # Event type
                'E': 123456789,      # Event time
                's': 'BTCUSDT',      # Symbol
                'U': 157,            # First update ID in event
                'u': 160,            # Final update ID in event
                'b': [               # Bids to be updated
                    ['0.0024', '10'],  # [price, quantity]
                    ...
                ],
                'a': [               # Asks to be updated
                    ['0.0026', '100'], # [price, quantity]
                    ...
                ]
            }
        """
        # Check if this update has already been processed
        # This prevents processing duplicate or old updates
        if event['u'] <= self.order_book['lastUpdateId']:
            return

        # Check for gaps in the update sequence
        # If the first update ID in this event is more than 1 greater than our last processed ID,
        # it means we've missed some updates. In this case, we need to restart the process
        # by getting a fresh snapshot.
        if event['U'] > self.order_book['lastUpdateId'] + 1:
            self.order_book['lastUpdateId'] = None
            self.get_snapshot()
            return

        # Process bid updates
        # Each update contains a list of [price, quantity] pairs
        # If quantity is 0, it means the price level should be removed
        # Otherwise, the price level should be updated with the new quantity
        for price, qty in event['b']:
            price = float(price)
            qty = float(qty)
            if qty == 0:
                self.order_book['bids'].pop(price, None)
            else:
                self.order_book['bids'][price] = qty

        # Process ask updates
        # Similar to bid updates, but for the ask side of the order book
        for price, qty in event['a']:
            price = float(price)
            qty = float(qty)
            if qty == 0:
                self.order_book['asks'].pop(price, None)
            else:
                self.order_book['asks'][price] = qty
                # Update max price if needed

        # get best bid and ask price
        # self.best_bid_price = max(self.order_book['bids'].keys())
        # self.best_ask_price = min(self.order_book['asks'].keys())
        # self.logger.info(f"Best bid price: {self.best_bid_price}; Best ask price: {self.best_ask_price}")
        self.current_price = min(self.order_book['asks'].keys())
        if self.initial_price == 0:
            self.initial_price = self.current_price

        # Update our last processed update ID
        # This is crucial for maintaining the correct sequence of updates
        # and detecting gaps in the future
        self.order_book['lastUpdateId'] = event['u']

    def update_ask_order_distribution_list(self, ask_order_distribution):
        self.ask_order_distribution_list.append({
            'ask': ask_order_distribution,
            'dt': datetime.now()
        })
        # Keep only the last N objects based on datetime, where N is specified in strategy parameters
        if len(self.ask_order_distribution_list) > self.strategy_parameters['last_i_ask_order_distribution']:
            self.ask_order_distribution_list.sort(key=lambda x: x['dt'], reverse=True)
            self.ask_order_distribution_list = self.ask_order_distribution_list[:self.strategy_parameters['last_i_ask_order_distribution']]
        
        return self.ask_order_distribution_list

    def update_bid_order_distribution_list(self, bid_order_distribution):
        self.bid_order_distribution_list.append({
            'bid': bid_order_distribution,
            'dt': datetime.now()
        })
        # Keep only the last N objects based on datetime, where N is specified in strategy parameters
        if len(self.bid_order_distribution_list) > self.strategy_parameters['last_i_bid_order_distribution']:
            self.bid_order_distribution_list.sort(key=lambda x: x['dt'], reverse=True)
            self.bid_order_distribution_list = self.bid_order_distribution_list[:self.strategy_parameters['last_i_bid_order_distribution']]
        
        return self.bid_order_distribution_list


    def analyze_order_book(self):
        # start_resources = self.monitor_resources()
        start_time = time.time()
        """Analyze the order book and update trading state"""        
        # Calculate price levels and distributions
        bid_orders = [(price, qty) for price, qty in self.order_book['bids'].items()]
        ask_orders = [(price, qty) for price, qty in self.order_book['asks'].items()]
        
        # Sort orders by price
        bid_orders.sort(key=lambda x: x[0], reverse=True)
        ask_orders.sort(key=lambda x: x[0])
        
        # Calculate cumulative volumes
        total_bid_volume = sum(price * qty for price, qty in bid_orders)
        total_ask_volume = sum(price * qty for price, qty in ask_orders)
        #self.logger.info(f"Coin: {self.coin}; Total bid volume: {total_bid_volume}, Total ask volume: {total_ask_volume}. Number script: {self.number_script}")

        # Calculate summary orders with delta intervals
        delta = 0.01
        summary_bid_orders = []
        summary_ask_orders = []
        next_delta_threshold = 0 + delta
        # self.logger.info(f"current_price: {self.current_price}")
        # self.logger.info(f"total_bid_volume: {total_bid_volume}")
        # self.logger.info(f"total_ask_volume: {total_ask_volume}")

        # Calculate summary bid orders
        cumulative_bid_volume = 0
        for price, qty in bid_orders:
            price_order = float(price)
            quantity_order = float(qty)
            cumulative_bid_volume += price_order * quantity_order
            cumulative_bid_volume_ratio = BinanceOrderBook.round_((cumulative_bid_volume / total_bid_volume), 2)
            
            if cumulative_bid_volume_ratio >= next_delta_threshold:
                summary_bid_orders.append((
                    BinanceOrderBook.round_((price_order - self.current_price) / self.current_price, 3),
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
            cumulative_ask_volume_ratio = BinanceOrderBook.round_((cumulative_ask_volume / total_ask_volume), 2)
            
            if cumulative_ask_volume_ratio >= next_delta_threshold:
                summary_ask_orders.append((
                    BinanceOrderBook.round_((price_order - self.current_price) / self.current_price, 3),
                    cumulative_ask_volume_ratio
                ))
                next_delta_threshold = cumulative_ask_volume_ratio + delta
        
        # Calculate price levels and distributions using summary orders
        self.bid_price_levels_dt, bid_order_distribution, bid_cumulative_level = self.get_price_levels(
            self.current_price, summary_bid_orders, 
            self.strategy_parameters['strategy_jump'],
            self.strategy_parameters['limit'],
            self.strategy_parameters['price_change_jump']
        )
        
        self.ask_price_levels_dt, ask_order_distribution, ask_cumulative_level = self.get_price_levels(
            self.current_price, summary_ask_orders,
            self.strategy_parameters['strategy_jump'],
            self.strategy_parameters['limit'],
            self.strategy_parameters['price_change_jump']
        )
        
        # Update order distribution list
        # if datetime.now().second == 10 or datetime.now().second == 40:
        #     self.logger.info(f'WSS: ask_order_distribution: {ask_order_distribution}')
        #     self.logger.info(f'WSS: jump price level: {self.summary_jump_price_level}')
        
        self.last_ask_order_distribution_1level = ask_order_distribution[str(self.strategy_parameters['price_change_jump'])]
        self.last_bid_order_distribution_1level = bid_order_distribution[str(self.strategy_parameters['price_change_jump'])]
        # print(f"Last ask od1l: {self.last_ask_order_distribution_1level}; Last bid od1l: {self.last_bid_order_distribution_1level}")

        # Check BUY Trading Conditions or Update Order Book Record only if the order distribution is less than 10%
        current_time = datetime.now()
        if self.last_ask_order_distribution_1level <= self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD:
            # if 10 seconds have passed or the last ask order distribution is less than 5%. This way, we can avoid updating the order book record too often and 
            # make sure that I track the exact moment when the order book distribution is less than 5%
            if (current_time - self.last_db_update_time).total_seconds() >= self.DB_UPDATE_MIN_WAITING_TIME or (self.last_ask_order_distribution_1level <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD and not self.ask_1firstlevel_orderlevel_detected):
                self.ask_order_distribution_list = self.update_ask_order_distribution_list(ask_order_distribution)
                if not self.BUY:
                    self.check_buy_trading_conditions()
                self.update_order_book_record(
                    total_bid_volume,
                    total_ask_volume,
                    summary_bid_orders,
                    summary_ask_orders
                )
                if self.last_ask_order_distribution_1level <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
                    self.ask_1firstlevel_orderlevel_detected = True
                self.last_db_update_time = current_time

        elif self.last_bid_order_distribution_1level <= self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD:
            # if 10 seconds have passed or the last ask order distribution is less than 5%. This way, we can avoid updating the order book record too often and 
            # make sure that I track the exact moment when the order book distribution is less than 5%
            if (current_time - self.last_db_update_time).total_seconds() >= self.DB_UPDATE_MIN_WAITING_TIME or (self.last_bid_order_distribution_1level <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD and not self.bid_1firstlevel_orderlevel_detected):
                self.update_order_book_record(
                    total_bid_volume,
                    total_ask_volume,
                    summary_bid_orders,
                    summary_ask_orders
                )
                if self.last_bid_order_distribution_1level <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
                    self.bid_1firstlevel_orderlevel_detected = True
                self.last_db_update_time = current_time
        else:
            self.last_db_update_time = current_time

        # Reset the low order level detected flag if the order distribution is greater than 5%
        if self.ask_1firstlevel_orderlevel_detected and self.last_ask_order_distribution_1level > self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
            self.ask_1firstlevel_orderlevel_detected = False
        if self.bid_1firstlevel_orderlevel_detected and self.last_bid_order_distribution_1level > self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
            self.bid_1firstlevel_orderlevel_detected = False

        # end_resources = self.monitor_resources()
        # end_time = time.time()
        # # Log analysis duration and resource changes
        # duration = end_time - start_time
        # self.logger.info(f"Analysis Duration: {duration:.2f}s")
        # self.logger.info(f"Resource Changes - CPU: {end_resources['cpu_cores_used'] - start_resources['cpu_cores_used']:.2f} cores used, "
        #                 f"Memory: {end_resources['memory_mb'] - start_resources['memory_mb']:.2f}MB")

    

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
            decimal_index = str_num.index('.')
            return len(str_num) - decimal_index
        except ValueError:
            # If no decimal point is found, it's an integer
            return 1

    

    def update_order_book_record(self, total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders):
        """Update the order book record in the database"""
        print(f"Updating order book record for {self.coin}")
        now = datetime.now().replace(microsecond=0).isoformat()
        new_data = [
            self.current_price,  # ask_price
            total_bid_volume,
            total_ask_volume,
            summary_bid_orders,
            summary_ask_orders
        ]

        filter_query = {"_id": self.id}
        update_doc = {
            "$set": {
                f"data.{now}": new_data,
                "weight": 250,
                "max_price": self.max_price,
                "initial_price": self.initial_price,
                'summary_jump_price_level': self.summary_jump_price_level,
                'ask_order_distribution_list': self.ask_order_distribution_list,
                'buy': self.BUY,
                'buy_price': self.buy_price
            }
        }
        
        result = self.db_collection.update_one(filter_query, update_doc)
        if result.modified_count != 1:
            self.logger.error(f"Order Book update failed for {self.event_key} with id {self.id}")

    def round_(number, decimal):
        return float(format(number, f".{decimal}f"))

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
        n_decimals = BinanceOrderBook.count_decimals(price)
        cumulative_level_without_jump = 0
        price_change_level = price_change_jump
        order_distribution = {}
        for i in arange(price_change_jump,price_change_limit+price_change_jump,price_change_jump):
            order_distribution[str(BinanceOrderBook.round_(i,3))] = 0
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
                order_distribution[str(price_change_level)] = BinanceOrderBook.round_(order_distribution[str(price_change_level)] - previous_cumulative_level,2)

                # now update the next price change level
                previous_cumulative_level += order_distribution[str(price_change_level)]
                price_change_level = BinanceOrderBook.round_(price_change_level + price_change_jump,3)

                # in case some price level is empty, skip to the next level
                while abs(price_change) > price_change_level:
                    price_change_level = BinanceOrderBook.round_(price_change_level + price_change_jump,3)

                # next chunk is below next thrshold
                if abs(price_change) <= price_change_level and abs(price_change) <= price_change_limit:
                    order_distribution[str(price_change_level)] = cumulative_level

            # here, I discover the jumps, the info is stored in "price_levels"
            if cumulative_level - previous_level >= cumulative_volume_jump and abs(price_change) <= price_change_limit and abs(price_change) >= 0.01:
                actual_jump = BinanceOrderBook.round_(cumulative_level - previous_level,3)
                price_level = price * (1+price_change)
                #info = (BinanceOrderBook.round_(price_level,n_decimals), price_change, cumulative_level, actual_jump)
                #price_levels.append(info)
                price_levels.append(BinanceOrderBook.round_(price_level,n_decimals))
            elif abs(price_change) <= price_change_limit:
                cumulative_level_without_jump = cumulative_level

            if abs(price_change) > price_change_limit:
                break
            previous_level = cumulative_level
        
        # scale order_distribution to [0,100] range
        for lvl in order_distribution:
            if previous_cumulative_level != 0:
                order_distribution[lvl] = BinanceOrderBook.round_(order_distribution[lvl] / previous_cumulative_level,3)
            else:
                order_distribution[lvl] = 0
        
        # if there are not jumps, at least I want to get the cumulative volume at the limit price level
        if len(price_levels) == 0:
            info = (None, None, cumulative_level_without_jump, False)
            price_levels.append(None)
        
        return price_levels, order_distribution, BinanceOrderBook.round_(previous_cumulative_level,3)

    def check_buy_trading_conditions(self):
        """Check if trading conditions are met"""
        if self.current_price > self.max_price:  
            self.max_price = self.current_price
        # self.logger.info(f"max price: {self.max_price}")
        max_change = (self.max_price - self.initial_price) / self.initial_price
        # self.logger.info(f"max change: {max_change}")
        current_price_drop = abs((self.current_price - self.max_price) / self.max_price)
        #self.logger.info(f"current price drop: {current_price_drop}")
        is_jump_price_level = self.hit_jump_price_levels_range()
        #self.logger.info(f'summary jump price level: {self.summary_jump_price_level}')
        #self.logger.info(f"is jump price level: {is_jump_price_level}")
        
        if (is_jump_price_level) and (current_price_drop >= self.strategy_parameters['price_drop_limit'] and 
            max_change <= self.strategy_parameters['max_limit']):
            
            # Check ask order distribution
            all_levels_valid = True
            avg_distribution = self.calculate_average_distribution(self.ask_order_distribution_list)
            keys_ask_order_distribution = list(avg_distribution.keys())[:self.strategy_parameters['lvl_ask_order_distribution_list']]
            # self.logger.info(f"keys ask order distribution: {keys_ask_order_distribution}")
            # self.logger.info(f"avg distribution: {avg_distribution}")
            for lvl in keys_ask_order_distribution:
                if avg_distribution[lvl] >= self.strategy_parameters['max_ask_order_distribution_level']:
                    all_levels_valid = False
                    break

            if all_levels_valid:
                self.BUY = True
                self.buy_price = self.current_price
                self.logger.info(f"BUY EVENT: {self.coin} at {self.current_price}")
                self.stop_script_datetime = max(datetime.now() + timedelta(hours=self.MAX_WAITING_TIME_AFTER_BUY), datetime.fromisoformat(self.id) + timedelta(minutes=self.minutes_timeframe) )
                #self.stop_script_datetime = max(datetime.now() + timedelta(minutes=5), datetime.fromisoformat(self.id) + timedelta(minutes=self.minutes_timeframe) )
                self.save_trading_event()

    def calculate_average_distribution(self, distributions):
        """Calculate average order distribution"""
        avg_distribution = {}
        for dist in distributions:
            for level, value in dist['ask'].items():
                if level not in avg_distribution:
                    avg_distribution[level] = []
                avg_distribution[level].append(value)
        
        return {level: sum(values) / len(values) for level, values in avg_distribution.items()}

    def hit_jump_price_levels_range(self, neighborhood_of_price_jump = 0.005):
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

        # iterate throgh 
        for abs_price in self.bid_price_levels_dt:
            if abs_price == None:
                continue
            if len(self.summary_jump_price_level) != 0:
                IS_NEW_X = True
                for x in self.summary_jump_price_level:
                    historical_price_level = self.summary_jump_price_level[x][0]
                    historical_price_level_n_obs = self.summary_jump_price_level[x][1]
                    if abs_price <= historical_price_level * (1 + neighborhood_of_price_jump) and abs_price >= historical_price_level * (1 - neighborhood_of_price_jump):
                        #historical_price_level_list.append(abs_price)
                        historical_price_level = (historical_price_level*historical_price_level_n_obs + abs_price) / (historical_price_level_n_obs + 1)
                        self.summary_jump_price_level[x] = (historical_price_level, historical_price_level_n_obs + 1)
                        IS_NEW_X = False
                        break
                if IS_NEW_X:
                    list_x = [int(i) for i in list(self.summary_jump_price_level.keys())]
                    new_x = str(max(list_x) + 1)
                    self.summary_jump_price_level[new_x] = (abs_price, 1)
            else:
                #print(abs_price)
                self.summary_jump_price_level['1'] = (abs_price, 1)
                
        #print(self.summary_jump_price_level)
        for x in self.summary_jump_price_level:
            #logger.info(self.summary_jump_price_level)
            jump = abs((self.current_price - self.summary_jump_price_level[x][0] ) / self.current_price )
            historical_price_level_n_obs = self.summary_jump_price_level[x][1]
            #logger.info(f'jump price level - {coin}: {jump} vs {distance_jump_to_current_price}')
            if jump <= self.strategy_parameters['distance_jump_to_current_price'] and historical_price_level_n_obs >= self.strategy_parameters['min_n_obs_jump_level']:
                return True
                #print(current_price, dt)
        return False
    
    def save_trading_event(self):
        """Save trading event to database"""
        client = DatabaseConnection()
        db = client.get_db(DATABASE_TRADING)
        user = os.getenv('SYS_ADMIN')
        db_collection = db[user]
        
        buy_ts = datetime.now().isoformat()
        db_collection.insert_one({
            "_id": self.id,
            "coin": self.coin,
            "gain": '',
            "buy_price": self.buy_price,
            "sell_price": '',
            "buy_ts": buy_ts,
            "sell_ts": '',
            "ranking": self.ranking,
            "initial_price": self.initial_price,
            "max_price": self.max_price,
            "strategy": self.strategy_parameters
        })
        client.close()

    def start(self):
        """Start the WebSocket connection"""
        with self.connection_lock:
            if self.running:
                return
            self.running = True
            
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
        
        self.ws_thread = threading.Thread(target=self.ws.run_forever, kwargs={
            'ping_interval': self.ping_interval,
            'ping_timeout': self.ping_timeout
        })
        self.ws_thread.daemon = True
        self.ws_thread.start()

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
                    self.logger.error(f"{self.coin} - Error closing WebSocket: {e}")
            if self.BUY:
                self.update_trading_event()
            self.client.close()

    def update_trading_event(self):
        """Update trading event with sell information"""
        client = DatabaseConnection()
        db = client.get_db(DATABASE_TRADING)
        user = os.getenv('SYS_ADMIN')
        db_collection = db[user]
        
        sell_price = self.current_price
        sell_ts = datetime.now().isoformat()
        gain = (sell_price - self.buy_price) / self.buy_price
        self.logger.info(f"SELL EVENT: {self.coin} at {sell_price}. GAIN: {gain}")
        
        filter_query = {"_id": self.id}
        update_doc = {
            "$set": {
                "gain": gain,
                "sell_ts": sell_ts,
                "sell_price": sell_price
            }
        }
        
        result = db_collection.update_one(filter_query, update_doc)
        if result.modified_count != 1:
            self.logger.error(f"Trading Collection update failed for {self.event_key} with id {self.id}")
        
        client.close()
    
    @staticmethod
    def get_current_number_of_orderbook_scripts(db, event_keys, restart):
        live_order_book_scripts_number = 0
        numbers_filled = []
        current_weight = 0
        last_minute_snapshots = []
        for collection in event_keys['event_keys']:
            if collection != COLLECTION_ORDERBOOK_METADATA:
                minutes_timeframe = int(BinanceOrderBook.extract_timeframe(collection))
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
                # logger.info(docs)
                # len_docs = len(docs)
                # logger.info(f'{len_docs} - {collection}')
                # Filter snapshots from last minute
        if not restart:
            last_minute_snapshots = [dt for dt in last_minute_snapshots if (datetime.now() - dt['dt']).total_seconds() <= 60]
            last_minute_snapshots.sort(key=lambda x: x['dt'], reverse=True)  # Sort timestamps from newest to oldest

        return numbers_filled, live_order_book_scripts_number, current_weight, last_minute_snapshots

    @staticmethod
    def extract_timeframe(input_string):
        """
        Extracts the timeframe value (e.g., 1440) from the given input string.

        Args:
        input_string: The input string containing the timeframe.

        Returns:
        The extracted timeframe value as a string, or None if no match is found.
        """

        match = re.search(r"timeframe:(\d+)", input_string)
        # logger = LoggingController.start_logging()
        # logger.info(f"coin: {coin}; input_string: {input_string};")
        
        if match:
            return match.group(1)
        else:
            return None

    @staticmethod
    def get_number_script(numbers_filled, live_order_book_scripts_number):
        for i in range(live_order_book_scripts_number + 1):
            if i not in numbers_filled:
                number_script = i
                break
        return number_script

if __name__ == "__main__":
    # Get command line arguments
    coin = sys.argv[1]
    event_key = sys.argv[2]
    id = sys.argv[3]
    ranking = sys.argv[4]
    RESTART = bool(int(sys.argv[5]))
    event_keys = json.loads(sys.argv[7])
    # logger = LoggingController.start_logging()

    # logger.info(f"coin: {coin}; event_key: {event_key}")

    minutes_timeframe = int(BinanceOrderBook.extract_timeframe(event_key))
    stop_script_datetime = datetime.fromisoformat(id) + timedelta(minutes=minutes_timeframe)
    
    client = DatabaseConnection()
    db = client.get_db(DATABASE_ORDER_BOOK)
    numbers_filled, live_order_book_scripts_number, _, _ = BinanceOrderBook.get_current_number_of_orderbook_scripts(db, event_keys, RESTART)
    number_script = BinanceOrderBook.get_number_script(numbers_filled, live_order_book_scripts_number)
    # logger = LoggingController.start_logging()


    
    # Initialize database record using order_book's database connection
    if not RESTART:
        # Initialize new record
        strategy_parameters = json.loads(sys.argv[6])
        # Create order book instance first
        order_book = BinanceOrderBook(coin=coin, event_key=event_key, id=id, ranking=ranking, strategy_parameters=strategy_parameters, number_script=number_script,
                                         event_keys=event_keys, stop_script_datetime=stop_script_datetime, restart=RESTART, minutes_timeframe=minutes_timeframe)    
        order_book.db_collection.insert_one({
            "_id": id,
            "coin": coin,
            "ranking": ranking,
            "data": {},
            "initial_price": 0,
            "max_price": 0,
            "buy_price": '',
            "buy": False,
            "number": number_script,  # This will be updated later
            "strategy_parameters": strategy_parameters,
            "weight": 250,
            "summary_jump_price_level": {},
            "ask_order_distribution_list": [],
            "last_restart": id
        })
        
        order_book.logger.info(f"OrderBook: {coin}. Number: {number_script}/{live_order_book_scripts_number} - event_key: {event_key}")
    else:
        # For restart, verify the record exists
        
        db_collection = db[event_key]
        existing_record = db_collection.find_one({"_id": id})
        if existing_record:
            number_script = existing_record["number"]
            initial_price = existing_record["initial_price"]
            max_price = existing_record["max_price"]
            buy_price = existing_record["buy_price"]
            BUY = existing_record["buy"]
            strategy_parameters = existing_record["strategy_parameters"]
            ask_order_distribution_list = existing_record["ask_order_distribution_list"]
            summary_jump_price_level = existing_record["summary_jump_price_level"]
            order_book = BinanceOrderBook(coin=coin, event_key=event_key, id=id, ranking=ranking, strategy_parameters=strategy_parameters, event_keys=event_keys,
                                          stop_script_datetime=stop_script_datetime,ask_order_distribution_list=ask_order_distribution_list, number_script=number_script,
                                          summary_jump_price_level=summary_jump_price_level, initial_price=initial_price, minutes_timeframe=minutes_timeframe,
                                          max_price=max_price, buy_price=buy_price, BUY=BUY, restart=RESTART)
        else:
            # order_book.logger.error(f"No existing record found for id {id} during restart")
            sys.exit(1)
    
    # Start the order book
    client.close()
    order_book.start()
    
    try:
        # Run until keyboard interrupt
        while True:
            sleep(0.1)
    except KeyboardInterrupt:
        order_book.stop() 