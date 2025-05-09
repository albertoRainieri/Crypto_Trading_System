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
import asyncio

class MultiConnectionOrderBook:
    """
    Manages multiple PooledBinanceOrderBook instances, each handling a subset of coins.
    This distributes the load across multiple WebSocket connections, reducing the risk of connection errors.
    """
    def __init__(self, coins: List[str], connection_count=10):
        # Logger
        self.logger = LoggingController.start_logging()
        self.logger.info(f"Initializing MultiConnectionOrderBook with {connection_count} connections")
        
        self.coins = [coin.upper() for coin in coins]
        self.connection_count = connection_count
        self.order_book_instances = []
        self.threads = []
        
        # Split coins into chunks for each connection
        coin_chunks = self.split_coins(self.coins, connection_count)
        
        # Create separate instances for each chunk
        for i, chunk in enumerate(coin_chunks):
            self.logger.info(f"Creating instance {i+1} with {len(chunk)} coins")
            instance = PooledBinanceOrderBook(coins=chunk, connection_id=i, connection_count=connection_count)
            self.order_book_instances.append(instance)
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def split_coins(self, coins, chunks):
        """Split the coins list into roughly equal chunks"""
        avg = len(coins) // chunks
        remainder = len(coins) % chunks
        result = []
        
        start = 0
        for i in range(chunks):
            end = start + avg + (1 if i < remainder else 0)
            result.append(coins[start:end])
            start = end
            
        return result
    
    def initialization_process(self):
        """Initialize all PooledBinanceOrderBook instances"""
        for instance in self.order_book_instances:
            instance.initialization_process()
    
    def wrap_search_volatility_event_trigger(self, start_script=False):
        """Call search_volatility_event_trigger on all instances, each in its own thread"""
        for instance in self.order_book_instances:
            # Create a separate thread for each instance
            t = threading.Thread(
                target=instance.search_volatility_event_trigger,
                args=(start_script,),
                daemon=True
            )
            t.start()
            # Add a small delay to avoid all threads starting at exactly the same time
    
    def start(self):
        """Start all WebSocket connections in separate threads"""
        for instance in self.order_book_instances:
            thread = threading.Thread(target=instance.start)
            thread.daemon = True
            thread.start()
            self.threads.append(thread)
            # Add slight delay to avoid hammering the server
            time.sleep(2)
    
    def restart_connection(self):
        """Call restart_connection on first instance only"""
        if self.order_book_instances:
            # Only one instance needs to handle this
            self.order_book_instances[0].restart_connection()
    
    def stop(self):
        """Stop all WebSocket connections"""
        for instance in self.order_book_instances:
            instance.stop()
    
    def signal_handler(self, signum, frame):
        """Handle signals gracefully"""
        self.logger.info(f"Received signal {signum}, shutting down gracefully")
        self.stop()
        sys.exit(0)

class PooledBinanceOrderBook:
    def __init__(self, coins: List[str], connection_id=0, connection_count=8):
        # Logger
        self.logger = LoggingController.start_logging()
        
        self.coins = [coin.upper() for coin in coins]
        self.coins_lower = [coin.lower() for coin in coins]
        self.retry_connection = 0
        self.connection_id = connection_id
        self.connection_count = connection_count
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
        self.ask_0firstlevel_orderlevel_detected = {}
        self.ask_1firstlevel_orderlevel_detected = {}
        self.bid_1firstlevel_orderlevel_detected = {}
        self.ask_1firstlevel_orderlevel_detected_datetime = {}
        self.bid_1firstlevel_orderlevel_detected_datetime = {}
        self.current_price = {}
        self.bid_price_levels_dt = {}
        self.ask_price_levels_dt = {}
        self.coin_orderbook_initialized = {}
        self.last_minute_snapshots = []
        self.benchmark = {}
        
        # Parameters
        # TODO: remove hardcoded values
        self.ORDER_DISTRIBUTION_0LEVEL_THRESHOLD = float(os.getenv('ORDER_DISTRIBUTION_0LEVEL_THRESHOLD')) #0.005
        self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD = float(os.getenv('ORDER_DISTRIBUTION_1LEVEL_THRESHOLD')) #0.05
        self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD = float(os.getenv('ORDER_DISTRIBUTION_2LEVEL_THRESHOLD')) #0.1
        self.DB_UPDATE_MIN_WAITING_TIME_1LEVEL = int(os.getenv('DB_UPDATE_MIN_WAITING_TIME_1LEVEL')) #15 seconds
        self.DB_UPDATE_MIN_WAITING_TIME_2LEVEL = int(os.getenv('DB_UPDATE_MIN_WAITING_TIME_2LEVEL')) #60 seconds
        self.DB_UPDATE_MAX_WAITING_TIME = int(os.getenv('DB_UPDATE_MAX_WAITING_TIME')) #300 seconds
        self.TIMEDELTA_MINUTES_FROM_1LEVEL_DETECTED_1 = int(os.getenv('TIMEDELTA_MINUTES_FROM_1LEVEL_DETECTED_1')) #10 minutes  
        self.TIMEDELTA_MINUTES_FROM_1LEVEL_DETECTED_2 = int(os.getenv('TIMEDELTA_MINUTES_FROM_1LEVEL_DETECTED_2')) #20minutes  
        self.MAX_WAITING_TIME_AFTER_BUY = int(os.getenv('MAX_WAITING_TIME_AFTER_BUY')) #60 minutes
        self.ping_interval = 30
        self.ping_timeout = 20  # Increased from 10 to 30 seconds
        self.connection_lock = threading.Lock()
        self.last_pong_time = None
        self.connection_restart_lock = threading.Lock()
        self.connection_restart_running = False
        
        # Database connectionprocess_order_book_update
        self.client = DatabaseConnection()
        self.db_orderbook = self.client.get_db(DATABASE_ORDER_BOOK)
        self.db_trading = self.client.get_db(DATABASE_TRADING)
        self.db_tracker = self.client.get_db(DATABASE_TRACKER)
        self.db_benchmark = self.client.get_db(DATABASE_BENCHMARK)

        self.orderbook_collection = {}
        self.tracker_collection = {}
        self.metadata_orderbook_collection = self.db_orderbook[COLLECTION_ORDERBOOK_METADATA]
        self.trading_collection = self.db_trading["albertorainieri"]
        
        # Combined stream URL
        self.parameters = [f"{coin.lower()}@depth" for coin in self.coins]
        self.ws_url = f"wss://stream.binance.com:9443/stream?streams="
        self._async_thread = threading.Thread(target=self._setup_async_loop, daemon=True)
        self._async_thread.start()
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def initialization_process(self):
        last_snapshot_time = datetime.now()
        for coin in self.coins:
            last_snapshot_time = last_snapshot_time + timedelta(seconds=30/self.connection_count)
            self.initialize_coin_status(coin=coin, last_snapshot_time=last_snapshot_time)
        #self.logger.info(f'benchmark: {self.benchmark}')
    
    def _setup_async_loop(self):
        """Create a dedicated event loop in a background thread"""
        import threading
        import asyncio
        
        def run_event_loop():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.run_forever()
        
        self._loop_thread = threading.Thread(target=run_event_loop, daemon=True)
        self._loop_thread.start()

    def initialize_coin_status(self, coin, last_snapshot_time=datetime.now(), start_script=True):

        if start_script:
            self.order_books[coin] = {
                'bids': {},
                'asks': {},
                'lastUpdateId': None
            }
            self.buffered_events[coin] = []
            self.last_db_update_time[coin] = datetime.now() - timedelta(seconds=self.DB_UPDATE_MAX_WAITING_TIME)
            self.current_price[coin] = 0
            self.coin_orderbook_initialized[coin] = {'status': False, 'next_snapshot_time': last_snapshot_time}
            # Get just the volume_30_avg field from benchmark collection
            benchmark_doc = self.db_benchmark[coin].find_one({}, {'volume_30_avg': 1})
            self.benchmark[coin] = benchmark_doc.get('volume_30_avg') if benchmark_doc else None
            self.ask_1firstlevel_orderlevel_detected_datetime[coin] = datetime(1970, 1, 1)
            self.bid_1firstlevel_orderlevel_detected_datetime[coin] = datetime(1970, 1, 1)

        else:
            self.coin_orderbook_initialized[coin] = {'status': True, 'next_snapshot_time': last_snapshot_time}

        self.under_observation[coin] = {'status': False}
        self.BUY[coin] = False
        self.summary_jump_price_level[coin] = {}
        self.ask_order_distribution_list[coin] = []
        self.bid_order_distribution_list[coin] = []
        self.max_price[coin] = 0
        self.initial_price[coin] = 0
        self.buy_price[coin] = 0
        self.last_ask_order_distribution_1level[coin] = 0
        self.last_bid_order_distribution_1level[coin] = 0
        self.ask_1firstlevel_orderlevel_detected[coin] = False
        self.ask_0firstlevel_orderlevel_detected[coin] = False
        self.bid_1firstlevel_orderlevel_detected[coin] = False
        self.bid_price_levels_dt[coin] = []
        self.ask_price_levels_dt[coin] = []

    def get_riskmanagement_configuration(self):
        with open('/tracker/riskmanagement/riskmanagement.json', 'r') as f:
            riskmanagement_configuration = json.load(f)
        return riskmanagement_configuration['parameters']


    def get_ongoing_analysis_coins(self):
        client = DatabaseConnection()
        db = client.get_db(DATABASE_ORDER_BOOK)
        db_collection = db[COLLECTION_ORDERBOOK_METADATA]
        timeframe_max_waiting_time_after_buy_hours = datetime.now() - timedelta(days=1) - timedelta(minutes=self.MAX_WAITING_TIME_AFTER_BUY)
        coins_ongoing_analysis = []
        coins_ongoing_buy = []
        docs = list(
            db_collection.find(
                {"_id": {"$gt": timeframe_max_waiting_time_after_buy_hours.isoformat()}, "status": "running"},
                {"_id": 1, "coin": 1, "buy_price": 1},
            )
        )

        for doc in docs:
            if doc["buy_price"] != 0:
                coins_ongoing_buy.append(doc["coin"])
            coins_ongoing_analysis.append(doc["coin"])
        client.close()
        return coins_ongoing_analysis, coins_ongoing_buy

    def initialize_order_book(self, LOG=True):
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
        #self.logger.info(f'event_keys: {self.event_keys}')
        coins_ongoing_analysis, coins_ongoing_buy = self.get_ongoing_analysis_coins()
        # Reorder coins list to prioritize coins under buy event and then coins under ongoing analysis
        self.coins = sorted(self.coins, key=lambda x: x not in coins_ongoing_buy)
        self.coins = sorted(self.coins, key=lambda x: x not in coins_ongoing_analysis)
        if LOG:
            self.logger.info(f'coins_ongoing_analysis: {coins_ongoing_analysis}')
            self.logger.info(f'coins_ongoing_buy: {coins_ongoing_buy}')
            self.logger.info(f'Reordered coins list. First 10 coins: {self.coins[:10]}')

        last_snapshot_time = datetime.now()
        for coin in self.coins:
            last_snapshot_time = last_snapshot_time + timedelta(seconds=30)
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
                self.logger.error(f"Connection {self.connection_id} - Received malformed message: {message[:100]}...")
                return
                
            # Extract coin from stream name (format: "btcusdt@depth")
            coin_with_depth = stream.split('@')[0]
            coin = coin_with_depth.upper()

            if self.under_observation[coin]['status'] and datetime.now() > datetime.fromisoformat(self.under_observation[coin]['end_observation']):
                if self.BUY[coin]:
                    self.update_trading_and_metadata_event(coin)
                else:
                    self.logger.info(f'Connection {self.connection_id} - Observation ended for coin: {coin}. Initializing coin status.')
                    self.update_metadata_event(coin)
                self.initialize_coin_status(coin=coin, start_script=False, last_snapshot_time=self.coin_orderbook_initialized[coin]['next_snapshot_time'])

            if self.coin_orderbook_initialized[coin]['status'] == False and datetime.now() > self.coin_orderbook_initialized[coin]['next_snapshot_time']:
                self.get_snapshot(coin)
                self.coin_orderbook_initialized[coin]['status'] = True
                        
            if self.order_books[coin]['lastUpdateId'] is None:
                self.buffered_events[coin].append(stream_data)
                return

            self.process_update(stream_data, coin)
            
            #if datetime.now().second <= 10 and datetime.now().second >= 5:
            # self.logger.info(f'coin: {coin}; position: {self.coins.index(coin)}')
            
            # If the order distribution is greater than threshold and min db update time (60seconds) not reached
            # if (self.under_observation[coin]['status']) and (self.last_ask_order_distribution_1level[coin]> self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD and
            #     self.last_bid_order_distribution_1level[coin]> self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD and
            #     (datetime.now() - self.last_db_update_time[coin]).total_seconds() < self.DB_UPDATE_MAX_WAITING_TIME):
            #     pass
            if (self.under_observation[coin]['status']) and (self.current_price[coin] != 0):
                self.analyze_order_book(coin)



        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error processing message: {e}")
            # Continue running despite errors

    def on_error(self, ws, error):
        self.logger.error(f"Connection {self.connection_id} - WebSocket Error: {error}")
        # Don't stop if there's an error, just log it

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        self.logger.info(f"Connection {self.connection_id} - WebSocket Connection Closed with code {close_status_code}: {close_msg}")
        if self.running and not self.should_exit:
            self.logger.info(f"Connection {self.connection_id} - Attempting to reconnect...")
            try:
                # Reset connection-related state
                with self.connection_lock:
                    # Clear WebSocket references
                    self.ws = None
                    self.ws_thread = None
                    self.last_pong_time = None
                    
                    # Don't reset order book state for all coins
                    # Instead, we'll get new snapshots for each coin when we reconnect
                    self.running = False
                
                # Fixed 10 second delay for reconnection
                self.logger.info("Waiting 10 seconds before reconnecting")
                sleep(10)
                
                # Start new connection
                self.logger.info(f"Connection {self.connection_id} - Starting new WebSocket connection...")
                self.start()
                # Reset retry counter on successful connection
                self.retry_connection = 0
            except Exception as e:
                self.logger.error(f"Connection {self.connection_id} - Error during reconnection: {e}")
                # If reconnection fails, try again after a delay
                self.logger.info(f"Connection {self.connection_id} - Retrying reconnection after error...")
                sleep(10)
                self.on_close(ws, close_status_code, close_msg)

    def on_open(self, ws):
        """Handle WebSocket connection open"""
        self.initialize_order_book(LOG=False)
        #self.logger.info(f'parameters: {self.parameters}')
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
            self.logger.error(f"Connection {self.connection_id} - Error sending pong: {e}")
            self.reconnect()

    def on_pong(self, ws, message):
        """Handle pong from server by updating last pong time"""
        try:
            with self.connection_lock:
                self.last_pong_time = datetime.now()
        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error handling pong: {e}")
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
                self.logger.error(f"Connection {self.connection_id} - Error in connection check: {e}")
                sleep(5)

    def get_snapshot(self, coin):
        """Get the initial order book snapshot"""
        # self.logger.info(f"Getting snapshot from {self.snapshot_url}")
        #self.logger.info(f'restart: {self.RESTART}')
        
        try:
            if self.order_books[coin]['lastUpdateId'] is not None:
                self.wait_for_snapshot(coin)
                self.logger.info(f'Connection {self.connection_id} - Getting snapshot for coin: {coin}')
            snapshot_url = f"https://api.binance.com/api/v3/depth?symbol={coin}&limit=5000"
            self.last_minute_snapshots.append(datetime.now())
            response = requests.get(snapshot_url)
        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - coin: {coin}; SNAPSHOT ERROR: {e}")
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
            self.logger.error(f"Connection {self.connection_id} - Error getting snapshot: {response.status_code}: {response.text}")

    def get_last_minute_aggregate_trades(self, coin):
        """
        Fetches all the aggregate trades that occurred in the last minute for a specific coin on Binance
        
        Parameters:
        coin (str): Trading pair coin (e.g., 'BTCUSDT')
        
        Returns:
        list: List of aggregate trades data
        """
        # Binance API endpoint for aggregate trades
        try:
            url = "https://api.binance.com/api/v3/aggTrades"
            
            # Calculate timestamps (in milliseconds)
            end_time = int(time.time() * 1000)  # Current time in milliseconds
            start_time = end_time - (60 * 1000)  # One minute ago
            
            # Request parameters
            params = {
                'symbol': coin,
                'startTime': start_time,
                #'endTime': end_time,
                'limit': 1000  # Maximum allowed
            }
            
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # Raise exception for HTTP errors
                
                trades = response.json()
                return trades
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from Binance: {e}")
                return []
        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error fetching aggregate trades: {e}")
            return []

    def calculate_last_minute_volume(self, coin):
        """
        Fetches trades from the last minute and calculates or estimates the total volume.
        
        Parameters:
        coin (str): Trading pair coin (e.g., 'BTCUSDT')
        
        Returns:
        tuple: (volume, is_estimated, time_span_seconds)
            - volume: The calculated or estimated trading volume for a minute
            - is_estimated: Boolean indicating if the volume was estimated
            - time_span_seconds: Actual time span of the collected data in seconds
        """
        try:
            trades = self.get_last_minute_aggregate_trades(coin)
            
            if not trades:
                return 0, False, 0
            
            # Get timestamps of oldest and newest trades
            oldest_time = trades[0]["T"]
            newest_time = trades[-1]["T"]
            
            # Calculate time span in seconds
            time_span_ms = newest_time - oldest_time
            time_span_seconds = time_span_ms / 1000
            
            # Calculate relative actual volume from available trades wrt benchmark
            actual_volume = sum(float(trade["p"]) * float(trade["q"]) for trade in trades)
            
            # Only estimate if we have reached the maximum number of trades (1000)
            # This indicates we might be missing trades due to API limitation
            if len(trades) >= 999 and time_span_seconds < 60:
                estimated_volume = (actual_volume / time_span_seconds) * 60
                return estimated_volume / self.benchmark[coin], True, time_span_seconds
            
            # Otherwise, return the actual volume without estimation
            return actual_volume / self.benchmark[coin], False, time_span_seconds
        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error calculating last minute volume: {e}")
            return 0, False, 0

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
            
        self.bid_order_distribution_list[coin].append({
            'bid': bid_order_distribution,
            'dt': datetime.now()
        })
        
        # Keep only the last N objects based on datetime
        if len(self.bid_order_distribution_list[coin]) > self.under_observation[coin]['riskmanagement_configuration']['last_i_ask_order_distribution']:
            self.bid_order_distribution_list[coin].sort(key=lambda x: x['dt'], reverse=True)
            self.bid_order_distribution_list[coin] = self.bid_order_distribution_list[coin][:self.under_observation[coin]['riskmanagement_configuration']['last_i_ask_order_distribution']]
        
        return self.bid_order_distribution_list[coin]

    def process_order_book_update(self, coin, total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, order_distribution, current_time, type_update):
        if type_update == 'ask':
            self.ask_order_distribution_list[coin] = self.update_ask_order_distribution_list(order_distribution, coin)
        # elif type_update == 'bid':
        #     self.bid_order_distribution_list[coin] = self.update_bid_order_distribution_list(order_distribution, coin)


        if type_update == 'ask' and not self.BUY[coin]:
            self.check_buy_trading_conditions(coin, summary_ask_orders)

        self.update_order_book_record(
            total_bid_volume,
            total_ask_volume,
            summary_bid_orders,
            summary_ask_orders,
            coin
        )

        if type_update == 'ask':
            if self.last_ask_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_0LEVEL_THRESHOLD:
                self.ask_0firstlevel_orderlevel_detected[coin] = True

            elif self.last_ask_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
                self.ask_1firstlevel_orderlevel_detected[coin] = True
                self.ask_1firstlevel_orderlevel_detected_datetime[coin] = current_time

        elif type_update == 'bid':
            if self.last_bid_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
                self.bid_1firstlevel_orderlevel_detected[coin] = True
                self.bid_1firstlevel_orderlevel_detected_datetime[coin] = current_time

        self.last_db_update_time[coin] = current_time

    def get_statistics_on_order_book(self, coin, delta=0.01):
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
            summary_bid_orders = []
            summary_ask_orders = []
            next_delta_threshold = 0 + delta
            
            # Calculate summary bid orders
            cumulative_bid_volume = 0
            for price, qty in bid_orders:
                price_order = float(price)
                quantity_order = float(qty)
                cumulative_bid_volume += price_order * quantity_order
                cumulative_bid_volume_ratio = self.round_((cumulative_bid_volume / total_bid_volume), self.count_decimals(delta)-1)
                
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
                cumulative_ask_volume_ratio = self.round_((cumulative_ask_volume / total_ask_volume), self.count_decimals(delta)-1)
                
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

            # if datetime.now().second == 0:
            #     print(delta)
            #     print(summary_ask_orders)

            #self.logger.info(f'coin: {coin}; ask_order_distribution: {ask_order_distribution}; bid_order_distribution: {bid_order_distribution}')
            
            # Update order distribution tracking
            self.last_ask_order_distribution_1level[coin] = ask_order_distribution[str(self.under_observation[coin]['riskmanagement_configuration']['price_change_jump'])]
            self.last_bid_order_distribution_1level[coin] = bid_order_distribution[str(self.under_observation[coin]['riskmanagement_configuration']['price_change_jump'])]
            
            return total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, ask_order_distribution, bid_order_distribution
        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error getting statistics on order book for {coin}: {e}")
            return 0, 0, [], [], {}, {}

    def analyze_order_book(self, coin):
        """Analyze the order book and update trading state for a specific coin"""
        try:
            #t1 = time.time()
            total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, ask_order_distribution, bid_order_distribution = self.get_statistics_on_order_book(coin)
            #t2 = time.time()

            #self.logger.info(f'time to get statistics on order book for {coin}: {self.round_(t2 - t1, 5)} seconds. Last ask : {self.last_ask_order_distribution_1level[coin]} Last bid : {self.last_bid_order_distribution_1level[coin]} -  {self.last_db_update_time[coin]}')
            
            # Check BUY Trading Conditions or Update Order Book Record based on thresholds
            current_time = datetime.now()

            # ASK SIDE ANALYSIS - CRITICAL PRIORITY
            # Check if ask order distribution is below critical threshold or in monitoring window
            if self.last_ask_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD or \
                (current_time - self.ask_1firstlevel_orderlevel_detected_datetime[coin] < timedelta(minutes=self.TIMEDELTA_MINUTES_FROM_1LEVEL_DETECTED_1)):
                if self.last_ask_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_0LEVEL_THRESHOLD and not self.ask_0firstlevel_orderlevel_detected[coin]:
                    total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, ask_order_distribution, bid_order_distribution = self.get_statistics_on_order_book(coin, delta=0.005)
                    self.process_order_book_update(coin, total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, ask_order_distribution, current_time, 'ask')
                # First detection: Update immediately and mark this low level as detected
                elif self.last_ask_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD and not self.ask_1firstlevel_orderlevel_detected[coin]:
                    total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, ask_order_distribution, bid_order_distribution = self.get_statistics_on_order_book(coin, delta=0.005)
                    self.process_order_book_update(coin, total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, ask_order_distribution, current_time, 'ask')
                # Ongoing monitoring: Update at regular intervals during monitoring window
                elif (current_time - self.last_db_update_time[coin]).total_seconds() >= self.DB_UPDATE_MIN_WAITING_TIME_1LEVEL:
                    total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, ask_order_distribution, bid_order_distribution = self.get_statistics_on_order_book(coin, delta=0.005)
                    self.process_order_book_update(coin, total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, ask_order_distribution, current_time, 'ask')

            # ASK SIDE ANALYSIS - MODERATE PRIORITY
            # Check if ask distribution is below secondary threshold or in secondary monitoring window
            elif self.last_ask_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD or \
                (current_time - self.ask_1firstlevel_orderlevel_detected_datetime[coin] < timedelta(minutes=self.TIMEDELTA_MINUTES_FROM_1LEVEL_DETECTED_2)):
                # Update less frequently for moderate conditions
                if ((current_time - self.last_db_update_time[coin]).total_seconds() >= self.DB_UPDATE_MIN_WAITING_TIME_2LEVEL):
                    self.process_order_book_update(coin, total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, ask_order_distribution, current_time, 'ask')

            # ASK SIDE ANALYSIS - NORMAL MARKET CONDITIONS
            # Normal market conditions for ask side, very infrequent updates
            elif self.last_ask_order_distribution_1level[coin] > self.ORDER_DISTRIBUTION_2LEVEL_THRESHOLD:
                if ((current_time - self.last_db_update_time[coin]).total_seconds() >= self.DB_UPDATE_MAX_WAITING_TIME):
                    self.process_order_book_update(coin, total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, ask_order_distribution, current_time, 'ask')

            # BID SIDE ANALYSIS - CRITICAL PRIORITY ONLY
            # Only process bid side in two scenarios:
            # 1. Critical threshold is breached (regardless of ask conditions)
            # 2. In monitoring window only if ask conditions weren't met
            if self.last_bid_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD or \
                (current_time - self.bid_1firstlevel_orderlevel_detected_datetime[coin] < timedelta(minutes=self.TIMEDELTA_MINUTES_FROM_1LEVEL_DETECTED_1)):
                # First detection: Update immediately and mark low level detected
                if self.last_bid_order_distribution_1level[coin] <= self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD and not self.bid_1firstlevel_orderlevel_detected[coin]:
                    self.process_order_book_update(coin, total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, bid_order_distribution, current_time, 'bid')
                # Ongoing monitoring: Update at regular intervals
                elif (current_time - self.last_db_update_time[coin]).total_seconds() >= self.DB_UPDATE_MIN_WAITING_TIME_1LEVEL:
                    self.process_order_book_update(coin, total_bid_volume, total_ask_volume, summary_bid_orders, summary_ask_orders, bid_order_distribution, current_time, 'bid')

            if self.ask_1firstlevel_orderlevel_detected[coin] == True and self.last_ask_order_distribution_1level[coin] > self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
                self.ask_1firstlevel_orderlevel_detected[coin] = False

            if self.bid_1firstlevel_orderlevel_detected[coin] == True and self.last_bid_order_distribution_1level[coin] > self.ORDER_DISTRIBUTION_1LEVEL_THRESHOLD:
                self.bid_1firstlevel_orderlevel_detected[coin] = False

            if self.ask_0firstlevel_orderlevel_detected[coin] == True and self.last_ask_order_distribution_1level[coin] > self.ORDER_DISTRIBUTION_0LEVEL_THRESHOLD:
                self.ask_0firstlevel_orderlevel_detected[coin] = False


        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error analyzing order book for {coin}: {e}")

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

            # Get the current document ID from under_observation
            current_doc_id = self.under_observation[coin]['current_doc_id']
            
            filter_query = {"_id": current_doc_id}

            new_doc = {
                f"data.{now}": new_data,
                "max_price": self.max_price[coin],
                "initial_price": self.initial_price[coin],
                "summary_jump_price_level": self.summary_jump_price_level.get(coin, {}),
                "ask_order_distribution_list": self.ask_order_distribution_list.get(coin, []),
                "buy": self.BUY[coin],
                "buy_price": self.buy_price[coin]
            }

            update_doc = {
                "$set": new_doc
            }
            
            try:
                result = self.orderbook_collection[coin].update_one(filter_query, update_doc)
                if result.modified_count != 1:
                    self.logger.error(f"Connection {self.connection_id} - Order Book update failed for coin event_key {self.under_observation[coin]['event_key']} for coin {coin}")
            except Exception as e:
                if "16777216" in str(e):
                    try:
                        # Create a new document with part number
                        if current_doc_id == self.under_observation[coin]['start_observation']:
                            new_doc_id = f"{self.under_observation[coin]['start_observation']}_part1"
                        else:
                            # Extract current part number and increment
                            current_part = int(current_doc_id.split('_part')[1])
                            new_doc_id = f"{self.under_observation[coin]['start_observation']}_part{current_part + 1}"
                        
                        new_doc = {
                            "_id": new_doc_id,
                            "parent_id": self.under_observation[coin]['start_observation'],
                            "coin": coin,
                            f"data.{now}": new_data,
                            "max_price": self.max_price[coin],
                            "initial_price": self.initial_price[coin],
                            "summary_jump_price_level": self.summary_jump_price_level.get(coin, {}),
                            "ask_order_distribution_list": self.ask_order_distribution_list.get(coin, []),
                            "buy": self.BUY[coin],
                            "buy_price": self.buy_price[coin],
                            "is_continuation": True
                        }
                        self.orderbook_collection[coin].insert_one(new_doc)
                        
                        # Update the current document ID in metadata
                        self.metadata_orderbook_collection.update_one(
                            {"_id": self.under_observation[coin]['start_observation']},
                            {"$set": {"current_doc_id": new_doc_id}}
                        )
                        
                        # Update local reference
                        self.under_observation[coin]['current_doc_id'] = new_doc_id
                        self.logger.info(f"Connection {self.connection_id} - Created new continuation document {new_doc_id} for coin {coin}")
                    except Exception as e:
                        self.logger.error(f"Connection {self.connection_id} - Error initializing new document for {coin}: {e}")
                        raise e
                else:
                    self.logger.error(f"Connection {self.connection_id} - Error Case Not Detected {coin}: {e}")

        except Exception as e:
            try:

                if datetime.now().second == 0 and datetime.now().minute % 10 == 0:
                    self.logger.error(f"Connection {self.connection_id} - Error updating order book record for {coin}: {e}")
                    self.logger.info(f'Connection {self.connection_id} - new_doc: {new_doc}')
            except Exception as e:
                self.logger.error(f"Connection {self.connection_id} - Error printing error {coin}: {e}")
                

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

    def wait_for_snapshot(self, coin):
        """Wait for the snapshot to be ready, in order to avoid rate limit ban"""
        try:
            not_ready_to_go = True

            while not_ready_to_go and not self.should_exit:
                
                self.last_minute_snapshots = [x for x in self.last_minute_snapshots if (datetime.now() - x).total_seconds() <= 60]
                self.last_minute_snapshots.sort(reverse=True)

                if len(self.last_minute_snapshots) > 20:
                    remaining_wait = 60 - (datetime.now() - self.last_minute_snapshots[-1]).total_seconds()
                    self.logger.info(f"Connection {self.connection_id} - Waiting for snapshot for {coin} for {remaining_wait} seconds")
                    if remaining_wait > 0:
                        sleep(remaining_wait)
                else:
                    not_ready_to_go = False

            # Update last restart time even if there's an error
            #self.logger.info(f'under_observation: {self.under_observation}')

        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error in wait_for_snapshot: {e}")
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
            self.initialize_order_book()
            websocket.enableTrace(False)
            
            # Create WebSocket connection with proper URL
            full_url = f"{self.ws_url}{','.join(self.parameters)}"
            self.logger.info(f"Connection {self.connection_id} - Starting WebSocket with {self.parameters}")
            
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open,
                on_ping=self.on_ping,
                on_pong=self.on_pong
            )

            # Run in a separate thread with ping interval and timeout
            self.ws_thread = threading.Thread(target=self.ws.run_forever, kwargs={
                'ping_interval': self.ping_interval,
                'ping_timeout': self.ping_timeout
            })
            self.ws_thread.daemon = True
            self.ws_thread.start()
            
            # Also start the connection health check thread
            threading.Thread(target=self.check_connection, daemon=True).start()
            
        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error in start method: {e}")
            # Release the lock if start fails
            with self.connection_lock:
                self.running = False
            # Try to restart after a delay
            if not self.should_exit:
                self.logger.info(f"Connection {self.connection_id} - Attempting to restart after error...")
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
                    self.logger.error(f"Connection {self.connection_id} - Error closing WebSocket: {e}")

    def update_metadata_event(self, coin):
        metadata_filter = {
                "_id": self.under_observation[coin]['start_observation'],
            }
        update_doc = {
                "$set": {
                    "status": "completed"
                }
            }
        result = self.metadata_orderbook_collection.update_one(metadata_filter, update_doc)
        if result.modified_count != 1:
            self.logger.error(f"Connection {self.connection_id} - Metadata Collection update failed for {coin}")

    def update_trading_and_metadata_event(self, coin):
        """Update trading event with sell information for a specific coin"""
        try:

            sell_price = self.current_price[coin]
            sell_ts = datetime.now().isoformat()
            gain = (sell_price - self.buy_price[coin]) / self.buy_price[coin]
            gain_print = PooledBinanceOrderBook.round_(gain*100, 3)
            self.logger.info(f"Connection {self.connection_id} - SELL EVENT: {coin} at {sell_price}. GAIN: {gain_print}%")
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
                self.logger.error(f"Connection {self.connection_id} - Metadata Collection update failed for {coin}")
            
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
                self.logger.error(f"Connection {self.connection_id} - Trading Collection update failed for {coin} with id {self.id}_{coin}")
            
        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error updating trading event for {coin}: {e}")

    def check_buy_trading_conditions(self, coin, summary_ask_orders):
        """Check if trading conditions are met for a specific coin"""
        try:
            if self.current_price[coin] > self.max_price[coin]:  
                self.max_price[coin] = self.current_price[coin]
                
            max_change = (self.max_price[coin] - self.initial_price[coin]) / self.initial_price[coin]
            current_price_drop = abs((self.current_price[coin] - self.max_price[coin]) / self.max_price[coin])
            is_jump_price_level = self.hit_jump_price_levels_range(coin)
            
            if (is_jump_price_level and 
                current_price_drop >= self.under_observation[coin]['riskmanagement_configuration']['price_drop_limit'] and 
                max_change <= self.under_observation[coin]['riskmanagement_configuration']['max_limit']):
                
                # Check ask order distribution
                all_levels_valid = True
                avg_distribution = self.calculate_average_distribution(self.ask_order_distribution_list[coin])
                keys_ask_order_distribution = list(avg_distribution.keys())[:self.under_observation[coin]['riskmanagement_configuration']['lvl_ask_order_distribution_list']]
                
                for lvl in keys_ask_order_distribution:
                    if avg_distribution[lvl] >= self.under_observation[coin]['riskmanagement_configuration']['max_ask_order_distribution_level']:
                        all_levels_valid = False
                        break

                if all_levels_valid:
                    self.BUY[coin] = True
                    self.buy_price[coin] = self.current_price[coin]
                    self.logger.info(f"Connection {self.connection_id} - BUY EVENT: {coin} at {self.current_price[coin]}")
                    self.logger.info(f"Connection {self.connection_id} - avg_distribution: {avg_distribution}")
                    self.logger.info(f"Connection {self.connection_id} - summary_ask_orders: {summary_ask_orders}")
                    self.discover_target_price(coin, summary_ask_orders, avg_distribution, n_target_levels=6)
                    #asyncio.run_coroutine_threadsafe(self.get_tracker_volume_coin(coin), self._loop)
                    self.get_tracker_volume_coin(coin)

                    self.save_trading_event(coin)
        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error checking buy trading conditions for {coin}: {e}")

    def discover_target_price(self, coin, summary_ask_orders, avg_distribution, n_target_levels=3):
        """Discover the target price for a specific coin"""
        try:
            avg_distribution_keys = sorted([0] + [float(x) for x in list(avg_distribution.keys())])
            n_targets = 0
            cumulative_volume = 0
            for level in summary_ask_orders:
                if n_targets < n_target_levels:
                    price_change_target = level[0]
                    target_price = self.round_(self.current_price[coin] * (1 + price_change_target), self.count_decimals(self.current_price[coin]))
                    for key in avg_distribution_keys:
                        if key == avg_distribution_keys[0]:
                            continue
                        # 0.025 > price_change_target <= 0.05 (for example)
                        if price_change_target <= key and price_change_target > avg_distribution_keys[avg_distribution_keys.index(key) - 1]:
                            cumulative_volume += self.round_(avg_distribution[str(key)] * 100, 2)
                            price_change_target_print = self.round_(price_change_target*100, self.count_decimals(self.current_price[coin]))
                            self.logger.info(f"Connection {self.connection_id} - Target price for {coin}: {target_price} (+{price_change_target_print}%) with cumulative volume {cumulative_volume}%")
                            n_targets += 1
                            break
        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error discovering target price for {coin}: {e}")

            
    #async def get_tracker_volume_coin(self, coin):
    def get_tracker_volume_coin(self, coin):
        self.tracker_collection[coin] = self.db_tracker[coin]
        # Find the most recent document in the collection (sorted by _id in descending order)
        last_tracker_doc = self.tracker_collection[coin].find_one(sort=[("_id", -1)])
        self.logger.info(f"Connection {self.connection_id} - Market Volume {coin}: {last_tracker_doc}")
        volume, is_estimated, time_span = self.calculate_last_minute_volume(coin)
        if is_estimated:
            self.logger.info(f"Connection {self.connection_id} - Estimated 1-minute volume for {coin}: {volume:.2f} (based on {time_span:.1f} seconds of data)")
        else:
            self.logger.info(f"Connection {self.connection_id} - Actual 1-minute volume for {coin}: {volume:.2f} (time span: {time_span:.1f} seconds)")
    

        # await asyncio.sleep(60 - datetime.now().second - (datetime.now().microsecond / 1000000) + 5)
        # # Try again after waiting
        # last_tracker_doc = self.tracker_collection[coin].find_one(sort=[("_id", -1)])
        # self.logger.info(f"Market Volume {coin}: {last_tracker_doc}")
        

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
            self.logger.error(f"Connection {self.connection_id} - Error calculating average distribution: {e}")
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
                
                if jump <= self.under_observation[coin]['riskmanagement_configuration']['distance_jump_to_current_price'] and historical_price_level_n_obs >= self.under_observation[coin]['riskmanagement_configuration']['min_n_obs_jump_level']:
                    return True
            
            return False
        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error in hit_jump_price_levels_range for {coin}: {e}")
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

            #end_observation is only used for observing the coin, it is not used for the trading event
            # the sell event is dependant to market conditions, and the sell event is ultimately executed in case end_observation is reached
            end_observation = max(datetime.fromisoformat(self.under_observation[coin]['end_observation']), datetime.now() + timedelta(minutes=self.MAX_WAITING_TIME_AFTER_BUY)).isoformat()
            self.under_observation[coin]['end_observation'] = end_observation
            self.metadata_orderbook_collection.update_one({"_id": self.under_observation[coin]['start_observation']}, {"$set": {"buy_price": self.buy_price[coin], "end_observation": end_observation}})
        except Exception as e:
            self.logger.error(f"Connection {self.connection_id} - Error saving_trading_event for {coin}: {e}")

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
        coins = coins_list_info[type_USDT]
        #coins = coins[:20]
        return coins

    def search_volatility_event_trigger(self, start_script=False):
        """Search for volatility event trigger"""
        self.logger.info(f"Connection {self.connection_id} - Searching for volatility event trigger")
        if start_script:
            status = "running"
            timeframe_max_waiting_time_after_buy_hours = (datetime.now() - timedelta(days=1) - timedelta(minutes=self.MAX_WAITING_TIME_AFTER_BUY)).isoformat()
            timeframe_24hours = (datetime.now() - timedelta(days=1)).isoformat()

            metadata_docs = list(self.metadata_orderbook_collection.find({ "_id": {"$gt": timeframe_max_waiting_time_after_buy_hours}, "status": status,"buy_price": {"$ne": 0},"sell_price": 0},
                                                                {"coin": 1, "event_key": 1, "end_observation": 1, "riskmanagement_configuration": 1, "buy_price": 1, "ranking": 1, "current_doc_id": 1} ))
            if len(metadata_docs) != 0:
                for doc in metadata_docs:
                    if doc["coin"] not in self.coins:
                        continue
                    self.under_observation[doc["coin"]] = {
                        'status': True, 
                        'start_observation': doc["_id"], 
                        'end_observation': doc["end_observation"], 
                        'riskmanagement_configuration': doc["riskmanagement_configuration"], 
                        "ranking": doc["ranking"],
                        'current_doc_id': doc.get('current_doc_id', doc["_id"])  # Use stored current_doc_id or default to start_observation
                    }
                    self.buy_price[doc["coin"]] = doc["buy_price"]
                    self.BUY[doc["coin"]] = True
            self.logger.info(f"Connection {self.connection_id} - There are {len(metadata_docs)} coins under observation in BUY status")
            metadata_docs = list(self.metadata_orderbook_collection.find({ "_id": {"$gt": timeframe_24hours}, "status": status}, {"coin": 1, "event_key": 1, "end_observation": 1, "riskmanagement_configuration": 1, "ranking": 1, "current_doc_id": 1} ))
            if len(metadata_docs) != 0:
                for doc in metadata_docs:
                    if doc["coin"] not in self.coins:
                        continue
                    self.orderbook_collection[doc["coin"]] = self.db_orderbook[doc["event_key"]]
                    if not self.BUY[doc["coin"]]:
                        self.under_observation[doc["coin"]] = {
                            'status': True, 
                            'start_observation': doc["_id"], 
                            'end_observation': doc["end_observation"], 
                            'riskmanagement_configuration': doc["riskmanagement_configuration"], 
                            "ranking": doc["ranking"],
                            'current_doc_id': doc.get('current_doc_id', doc["_id"])  # Use stored current_doc_id or default to start_observation
                        }
            self.logger.info(f"Connection {self.connection_id} - There are {len(metadata_docs)} coins under observation.")
            return
        else:
            self.logger.info(f"Connection {self.connection_id} - Calling search_volatility_event_trigger on instance {self.connection_id}")
            while True:
                try:
                    n_coins_under_observation = 0
                    numbers_filled = []
                    status = "pending"
                    timeframe_1day = (datetime.now() - timedelta(days=1)).isoformat()
                    update_doc = {}
                    coins_under_observation = []
                    metadata_docs = list(self.metadata_orderbook_collection.find({"_id": {"$gt": timeframe_1day}}, {"coin": 1, "event_key": 1, "end_observation": 1,"ranking": 1, "status": 1, "number": 1, "current_doc_id": 1}))
                    # self.logger.info(f'metadata_docs: {metadata_docs}')
                    if len(metadata_docs) != 0:
                        riskmanagement_configuration = self.get_riskmanagement_configuration()
                        for doc in metadata_docs:
                            if doc["coin"] not in self.coins:
                                continue
                            if doc["status"] == "pending":
                                self.under_observation[doc["coin"]] = {
                                    'status': True, 
                                    'start_observation': doc["_id"], 
                                    'end_observation': doc["end_observation"], 
                                    'riskmanagement_configuration': riskmanagement_configuration, 
                                    "ranking": doc["ranking"], 
                                    "event_key": doc["event_key"],
                                    'current_doc_id': doc["_id"]  # Use stored current_doc_id or default to start_observation
                                }
                                coins_under_observation.append(doc["coin"])
                                
                            elif doc["status"] == "running":
                                n_coins_under_observation += 1
                                numbers_filled.append(doc["number"])

                        for coin in coins_under_observation:
                            for number in range(len(numbers_filled)+1):
                                if number not in numbers_filled:
                                    update_doc[coin] = { "$set": { "status": "running", "riskmanagement_configuration": riskmanagement_configuration, "number": number } }
                                    numbers_filled.append(number)
                                    break

                            self.metadata_orderbook_collection.update_one({"_id": self.under_observation[coin]["start_observation"]}, update_doc[coin])
                            event_key = self.under_observation[coin]["event_key"]
                            ranking = self.under_observation[coin]["ranking"]
                            self.orderbook_collection[coin] = self.db_orderbook[event_key]
                            self.orderbook_collection[coin].insert_one({
                                "_id": self.under_observation[coin]["start_observation"],
                                "coin": coin,
                                "ranking": ranking,
                                "data": {},
                                "initial_price": 0,
                                "max_price": 0,
                                "buy_price": '',
                                "buy": False,
                                "number": number,  # This will be updated later
                                "strategy_parameters": riskmanagement_configuration,
                                "summary_jump_price_level": {},
                                "ask_order_distribution_list": []
                            })
                            self.logger.info(f"Connection {self.connection_id} - Coin {coin} - event_key: {event_key} - {number+1}/{len(numbers_filled)} - ranking: {ranking}")
                        
                    next_run = 60 - datetime.now().second - datetime.now().microsecond / 1000000 + 5
                    sleep(next_run)
                except Exception as e:
                    self.logger.error(f"Connection {self.connection_id} - Error in search_volatility_event_trigger: {e}")
                            

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
            # total_remaining_seconds = 240 + remaining_seconds

            # define timestamp for next wss restart
            wss_restart_timestamp = (now + timedelta(seconds=total_remaining_seconds)).isoformat()
            self.logger.info(f"Connection {self.connection_id} - on_restart_connection: Next wss restart {wss_restart_timestamp}")

            sleep(total_remaining_seconds)
            self.logger.info(f"Connection {self.connection_id} - Restart Connection")
        finally:
            with self.connection_restart_lock:
                self.connection_restart_running = False
                self.ws.close()

if __name__ == "__main__":
    # Get command line arguments
    coins = PooledBinanceOrderBook.get_coins()
    
    # Create a MultiConnectionOrderBook with multiple connections
    # Adjust the connection_count as needed (5-10 suggested)
    multi_order_book = MultiConnectionOrderBook(coins=coins, connection_count=10)
    
    # Initialize all order book instances
    multi_order_book.initialization_process()
    
    # In case of restart, check if there are already coins under observation
    multi_order_book.wrap_search_volatility_event_trigger(start_script=True)
    
    # Set a restart connection thread to be executed at midnight
    threading.Thread(target=multi_order_book.restart_connection, daemon=True).start()
    
    # Set a thread to be executed every 60 seconds to check if there are coins under observation
    threading.Thread(target=multi_order_book.wrap_search_volatility_event_trigger, daemon=True).start()
    
    # Start all the connections
    multi_order_book.start()
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        multi_order_book.stop()