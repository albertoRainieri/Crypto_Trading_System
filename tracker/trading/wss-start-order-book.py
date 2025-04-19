import websocket
import json
import requests
from typing import Dict, List, Optional
import threading
import time

class BinanceOrderBook:
    def __init__(self, symbol: str = "BNBBTC"):
        self.symbol = symbol
        self.order_book = {
            'bids': {},  # price -> quantity
            'asks': {},  # price -> quantity
            'lastUpdateId': None
        }
        self.buffered_events = []
        self.ws = None
        self.running = False
        self.snapshot_url = f"https://api.binance.com/api/v3/depth?symbol={self.symbol}&limit=5000"
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@depth"

    def on_message(self, ws, message):
        data = json.loads(message)
        
        # If we haven't received the snapshot yet, buffer the events
        if self.order_book['lastUpdateId'] is None:
            self.buffered_events.append(data)
            return

        # Process the update
        self.process_update(data)

    def on_error(self, ws, error):
        print(f"WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print("WebSocket Connection Closed")
        if self.running:
            print("Attempting to reconnect...")
            time.sleep(5)
            self.start()

    def on_open(self, ws):
        print("WebSocket Connection Established")
        self.get_snapshot()

    def get_snapshot(self):
        """Get the initial order book snapshot"""
        response = requests.get(self.snapshot_url)
        print("########################################################")
        print('getting snapshot')
        print("########################################################")
        if response.status_code == 200:
            snapshot = response.json()
            print(snapshot)
            print()
            self.order_book['bids'] = {float(price): float(qty) for price, qty in snapshot['bids']}
            self.order_book['asks'] = {float(price): float(qty) for price, qty in snapshot['asks']}
            self.order_book['lastUpdateId'] = snapshot['lastUpdateId']
            
            # Process buffered events
            self.process_buffered_events()
        else:
            print(f"Error getting snapshot: {response.status_code}")

    def process_buffered_events(self):
        """Process events that were received before the snapshot"""
        print(f"Processing {len(self.buffered_events)} buffered events")
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
            print(f"Skipping update {event['u']} because it is less than or equal to {self.order_book['lastUpdateId']}")
            return

        # Check for gaps in the update sequence
        # If the first update ID in this event is more than 1 greater than our last processed ID,
        # it means we've missed some updates. In this case, we need to restart the process
        # by getting a fresh snapshot.
        if event['U'] > self.order_book['lastUpdateId'] + 1:
            print("Gap detected in updates, restarting...")
            self.order_book['lastUpdateId'] = None
            self.get_snapshot()
            return

        # Process bid updates
        # Each update contains a list of [price, quantity] pairs
        # If quantity is 0, it means the price level should be removed
        # Otherwise, the price level should be updated with the new quantity
        for price, qty in event['b']:
            price = float(price)  # Convert string to float for price
            qty = float(qty)      # Convert string to float for quantity
            
            if qty == 0:
                print(f"Removing price level {price} with quantity {qty}")
                # Remove the price level if quantity is 0
                self.order_book['bids'].pop(price, None)
            else:
                print(f"Adding price level {price} with quantity {qty}")
                # Update or add the price level with new quantity
                self.order_book['bids'][price] = qty

        # Process ask updates
        # Similar to bid updates, but for the ask side of the order book
        for price, qty in event['a']:
            price = float(price)  # Convert string to float for price
            qty = float(qty)      # Convert string to float for quantity
            
            if qty == 0:
                # Remove the price level if quantity is 0
                self.order_book['asks'].pop(price, None)
            else:
                # Update or add the price level with new quantity
                self.order_book['asks'][price] = qty

        # Update our last processed update ID
        # This is crucial for maintaining the correct sequence of updates
        # and detecting gaps in the future
        self.order_book['lastUpdateId'] = event['u']

    def start(self):
        """Start the WebSocket connection"""
        self.running = True
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        
        # Run WebSocket in a separate thread
        self.ws_thread = threading.Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()

    def stop(self):
        """Stop the WebSocket connection"""
        self.running = False
        if self.ws:
            self.ws.close()

    def get_best_bid(self) -> Optional[float]:
        """Get the best bid price"""
        if self.order_book['bids']:
            return max(self.order_book['bids'].keys())
        return None

    def get_best_ask(self) -> Optional[float]:
        """Get the best ask price"""
        if self.order_book['asks']:
            return min(self.order_book['asks'].keys())
        return None

    def get_spread(self) -> Optional[float]:
        """Get the current spread"""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid and best_ask:
            return best_ask - best_bid
        return None

    def print_order_book_summary(self):
        """Print a summary of the current order book"""
        print("==================================================")
        print(f"Order Book Summary for {self.symbol}")
        print("==================================================")
        print(self.order_book)
        print(len(self.order_book['bids']))
        # best_bid = self.get_best_bid()
        # best_ask = self.get_best_ask()
        # spread = self.get_spread()
        # if best_bid:
        #     print(f"Best Bid: {best_bid:.2f}")
        # if best_ask:
        #     print(f"Best Ask: {best_ask:.2f}")
        # if spread:
        #     print(f"Spread: {spread:.2f}")
        print("==================================================")

if __name__ == "__main__":
    # Example usage
    order_book = BinanceOrderBook("TUSDUSDT")
    order_book.start()
    
    try:
        while True:
            time.sleep(1)
            order_book.print_order_book_summary()
    except KeyboardInterrupt:
        order_book.stop() 