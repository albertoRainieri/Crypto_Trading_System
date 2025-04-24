import os, sys
sys.path.insert(0, "..")
import json
from datetime import datetime, timedelta
from time import sleep
import subprocess
from app.Controller.LoggingController import LoggingController

def main():
    logger = LoggingController.start_logging()
    
    # Test parameters
    coin = "OPUSDT"  # Example coin
    event_key = "vol_60m:0.85/vol_60m:15/timeframe:360"  # Example event key
    id = datetime.now().isoformat()
    id2 = datetime.now().isoformat()
    ranking = "1"  # Example ranking
    
    # Strategy parameters (example values)
    strategy_parameters = {
        "strategy_jump": 0.03,
        "limit": 0.4,
        "price_change_jump": 0.025,
        "max_limit": 0.1,
        "price_drop_limit": 0,
        "distance_jump_to_current_price": 0.3,
        "max_ask_order_distribution_level": 0.05,
        "last_i_ask_order_distribution": 1,
        "min_n_obs_jump_level": 2,
        "lvl_ask_order_distribution_list": 1
    }
    
    # Event keys (example)
    event_keys = {
        "event_keys": [
            "vol_60m:0.85/vol_60m:15/timeframe:360",
            "vol_60m:0.85/vol_60m:15/timeframe:720"
        ]
    }
    
    # Start the WebSocket order book script
    logger.info(f"Starting WebSocket order book for {coin} with event key {event_key}")
    
    # Execute the WebSocket order book script with the same parameters as start-order-book.py
    subprocess.Popen([
        "python3", 
        "/tracker/trading/wss-start-order-book.py",
        coin,
        event_key,
        id,
        ranking,
        "0",  # RESTART flag
        json.dumps(strategy_parameters),
        json.dumps(event_keys)
    ])


    subprocess.Popen([
        "python3", 
        "/tracker/trading/start-order-book.py",
        coin,
        event_key,
        id2,
        ranking,
        "0",  # RESTART flag
        json.dumps(strategy_parameters),
        json.dumps(event_keys)
    ])

    logger.info(f"WebSocket order book script started for {coin}")
    
    # Keep the script running to observe the WebSocket connection
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        logger.info("Test script terminated by user")

if __name__ == "__main__":
    main()
