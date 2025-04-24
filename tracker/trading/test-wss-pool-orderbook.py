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
    #coins = {"coins": ['XRPUSDT', 'SOLUSDT', 'ADAUSDT', 'DOGEUSDT', 'TRUMPUSDT', 'BNBUSDT', 'SUIUSDT', 'PEPEUSDT', 'LTCUSDT', 'TRXUSDT']}  # Example coin
    coins = {"coins": [ "GMTUSDT", "TRBUSDT","TONUSDT","DOGEUSDT","JTOUSDT","WAVESUSDT","ARBUSDT","UTKUSDT","BABYUSDT","BCHUSDT","GALAUSDT","PENDLEUSDT","TUTUSDT","HBARUSDT","ACTUSDT","SYNUSDT","FRONTUSDT","BATUSDT","FDUSDUSDT","IOUSDT","PENGUUSDT","ETHUSDT","SEIUSDT","ARUSDT","WLDUSDT","BBUSDT","XRPUSDT","NOTUSDT","CVCUSDT","TIAUSDT","GTOUSDT","OSMOUSDT","ETHFIUSDT","CATIUSDT","ARKMUSDT","BTTCUSDT","INJUSDT","AVAXUSDT","ATOMUSDT","MEMEUSDT","ONTUSDT","AIXBTUSDT","SANDUSDT","KAITOUSDT","ERDUSDT","BTTUSDT","CHESSUSDT","MUBARAKUSDT","NEIROUSDT","REZUSDT","BCHSVUSDT","1000SATSUSDT","COWUSDT","MOVEUSDT","BANANAUSDT","DOTUSDT","CGPTUSDT","WINUSDT","STEEMUSDT","LAYERUSDT","AUDUSDT","IDEXUSDT","TNSRUSDT","VANRYUSDT","ALTUSDT","RAYUSDT","NILUSDT","DYDXUSDT","RENDERUSDT","ORCAUSDT","NEOUSDT","BCHABCUSDT","SUIUSDT","DUSKUSDT","ETCUSDT","DOGSUSDT","USUALUSDT","STRKUSDT","BUSDUSDT","SLFUSDT","TURBOUSDT","ACHUSDT","MANTAUSDT","1MBABYDOGEUSDT","VETUSDT","TUSDT","BNXUSDT","FTMUSDT","PEPEUSDT","PAXUSDT","LDOUSDT","RUNEUSDT","NEARUSDT","RNDRUSDT","ONEUSDT","ADAUSDT","GPSUSDT","MKRUSDT","BTCUSDT","ANKRUSDT","JUPUSDT","APEUSDT","EIGENUSDT","POLUSDT","PHBUSDT","IOTAUSDT","BLURUSDT","PNUTUSDT","CETUSUSDT","ZECUSDT","CFXUSDT","ENAUSDT","BIOUSDT","TROYUSDT","EURIUSDT","VANAUSDT","HMSTRUSDT","EOSUSDT","EURUSDT","XTZUSDT","BONKUSDT","PYTHUSDT","ACXUSDT","BERAUSDT","PHAUSDT","ZENUSDT","BNBUSDT","SAGAUSDT","LINKUSDT","REDUSDT","USDSUSDT","RAREUSDT","ORDIUSDT","CRVUSDT","1000CATUSDT","TUSDUSDT","TSTUSDT","APTUSDT","PARTIUSDT","NPXSUSDT","DFUSDT","ICPUSDT","CKBUSDT","QNTUSDT","WIFUSDT","1000CHEEMSUSDT","AAVEUSDT","ZROUSDT","OMUSDT","CAKEUSDT","ANIMEUSDT","ZKUSDT","FORMUSDT","RPLUSDT","EGLDUSDT","PIXELUSDT","TFUELUSDT","TRUMPUSDT","ALGOUSDT","XLMUSDT","EPICUSDT","PAXGUSDT","KERNELUSDT","THETAUSDT","HIVEUSDT","SHELLUSDT","HYPERUSDT","KAIAUSDT","BEAMXUSDT","BIGTIMEUSDT","TAOUSDT","RSRUSDT","COOKIEUSDT","PEOPLEUSDT","BANANAS31USDT","AUCTIONUSDT","ENSUSDT","SUSDT","STXUSDT","ONDOUSDT","LTCUSDT","CHZUSDT","TRXUSDT","SOLUSDT","SHIBUSDT","THEUSDT","MATICUSDT","OPUSDT","WCTUSDT","GMXUSDT","GUNUSDT","BROCCOLI714USDT","BMTUSDT","API3USDT","TLMUSDT","FLOKIUSDT","FETUSDT","YGGUSDT","OMNIUSDT","TOMOUSDT","BOMEUSDT","WUSDT","JUVUSDT","HEIUSDT","UNIUSDT","FILUSDT","PERLUSDT","VELODROMEUSDT","VIRTUALUSDT"] }
    #coins = {"coins": ['XRPUSDT']}  # Example coin
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
    
    # Execute the WebSocket order book script with the same parameters as start-order-book.py
    subprocess.Popen([
        "python3", 
        "/tracker/trading/wss-pool-order-book.py",
        json.dumps(coins),
        event_key,
        id,
        ranking,
        "0",  # RESTART flag
        json.dumps(strategy_parameters),
        json.dumps(event_keys)
    ])


    # subprocess.Popen([
    #     "python3", 
    #     "/tracker/trading/start-order-book.py",
    #     coin,
    #     event_key,
    #     id2,
    #     ranking,
    #     "0",  # RESTART flag
    #     json.dumps(strategy_parameters),
    #     json.dumps(event_keys)
    # ])

    # logger.info(f"WebSocket order book script started for {coin}")
    
    # Keep the script running to observe the WebSocket connection
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        logger.info("Test script terminated by user")

if __name__ == "__main__":
    main()
