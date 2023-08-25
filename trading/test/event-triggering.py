import os,sys
sys.path.insert(0,'..')
from database.DatabaseConnection import DatabaseConnection
from datetime import datetime
from constants.constants import *

db = DatabaseConnection()
db_trading = db.get_db(DATABASE_TRADING)


timestamp_iso = datetime.now().isoformat()
doc_db = {'_id': timestamp_iso, 'coin': 'BTCUSDT', 'purchase_price': 29300, 'current_price': None, 'change': None, 'on_trade': False, 'pid': None}
db_trading[COLLECTION_TRADING_LIVE].insert_one(doc_db)