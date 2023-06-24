import os,sys
sys.path.insert(0,'..')
from database.DatabaseConnection import DatabaseConnection
from datetime import datetime
from constants.constants import *

coin = sys.argv[1]
price = sys.argv[2]
db = DatabaseConnection()
db_trading = db.get_db(DATABASE_TRADING)


timestamp_iso = datetime.now().isoformat()
doc_db = {'_id': timestamp_iso, 'coin': coin, 'profit': None, 'purchase_price': price, 'current_price': None, 'on_trade': False, 'event': None, 'pid': None}
db_trading[COLLECTION_TRADING_LIVE].insert_one(doc_db)
db_trading[COLLECTION_TRADING_STRATEGY1].insert_one(doc_db)
