from fastapi import APIRouter
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from routes import CryptoCom, Binance

router_api = APIRouter()
router_api.include_router(CryptoCom.router, prefix="/crypto", tags=["Crypto"])
router_api.include_router(Binance.router, prefix="/binance", tags=["Binance"])