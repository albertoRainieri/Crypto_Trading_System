
import sys, os
from fastapi import APIRouter
from app.Controller.AnalysisController import AnalysisController
from app.Controller.AuthController import Authorization
from fastapi import Depends, Request, Body

sys.path.insert(1, os.path.join(sys.path[0], '..'))

router = APIRouter(
)

@router.get("/get-data-tracker")
async def getTradesTracker(datetime_start, datetime_end):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.getDataTracker(datetime_start, datetime_end)

@router.get("/get-data-market")
async def getTradesMarket(datetime_start, datetime_end):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.getDataMarket(datetime_start, datetime_end)

@router.get("/get-mosttradedcoins")
async def getMostTradedCoins():#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.getMostTradedCoins()

@router.get("/get-volumeinfo")
async def getVolumeInfo():#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.getVolumeInfo()

@router.get("/get-benchmarkinfo")
async def getBenchmarkInfo():#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.getBenchmarkInfo()

@router.post("/get-timeseries")
async def get_timeseries(obj: dict = Body(...)):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.get_timeseries(obj)

@router.post("/riskmanagement-configuration")
async def riskmanagement_configuration(obj: dict = Body(...)):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.riskmanagement_configuration(obj)

@router.post("/user-configuration")
async def user_configuration(obj: dict = Body(...)):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.user_configuration(obj)

@router.post("/get-pricechanges")
async def get_price_changes(obj: dict = Body(...)):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.get_price_changes(obj)

@router.post("/get-crypto-timeseries")
async def get_crypto_timeseries(obj: dict = Body(...)):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.get_crypto_timeseries(obj)

@router.post("/get-order-book")
async def get_orderbook(obj: dict = Body(...)):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.get_orderbook(obj)

@router.post("/get-order-book-metadata")
async def get_orderbook_metadata(obj: dict = Body(...)):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.get_orderbook_metadata(obj)

@router.get("/get-last-timestamp-tracker")
async def get_last_timestamp_tracker():#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.get_last_timestamp_tracker()