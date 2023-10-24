
import sys, os
from fastapi import APIRouter
from app.Controller.AnalysisController import AnalysisController
from app.Controller.AuthController import Authorization
from fastapi import Depends, Request, Body

sys.path.insert(1, os.path.join(sys.path[0], '..'))

router = APIRouter(
)

@router.get("/get-data")
async def getTrades(datetime_start, datetime_end):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.getData(datetime_start, datetime_end)

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
async def getTimeseries(obj: dict = Body(...)):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.getTimeseries(obj)

@router.post("/riskmanagement-configuration")
async def riskmanagement_configuration(obj: dict = Body(...)):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.riskmanagement_configuration(obj)

@router.post("/user-configuration")
async def user_configuration(obj: dict = Body(...)):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.user_configuration(obj)

@router.post("/get-pricechanges")
async def get_price_changes(obj: dict = Body(...)):#, request: Request = Depends(Authorization.get_current_active_user)):
    return AnalysisController.get_price_changes(obj)
