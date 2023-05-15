
import sys, os
from fastapi import APIRouter
from app.Controller.AnalysisController import AnalysisController
from app.Controller.AuthController import Authorization
from fastapi import Depends, Request

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
