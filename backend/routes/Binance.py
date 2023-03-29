import sys, os
from fastapi import APIRouter
from app.Controller.BinanceController import BinanceController
sys.path.insert(1, os.path.join(sys.path[0], '..'))

router = APIRouter(
)

@router.get("/get-trades")
async def getTrades():
    return BinanceController.getTrades()