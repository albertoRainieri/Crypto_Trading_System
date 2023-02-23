import sys, os
from fastapi import APIRouter
from app.Controller.CryptoController import CryptoController
sys.path.insert(1, os.path.join(sys.path[0], '..'))

router = APIRouter(
)

@router.get("/get-instruments")
async def getInstruments():
    return CryptoController.getInstruments()

@router.get("/get-all-instruments")
async def getAllInstruments():
    return CryptoController.getAllinstruments()


@router.get("/get-book")
async def getAllInstruments():
    return CryptoController.getBook()

@router.get("/get-candlestick")
async def getAllInstruments():
    return CryptoController.getCandlestick()

@router.get("/get-ticker")
async def getAllInstruments():
    return CryptoController.getTicker()

@router.get("/get-trades")
async def getAllInstruments():
    return CryptoController.getTrades()

@router.get("/get-trades_over-q")
async def getTrades_BTC_over_Q():
    return CryptoController.getTrades_BTC_over_Q()