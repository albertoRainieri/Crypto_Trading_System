import sys, os
from fastapi import APIRouter
from app.Controller.AnalysisController import AnalysisController
sys.path.insert(1, os.path.join(sys.path[0], '..'))

router = APIRouter(
)

@router.get("/get-data")
async def getTrades(datetime_start = None):
    return AnalysisController.getData(datetime_start)