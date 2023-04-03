from backend.routes import Analysis, Authorization
from fastapi import APIRouter
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from routes import CryptoCom

router_api = APIRouter()
#router_api.include_router(CryptoCom.router, prefix="/crypto", tags=["Crypto"])
router_api.include_router(Analysis.router, prefix="/analysis", tags=["Analysis"])
router_api.include_router(Authorization.router, prefix="/auth", tags=["Auth"])