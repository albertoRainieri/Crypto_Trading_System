import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from routes import Router
from pydantic import BaseSettings
from app.Controller.LoggingController import LoggingController



class Settings(BaseSettings):
    openapi_url: str = ""


def get_application():

    PRODUCTION = int(os.getenv("PRODUCTION"))

    # PRODUCTION
    if bool(PRODUCTION):
        settings = Settings()
        app = FastAPI(openapi_url=settings.openapi_url)
        
    # DEVELOPMENT
    else:
        app = FastAPI(
            docs_url='/docs',
            redoc_url='/docs/redoc',
            openapi_url='/docs/openapi.json')

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"]
    )

    app.include_router(Router.router_api)
    return app

app = get_application()