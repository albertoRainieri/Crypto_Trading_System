import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from routes import Router


def get_application():
    app = FastAPI()

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