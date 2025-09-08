from api.v1.analysis import analysis
from api.v1.health import health
from fastapi import APIRouter

v1 = APIRouter(prefix="/v1")

v1.include_router(analysis)
v1.include_router(health)
