from api.v1.analysis import analysis
from fastapi import APIRouter

v1 = APIRouter(prefix="/v1")

v1.include_router(analysis)
