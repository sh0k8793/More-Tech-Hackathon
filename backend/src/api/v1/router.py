from fastapi import APIRouter
from api.v1.analysis import analysis

v1 = APIRouter(prefix="/v1")

v1.include_router(analysis)
