from fastapi import APIRouter
from api.v1.analys import analys

v1 = APIRouter(prefix="/v1")

v1.include_router(analys)
