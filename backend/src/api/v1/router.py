from api.v1.analys import analys
from fastapi import APIRouter

v1 = APIRouter(prefix="/v1")

v1.include_router(analys)
