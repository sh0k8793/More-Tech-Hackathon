from core.pool import get_conn
from fastapi import APIRouter

health = APIRouter(prefix="/health")

@health.get("/")
def status():
    return {
        "api_status":"health",
        "db_status": "health" if get_conn() else "death"
    }

