from fastapi import APIRouter, Depends
from core.pool import get_conn


analys = APIRouter(prefix="/analys") 


@analys.post("/", status_code=200)
def analys_one(
    conn=Depends(get_conn)  
):
    return "Hello, World!"
