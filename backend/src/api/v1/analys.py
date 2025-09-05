from fastapi import APIRouter, Depends
from typing import List
from core.pool import get_conn


analys = APIRouter(prefix="/analys") 


@analys.post("/", status_code=200)
def analys_one(
    query: str,
    conn=Depends(get_conn)  
):
    return "Hello, World!"


@analys.post("/bulk", status_code=200)
def analys_one(
    queries: List[str],
    conn=Depends(get_conn)  
):
    return "Hello, World!"
