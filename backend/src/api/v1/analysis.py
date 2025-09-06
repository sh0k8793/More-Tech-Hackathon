from typing import List

from fastapi import APIRouter, Depends
from src.core.pool import get_conn

analysis = APIRouter(prefix="/analysis")


@analysis.post("/", status_code=200)
def analyse_single_query(
    query: str,
    conn=Depends(get_conn)
):
    return "Hello, World!"


@analysis.post("/bulk", status_code=200)
def analyse_multiple_queries(
    queries: List[str],
    conn=Depends(get_conn)
):
    return "Hello, World!"
