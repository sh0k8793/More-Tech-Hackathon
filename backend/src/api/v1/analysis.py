from fastapi import APIRouter, Depends
from core.pool import get_conn
from core.models.lint_request import LintRequest, LintRequests


analysis = APIRouter(prefix="/analysis")


@analysis.post("/", status_code=200)
def analyse_single_query(
    lint_request: LintRequest,
    conn=Depends(get_conn)
):
    return "Hello, World!"


@analysis.post("/bulk", status_code=200)
def analyse_multiple_queries(
    lint_request: LintRequests,
    conn=Depends(get_conn)
):
    return "Hello, World!"
