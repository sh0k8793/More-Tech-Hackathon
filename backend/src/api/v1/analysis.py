from core.analysis.analyzer import analyzer
from core.models.lint_request import LintRequest, LintRequests
from core.pool import get_conn
from fastapi import APIRouter, Depends

analysis = APIRouter(prefix="/analysis")


@analysis.post("/", status_code=200)
async def analyse_single_query(
    lint_request: LintRequest,
    conn=Depends(get_conn)
):
    return await analyzer.analyze_one(
        lint_request, 
        conn
    )


@analysis.post("/bulk", status_code=200)
async def analyse_multiple_queries(
    lint_request: LintRequests,
    conn=Depends(get_conn)
):
    return "Hello, World!"
