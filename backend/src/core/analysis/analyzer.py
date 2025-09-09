from typing import Any, Dict, List

from psycopg import AsyncConnection

from ..models.analysis_result import AnalysisResult
from ..models.lint_request import LintRequest, LintRequests
from .context import get_database_context
from .rules.analyze_with_rules import analyze_with_rules


class SQLAnalyzer():
    async def _get_explain_plan(self, conn: AsyncConnection, query: str) -> Dict[str, Any]:
        async with conn.cursor() as cur:
            await cur.execute(f"EXPLAIN (FORMAT JSON) {query}") # type: ignore
            result = await cur.fetchone()

            return result[0] # type: ignore

    async def analyze_one(self, lint_request: LintRequest, conn: AsyncConnection) -> AnalysisResult:
        plan = await self._get_explain_plan(conn, lint_request.sql_query)
        context =  await get_database_context(conn)

        lint_diagnoses = analyze_with_rules(
            lint_request.sql_query,
            plan,
            context
        )

        return AnalysisResult(
            lint_diagnoses=lint_diagnoses,
            summary_recommendation="None"
        )

    def analyze_many(lint_requests: LintRequests) -> List[AnalysisResult]: # type: ignore
        pass

analyzer = SQLAnalyzer()
