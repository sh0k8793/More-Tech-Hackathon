from typing import Any, Dict, List

from psycopg import AsyncConnection

from core.models.analysis_result import AnalysisResult
from core.models.lint_request import LintRequest, LintRequests
from core.analysis.context import get_database_context
from core.analysis.rules.analyze_with_rules import analyze_with_rules


class SQLAnalyzer():
    async def _get_explain_plan(self, conn: AsyncConnection, query: str) -> Dict[str, Any]:
        async with conn.cursor() as cur:
            await cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
            result = await cur.fetchone()

            return result[0] # type: ignore

    async def analyze_one(self, lint_request: LintRequest, conn: AsyncConnection) -> AnalysisResult:
        plan = await self._get_explain_plan(conn, lint_request.sql_query)
        context =  await get_database_context(conn)

        recommendation, lint_diagnoses = analyze_with_rules(
            lint_request.sql_query,
            plan,
            context
        )

        return AnalysisResult(
            lint_diagnoses=lint_diagnoses,
            summary_recommendation=recommendation
        )

    async def analyze_many(self, lint_requests: LintRequests, conn: AsyncConnection) -> List[AnalysisResult]: # type: ignore
        result = []

        for sql_query in lint_requests.sql_query:
            lint_request = LintRequest(sql_query=sql_query)

            result.extend(await self.analyze_one(lint_request, conn))

        return result

analyzer = SQLAnalyzer()
