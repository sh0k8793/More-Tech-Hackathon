from core.models.lint_diagnose import LintDiagnose
from core.models.analysis_result import AnalysisResult
from core.models.lint_request import LintRequest, LintRequests

from typing import List, Dict, Any

from core.analysis.rules.__init__ import analyze_with_rules  # TODO: переделать

from psycopg import AsyncConnection

from core.analysis.context import get_database_context


class SQLAnalyzer():
    async def _get_explain_plan(self, conn: AsyncConnection, query: str) -> Dict[str, Any]:
        async with conn.cursor() as cur:
            await cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
            result = await cur.fetchone()
            
            return result[0]

    async def analyze_one(self, lint_request: LintRequest, conn: AsyncConnection) -> AnalysisResult:
        plan = await self._get_explain_plan(conn, lint_request.sql_query)
        context =  await get_database_context(conn)

        recommendations = analyze_with_rules(
            lint_request.sql_query,
            plan,
            context 
        )

        return AnalysisResult(
            recommendations=recommendations
        )

    def analyze_many(lint_requests: LintRequests) -> List[AnalysisResult]:
        pass

analyzer = SQLAnalyzer()
