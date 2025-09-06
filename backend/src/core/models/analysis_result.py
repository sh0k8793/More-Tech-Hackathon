from pydantic import BaseModel
from src.core.models.lint_result import LintResult
from src.core.models.recommendation import Recommendation


class AnalysisResult(BaseModel):
    lint_result: LintResult # All lint errors
    recommendations: list[Recommendation] # Only errors with available fixes
