from core.models.lint_diagnose import LintDiagnose
from core.models.recommendation import Recommendation
from pydantic import BaseModel


class AnalysisResult(BaseModel):
    lint_result: list[LintDiagnose] # All lint errors
    recommendations: list[Recommendation] # Only errors with available fixes
