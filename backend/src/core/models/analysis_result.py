from pydantic import BaseModel
from core.models.lint_diagnose import LintDiagnose
from core.models.recommendation import Recommendation


class AnalysisResult(BaseModel):
    lint_result: list[LintDiagnose] # All lint errors
    recommendations: list[Recommendation] # Only errors with available fixes
