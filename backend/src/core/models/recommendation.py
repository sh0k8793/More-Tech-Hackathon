from core.models.lint_diagnose import LintDiagnose
from pydantic import BaseModel


class Recommendation(BaseModel):
    diagnose: LintDiagnose
    recommendation: str
