from pydantic import BaseModel
from core.models.lint_diagnose import LintDiagnose


class Recommendation(BaseModel):
    diagnose: LintDiagnose
    recommendation: str
