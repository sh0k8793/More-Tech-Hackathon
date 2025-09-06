from pydantic import BaseModel
from src.core.models.lint_diagnose import LintDiagnose


class Recommendation(BaseModel):
    diagnose: LintDiagnose
    recommendation: str
