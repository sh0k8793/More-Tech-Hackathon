from pydantic import BaseModel
from src.core.models.lint_diagnose import LintDiagnose


class LintResult(BaseModel):
    result: list[LintDiagnose]
