from pydantic import BaseModel

from typing import List

from core.models.lint_diagnose import LintDiagnose


class AnalysisResult(BaseModel):
    lint_diagnoses: List[LintDiagnose] # Only errors with available fixes
    summary_recommendation: str # IDEA: generate summary with AI
