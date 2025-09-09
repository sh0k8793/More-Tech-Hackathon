from .lint_diagnose import LintDiagnose
from pydantic import BaseModel


class AnalysisResult(BaseModel):
    lint_diagnoses: list[LintDiagnose] # Only errors with available fixes
    summary_recommendation: str # IDEA: generate summary with AI
