from pydantic import BaseModel

from .lint_diagnose import LintDiagnose


class AnalysisResult(BaseModel):
    lint_diagnoses: list[LintDiagnose] # Only errors with available fixes
    summary_recommendation: str # IDEA: generate summary with AI
