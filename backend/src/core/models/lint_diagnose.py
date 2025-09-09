from typing import Optional
from pydantic import BaseModel


class LintDiagnose(BaseModel):
    line: int
    col: int
    severity: str  # = Field(regex="^(HIGH|MEDIUM|LOW)$")
    message: str
    recommendation: Optional[str]
