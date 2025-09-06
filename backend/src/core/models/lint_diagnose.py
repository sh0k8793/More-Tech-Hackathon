from pydantic import BaseModel, Field


class LintDiagnose(BaseModel):
    line: int
    col: int
    severity: str = Field(regex="^(HIGH|MEDIUM|LOW)$")
    message: str
