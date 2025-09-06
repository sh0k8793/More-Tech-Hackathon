from pydantic import BaseModel, Field, field_validator


class LintRequest(BaseModel):
    sql_query: str = Field(..., example="select * from users where email = 'a@b.com'")

    @field_validator("sql_query")
    def sql_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("SQL must be non-empty")
        return v
