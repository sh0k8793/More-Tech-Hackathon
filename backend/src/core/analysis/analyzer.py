from backend.src.core.models.lint_diagnose import LintDiagnose


class SQLAnalyzer():
    def apply_ast_rules(query: str) -> list[LintDiagnose]:
        ...
    def apply_custom_rules(query) -> list[LintDiagnose]:
        ...
    def _normalize_query(query) -> str:
        ...

