from abc import ABC, abstractmethod

from core.models.lint_diagnose import LintDiagnose


# TODO: Initialize sql analyzer to combine explain, ast_rules, custom_rules,
class SQLAnalyzer(ABC):
    @abstractmethod
    def apply_ast_rules(query: str) -> list[LintDiagnose]:
        ...
    @abstractmethod
    def apply_custom_rules(query) -> list[LintDiagnose]:
        ...
    @abstractmethod
    def _normalize_query(query) -> str:
        ...

