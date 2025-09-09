from typing import Any, Dict, List, Optional
from importlib import import_module

from core.models.lint_diagnose import LintDiagnose
from . import all_rules


def analyze_with_rules(
    query: str,
    plan: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None
) -> List[LintDiagnose]:
    """
    Анализирует запрос с помощью всех загруженных правил
    """
    if context is None:
        context = {}
    if plan is None:
        plan = {}

    lint_diagnoses = []
    
    for rule_func in all_rules:
        try:            
            lint_diagnoses_new, query = rule_func(query, plan, context)
            lint_diagnoses += lint_diagnoses_new
        except Exception as e:
            print(f"Error executing rule: {e}")

    return query, lint_diagnoses
