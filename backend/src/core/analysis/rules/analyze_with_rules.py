from ...models.lint_diagnose import LintDiagnose
from . import all_rules


from typing import Any, Dict, List, Optional


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

    recommendations = []

    for rule_func in all_rules:
        try:
            rule_results = rule_func(query, plan, context)
            recommendations.extend(rule_results)
        except Exception as e:
            print(f"Error executing rule {rule_func.__name__}: {e}")

    return recommendations
