# backend/src/core/analysis/rules/custom/many_rows.py
from typing import Any, Dict, List, Tuple
from core.models.lint_diagnose import LintDiagnose

def rule_many_rows(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> Tuple[List[LintDiagnose], str]:
    """Обнаруживает запросы, возвращающие много строк"""
    recommendations = []
    optimized_query = query
    
    try:
        if plan and 'Plan' in plan:
            plan_rows = plan['Plan'].get('Plan Rows', 0)
            
            if plan_rows > 1000 and 'LIMIT' not in query.upper():
                diagnose = LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Запрос может вернуть много строк: {plan_rows}",
                    recommendation="Добавьте LIMIT для ограничения количества возвращаемых строк"
                )
                recommendations.append(diagnose)
    
    except Exception as e:
        print(f"Error in many_rows rule: {e}")
    
    return recommendations, optimized_query
