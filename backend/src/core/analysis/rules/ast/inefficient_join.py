import re
from typing import Any, Dict, List

from core.models.lint_diagnose import LintDiagnose


def rule_inefficient_join(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаруживает неэффективные операции JOIN
    """
    recommendations = []
    optimized_query = query

    try:
        # Проверяем наличие Nested Loop в плане выполнения с большими таблицами
        if 'plan' in plan:
            plan_content = str(plan['plan'])
            if 'Nested Loop' in plan_content:
                # Проверяем, есть ли предупреждения о больших таблицах
                if 'rows=' in plan_content:
                    rows_match = re.search(r'rows=(\d+)', plan_content)
                    if rows_match and int(rows_match.group(1)) > 10000:
                        diagnose = LintDiagnose(
                            line=1,
                            col=1,
                            severity="MEDIUM",
                            message="Обнаружен потенциально неэффективный JOIN с большими таблицами",
                            recommendation="Рассмотрите возможность использования Hash Join или Merge Join вместо Nested Loop для больших таблиц. Убедитесь, что есть индексы по столбцам соединения."
                        )
                        recommendations.append(diagnose)
    except Exception as e:
        print(f"Error in inefficient_join rule: {e}")

    return recommendations, optimized_query #ty: ignore
