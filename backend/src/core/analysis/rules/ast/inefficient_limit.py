import re
from core.models.lint_diagnose import LintDiagnose


from typing import Any, Dict, List


def rule_inefficient_limit(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаруживает неэффективное использование LIMIT
    """
    recommendations = []
    optimized_query = query

    try:
        # Проверяем наличие LIMIT в запросе
        limit_match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        if limit_match:
            limit_value = int(limit_match.group(1))

            # Проверяем наличие LIMIT в плане выполнения
            if 'plan' in plan:
                plan_content = str(plan['plan'])
                if 'Limit' in plan_content:
                    # Проверяем, есть ли индекс для оптимизации LIMIT
                    has_index_for_limit = False
                    if 'indexes' in context and 'order_by_columns' in context:
                        for index in context['indexes']:
                            if 'index_columns' in index:
                                # Простая проверка: если столбцы индекса совпадают со столбцами ORDER BY
                                if set(index['index_columns']) & set(context['order_by_columns']):
                                    has_index_for_limit = True
                                    break

                    if not has_index_for_limit and limit_value < 100:
                        # Проверяем общее количество строк
                        rows_match = re.search(r'rows=(\d+)', plan_content)
                        if rows_match and int(rows_match.group(1)) > 10000:
                            diagnose = LintDiagnose(
                                line=1,
                                col=1,
                                severity="MEDIUM",
                                message=f"Обнаружено неэффективное использование LIMIT {limit_value} на большом наборе данных",
                                recommendation="Рассмотрите возможность добавления индекса по столбцам, используемым в ORDER BY, для оптимизации операции LIMIT. Без индекса база данных может обработать все строки перед применением LIMIT."
                            )
                            recommendations.append(diagnose)
    except Exception as e:
        print(f"Error in inefficient_limit rule: {e}")

    return recommendations, optimized_query #ty: ignore
